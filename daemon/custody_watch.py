"""Pages Discord on every custody and authority move that ``custody_rules`` classifies.

Each source is polled on its own cadence. A block or transaction is classified and
its findings are committed in the same SQLite transaction as the source cursor, so
a restart resumes after the last committed item and never classifies one twice.
ALERT and CRITICAL findings are paged at once and marked sent only after Discord
accepts them; INFO findings wait for the daily digest, whose arrival is also the
proof that the watcher is alive. A source that cannot be read for longer than
``source_stale_seconds`` is itself paged.

    python -m daemon.custody_watch --config /etc/custody-watch/config.json run
"""

from __future__ import annotations

import argparse
import contextlib
import functools
import json
import logging
import os
import socket
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from substrateinterface.exceptions import SubstrateRequestException

from daemon import custody_rules as rules
from daemon import discord
from daemon.config import DaemonConfig
from daemon.substrate_client import SubstrateClient

logger = logging.getLogger("custody_watch")

DISCORD_LIMIT = 2000
DAY = 86400


class SourceError(Exception):
    """A source could not be read this cycle; the cursor stays where it was."""


# --- state -----------------------------------------------------------------------


@dataclass(frozen=True)
class StoredFinding(rules.Finding):
    found_at: float = 0.0
    sent_at: float | None = None

    @property
    def text(self) -> str:
        return self.render()


_SCHEMA = """
CREATE TABLE IF NOT EXISTS state (name TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS finding (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    key TEXT NOT NULL UNIQUE,
    severity INTEGER NOT NULL,
    kind TEXT NOT NULL,
    amount TEXT NOT NULL,
    headline TEXT NOT NULL,
    details TEXT NOT NULL,
    found_at REAL NOT NULL,
    sent_at REAL
);
CREATE TABLE IF NOT EXISTS processed (key TEXT PRIMARY KEY, at REAL NOT NULL);
"""


class Store:
    """Cursors, findings and delivery state in one SQLite file."""

    def __init__(self, path: str):
        self._db = sqlite3.connect(path, isolation_level=None)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=NORMAL")
        self._db.executescript(_SCHEMA)
        self._depth = 0

    def close(self) -> None:
        self._db.close()

    @contextlib.contextmanager
    def transaction(self):
        if self._depth:
            self._depth += 1
            try:
                yield
            finally:
                self._depth -= 1
            return
        self._db.execute("BEGIN IMMEDIATE")
        self._depth = 1
        try:
            yield
        except BaseException:
            self._db.execute("ROLLBACK")
            raise
        else:
            self._db.execute("COMMIT")
        finally:
            self._depth = 0

    def get(self, name: str) -> str | None:
        row = self._db.execute("SELECT value FROM state WHERE name = ?", (name,)).fetchone()
        return row[0] if row else None

    def put(self, name: str, value: str) -> None:
        self._db.execute("INSERT INTO state (name, value) VALUES (?, ?) "
                         "ON CONFLICT(name) DO UPDATE SET value = excluded.value", (name, value))

    def delete(self, name: str) -> None:
        self._db.execute("DELETE FROM state WHERE name = ?", (name,))

    def add(self, finding: rules.Finding, now: float) -> bool:
        """Store a finding unless one with its key exists; True when it is new."""
        cursor = self._db.execute(
            "INSERT OR IGNORE INTO finding (key, severity, kind, amount, headline, details, found_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (finding.key, int(finding.severity), finding.kind, str(finding.amount), finding.headline,
             json.dumps(finding.details), now))
        return cursor.rowcount == 1

    def processed(self, key: str) -> bool:
        return self._db.execute("SELECT 1 FROM processed WHERE key = ?", (key,)).fetchone() is not None

    def mark_processed(self, key: str, now: float) -> None:
        self._db.execute("INSERT OR IGNORE INTO processed (key, at) VALUES (?, ?)", (key, now))

    def _rows(self, where: str, params: tuple = ()) -> list[StoredFinding]:
        rows = self._db.execute(
            "SELECT key, severity, kind, amount, headline, details, found_at, sent_at FROM finding "
            f"WHERE {where} ORDER BY seq", params).fetchall()
        return [StoredFinding(severity=rules.Severity(s), key=k, headline=h, details=tuple(json.loads(d)),
                              kind=kind, amount=int(a), found_at=f, sent_at=sent)
                for k, s, kind, a, h, d, f, sent in rows]

    def findings(self) -> list[StoredFinding]:
        return self._rows("1")

    def unsent_pages(self) -> list[StoredFinding]:
        return self._rows("sent_at IS NULL AND severity >= ?", (int(rules.ALERT),))

    def unsent_routine(self) -> list[StoredFinding]:
        return self._rows("sent_at IS NULL AND severity < ?", (int(rules.ALERT),))

    def paged_since(self, since: float) -> list[StoredFinding]:
        return self._rows("sent_at >= ? AND severity >= ?", (since, int(rules.ALERT)))

    def mark_sent(self, keys: list[str], now: float) -> None:
        self._db.executemany("UPDATE finding SET sent_at = ? WHERE key = ?", [(now, k) for k in keys])


# --- Discord ----------------------------------------------------------------------


_BADGE = {rules.CRITICAL: "\U0001f6a8 **CRITICAL** @here", rules.ALERT: "\u26a0\ufe0f **ALERT**",
          rules.INFO: "\u2139\ufe0f **INFO**"}


def _fenced(text: str, room: int) -> str:
    # A fence inside chain-supplied text would end the code block early.
    text = text.replace("```", "`\u200b``")
    if len(text) > room:
        text = text[:room - 12] + "\n(truncated)"
    return f"```\n{text}\n```"


def page_message(finding: rules.Finding) -> dict:
    """One Discord message per finding. Only CRITICAL may ping, so text taken from the
    chain can never mention anyone on an ALERT."""
    head = f"{_BADGE[finding.severity]} custody-watch\n**{finding.headline}**\n"
    return {
        "content": head + _fenced("\n".join(finding.details), DISCORD_LIMIT - len(head) - 8),
        "allowed_mentions": {"parse": ["everyone"] if finding.severity == rules.CRITICAL else []},
    }


class Pager:
    def __init__(self, post: Callable[[dict], None]):
        self._post = post

    def flush(self, store: Store, now: float) -> int:
        """Page every unsent ALERT and CRITICAL in order; stop at the first rejection so
        the rest are retried, in order, on the next flush."""
        sent = 0
        for finding in store.unsent_pages():
            try:
                self._post(page_message(finding))
            except discord.DiscordError as e:
                logger.warning("page for %s not delivered, retrying next cycle: %s", finding.key, e)
                break
            store.mark_sent([finding.key], now)
            sent += 1
        return sent


# --- the watch loop ------------------------------------------------------------------


def _utc(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")


def _plural(n: int, word: str) -> str:
    return f"{n} {word}" + ("" if n == 1 else "s")


class Watch:
    def __init__(self, config: rules.WatchConfig, store: Store, sources: list, post: Callable[[dict], None],
                 clock: Callable[[], float] = time.time):
        self._config = config
        self._store = store
        self._sources = sources
        self._post = post
        self._pager = Pager(post)
        self._clock = clock
        self._started = clock()
        self._due = {s.name: 0.0 for s in sources}
        self._decimals = {n.name: n.pool.cmatra_decimals for n in config.cardano if n.pool}

    def cycle(self) -> None:
        now = self._clock()
        for source in self._sources:
            if now < self._due[source.name]:
                continue
            try:
                caught_up = source.poll(now)
            except Exception as e:  # one unreadable source must not blind the others; staleness pages it
                logger.exception("%s: poll failed", source.name)
                self._store.put(f"health:{source.name}:error", f"{type(e).__name__}: {e}"[:300])
                self._due[source.name] = now + source.poll_seconds
                continue
            self._due[source.name] = now if not caught_up else now + source.poll_seconds
            self._healthy(source, now)
        self._check_stale(now)
        self._pager.flush(self._store, now)
        self._digest(now)

    def _healthy(self, source, now: float) -> None:
        store, name = self._store, source.name
        with store.transaction():
            store.put(f"health:{name}:ok", repr(now))
            store.put(f"health:{name}:position", source.position)
            store.delete(f"health:{name}:error")
            since = store.get(f"health:{name}:stale-since")
            if since is not None:
                store.add(rules.Finding(
                    rules.ALERT, f"custody-watch:{name}:recovered:{since}",
                    f"custody-watch: {name} recovered, watching again at {source.position}",
                    details=(f"unreadable since {_utc(float(since))}",), kind="watcher"), now)
                store.delete(f"health:{name}:stale-since")

    def _check_stale(self, now: float) -> None:
        store = self._store
        for source in self._sources:
            name = source.name
            last_ok = max(float(store.get(f"health:{name}:ok") or 0), self._started)
            if now - last_ok <= self._config.source_stale_seconds or store.get(f"health:{name}:stale-since"):
                continue
            error = store.get(f"health:{name}:error") or "no poll has completed"
            with store.transaction():
                store.add(rules.Finding(
                    rules.ALERT, f"custody-watch:{name}:stale:{int(last_ok)}",
                    f"custody-watch: {name} stale for {int(now - last_ok) // 60} min; "
                    f"moves on it are not being watched",
                    details=(f"last successful poll {_utc(last_ok)}", f"last error: {error}"), kind="watcher"), now)
                store.put(f"health:{name}:stale-since", str(int(last_ok)))

    def _digest(self, now: float) -> None:
        moment = datetime.fromtimestamp(now, timezone.utc)
        day = moment.date().isoformat()
        if moment.hour < self._config.digest_hour_utc or self._store.get("digest:last-day") == day:
            return
        routine = self._store.unsent_routine()
        try:
            self._post(self.digest_message(routine, now))
        except discord.DiscordError as e:
            logger.warning("daily digest not delivered, retrying next cycle: %s", e)
            return
        with self._store.transaction():
            self._store.mark_sent([f.key for f in routine], now)
            self._store.put("digest:last-day", day)

    def digest_message(self, routine: list[StoredFinding], now: float) -> dict:
        store = self._store
        lines = [f"\U0001f4cb **custody-watch daily digest** {_utc(now)}: alive", "sources:"]
        for source in self._sources:
            ok = store.get(f"health:{source.name}:ok")
            position = store.get(f"health:{source.name}:position")
            state = f"{position} (last read {_utc(float(ok))})" if ok else "never read"
            lines.append(f"  {source.name}: {state}")
        paged = store.paged_since(now - DAY)
        critical = sum(1 for f in paged if f.severity == rules.CRITICAL)
        lines.append(f"paged in the last 24h: {len(paged)} ({critical} critical)")

        surrenders: dict[str, list[int]] = defaultdict(list)
        committees: dict[str, int] = defaultdict(int)
        other: list[StoredFinding] = []
        for f in routine:
            network = f.key.split(":", 1)[0]
            if f.kind == "surrender":
                surrenders[network].append(f.amount)
            elif f.kind == "committee":
                committees[network] += 1
            else:
                other.append(f)
        if not routine:
            lines.append("no routine moves since the last digest")
        else:
            lines.append("routine moves since the last digest:")
        for network, amounts in surrenders.items():
            decimals = self._decimals.get(network, 6)
            whole, frac = divmod(sum(amounts), 10 ** decimals)
            lines.append(f"  {network}: {_plural(len(amounts), 'surrender')} paying "
                         f"{whole:,}.{frac:0{decimals}d} cMATRA")
        for network, count in committees.items():
            lines.append(f"  {network}: {_plural(count, 'committee rotation')}, membership unchanged")
        if other:
            lines.append(f"  {_plural(len(other), 'other routine finding')}:")
            lines.extend(f"    {f.headline}" for f in other[:10])
        content = "\n".join(lines)
        if len(content) > DISCORD_LIMIT:
            content = content[:DISCORD_LIMIT - 12] + "\n(truncated)"
        return {"content": content, "allowed_mentions": {"parse": []}}


# --- Materios ---------------------------------------------------------------------------


SUDO_KEY_STORAGE = "0x5c0d1176a568c1f92944340dbfed9e9c530ebca703c85910e7164cb7d1c9e47b"
SYSTEM_EVENTS_STORAGE = "0x26aa394eea5630e07c48ae0c9558cef780d41e5e16056765bc8461851072c9d7"
MAX_BLOCKS_PER_POLL = 600
BLOCK_SECONDS = 6


def _account(storage_hex: str) -> str:
    return rules.render_account(bytes.fromhex(storage_hex[2:])) if storage_hex else "none"


class MateriosSource:
    """Finalized Materios blocks, from the cursor to the finalized head.

    ``client`` is daemon-core's ``SubstrateClient`` (bounded per-call timeout and
    reconnect) or anything with the same ``connected``/``connect``/``rpc`` surface.
    """

    def __init__(self, config: rules.MateriosConfig, client, store: Store):
        self.name = config.name
        self.poll_seconds = config.poll_seconds
        self._config = config
        self._client = client
        self._store = store
        self._cursor_key = f"cursor:{self.name}"
        self._decoders: dict[int, rules.RuntimeDecoder] = {}
        self._decoder: rules.RuntimeDecoder | None = None

    @property
    def position(self) -> str:
        return f"block #{self._store.get(self._cursor_key)}"

    def start_at(self, block: int) -> None:
        """Treat ``block`` as already processed; the next poll starts after it."""
        self._store.put(self._cursor_key, str(block))

    def _rpc(self, method: str, params: list):
        return self._client.rpc(method, params)

    def _head(self) -> tuple[str, int]:
        if not self._client.connected and not self._client.connect():
            raise SourceError(f"{self.name}: cannot connect to {self._config.rpc_url}")
        head_hash = self._rpc("chain_getFinalizedHead", [])
        return head_hash, int(self._rpc("chain_getHeader", [head_hash])["number"], 16)

    def rewind(self, seconds: int) -> None:
        """Start ``seconds`` of blocks behind the finalized head."""
        self.start_at(self._head()[1] - seconds // BLOCK_SECONDS)

    def poll(self, now: float) -> bool:
        """Process up to ``MAX_BLOCKS_PER_POLL`` blocks; True once at the finalized head."""
        head_hash, head = self._head()
        if self._reset(head, now):
            return True
        sudo_key = self._sudo_key(head_hash, head, now)

        cursor = self._store.get(self._cursor_key)
        if cursor is None:
            start = self._config.start_block
            self.start_at(head if start is None else start - 1)
            if start is None:
                return True
            cursor = str(start - 1)
        first = int(cursor) + 1
        if first > head:
            return True
        last = min(head, first + MAX_BLOCKS_PER_POLL - 1)
        hashes = self._rpc("chain_getBlockHash", [list(range(first, last + 1))])
        for number, block_hash in zip(range(first, last + 1), hashes):
            self._block(number, block_hash, sudo_key, now)
        return last == head

    def _reset(self, head: int, now: float) -> bool:
        """A new genesis means the chain was reset: page it, forget what was learned about
        the old chain, and watch the new one from its finalized head."""
        genesis = self._rpc("chain_getBlockHash", [0])
        name = f"genesis:{self.name}"
        stored = self._store.get(name)
        with self._store.transaction():
            self._store.put(name, genesis)
            if stored is None or stored == genesis:
                return False
            self._store.add(rules.Finding(
                rules.CRITICAL, f"{self.name}:genesis:{genesis}",
                f"{self.name} genesis changed: the chain was reset",
                details=(f"from {stored}", f"to   {genesis}", f"watching again from finalized #{head}")), now)
            self._store.delete(f"sudo-key:{self.name}")
            self._store.delete(f"committee:{self.name}")
            self.start_at(head)
        self._decoder = None
        return True

    def _sudo_key(self, head_hash: str, head: int, now: float) -> bytes | None:
        raw = self._rpc("state_getStorage", [SUDO_KEY_STORAGE, head_hash]) or ""
        name = f"sudo-key:{self.name}"
        stored = self._store.get(name)
        with self._store.transaction():
            if stored is not None and stored != raw:
                self._store.add(rules.Finding(
                    rules.CRITICAL, f"{self.name}:sudo-key:{head}",
                    f"{self.name} Sudo.Key changed (seen at finalized #{head})",
                    details=(f"from {_account(stored)}", f"to   {_account(raw)}")), now)
            self._store.put(name, raw)
        return bytes.fromhex(raw[2:]) if raw else None

    def _load_decoder(self, at_hash: str) -> rules.RuntimeDecoder:
        """The decoder for blocks built on ``at_hash``; the node's current runtime when that
        state is pruned, in which case an extrinsic it cannot read is still reported."""
        try:
            version = self._rpc("state_getRuntimeVersion", [at_hash])["specVersion"]
            params = [at_hash]
        except SubstrateRequestException:
            version = self._rpc("state_getRuntimeVersion", [])["specVersion"]
            params = []
            logger.warning("%s: state at %s is pruned; decoding with the current runtime, spec %s",
                           self.name, at_hash, version)
        if version not in self._decoders:
            metadata = self._store.get(f"metadata:{self.name}:{version}")
            if metadata is None:
                metadata = self._rpc("state_getMetadata", params)
                self._store.put(f"metadata:{self.name}:{version}", metadata)
            self._decoders[version] = rules.RuntimeDecoder(metadata)
        return self._decoders[version]

    def _events(self, block_hash: str, decoder: rules.RuntimeDecoder) -> dict[int, list[str]] | None:
        try:
            raw = self._rpc("state_getStorage", [SYSTEM_EVENTS_STORAGE, block_hash])
        except SubstrateRequestException:
            return None
        return decoder.events(raw) if raw else {}

    def _block(self, number: int, block_hash: str, sudo_key: bytes | None, now: float) -> None:
        block = self._rpc("chain_getBlock", [block_hash])["block"]
        header = block["header"]
        if self._decoder is None:
            self._decoder = self._load_decoder(header["parentHash"])
        decoder = self._decoder
        extrinsics = decoder.extrinsics(block["extrinsics"])
        findings = rules.classify_materios_block(self.name, number, extrinsics, events=None, sudo_key=sudo_key)
        if findings:
            events = self._events(block_hash, decoder)
            if events is not None:
                findings = rules.classify_materios_block(self.name, number, extrinsics, events, sudo_key)

        committee = rules.committee_of(extrinsics)
        with self._store.transaction():
            if committee is not None:
                name = f"committee:{self.name}"
                previous = self._store.get(name)
                if previous is not None:
                    seated = tuple(tuple(m) for m in json.loads(previous))
                    findings.append(rules.committee_change(self.name, number, seated, committee))
                self._store.put(name, json.dumps(committee))
            for finding in findings:
                self._store.add(finding, now)
            self._store.put(self._cursor_key, str(number))
        if rules.runtime_upgraded(header):
            self._decoder = None


# --- Cardano ------------------------------------------------------------------------------


class Blockfrost:
    """Blockfrost REST reads. The project key never appears in an error."""

    def __init__(self, base_url: str, project_id_file: str, min_interval: float = 0.12, timeout: float = 30.0):
        self._base = base_url.rstrip("/")
        self._key = Path(project_id_file).read_text().strip()
        self._min_interval = min_interval
        self._timeout = timeout
        self._last = 0.0

    def get(self, path: str, **params):
        """The decoded JSON body, or None when Blockfrost answers 404."""
        wait = self._last + self._min_interval - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        self._last = time.monotonic()
        url = self._base + path + ("?" + urllib.parse.urlencode(params) if params else "")
        request = urllib.request.Request(url, headers={
            "project_id": self._key, "User-Agent": discord.USER_AGENT, "Accept": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=self._timeout) as response:
                return json.load(response)
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            raise SourceError(f"blockfrost answered HTTP {e.code} for {path}") from None
        except (urllib.error.URLError, OSError, ValueError) as e:
            raise SourceError(f"blockfrost unreachable for {path}: {type(e).__name__}") from None


PAGE = 100


class CardanoSource:
    """Transactions at watched addresses and mints or burns under watched policies.

    The first poll records where each address and asset stands without classifying
    history. Every later poll re-lists the last ``reorg_depth_blocks`` blocks, so a
    transaction moved by a rollback is still seen; one already classified is skipped.
    """

    def __init__(self, network: rules.CardanoNetwork, api, store: Store, stale_seconds: int):
        self.name = network.name
        self.poll_seconds = network.poll_seconds
        self._network = network
        self._api = api
        self._store = store
        self._stale_seconds = stale_seconds
        addresses = [w.address for w in network.addresses] + ([network.pool.address] if network.pool else [])
        self._addresses = list(dict.fromkeys(addresses))

    @property
    def position(self) -> str:
        return f"height {self._store.get(f'cursor:{self.name}:tip')}"

    def _address_key(self, address: str) -> str:
        return f"cursor:{self.name}:address:{address}"

    def _policy_key(self, policy_id: str) -> str:
        return f"cursor:{self.name}:policy:{policy_id}"

    def start_at(self, height: int) -> None:
        """Watch addresses from ``height`` and every asset under the policies from its first mint."""
        with self._store.transaction():
            for address in self._addresses:
                self._store.put(self._address_key(address), str(height))
            for policy in self._network.policies:
                self._store.put(self._policy_key(policy.policy_id), "baselined")
            self._store.put(f"cursor:{self.name}:tip", str(height))

    def rewind(self, seconds: int) -> None:
        """Start ``seconds`` of blocks behind the tip, at the 20 s mean block interval."""
        self.start_at(self._api.get("/blocks/latest")["height"] - seconds // 20)

    def _pages(self, path: str, first_page: int = 1, **params):
        page = first_page
        while True:
            rows = self._api.get(path, count=PAGE, page=page, **params) or []
            yield from rows
            if len(rows) < PAGE:
                return
            page += 1

    def poll(self, now: float) -> bool:
        tip = self._api.get("/blocks/latest")
        if tip is None:
            raise SourceError(f"{self.name}: blockfrost served no chain tip")
        age = now - tip["time"]
        if age > self._stale_seconds:
            raise SourceError(f"{self.name}: chain tip {tip['height']} is {int(age) // 60} min old")
        height = tip["height"]
        pending: dict[str, None] = {}
        cursors: dict[str, str] = {f"cursor:{self.name}:tip": str(height)}

        for address in self._addresses:
            key = self._address_key(address)
            cursor = self._store.get(key)
            cursors[key] = str(height)
            if cursor is None:
                continue
            start = max(int(cursor) - self._network.reorg_depth_blocks, 0)
            for row in self._pages(f"/addresses/{address}/transactions", order="asc", **{"from": str(start)}):
                pending[row["tx_hash"]] = None

        for policy in self._network.policies:
            baselined = self._store.get(self._policy_key(policy.policy_id)) is not None
            cursors[self._policy_key(policy.policy_id)] = "baselined"
            for asset in self._pages(f"/assets/policy/{policy.policy_id}"):
                unit = asset["asset"]
                count = int(self._api.get(f"/assets/{unit}")["mint_or_burn_count"])
                key = f"cursor:{self.name}:asset:{unit}"
                seen = self._store.get(key)
                seen = int(seen) if seen is not None else (0 if baselined else count)
                if count > seen:
                    first_page = seen // PAGE + 1
                    history = list(self._pages(f"/assets/{unit}/history", first_page=first_page, order="asc"))
                    offset = (first_page - 1) * PAGE
                    for entry in history[seen - offset:count - offset]:
                        pending[entry["tx_hash"]] = None
                cursors[key] = str(count)

        for tx_hash in pending:
            if not self._store.processed(f"{self.name}:{tx_hash}"):
                self._classify(tx_hash, now)
        with self._store.transaction():
            for key, value in cursors.items():
                self._store.put(key, value)
        return True

    def _classify(self, tx_hash: str, now: float) -> None:
        tx = self._api.get(f"/txs/{tx_hash}")
        utxos = self._api.get(f"/txs/{tx_hash}/utxos")
        if tx is None or utxos is None:
            raise SourceError(f"{self.name}: blockfrost does not serve transaction {tx_hash} yet")
        redeemers = self._api.get(f"/txs/{tx_hash}/redeemers") or []
        for redeemer in redeemers:
            datum = self._api.get(f"/scripts/datum/{redeemer['redeemer_data_hash']}")
            redeemer["json_value"] = datum["json_value"] if datum else None
        finding = rules.classify_cardano_tx(self._network, tx, utxos, redeemers)
        with self._store.transaction():
            self._store.mark_processed(f"{self.name}:{tx_hash}", now)
            if finding is not None:
                self._store.add(finding, now)


# --- entry points --------------------------------------------------------------------------


def backtest(sources: list, store: Store, now: float) -> list[StoredFinding]:
    """Run each source from where ``start_at`` put it to its head; nothing is paged."""
    for source in sources:
        while not source.poll(now):
            pass
    return store.findings()


def sd_notify(message: str) -> None:
    """Tell systemd ``message`` (READY=1, WATCHDOG=1) when it supervises this process."""
    path = os.environ.get("NOTIFY_SOCKET")
    if not path:
        return
    if path.startswith("@"):
        path = "\0" + path[1:]
    with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as notify:
        notify.sendto(message.encode(), path)


def build_sources(config: rules.WatchConfig, store: Store, materios_rpc: str | None = None) -> list:
    sources: list = []
    if config.materios:
        client = SubstrateClient(DaemonConfig(rpc_url=materios_rpc or config.materios.rpc_url))
        sources.append(MateriosSource(config.materios, client, store))
    for network in config.cardano:
        api = Blockfrost(network.blockfrost_url, network.project_id_file)
        sources.append(CardanoSource(network, api, store, config.source_stale_seconds))
    return sources


def _run(config: rules.WatchConfig, webhook: str) -> int:
    store = Store(config.state_db)
    watch = Watch(config, store, build_sources(config, store),
                  functools.partial(discord.post_json, webhook))
    sd_notify("READY=1")
    logger.info("watching %d sources", len(config.cardano) + (1 if config.materios else 0))
    while True:
        watch.cycle()
        sd_notify("WATCHDOG=1")
        time.sleep(1)


def _backtest(config: rules.WatchConfig, days: int, state: str, materios_rpc: str | None) -> int:
    store = Store(state)
    sources = build_sources(config, store, materios_rpc)
    now = time.time()
    for source in sources:
        source.rewind(days * DAY)
        logger.info("%s: backtest from %s", source.name, source.position)
    started = time.time()
    findings = backtest(sources, store, now)
    for f in findings:
        print(json.dumps({"key": f.key, "severity": f.severity.name, "kind": f.kind, "amount": f.amount,
                          "text": f.text}))
    counts = defaultdict(int)
    for f in findings:
        counts[f.severity.name] += 1
    print(json.dumps({"summary": dict(counts), "sources": {s.name: s.position for s in sources},
                      "seconds": round(time.time() - started)}))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="custody_watch", description=__doc__.splitlines()[0])
    parser.add_argument("--config", required=True)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("run", help="watch and page until stopped")
    replay = commands.add_parser("backtest", help="replay recent history, print findings, page nothing")
    replay.add_argument("--days", type=int, default=30)
    replay.add_argument("--state", required=True, help="a fresh SQLite file, never the live state")
    replay.add_argument("--materios-rpc", help="read Materios from this node instead of the configured one")
    commands.add_parser("test-page", help="send one test message through the webhook")
    failed = commands.add_parser("page-failure", help="page that a systemd unit failed (OnFailure hook)")
    failed.add_argument("--unit", required=True)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    config_path = Path(args.config)
    config = rules.parse_config(json.loads(config_path.read_text()), base_dir=config_path.parent)
    if args.command == "backtest":
        return _backtest(config, args.days, args.state, args.materios_rpc)

    webhook = os.environ.get("DISCORD_WEBHOOK_URL", "")
    if not webhook:
        print("custody_watch: DISCORD_WEBHOOK_URL is not set; refusing to watch without a way to page",
              file=sys.stderr)
        return 2
    if args.command == "run":
        return _run(config, webhook)
    if args.command == "test-page":
        message = {"content": f"\u2139\ufe0f custody-watch test page from {socket.gethostname()}",
                   "allowed_mentions": {"parse": []}}
    else:
        message = {"content": f"{_BADGE[rules.CRITICAL]} custody-watch: systemd unit {args.unit} failed on "
                              f"{socket.gethostname()}; custody and authority moves are NOT being watched",
                   "allowed_mentions": {"parse": ["everyone"]}}
    try:
        discord.post_json(webhook, message)
    except discord.DiscordError as e:
        print(f"custody_watch: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
