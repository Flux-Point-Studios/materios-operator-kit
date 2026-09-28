"""Pages Discord on every custody and authority move that ``custody_rules`` classifies.

Each source is polled on its own cadence. A block or transaction is classified and
its findings are committed in the same SQLite transaction as the source cursor, so
a restart resumes after the last committed item and never classifies one twice.
ALERT and CRITICAL findings are paged at once and marked sent only after Discord
accepts them; INFO findings wait for the daily digest, whose arrival is also the
proof that the watcher is alive. A source that cannot be read up to its head for
longer than ``source_stale_seconds`` is itself paged.

    python -m daemon.custody_watch run --config /etc/custody-watch/config.json
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
from substrateinterface.utils.hasher import blake2_128_concat, two_x64_concat, xxh128

from daemon import custody_rules as rules
from daemon import discord
from daemon.config import DaemonConfig
from daemon.substrate_client import SubstrateClient

logger = logging.getLogger("custody_watch")

# Discord's limit is 2000 characters, and it does not say whether an emoji outside the
# Basic Multilingual Plane counts as one or two; every message stays well inside it.
MESSAGE_LIMIT = 1900
# Leaves room for the badge, the fallback note and a useful part of the body.
MAX_TITLE = 400
HOUR = 3600
DAY = rules.DAY


class SourceError(Exception):
    """A source could not be read this cycle; the cursor stays where it was."""


# --- state -----------------------------------------------------------------------


@dataclass(frozen=True)
class StoredFinding(rules.Finding):
    found_at: float = 0.0
    sent_at: float | None = None
    failures: int = 0

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
    grp TEXT,
    found_at REAL NOT NULL,
    sent_at REAL,
    failures INTEGER NOT NULL DEFAULT 0
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

    def delete_prefix(self, prefix: str) -> None:
        self._db.execute("DELETE FROM state WHERE substr(name, 1, ?) = ?", (len(prefix), prefix))

    def add(self, finding: rules.Finding, now: float) -> bool:
        """Store a finding unless one with its key exists; True when it is new."""
        cursor = self._db.execute(
            "INSERT OR IGNORE INTO finding (key, severity, kind, amount, headline, details, grp, found_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (finding.key, int(finding.severity), finding.kind, str(finding.amount), finding.headline,
             json.dumps(finding.details), finding.group, now))
        return cursor.rowcount == 1

    def retire(self, prefix: str, suffix: str) -> None:
        """Append ``suffix`` to every finding key under ``prefix``, so a reset chain's
        findings at heights the old chain already used are not taken for duplicates."""
        self._db.execute("UPDATE finding SET key = key || ? WHERE substr(key, 1, ?) = ?",
                         (suffix, len(prefix), prefix))

    def processed(self, key: str) -> bool:
        return self._db.execute("SELECT 1 FROM processed WHERE key = ?", (key,)).fetchone() is not None

    def mark_processed(self, key: str, now: float) -> None:
        self._db.execute("INSERT OR IGNORE INTO processed (key, at) VALUES (?, ?)", (key, now))

    def _rows(self, where: str, params: tuple = (), order: str = "seq") -> list[StoredFinding]:
        rows = self._db.execute(
            "SELECT key, severity, kind, amount, headline, details, grp, found_at, sent_at, failures FROM finding "
            f"WHERE {where} ORDER BY {order}", params).fetchall()
        return [StoredFinding(severity=rules.Severity(s), key=k, headline=h, details=tuple(json.loads(d)),
                              kind=kind, amount=int(a), group=g, found_at=f, sent_at=sent, failures=n)
                for k, s, kind, a, h, d, g, f, sent, n in rows]

    def findings(self) -> list[StoredFinding]:
        return self._rows("1")

    def unsent_pages(self) -> list[StoredFinding]:
        """Most severe first; within a severity, findings that page alone come first."""
        return self._rows("sent_at IS NULL AND severity >= ?", (int(rules.ALERT),),
                          order="severity DESC, grp IS NOT NULL, seq")

    def unsent_routine(self) -> list[StoredFinding]:
        return self._rows("sent_at IS NULL AND severity < ?", (int(rules.ALERT),))

    def paged_since(self, since: float) -> list[StoredFinding]:
        return self._rows("sent_at >= ? AND severity >= ?", (since, int(rules.ALERT)))

    def mark_sent(self, keys: list[str], now: float) -> None:
        self._db.executemany("UPDATE finding SET sent_at = ? WHERE key = ?", [(now, k) for k in keys])

    def reject(self, keys: list[str]) -> None:
        self._db.executemany("UPDATE finding SET failures = failures + 1 WHERE key = ?", [(k,) for k in keys])


# --- Discord ----------------------------------------------------------------------


_BADGE = {rules.CRITICAL: "\U0001f6a8 **CRITICAL** @here", rules.ALERT: "\u26a0\ufe0f **ALERT**",
          rules.INFO: "\u2139\ufe0f **INFO**"}


def _plain(text: str) -> str:
    """``text`` with every backtick replaced, so text taken from the chain can neither
    close the page's code block nor open one of its own."""
    return text.replace("`", "\u02cb")


def _fenced(text: str, room: int) -> str:
    text = _plain(text)
    if len(text) > room:
        text = text[:room - 12] + "\n(truncated)"
    return f"```\n{text}\n```"


def page_message(findings: list[rules.Finding], headline_only: bool = False) -> dict:
    """One Discord message for one finding, for every pending finding of a group, or
    for the pending findings of many groups, most severe first. Only CRITICAL may ping,
    so text taken from the chain can never mention anyone on an ALERT."""
    severity = max(f.severity for f in findings)
    groups: dict[str | None, list[rules.Finding]] = {}
    for finding in findings:
        groups.setdefault(finding.group, []).append(finding)
    if len(findings) == 1:
        title, body = findings[0].headline, "\n".join(findings[0].details)
    elif len(groups) == 1:
        title = f"{len(findings)} findings from {findings[0].group}"
        body = "\n".join(f"[{f.severity.name}] {f.headline}" for f in findings)
    else:
        title = f"{len(findings)} findings in {len(groups)} groups"
        ranked = sorted(groups.items(), key=lambda g: -max(f.severity for f in g[1]))
        body = "\n".join(f"[{max(f.severity for f in members).name}] {group}: "
                         f"{rules.plural(len(members), 'finding')}, first: {members[0].headline}"
                         for group, members in ranked)
    head = f"{_BADGE[severity]} custody-watch\n**{_plain(title)[:MAX_TITLE]}**\n"
    if headline_only:
        content = head + f"(the full page was rejected by the webhook {FALLBACK_AFTER} times; " \
                         "its details are in the watcher's state database)"
    else:
        content = head + _fenced(body, MESSAGE_LIMIT - len(head) - 8)
    return {"content": content, "allowed_mentions": {"parse": ["everyone"] if severity == rules.CRITICAL else []}}


FALLBACK_AFTER = 3
MAX_RETRY_AFTER = 600.0
MAX_HOLD = 60.0
# Statuses that refuse the message itself; any other failure is the webhook's.
PAYLOAD_REFUSED = frozenset({400, 413})
# Every post the watcher makes, page or digest, spends a token; tokens return one per
# PAGE_INTERVAL up to PAGE_BURST. Ten a minute, at most three at once, keeps the watcher
# a third of Discord's 30 messages a minute per channel and under its 5 per 2 s per
# webhook, so a watchdog sharing the webhook still gets through a flood.
PAGE_BURST = 3
PAGE_INTERVAL = 6.0
# Past this many groups waiting, every group goes out in one summary message.
MAX_GROUP_MESSAGES = 3


def _doubling(n: int) -> float:
    return min(2.0 ** (n - 1), MAX_HOLD)


def _messages(pending: list[StoredFinding]) -> list[list[StoredFinding]]:
    """Pending pages as messages, in order: a finding alone, or its whole group at the
    place of the group's first finding. When more than MAX_GROUP_MESSAGES groups wait,
    they go as one summary at the place of the first of them."""
    messages: list[list[StoredFinding]] = []
    groups: dict[str, list[StoredFinding]] = {}
    for finding in pending:
        if finding.group is None:
            messages.append([finding])
        elif finding.group in groups:
            groups[finding.group].append(finding)
        else:
            groups[finding.group] = [finding]
            messages.append(groups[finding.group])
    if len(groups) <= MAX_GROUP_MESSAGES:
        return messages
    first = next(i for i, m in enumerate(messages) if m[0].group is not None)
    alone = [m for m in messages if m[0].group is None]
    return alone[:first] + [[f for members in groups.values() for f in members]] + alone[first:]


def _message_name(message: list[StoredFinding]) -> str:
    groups = {f.group for f in message}
    if len(groups) > 1:
        return "summary"
    return message[0].group or message[0].key


class Pager:
    """Posts every message through the one webhook without hammering it.

    Posts are paced by a token bucket (PAGE_BURST, PAGE_INTERVAL), most severe first, so
    no flood of findings can hold the webhook at Discord's rate limit. A failure of the
    webhook (unreachable, a server error, a rate limit, or a refusal of every post, as a
    revoked or deleted webhook answers) holds all posting: for a rate limit's
    Retry-After, otherwise for a delay that doubles with each consecutive failure up to
    MAX_HOLD. A refusal of one message's content holds only that message, on its own
    doubling delay. A webhook that refuses everything is then asked about once a minute
    rather than once per page per cycle, since Discord's edge bans an address that sends
    it thousands of refused requests, and the pages wait intact.
    """

    def __init__(self, post: Callable[[dict], None]):
        self._post = post
        self._resume_at = 0.0
        self._failures = 0
        # message name -> (consecutive refusals, not before)
        self._refused: dict[str, tuple[int, float]] = {}
        self._tokens = float(PAGE_BURST)
        self._counted_at: float | None = None

    def _available(self, now: float) -> float:
        if self._counted_at is not None:
            self._tokens = min(float(PAGE_BURST), self._tokens + max(0.0, now - self._counted_at) / PAGE_INTERVAL)
        self._counted_at = now
        return self._tokens

    def ready(self, name: str, now: float) -> bool:
        """Whether the message ``name`` is off hold and a token is left for it."""
        return now >= max(self._resume_at, self._refused.get(name, (0, 0.0))[1]) and self._available(now) >= 1

    def post(self, name: str, payload: dict, now: float) -> None:
        """Post ``payload`` as the message ``name``, spending a token, or raise
        DiscordError after holding what the failure calls for."""
        self._tokens = self._available(now) - 1
        try:
            self._post(payload)
        except discord.DiscordError as e:
            if e.status in PAYLOAD_REFUSED:
                refusals = self._refused.get(name, (0, 0.0))[0] + 1
                self._refused[name] = (refusals, now + _doubling(refusals))
            else:
                self._failures += 1
                wait = e.retry_after if e.status == 429 and e.retry_after else _doubling(self._failures)
                self._resume_at = now + min(wait, MAX_RETRY_AFTER)
            raise
        self._failures = 0
        self._refused.pop(name, None)

    def flush(self, store: Store, now: float, keep: int = 0) -> int:
        """Page unsent ALERTs and CRITICALs, most severe first, while tokens beyond
        ``keep`` remain; returns how many findings went out. Undelivered pages stay
        pending, in order, and a message refused FALLBACK_AFTER times goes out as its
        headline alone."""
        delivered = 0
        for message in _messages(store.unsent_pages()):
            if self._available(now) < keep + 1:
                break
            keys = [f.key for f in message]
            name = _message_name(message)
            if not self.ready(name, now):
                continue
            headline_only = max(f.failures for f in message) >= FALLBACK_AFTER
            try:
                self.post(name, page_message(message, headline_only), now)
            except discord.DiscordError as e:
                if e.status in PAYLOAD_REFUSED:
                    store.reject(keys)
                logger.warning("page for %s not delivered: %s", keys[0], e)
                continue
            store.mark_sent(keys, now)
            delivered += len(keys)
        return delivered


# --- the watch loop ------------------------------------------------------------------


def _utc(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")


class Watch:
    def __init__(self, config: rules.WatchConfig, store: Store, sources: list, post: Callable[[dict], None],
                 clock: Callable[[], float] = time.time, notify: Callable[[str], None] = lambda message: None):
        self._config = config
        self._store = store
        self._sources = sources
        self._pager = Pager(post)
        self._clock = clock
        self._notify = notify
        self._started = clock()
        self._due = {s.name: 0.0 for s in sources}
        self._decimals = {n.name: n.pool.cmatra_decimals for n in config.cardano if n.pool}

    def cycle(self) -> None:
        """Poll each source that is due, paging what it found and pinging the systemd
        watchdog before the next, so one slow source never holds back another's pages.
        A digest that is due goes out before any further page."""
        for source in self._sources:
            now = self._clock()
            if now < self._due[source.name]:
                continue
            self._poll(source, now)
            self._flush(self._clock())
            self._notify("WATCHDOG=1")
        now = self._clock()
        self._check_stale(now)
        self._digest(now)
        self._flush(now)

    def _flush(self, now: float) -> None:
        """Page what waits, keeping a token back for a digest that is due."""
        self._pager.flush(self._store, now, keep=1 if self._digest_due(now) else 0)

    def _digest_due(self, now: float) -> bool:
        moment = datetime.fromtimestamp(now, timezone.utc)
        return (moment.hour >= self._config.digest_hour_utc
                and self._store.get("digest:last-day") != moment.date().isoformat())

    def _poll(self, source, now: float) -> None:
        try:
            caught_up = source.poll(now)
        except Exception as e:  # one unreadable source must not blind the others; staleness pages it
            logger.exception("%s: poll failed", source.name)
            self._store.put(f"health:{source.name}:error", f"{type(e).__name__}: {e}"[:300])
            self._due[source.name] = now + source.poll_seconds
            return
        self._due[source.name] = now if not caught_up else now + source.poll_seconds
        if caught_up:
            self._healthy(source, now)
            return
        # Read without error but still behind: blocks that take longer to read than the
        # chain takes to make them leave the cursor further back on every poll.
        with self._store.transaction():
            self._store.put(f"health:{source.name}:position", source.position)
            self._store.put(f"health:{source.name}:error", f"behind its head at {source.position}")

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
        """Page a source that has not been read up to its head for ``source_stale_seconds``,
        and page it again every hour it stays that way."""
        store = self._store
        for source in self._sources:
            name = source.name
            last_ok = max(float(store.get(f"health:{name}:ok") or 0), self._started)
            overdue = now - last_ok - self._config.source_stale_seconds
            if overdue <= 0:
                continue
            error = store.get(f"health:{name}:error") or "no poll has completed"
            with store.transaction():
                store.add(rules.Finding(
                    rules.CRITICAL, f"custody-watch:{name}:stale:{int(last_ok)}:{int(overdue // HOUR)}",
                    f"custody-watch: {name} stale for {int(now - last_ok) // 60} min; "
                    f"moves on it are not being watched",
                    details=(f"last caught up {_utc(last_ok)}", f"last error: {error}"), kind="watcher"), now)
                store.put(f"health:{name}:stale-since", str(int(last_ok)))

    def _digest(self, now: float) -> None:
        if not self._digest_due(now) or not self._pager.ready("digest", now):
            return
        day = datetime.fromtimestamp(now, timezone.utc).date().isoformat()
        routine = self._store.unsent_routine()
        try:
            self._pager.post("digest", self.digest_message(routine, now, self._coverage(now, day)), now)
        except discord.DiscordError as e:
            logger.warning("daily digest not delivered, retrying: %s", e)
            return
        with self._store.transaction():
            self._store.mark_sent([f.key for f in routine], now)
            self._store.put("digest:last-day", day)

    def _coverage(self, now: float, day: str) -> list[str]:
        """Today's surrender-pool coverage lines, read once a day, and its page when the
        pool falls short. A reading that fails is reported in the digest in its place."""
        lines = []
        for source in self._sources:
            if not (isinstance(source, CardanoSource) and source.covers_pool):
                continue
            name = f"coverage:{source.name}"
            stored = json.loads(self._store.get(name) or "{}")
            if stored.get("day") != day:
                page = None
                try:
                    found, page = source.coverage(now)
                except Exception as e:  # the digest is the liveness signal; nothing it reports may hold it back
                    logger.exception("%s: surrender pool coverage not read", source.name)
                    found = [f"{source.name} surrender pool coverage not read: {type(e).__name__}: {e}"[:300]]
                stored = {"day": day, "lines": found}
                with self._store.transaction():
                    self._store.put(name, json.dumps(stored))
                    if page is not None:
                        self._store.add(page, now)
            lines += stored["lines"]
        return lines

    def digest_message(self, routine: list[StoredFinding], now: float, coverage: list[str] = ()) -> dict:
        store = self._store
        stale = [s.name for s in self._sources if store.get(f"health:{s.name}:stale-since")]
        state = f"STALE: {', '.join(stale)}" if stale else "alive"
        lines = [f"\U0001f4cb **custody-watch daily digest** {_utc(now)}: {state}", "sources:"]
        for source in self._sources:
            ok = store.get(f"health:{source.name}:ok")
            position = store.get(f"health:{source.name}:position")
            state = f"{position} (last read {_utc(float(ok))})" if ok else "never read"
            lines.append(f"  {source.name}: {state}")
        lines.extend(coverage)
        paged = store.paged_since(now - DAY)
        critical = sum(1 for f in paged if f.severity == rules.CRITICAL)
        lines.append(f"paged in the last 24h: {len(paged)} ({critical} critical)")
        lines.append(f"pages waiting for delivery: {len(store.unsent_pages())}")

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
            lines.append(f"  {network}: {rules.plural(len(amounts), 'surrender')} paying "
                         f"{whole:,}.{frac:0{decimals}d} cMATRA")
        for network, count in committees.items():
            lines.append(f"  {network}: {rules.plural(count, 'committee rotation')}, membership unchanged")
        if other:
            lines.append(f"  {rules.plural(len(other), 'other routine finding')}:")
            lines.extend(f"    {f.headline}" for f in other[:10])
        content = "\n".join(lines)
        if len(content) > MESSAGE_LIMIT:
            content = content[:MESSAGE_LIMIT - 12] + "\n(truncated)"
        return {"content": content, "allowed_mentions": {"parse": []}}


# --- Materios ---------------------------------------------------------------------------


SUDO_KEY_STORAGE = "0x" + (xxh128(b"Sudo") + xxh128(b"Key")).hex()
SYSTEM_EVENTS_STORAGE = "0x" + (xxh128(b"System") + xxh128(b"Events")).hex()
CODE_STORAGE = "0x" + b":code".hex()
RECOVERY = xxh128(b"Recovery")
MAX_BLOCKS_PER_POLL = 600
BLOCK_SECONDS = 6
KEYS_PAGE = 1000


PROXY_PREFIX = "0x" + (RECOVERY + xxh128(b"Proxy")).hex()


def recovery_proxy_key(rescuer: bytes) -> str:
    """Recovery.Proxy(rescuer): the account the rescuer may act as."""
    return PROXY_PREFIX + blake2_128_concat(rescuer).hex()


def _recoverable_key(account: bytes) -> str:
    """Recovery.Recoverable(account): the friends, threshold and delay that recover it."""
    return "0x" + (RECOVERY + xxh128(b"Recoverable") + two_x64_concat(account)).hex()


def _active_prefix(lost: bytes) -> str:
    """Recovery.ActiveRecoveries(lost, *): every recovery of ``lost`` under way, by rescuer."""
    return "0x" + (RECOVERY + xxh128(b"ActiveRecoveries") + two_x64_concat(lost)).hex()


def _trailing_account(key: str) -> bytes:
    """The account an ActiveRecoveries or Proxy key ends with, after its twox64 or blake2_128 hash."""
    return bytes.fromhex(key[-64:])


def _account(storage_hex: str) -> str:
    return rules.render_account(bytes.fromhex(storage_hex[2:])) if storage_hex else "none"


class MateriosSource:
    """Finalized Materios blocks, from the cursor to the finalized head.

    ``client`` is daemon-core's ``SubstrateClient`` (bounded per-call timeout and
    reconnect) or anything with the same ``connected``/``connect``/``rpc`` surface.
    A finalized head that stops moving for ``stale_seconds`` is a failure to read the
    source, since a node that has stopped following the chain still answers.
    """

    def __init__(self, config: rules.MateriosConfig, client, store: Store, stale_seconds: int):
        self.name = config.name
        self.poll_seconds = config.poll_seconds
        self._config = config
        self._client = client
        self._store = store
        self._stale_seconds = stale_seconds
        # (finalized head, when it was first seen)
        self._moved: tuple[int, float] | None = None
        self._cursor_key = f"cursor:{self.name}"
        self._authorities = frozenset(rules.account_bytes(a) for a in config.authority_accounts)
        # Keyed by genesis and the runtime code's hash: a reset chain, or a runtime written
        # without an upgrade, may reuse a spec version with another pallet layout.
        self._decoders: dict[tuple[str, str], rules.RuntimeDecoder] = {}
        self._decoder: rules.RuntimeDecoder | None = None
        self._genesis: str | None = None
        # The recovery entries last read, with the friends and the rescuers they name.
        self._recovering: tuple[dict[str, list], tuple[frozenset[bytes], frozenset[bytes]]] | None = None

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
        """Process blocks towards the finalized head; True once there. A poll ends after
        ``MAX_BLOCKS_PER_POLL`` blocks, or after the block that brings what it decoded
        to ``DECODE_BUDGET`` values, so blocks built to be expensive to read cannot hold
        back the other sources."""
        head_hash, head = self._head()
        if self._moved is None or self._moved[0] != head:
            self._moved = (head, now)
        elif now - self._moved[1] > self._stale_seconds:
            raise SourceError(f"{self.name}: finalized head #{head} has not advanced for "
                              f"{int(now - self._moved[1]) // 60} min")
        if self._reset(head, now):
            return True
        sudo_key = self._sudo_key(head_hash, head, now)
        code = self._code(head_hash, head, now)
        recovering = self._recovery(head_hash, head, sudo_key, code, now)

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
        decoded = 0
        for number, block_hash in zip(range(first, last + 1), hashes):
            decoded += self._block(number, block_hash, sudo_key, recovering, now)
            if decoded >= rules.DECODE_BUDGET:
                return number == head
        return last == head

    def _reset(self, head: int, now: float) -> bool:
        """A new genesis means the chain was reset: page it, forget what was learned about
        the old chain, and watch the new one from its finalized head."""
        genesis = self._rpc("chain_getBlockHash", [0])
        self._genesis = genesis
        name = f"genesis:{self.name}"
        stored = self._store.get(name)
        with self._store.transaction():
            self._store.put(name, genesis)
            if stored is None or stored == genesis:
                return False
            self._store.retire(f"{self.name}:", f"@{stored}")
            self._store.delete_prefix(f"metadata:{self.name}:{stored}:")
            self._store.add(rules.Finding(
                rules.CRITICAL, f"{self.name}:genesis:{genesis}",
                f"{self.name} genesis changed: the chain was reset",
                details=(f"from {stored}", f"to   {genesis}", f"watching again from finalized #{head}")), now)
            for kept in ("sudo-key", "committee", "code", "recovery"):
                self._store.delete(f"{kept}:{self.name}")
            self.start_at(head)
        self._decoder = None
        self._decoders.clear()
        self._recovering = None
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

    def _code(self, head_hash: str, head: int, now: float) -> str:
        """The hash of the runtime code at the finalized head. A change pages, and the
        blocks after it are read with the new code's metadata: a raw write of ``:code``
        deposits no upgrade digest, so this is what notices it."""
        code = self._rpc("state_getStorageHash", [CODE_STORAGE, head_hash])
        name = f"code:{self.name}"
        stored = self._store.get(name)
        with self._store.transaction():
            if stored is not None and stored != code:
                self._store.add(rules.Finding(
                    rules.CRITICAL, f"{self.name}:code:{head}",
                    f"{self.name}: runtime code changed (seen at finalized #{head})",
                    details=(f"from blake2_256 {stored}", f"to   blake2_256 {code}",
                             "blocks read from here on are decoded with the new code's metadata")), now)
                self._decoder = None
            self._store.put(name, code)
        return code

    def _recovery(self, head_hash: str, head: int, sudo_key: bytes | None, code: str,
                  now: float) -> tuple[frozenset[bytes], frozenset[bytes]]:
        """Read at the finalized head who can recover Sudo.Key or a configured authority,
        who is recovering one, and who may already act as one or as whom one may act, and
        page any change. Returns those friends, then those rescuers, whose extrinsics
        decode ahead of other accounts' but count as nobody's authority: any funded
        account can start a recovery of Sudo.Key. Entries are decoded only when they change."""
        entries = self._recovery_entries(rules.accountable(sudo_key, self._authorities), head_hash)
        if self._recovering is not None and self._recovering[0] == entries:
            return self._recovering[1]

        def who(account: bytes) -> str:
            role = ", Sudo.Key" if account == sudo_key else ", authority" if account in self._authorities else ""
            return rules.render_account(account) + role

        def read(entry: list) -> tuple[str, object]:
            """An entry's line, and its value decoded, or None when it does not decode."""
            item, accounts, value = entry[0], tuple(bytes.fromhex(a) for a in entry[1]), entry[2]
            decoder = self._decoder_for(code, [head_hash])
            try:
                decoded = decoder.storage("Recovery", item, value)
            except Exception as e:  # a runtime may change the layout; the entry still pages, as its hash
                return (f"Recovery.{item}({', '.join(who(a) for a in accounts)}): "
                        f"{rules.hex_digest(value)}, not decodable: {type(e).__name__}: {e}"[:400]), None
            return rules.describe_recovery(item, accounts, decoded, who), decoded

        self._recovery_change(entries, read, head, now)
        friends: set[bytes] = set()
        for entry in entries.values():
            decoded = read(entry)[1] if entry[0] == "Recoverable" else None
            if decoded is not None:
                friends.update(rules.recovery_friends(decoded))
        rescuers = frozenset(bytes.fromhex(entry[1][-1]) for entry in entries.values() if entry[0] != "Recoverable")
        self._recovering = (entries, (frozenset(friends), rescuers))
        return self._recovering[1]

    def _recovery_entries(self, watched: frozenset[bytes], at_hash: str) -> dict[str, list]:
        """Every Recovery entry naming a watched account, by storage key, as [item, the
        accounts its key names in hex, its value]: Recoverable(account),
        ActiveRecoveries(account, rescuer), and each Proxy that acts as or for one. The
        whole Proxy map is read, since Root's set_recovered, and a recovery closed or
        older than the watcher, leave a proxy that no recovery under way names."""
        keyed: dict[str, tuple[str, tuple[bytes, ...]]] = {}
        for account in sorted(watched):
            keyed[_recoverable_key(account)] = ("Recoverable", (account,))
            for key in self._keys(_active_prefix(account), at_hash):
                keyed[key] = ("ActiveRecoveries", (account, _trailing_account(key)))
        proxies = self._keys(PROXY_PREFIX, at_hash)
        values = self._storage_at([*keyed, *proxies], at_hash)
        for key in proxies:
            value, rescuer = values.get(key), _trailing_account(key)
            if value and (rescuer in watched or bytes.fromhex(value[2:]) in watched):
                keyed[key] = ("Proxy", (rescuer,))
        return {key: [item, [a.hex() for a in accounts], values[key]]
                for key, (item, accounts) in keyed.items() if values.get(key)}

    def _recovery_change(self, entries: dict[str, list], read: Callable[[list], tuple[str, object]], head: int,
                         now: float) -> None:
        """Keep ``entries`` as the recovery state; its first read goes to the digest, and
        any entry added, changed or removed after it pages."""
        name = f"recovery:{self.name}"
        stored = self._store.get(name)
        previous = json.loads(stored) if stored is not None else None
        if previous == entries:
            return
        with self._store.transaction():
            self._store.put(name, json.dumps(entries, sort_keys=True))
            if previous is None:
                if entries:
                    self._store.add(rules.Finding(
                        rules.INFO, f"{self.name}:recovery:{head}",
                        f"{self.name}: recovery of Sudo.Key and the authorities as first read at #{head}",
                        details=tuple(read(entries[k])[0] for k in sorted(entries)), kind="recovery"), now)
                return
            added = [k for k in sorted(entries) if k not in previous]
            changed = [k for k in sorted(entries) if k in previous and previous[k] != entries[k]]
            removed = sorted(set(previous) - set(entries))
            current = {k: read(entries[k]) for k in added + changed}
            lines = ([f"+ {current[k][0]}" for k in added] + [f"~ {current[k][0]}" for k in changed]
                     + [f"- {read(previous[k])[0]}" for k in removed])
            # Any funded account can start a recovery of Sudo.Key, once a poll; until a
            # friend vouches it can do nothing, so such starts page grouped, behind what
            # pages alone.
            started = not changed and not removed and all(
                entries[k][0] == "ActiveRecoveries" and current[k][1] is not None and not current[k][1]["friends"]
                for k in added)
            self._store.add(rules.Finding(
                rules.CRITICAL, f"{self.name}:recovery:{head}",
                f"{self.name}: a recovery of Sudo.Key or an authority started (seen at finalized #{head})" if started
                else f"{self.name}: recovery of Sudo.Key or an authority changed (seen at finalized #{head})",
                details=tuple(lines), group=f"{self.name} recovery started" if started else None), now)

    def _keys(self, prefix: str, at_hash: str) -> list[str]:
        keys: list[str] = []
        while True:
            page = self._rpc("state_getKeysPaged", [prefix, KEYS_PAGE, keys[-1] if keys else prefix, at_hash])
            keys += page
            if len(page) < KEYS_PAGE:
                return keys

    def _load_decoder(self, at_hash: str) -> rules.RuntimeDecoder:
        """The decoder for blocks built on ``at_hash``; the node's current runtime when that
        state is pruned, in which case an extrinsic it cannot read is still reported."""
        try:
            code = self._rpc("state_getStorageHash", [CODE_STORAGE, at_hash])
            params = [at_hash]
        except SubstrateRequestException:
            code = self._rpc("state_getStorageHash", [CODE_STORAGE])
            params = []
            logger.warning("%s: state at %s is pruned; decoding with the current runtime, code %s",
                           self.name, at_hash, code)
        return self._decoder_for(code, params)

    def _decoder_for(self, code: str, params: list) -> rules.RuntimeDecoder:
        """The decoder for the runtime whose code hashes to ``code``, its metadata read
        with ``params`` the first time."""
        cache = (self._genesis, code)
        if cache not in self._decoders:
            name = f"metadata:{self.name}:{self._genesis}:{code}"
            metadata = self._store.get(name)
            if metadata is None:
                metadata = self._rpc("state_getMetadata", params)
                self._store.put(name, metadata)
            self._decoders[cache] = rules.RuntimeDecoder(metadata)
        return self._decoders[cache]

    def _events(self, block_hash: str, decoder: rules.RuntimeDecoder) -> dict[int, list[str]] | None:
        """The block's events by extrinsic, or None when the node has pruned them, serves
        none, or they do not decode; the finding then says its dispatch result is
        unverified. Every block has events, so an empty read is never taken to mean
        that nothing happened."""
        try:
            raw = self._rpc("state_getStorage", [SYSTEM_EVENTS_STORAGE, block_hash])
        except SubstrateRequestException:
            return None
        if not raw:
            return None
        try:
            return decoder.events(raw)
        except Exception as e:  # scalecodec raises any type on bytes it cannot place
            logger.warning("%s: events at %s do not decode: %s: %s", self.name, block_hash, type(e).__name__, e)
            return None

    def _storage_at(self, keys: list[str], at_hash: str) -> dict[str, str | None]:
        """Each of ``keys`` at ``at_hash``, None for one that holds nothing, KEYS_PAGE keys
        a read, so no answer outgrows the node's response limit however many recoveries
        of Sudo.Key strangers start."""
        values: dict[str, str | None] = {}
        for start in range(0, len(keys), KEYS_PAGE):
            [changes] = self._rpc("state_queryStorageAt", [keys[start:start + KEYS_PAGE], at_hash])
            values.update(changes["changes"])
        return values

    def _dispatch_facts(self, parent_hash: str, block_hash: str, rescuers: frozenset[bytes]) -> dict | None:
        """Sudo.Key and each rescuer's Recovery.Proxy, for those that held one value at
        both the block's parent and the block, so none changed inside it; None once the
        node has pruned either state."""
        keys = {rules.SUDO: SUDO_KEY_STORAGE, **{rules.proxy_fact(r): recovery_proxy_key(r) for r in rescuers}}
        try:
            before = self._storage_at(list(keys.values()), parent_hash)
            after = self._storage_at(list(keys.values()), block_hash)
        except SubstrateRequestException:
            return None
        return {fact: bytes.fromhex(after[key][2:]) if after.get(key) else None
                for fact, key in keys.items() if before.get(key) == after.get(key)}

    def _block(self, number: int, block_hash: str, sudo_key: bytes | None,
               recovering: tuple[frozenset[bytes], ...], now: float) -> int:
        """Classify one block and commit its findings with the cursor; returns the values
        its extrinsics and events decoded into. What the accounts in ``recovering`` sign
        decodes ahead of other accounts' extrinsics, from the budget they share."""
        block = self._rpc("chain_getBlock", [block_hash])["block"]
        header = block["header"]
        if self._decoder is None:
            self._decoder = self._load_decoder(header["parentHash"])
        decoder = self._decoder
        before = decoder.values
        extrinsics = decoder.extrinsics(block["extrinsics"], rules.accountable(sudo_key, self._authorities),
                                        recovering)
        findings = rules.classify_materios_block(
            self.name, number, extrinsics, lambda: self._events(block_hash, decoder), sudo_key, self._authorities,
            lambda rescuers: self._dispatch_facts(header["parentHash"], block_hash, rescuers))
        if rules.runtime_upgraded(header):
            findings.append(rules.runtime_changed(self.name, number))
        try:
            committee = rules.committee_of(extrinsics)
        except Exception as e:  # the inherent's shape is the block author's; it must not stall the cursor
            committee = None
            findings.append(rules.unclassifiable(f"{self.name}:{number}:committee",
                                                 f"{self.name} #{number}: the committee inherent", e))
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
        return decoder.values - before


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
MAX_TX_PER_POLL = 200
EXACT_POLICY_ASSETS = 10
# Cardano's mean block interval, and how many blocks further back than that a trailing
# window's listing starts, so a slow week never leaves a transaction out.
CARDANO_BLOCK_SECONDS = 20
WINDOW_MARGIN_BLOCKS = 2_000


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
        """Watch addresses and the mints and burns under every policy from ``height``."""
        with self._store.transaction():
            for address in self._addresses:
                self._store.put(self._address_key(address), str(height))
            for policy in self._network.policies:
                self._store.put(self._policy_key(policy.policy_id), str(height))
            self._store.put(f"cursor:{self.name}:tip", str(height))

    def rewind(self, seconds: int) -> None:
        """Start ``seconds`` of blocks behind the tip, at the mean block interval."""
        self.start_at(self._api.get("/blocks/latest")["height"] - seconds // CARDANO_BLOCK_SECONDS)

    def _pages(self, path: str, first_page: int = 1, **params):
        page = first_page
        while True:
            rows = self._api.get(path, count=PAGE, page=page, **params) or []
            yield from rows
            if len(rows) < PAGE:
                return
            page += 1

    def poll(self, now: float) -> bool:
        """Classify up to ``MAX_TX_PER_POLL`` new transactions; True once none are left and
        the cursors have moved to the tip. A larger backlog is worked off over successive
        polls, so pages and the systemd watchdog run between them."""
        tip = self._api.get("/blocks/latest")
        if tip is None:
            raise SourceError(f"{self.name}: blockfrost served no chain tip")
        age = now - tip["time"]
        if age > self._stale_seconds:
            raise SourceError(f"{self.name}: chain tip {tip['height']} is {int(age) // 60} min old")
        height = tip["height"]
        # tx hash -> the height at or below which it is history rather than a move; an
        # asset's history carries no heights, so a mint older than ``start_at`` is only
        # recognized once its transaction is read.
        pending: dict[str, int | None] = {}
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
            marker = self._store.get(self._policy_key(policy.policy_id))
            baselined = marker is not None
            floor = int(marker) if baselined and marker.isdigit() else None
            cursors[self._policy_key(policy.policy_id)] = "baselined"
            assets = list(self._pages(f"/assets/policy/{policy.policy_id}"))
            # A mint count is one read per asset, so a large policy has its counts read
            # only for an asset whose supply moved, and every asset's once an hour for a
            # mint and burn that cancel out.
            reconciled = f"cursor:{self.name}:reconciled:{policy.policy_id}"
            every = len(assets) <= EXACT_POLICY_ASSETS or now - float(self._store.get(reconciled) or 0) >= HOUR
            if every:
                cursors[reconciled] = repr(now)
            for asset in assets:
                unit = asset["asset"]
                supply = f"cursor:{self.name}:supply:{unit}"
                cursors[supply] = asset["quantity"]
                if not every and self._store.get(supply) == asset["quantity"]:
                    continue
                count = int(self._api.get(f"/assets/{unit}")["mint_or_burn_count"])
                key = f"cursor:{self.name}:asset:{unit}"
                seen = self._store.get(key)
                seen = int(seen) if seen is not None else (0 if baselined else count)
                if count > seen:
                    first_page = seen // PAGE + 1
                    history = list(self._pages(f"/assets/{unit}/history", first_page=first_page, order="asc"))
                    offset = (first_page - 1) * PAGE
                    for entry in history[seen - offset:count - offset]:
                        pending.setdefault(entry["tx_hash"], floor)
                cursors[key] = str(count)

        todo = [(h, floor) for h, floor in pending.items() if not self._store.processed(f"{self.name}:{h}")]
        for tx_hash, floor in todo[:MAX_TX_PER_POLL]:
            self._classify(tx_hash, now, floor)
        if len(todo) > MAX_TX_PER_POLL:
            return False
        overruns = self._redemption_overruns()
        with self._store.transaction():
            for key, value in cursors.items():
                self._store.put(key, value)
            for finding in overruns:
                self._store.add(finding, now)
        return True

    def _redemption_overruns(self) -> list[rules.Finding]:
        pool = self._network.pool
        if pool is None or pool.quarantine_address is None:
            return []
        return rules.redemption_overruns(self._network, self._holding(pool.quarantine_address))

    def _holding(self, address: str) -> dict[str, int]:
        """What ``address`` holds now, by unit; nothing for an address Blockfrost has never seen."""
        holding = self._api.get(f"/addresses/{address}")
        return {a["unit"]: int(a["quantity"]) for a in holding["amount"]} if holding else {}

    @property
    def covers_pool(self) -> bool:
        return self._network.pool is not None and self._network.pool.coverage is not None

    def coverage(self, now: float) -> tuple[list[str], rules.Finding | None]:
        """Whether the surrender pool can pay what is still redeemable: its balance, what
        the quarantine address holds, and what the pool paid in the last seven days."""
        pool = self._network.pool
        balance = self._holding(pool.address).get(pool.cmatra_unit, 0)
        held = self._holding(pool.quarantine_address)
        week, unread = self._paid_since(now - 7 * DAY)
        return rules.pool_coverage(self._network, balance, held, week, unread, now)

    def _paid_since(self, since: float) -> tuple[int, int]:
        """The cMATRA the pool paid out since ``since``, read from at most MAX_TX_PER_POLL
        of its transactions, and how many more there were."""
        pool = self._network.pool
        tip = self._api.get("/blocks/latest")
        if tip is None:
            raise SourceError(f"{self.name}: blockfrost served no chain tip")
        start = tip["height"] - int(tip["time"] - since) // CARDANO_BLOCK_SECONDS - WINDOW_MARGIN_BLOCKS
        rows = [row for row in self._pages(f"/addresses/{pool.address}/transactions", order="asc",
                                           **{"from": str(max(start, 0))})
                if row["block_time"] >= since]
        paid = 0
        for row in rows[:MAX_TX_PER_POLL]:
            utxos = self._api.get(f"/txs/{row['tx_hash']}/utxos")
            if utxos is None:
                raise SourceError(f"{self.name}: blockfrost does not serve transaction {row['tx_hash']} yet")
            paid += rules.pool_outflow(pool, utxos)
        return paid, max(0, len(rows) - MAX_TX_PER_POLL)

    def _classify(self, tx_hash: str, now: float, floor: int | None) -> None:
        tx = self._api.get(f"/txs/{tx_hash}")
        if tx is not None and floor is not None and tx["block_height"] <= floor:
            with self._store.transaction():
                self._store.mark_processed(f"{self.name}:{tx_hash}", now)
            return
        utxos = self._api.get(f"/txs/{tx_hash}/utxos")
        if tx is None or utxos is None:
            raise SourceError(f"{self.name}: blockfrost does not serve transaction {tx_hash} yet")
        redeemers = self._api.get(f"/txs/{tx_hash}/redeemers") or []
        for redeemer in redeemers:
            datum = self._api.get(f"/scripts/datum/{redeemer['redeemer_data_hash']}")
            redeemer["json_value"] = datum["json_value"] if datum else None
        try:
            finding = rules.classify_cardano_tx(self._network, tx, utxos, redeemers)
        except Exception as e:  # a transaction's contents are its builder's; none may stall the cursor
            finding = rules.unclassifiable(f"{self.name}:{tx_hash}", f"{self.name} tx {tx_hash}", e,
                                           group=f"{self.name} unclassifiable")
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
        sources.append(MateriosSource(config.materios, client, store, config.source_stale_seconds))
    for network in config.cardano:
        api = Blockfrost(network.blockfrost_url, network.project_id_file)
        sources.append(CardanoSource(network, api, store, config.source_stale_seconds))
    return sources


def _run(config: rules.WatchConfig, webhook: str) -> int:
    store = Store(config.state_db)
    watch = Watch(config, store, build_sources(config, store),
                  functools.partial(discord.post_json, webhook), notify=sd_notify)
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


def _webhook(config: rules.WatchConfig | None) -> str:
    """The webhook the watcher pages: its own channel's, when the config names a file
    holding one, otherwise DISCORD_WEBHOOK_URL."""
    if config is not None and config.discord_webhook_file:
        return Path(config.discord_webhook_file).read_text().strip()
    return os.environ.get("DISCORD_WEBHOOK_URL", "")


def _load_config(path: str) -> rules.WatchConfig:
    config_path = Path(path)
    return rules.parse_config(json.loads(config_path.read_text()), base_dir=config_path.parent)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="custody_watch", description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    watch = commands.add_parser("run", help="watch and page until stopped")
    replay = commands.add_parser("backtest", help="replay recent history, print findings, page nothing")
    for command in (watch, replay):
        command.add_argument("--config", required=True)
    replay.add_argument("--days", type=int, default=30)
    replay.add_argument("--state", required=True, help="a fresh SQLite file, never the live state")
    replay.add_argument("--materios-rpc", help="read Materios from this node instead of the configured one")
    test = commands.add_parser("test-page", help="send one test message through the webhook")
    test.add_argument("--config", help="through the webhook this config's watcher pages")
    # The failure page reads no config, so a config that stops the watcher cannot also
    # silence the page saying it stopped; it goes through DISCORD_WEBHOOK_URL.
    failed = commands.add_parser("page-failure", help="page that a systemd unit failed (OnFailure hook)")
    failed.add_argument("--unit", required=True)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    if args.command == "backtest":
        return _backtest(_load_config(args.config), args.days, args.state, args.materios_rpc)

    config = _load_config(args.config) if getattr(args, "config", None) else None
    webhook = _webhook(config)
    if not webhook.startswith("https://"):
        print("custody_watch: neither the config's discord_webhook_file nor DISCORD_WEBHOOK_URL holds an https "
              "webhook; refusing to watch without a way to page", file=sys.stderr)
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
