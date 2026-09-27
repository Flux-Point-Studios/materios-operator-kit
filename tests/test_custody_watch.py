"""The custody watcher's runtime: cursors, dedupe, paging, the daily digest,
source health, and both sources driven by real chain history.

Materios is served from the captured preprod blocks and runtime metadata;
Cardano from the captured Blockfrost transactions (see test_custody_rules).
"""

import copy
import gzip
import http.server
import json
import socket
import threading
from datetime import datetime, timezone
from pathlib import Path

import pytest
from substrateinterface.exceptions import SubstrateRequestException

from daemon import custody_rules as rules
from daemon import custody_watch as cw
from daemon import discord

FIX = Path(__file__).parent / "fixtures" / "custody"
SUDO_KEY = "5H2M5Dbt8hSfSCXS6hfEBPR1N21yh679finzcfMEwD62i7iP"
ROUTINE_BLOCK = "block_2034370_with_events.json"


def _read(name: str) -> bytes:
    raw = (FIX / name).read_bytes()
    return gzip.decompress(raw) if name.endswith(".gz") else raw


def _at(text: str) -> float:
    return datetime.fromisoformat(text).replace(tzinfo=timezone.utc).timestamp()


@pytest.fixture
def config():
    return rules.parse_config(json.loads((FIX / "config.json").read_text()))


def _finding(key="k", severity=rules.CRITICAL, kind="event", amount=0, details=("d",)):
    return rules.Finding(severity=severity, key=key, headline=f"headline {key}", details=details,
                         kind=kind, amount=amount)


class Posts:
    """Records Discord payloads; fails the posts listed in ``fail`` (1-based)."""

    def __init__(self, fail=()):
        self.payloads = []
        self.attempts = 0
        self.fail = set(fail)

    def __call__(self, payload):
        self.attempts += 1
        if self.attempts in self.fail:
            raise discord.DiscordError("webhook answered HTTP 502")
        self.payloads.append(payload)

    def text(self):
        return "\n".join(p["content"] for p in self.payloads)


# --- store and pager ------------------------------------------------------------


def test_a_finding_is_stored_once_and_paged_once_across_a_restart(tmp_path):
    db = str(tmp_path / "state.db")
    posts = Posts()
    store = cw.Store(db)
    assert store.add(_finding("materios:1:2"), now=1.0)
    assert not store.add(_finding("materios:1:2"), now=2.0)
    assert cw.Pager(posts).flush(store, now=3.0) == 1
    store.close()

    store = cw.Store(db)
    assert not store.add(_finding("materios:1:2"), now=4.0)
    assert cw.Pager(posts).flush(store, now=5.0) == 0
    assert len(posts.payloads) == 1


def test_a_rejected_page_stays_pending_and_is_retried_in_order(tmp_path):
    store = cw.Store(str(tmp_path / "state.db"))
    for key in ("a", "b", "c"):
        store.add(_finding(key), now=1.0)
    posts = Posts(fail={2})
    pager = cw.Pager(posts)
    assert pager.flush(store, now=2.0) == 1
    assert pager.flush(store, now=3.0) == 2
    assert pager.flush(store, now=4.0) == 0
    assert [p["content"].split("headline ")[1][0] for p in posts.payloads] == ["a", "b", "c"]


def test_routine_findings_wait_for_the_digest(tmp_path):
    store = cw.Store(str(tmp_path / "state.db"))
    store.add(_finding("s", severity=rules.INFO, kind="surrender", amount=5), now=1.0)
    posts = Posts()
    assert cw.Pager(posts).flush(store, now=2.0) == 0
    assert posts.payloads == []


def test_a_critical_page_mentions_here_and_fits_one_discord_message():
    critical = cw.page_message(_finding(details=tuple("x" * 300 for _ in range(40))))
    alert = cw.page_message(_finding(severity=rules.ALERT))
    assert critical["content"].startswith("\U0001f6a8 **CRITICAL** @here")
    assert len(critical["content"]) <= 2000
    assert "@here" not in alert["content"]
    assert alert["allowed_mentions"] == {"parse": []}
    assert critical["allowed_mentions"] == {"parse": ["everyone"]}


# --- digest and source health -----------------------------------------------------


class StubSource:
    def __init__(self, name, fail=False):
        self.name = name
        self.poll_seconds = 60
        self.position = "height 100"
        self.fail = fail
        self.polls = 0

    def poll(self, now):
        self.polls += 1
        if self.fail:
            raise cw.SourceError(f"{self.name} unreachable")
        return True


def _watch(config, store, sources, posts, clock):
    return cw.Watch(config, store, sources, posts, clock=lambda: clock[0])


def test_the_digest_posts_once_a_day_after_its_hour_and_proves_the_watcher_alive(config, tmp_path):
    db = str(tmp_path / "state.db")
    store = cw.Store(db)
    store.add(_finding("cardano-mainnet:aa", severity=rules.INFO, kind="surrender", amount=1_500_000), now=1.0)
    store.add(_finding("cardano-mainnet:bb", severity=rules.INFO, kind="surrender", amount=2_000_000), now=1.0)
    store.add(_finding("materios-preprod:7:committee", severity=rules.INFO, kind="committee"), now=1.0)
    posts = Posts()
    clock = [_at("2026-09-27T12:59:00")]
    source = StubSource("cardano-mainnet")
    watch = _watch(config, store, [source], posts, clock)
    watch.cycle()
    assert posts.payloads == []

    clock[0] = _at("2026-09-27T13:00:05")
    watch.cycle()
    [digest] = posts.payloads
    text = digest["content"]
    assert "daily digest" in text and "alive" in text
    assert "cardano-mainnet: height 100" in text
    assert "cardano-mainnet: 2 surrenders paying 3.500000 cMATRA" in text
    assert "materios-preprod: 1 committee rotation, membership unchanged" in text

    clock[0] = _at("2026-09-27T18:00:00")
    watch.cycle()
    store.close()
    store = cw.Store(db)
    _watch(config, store, [source], posts, clock).cycle()
    assert len(posts.payloads) == 1

    clock[0] = _at("2026-09-28T13:00:00")
    _watch(config, store, [source], posts, clock).cycle()
    assert len(posts.payloads) == 2
    assert "no routine moves" in posts.payloads[1]["content"]


def test_a_digest_that_fails_to_post_is_retried(config, tmp_path):
    store = cw.Store(str(tmp_path / "state.db"))
    posts = Posts(fail={1})
    clock = [_at("2026-09-27T13:00:00")]
    watch = _watch(config, store, [StubSource("materios-preprod")], posts, clock)
    watch.cycle()
    assert posts.payloads == []
    clock[0] += 60
    watch.cycle()
    assert "daily digest" in posts.text()


def test_a_failing_source_raises_one_stale_alert_then_one_recovery(config, tmp_path):
    store = cw.Store(str(tmp_path / "state.db"))
    posts = Posts()
    clock = [_at("2026-09-27T01:00:00")]
    source = StubSource("cardano-mainnet", fail=True)
    watch = _watch(config, store, [source], posts, clock)
    watch.cycle()
    clock[0] += config.source_stale_seconds - 60
    watch.cycle()
    assert posts.payloads == []

    for _ in range(3):
        clock[0] += 120
        watch.cycle()
    assert len(posts.payloads) == 1
    assert "cardano-mainnet" in posts.text() and "stale" in posts.text()
    assert "unreachable" in posts.text()

    source.fail = False
    clock[0] += 120
    watch.cycle()
    assert len(posts.payloads) == 2
    assert "recovered" in posts.payloads[1]["content"]


def test_a_restart_after_downtime_gives_sources_a_chance_before_calling_them_stale(config, tmp_path):
    db = str(tmp_path / "state.db")
    clock = [_at("2026-09-27T01:00:00")]
    store = cw.Store(db)
    _watch(config, store, [StubSource("materios-preprod")], Posts(), clock).cycle()
    store.close()

    clock[0] += 6 * 3600
    posts = Posts()
    _watch(config, cw.Store(db), [StubSource("materios-preprod")], posts, clock).cycle()
    assert posts.payloads == []


def test_sources_are_polled_on_their_own_cadence(config, tmp_path):
    store = cw.Store(str(tmp_path / "state.db"))
    clock = [_at("2026-09-27T01:00:00")]
    source = StubSource("cardano-mainnet")
    watch = _watch(config, store, [source], Posts(), clock)
    watch.cycle()
    clock[0] += 30
    watch.cycle()
    clock[0] += 31
    watch.cycle()
    assert source.polls == 2


# --- Materios -------------------------------------------------------------------------


EVENTS_KEY = "0x26aa394eea5630e07c48ae0c9558cef780d41e5e16056765bc8461851072c9d7"
SUDO_STORAGE_KEY = "0x5c0d1176a568c1f92944340dbfed9e9c530ebca703c85910e7164cb7d1c9e47b"


class FakeChain:
    """Serves the Materios RPC reads the watcher makes from captured blocks.

    ``blocks`` maps a number to a fixture name; state (events, runtime version,
    metadata, Sudo.Key) is served only for hashes listed in ``state_at``, which
    stands for the node's unpruned window.
    """

    def __init__(self, head: int, blocks: dict[int, str], state_at=None, events_hex=None):
        self.head = head
        self.blocks = blocks
        self.state_at = set(state_at or ())
        self.events_hex = events_hex
        self.sudo_key = "0x" + rules.account_bytes(SUDO_KEY).hex()
        self.metadata = _read("materios/metadata_spec238.hex.gz").decode()
        self.headers = json.loads(_read("materios/headers_spec238_upgrade.json"))
        self.calls = []
        self.connected = True
        self.connects = 0

    def connect(self):
        self.connects += 1
        self.connected = True
        return True

    @staticmethod
    def hash_of(number: int) -> str:
        return "0x" + number.to_bytes(32, "big").hex()

    @staticmethod
    def number_of(block_hash: str) -> int:
        return int(block_hash, 16)

    def _block(self, number: int) -> dict:
        name = self.blocks.get(number, ROUTINE_BLOCK)
        extrinsics = json.loads(_read(f"materios/{name}"))["extrinsics"]
        logs = self.headers.get(str(number), {}).get("digest", {}).get("logs", [])
        header = {"number": hex(number), "parentHash": self.hash_of(number - 1), "digest": {"logs": logs}}
        return {"block": {"header": header, "extrinsics": extrinsics}}

    def _state(self, block_hash):
        if block_hash is not None and self.number_of(block_hash) not in self.state_at:
            raise SubstrateRequestException({"code": 4003, "message": "State already discarded"})

    def rpc(self, method, params):
        self.calls.append((method, params))
        if method == "chain_getFinalizedHead":
            return self.hash_of(self.head)
        if method == "chain_getHeader":
            return {"number": hex(self.number_of(params[0]))}
        if method == "chain_getBlockHash":
            numbers = params[0]
            return [self.hash_of(n) for n in numbers] if isinstance(numbers, list) else self.hash_of(numbers)
        if method == "chain_getBlock":
            return self._block(self.number_of(params[0]))
        if method == "state_getRuntimeVersion":
            self._state(params[0] if params else None)
            return {"specVersion": 238}
        if method == "state_getMetadata":
            self._state(params[0] if params else None)
            return self.metadata
        if method == "state_getStorage":
            key, at = params
            self._state(at)
            if key == SUDO_STORAGE_KEY:
                return self.sudo_key
            if key == EVENTS_KEY:
                return self.events_hex
        raise AssertionError(f"unexpected RPC {method} {params}")


def _materios(config, store, chain):
    return cw.MateriosSource(config.materios, chain, store)


def _drain(source, now=1.0):
    while not source.poll(now):
        pass


def test_first_start_begins_at_the_finalized_head_without_replaying_history(config, tmp_path):
    store = cw.Store(str(tmp_path / "state.db"))
    chain = FakeChain(head=1829300, blocks={1829210: "block_1829210.json"}, state_at={1829300})
    source = _materios(config, store, chain)
    assert source.poll(1.0)
    assert store.get("cursor:materios-preprod") == "1829300"
    assert not [c for c in chain.calls if c[0] == "chain_getBlock"]
    assert store.findings() == []


def test_the_spec_238_upgrade_authorization_and_apply_page_as_critical(config, tmp_path):
    store = cw.Store(str(tmp_path / "state.db"))
    blocks = {1829210: "block_1829210.json", 1829226: "block_1829226.json", 1829227: "block_1829227.json.gz"}
    chain = FakeChain(head=1829228, blocks=blocks, state_at={1829228})
    source = _materios(config, store, chain)
    source.start_at(1829209)
    _drain(source)

    critical = {f.key: f for f in store.findings() if f.severity == rules.CRITICAL}
    assert set(critical) == {"materios-preprod:1829210:2", "materios-preprod:1829226:2",
                             "materios-preprod:1829227:4"}
    assert "System.authorize_upgrade" in critical["materios-preprod:1829226:2"].text
    assert "multisig account is Sudo.Key" in critical["materios-preprod:1829210:2"].text
    assert "System.apply_authorized_upgrade" in critical["materios-preprod:1829227:4"].text
    assert store.get("cursor:materios-preprod") == "1829228"
    posts = Posts()
    assert cw.Pager(posts).flush(store, now=2.0) == 3
    assert "System.authorize_upgrade" in posts.text()


def test_a_restart_resumes_after_the_last_committed_block_without_repaging(config, tmp_path):
    db = str(tmp_path / "state.db")
    store = cw.Store(db)
    chain = FakeChain(head=1829211, blocks={1829210: "block_1829210.json"}, state_at={1829211})
    source = _materios(config, store, chain)
    source.start_at(1829209)
    _drain(source)
    posts = Posts()
    cw.Pager(posts).flush(store, now=2.0)
    store.close()

    store = cw.Store(db)
    chain = FakeChain(head=1829212, blocks={1829210: "block_1829210.json"}, state_at={1829212})
    _drain(_materios(config, store, chain))
    fetched = [chain.number_of(p[0]) for m, p in chain.calls if m == "chain_getBlock"]
    assert fetched == [1829212]
    cw.Pager(posts).flush(store, now=3.0)
    assert len(posts.payloads) == 1


def test_dispatch_events_are_attached_while_the_block_state_is_available(config, tmp_path):
    # The events are the captured System.Events of a routine block, served at the
    # upgrade block's hash to exercise the fetch and attachment.
    store = cw.Store(str(tmp_path / "state.db"))
    events = json.loads(_read(f"materios/{ROUTINE_BLOCK}"))["events"]
    chain = FakeChain(head=1829226, blocks={1829226: "block_1829226.json"},
                      state_at={1829225, 1829226}, events_hex=events)
    source = _materios(config, store, chain)
    source.start_at(1829225)
    _drain(source)
    [finding] = store.findings()
    assert "result: " in finding.text and "System.ExtrinsicSuccess" in finding.text
    assert "events unavailable" not in finding.text


def test_a_block_whose_state_is_pruned_is_still_classified(config, tmp_path):
    store = cw.Store(str(tmp_path / "state.db"))
    chain = FakeChain(head=1829300, blocks={1829226: "block_1829226.json"}, state_at={1829300})
    source = _materios(config, store, chain)
    source.start_at(1829225)
    source.poll(1.0)
    [finding] = [f for f in store.findings() if f.key == "materios-preprod:1829226:2"]
    assert finding.severity == rules.CRITICAL
    assert "events unavailable" in finding.text


def test_a_runtime_upgrade_block_reloads_metadata_for_the_blocks_after_it(config, tmp_path):
    store = cw.Store(str(tmp_path / "state.db"))
    chain = FakeChain(head=1829228, blocks={1829227: "block_1829227.json.gz"},
                      state_at={1829225, 1829226, 1829227, 1829228})
    source = _materios(config, store, chain)
    source.start_at(1829225)
    _drain(source)
    versions = [p for m, p in chain.calls if m == "state_getRuntimeVersion"]
    assert versions == [[FakeChain.hash_of(1829225)], [FakeChain.hash_of(1829227)]]


def test_a_changed_committee_alerts_and_an_unchanged_one_goes_to_the_digest(config, tmp_path):
    store = cw.Store(str(tmp_path / "state.db"))
    chain = FakeChain(head=1000, blocks={999: "block_2029707.json", 1000: "block_2029707.json"},
                      state_at={1000})
    source = _materios(config, store, chain)
    source.start_at(998)
    _drain(source)
    findings = {f.key: f for f in store.findings()}
    assert "materios-preprod:999:committee" not in findings
    assert findings["materios-preprod:1000:committee"].severity == rules.INFO

    seated = json.loads(store.get("committee:materios-preprod"))
    store.put("committee:materios-preprod", json.dumps(seated[1:]))
    chain.head = 1001
    chain.state_at.add(1001)
    chain.blocks[1001] = "block_2029707.json"
    _drain(source)
    changed = [f for f in store.findings() if f.key == "materios-preprod:1001:committee"][0]
    assert changed.severity == rules.ALERT and "membership changed" in changed.text


def test_a_sudo_key_that_changes_is_critical(config, tmp_path):
    store = cw.Store(str(tmp_path / "state.db"))
    chain = FakeChain(head=1000, blocks={}, state_at={1000, 1001})
    source = _materios(config, store, chain)
    source.poll(1.0)
    assert store.findings() == []
    chain.sudo_key = "0x" + rules.account_bytes("5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY").hex()
    chain.head = 1001
    source.poll(2.0)
    [finding] = store.findings()
    assert finding.severity == rules.CRITICAL
    assert "Sudo.Key changed" in finding.text and SUDO_KEY in finding.text


def test_a_dropped_connection_is_reopened_before_polling(config, tmp_path):
    store = cw.Store(str(tmp_path / "state.db"))
    chain = FakeChain(head=1000, blocks={}, state_at={1000})
    chain.connected = False
    _materios(config, store, chain).poll(1.0)
    assert chain.connects == 1


# --- Cardano ----------------------------------------------------------------------------


def _tx(name: str) -> dict:
    return json.loads((FIX / "cardano" / f"{name}.json").read_text())


class FakeBlockfrost:
    """Serves Blockfrost routes from captured transactions."""

    def __init__(self, tip: int, time: int):
        self.routes = {"/blocks/latest": {"height": tip, "time": time}}
        self.calls = []

    def add_tx(self, name: str, address: str | None = None, height: int | None = None):
        doc = copy.deepcopy(_tx(name))
        tx_hash = doc["tx"]["hash"]
        if height is not None:
            doc["tx"]["block_height"] = height
        redeemers = []
        for r in doc["redeemers"]:
            self.routes[f"/scripts/datum/{r['redeemer_data_hash']}"] = {"json_value": r.pop("json_value")}
            redeemers.append(r)
        self.routes[f"/txs/{tx_hash}"] = doc["tx"]
        self.routes[f"/txs/{tx_hash}/utxos"] = doc["utxos"]
        self.routes[f"/txs/{tx_hash}/redeemers"] = redeemers
        if address is not None:
            listing = self.routes.setdefault(f"/addresses/{address}/transactions", [])
            listing.append({"tx_hash": tx_hash, "tx_index": 0, "block_height": doc["tx"]["block_height"],
                            "block_time": doc["tx"]["block_time"]})
        return tx_hash

    def get(self, path, **params):
        self.calls.append((path, params))
        value = self.routes.get(path)
        if path.endswith("/transactions") and value is not None:
            start = int(params.get("from", 0))
            value = [row for row in value if row["block_height"] >= start]
        return copy.deepcopy(value)


def _mainnet(config):
    return next(n for n in config.cardano if n.name == "cardano-mainnet")


def test_the_first_poll_baselines_addresses_and_policies_without_alerting(config, tmp_path):
    store = cw.Store(str(tmp_path / "state.db"))
    network = _mainnet(config)
    api = FakeBlockfrost(tip=13_500_000, time=int(_at("2026-09-27T01:00:00")))
    api.add_tx("surrender_agent", address=network.pool.address)
    policy = network.policies[0].policy_id
    api.routes[f"/assets/policy/{policy}"] = [{"asset": network.pool.cmatra_unit, "quantity": "1"}]
    api.routes[f"/assets/{network.pool.cmatra_unit}"] = {"mint_or_burn_count": 1}
    source = cw.CardanoSource(network, api, store, stale_seconds=900)
    assert source.poll(_at("2026-09-27T01:00:30"))
    assert store.findings() == []
    assert store.get(f"cursor:cardano-mainnet:address:{network.pool.address}") == "13500000"
    assert store.get(f"cursor:cardano-mainnet:asset:{network.pool.cmatra_unit}") == "1"


def _baselined(config, tmp_path, tip=13_500_000):
    store = cw.Store(str(tmp_path / "state.db"))
    network = _mainnet(config)
    api = FakeBlockfrost(tip=tip, time=int(_at("2026-09-27T01:00:00")))
    source = cw.CardanoSource(network, api, store, stale_seconds=900)
    source.start_at(tip)
    return store, network, api, source


def test_a_new_surrender_is_classified_once_though_the_reorg_window_rescans_it(config, tmp_path):
    store, network, api, source = _baselined(config, tmp_path)
    tx_hash = api.add_tx("surrender_agent", address=network.pool.address, height=13_500_010)
    api.routes["/blocks/latest"]["height"] = 13_500_012
    source.poll(_at("2026-09-27T01:01:00"))
    source.poll(_at("2026-09-27T01:02:00"))
    [finding] = store.findings()
    assert finding.key == f"cardano-mainnet:{tx_hash}"
    assert finding.severity == rules.INFO and finding.kind == "surrender"
    assert finding.amount == 1056778496
    assert [c for c in api.calls if c[0] == f"/txs/{tx_hash}"] == [(f"/txs/{tx_hash}", {})]
    rescans = [c[1]["from"] for c in api.calls if c[0] == f"/addresses/{network.pool.address}/transactions"]
    assert rescans == ["13499970", "13499982"]


def test_a_custody_wallet_outflow_pages_critical(config, tmp_path):
    store, network, api, source = _baselined(config, tmp_path)
    custody = next(a for a in network.addresses if a.label == "custody-a")
    api.add_tx("custody_outflow", address=custody.address, height=13_500_001)
    source.poll(_at("2026-09-27T01:01:00"))
    [finding] = store.findings()
    assert finding.severity == rules.CRITICAL and "outflow from custody-a" in finding.text
    posts = Posts()
    cw.Pager(posts).flush(store, now=1.0)
    assert "@here" in posts.text()


def test_a_mint_under_a_watched_policy_pages_critical(config, tmp_path):
    store = cw.Store(str(tmp_path / "state.db"))
    network = _mainnet(config)
    api = FakeBlockfrost(tip=13_500_000, time=int(_at("2026-09-27T01:00:00")))
    source = cw.CardanoSource(network, api, store, stale_seconds=900)
    unit = network.pool.cmatra_unit
    policy = network.policies[0].policy_id
    api.routes[f"/assets/policy/{policy}"] = [{"asset": unit, "quantity": "1"}]
    api.routes[f"/assets/{unit}"] = {"mint_or_burn_count": 1}
    source.poll(_at("2026-09-27T01:01:00"))
    assert store.findings() == []

    mint_hash = api.add_tx("mint_v2")
    api.routes[f"/assets/{unit}"] = {"mint_or_burn_count": 2}
    api.routes[f"/assets/{unit}/history"] = [{"tx_hash": "0" * 64, "action": "minted", "amount": "1"},
                                             {"tx_hash": mint_hash, "action": "minted", "amount": "1"}]
    source.poll(_at("2026-09-27T01:02:00"))
    [finding] = store.findings()
    assert finding.severity == rules.CRITICAL and "minted" in finding.text
    assert store.get(f"cursor:cardano-mainnet:asset:{unit}") == "2"


def test_a_new_asset_under_a_watched_policy_is_read_from_its_first_mint(config, tmp_path):
    store, network, api, source = _baselined(config, tmp_path)
    policy = network.policies[0].policy_id
    api.routes[f"/assets/policy/{policy}"] = []
    source.poll(_at("2026-09-27T01:01:00"))
    unit = network.pool.cmatra_unit
    mint_hash = api.add_tx("mint_v2")
    api.routes[f"/assets/policy/{policy}"] = [{"asset": unit, "quantity": "1"}]
    api.routes[f"/assets/{unit}"] = {"mint_or_burn_count": 1}
    api.routes[f"/assets/{unit}/history"] = [{"tx_hash": mint_hash, "action": "minted", "amount": "1"}]
    source.poll(_at("2026-09-27T01:02:00"))
    [finding] = store.findings()
    assert finding.severity == rules.CRITICAL


def test_an_address_blockfrost_has_never_seen_has_no_transactions(config, tmp_path):
    store, network, api, source = _baselined(config, tmp_path)
    assert source.poll(_at("2026-09-27T01:01:00"))
    assert store.findings() == []


def test_a_cardano_tip_older_than_the_stale_window_is_a_source_failure(config, tmp_path):
    store, network, api, source = _baselined(config, tmp_path)
    with pytest.raises(cw.SourceError, match="tip"):
        source.poll(_at("2026-09-27T01:30:00"))


class _Blockfrost(http.server.BaseHTTPRequestHandler):
    status = 200
    body = b"{}"
    seen = []

    def do_GET(self):
        type(self).seen.append((self.path, self.headers.get("project_id"), self.headers.get("User-Agent")))
        self.send_response(type(self).status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(type(self).body)

    def log_message(self, *args):
        pass


@pytest.fixture
def blockfrost(tmp_path):
    _Blockfrost.seen = []
    server = http.server.HTTPServer(("127.0.0.1", 0), _Blockfrost)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    key = tmp_path / "bf.key"
    key.write_text("mainnetSECRETKEY\n")
    yield cw.Blockfrost(f"http://127.0.0.1:{server.server_port}/api/v0", str(key), min_interval=0)
    server.shutdown()


def test_blockfrost_sends_the_project_key_and_a_named_agent(blockfrost):
    _Blockfrost.status, _Blockfrost.body = 200, b'{"height": 5}'
    assert blockfrost.get("/blocks/latest") == {"height": 5}
    path, key, agent = _Blockfrost.seen[0]
    assert path == "/api/v0/blocks/latest"
    assert key == "mainnetSECRETKEY"
    assert agent and not agent.startswith("Python-urllib")


def test_blockfrost_not_found_is_none_and_errors_never_carry_the_key(blockfrost):
    _Blockfrost.status, _Blockfrost.body = 404, b'{"status_code": 404}'
    assert blockfrost.get("/addresses/addr1x/transactions", order="asc") is None
    _Blockfrost.status = 429
    with pytest.raises(cw.SourceError) as err:
        blockfrost.get("/blocks/latest")
    assert "429" in str(err.value) and "SECRETKEY" not in str(err.value)


# --- backtest, CLI and systemd --------------------------------------------------------


def test_a_backtest_reports_every_finding_in_its_window_and_pages_nothing(config, tmp_path):
    store = cw.Store(str(tmp_path / "state.db"))
    blocks = {1829210: "block_1829210.json", 1829226: "block_1829226.json"}
    chain = FakeChain(head=1829226, blocks=blocks, state_at={1829226})
    network = _mainnet(config)
    api = FakeBlockfrost(tip=13_500_100, time=int(_at("2026-09-27T01:00:00")))
    api.add_tx("surrender_agent", address=network.pool.address, height=13_500_050)
    sources = [cw.MateriosSource(config.materios, chain, store),
               cw.CardanoSource(network, api, store, stale_seconds=10 ** 9)]
    sources[0].start_at(1829209)
    sources[1].start_at(13_500_000)
    report = cw.backtest(sources, store, now=_at("2026-09-27T01:00:30"))
    keys = [f.key for f in report]
    assert "materios-preprod:1829210:2" in keys and "materios-preprod:1829226:2" in keys
    surrender = next(f for f in report if f.kind == "surrender")
    assert surrender.severity == rules.INFO
    assert all(f.sent_at is None for f in report)


def test_run_refuses_to_start_without_a_webhook(tmp_path, monkeypatch, capsys):
    monkeypatch.delenv("DISCORD_WEBHOOK_URL", raising=False)
    path = tmp_path / "config.json"
    path.write_text((FIX / "config.json").read_text())
    assert cw.main(["--config", str(path), "run"]) == 2
    assert "DISCORD_WEBHOOK_URL" in capsys.readouterr().err


def test_the_systemd_watchdog_is_pinged_through_the_notify_socket(tmp_path, monkeypatch):
    path = str(tmp_path / "notify")
    receiver = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    receiver.bind(path)
    monkeypatch.setenv("NOTIFY_SOCKET", path)
    cw.sd_notify("WATCHDOG=1")
    assert receiver.recv(64) == b"WATCHDOG=1"
    monkeypatch.delenv("NOTIFY_SOCKET")
    cw.sd_notify("WATCHDOG=1")


def test_a_relative_key_file_resolves_against_the_config_directory(tmp_path):
    doc = json.loads((FIX / "config.json").read_text())
    doc["cardano"][0]["project_id_file"] = "blockfrost-mainnet.key"
    config = rules.parse_config(doc, base_dir=tmp_path)
    assert config.cardano[0].project_id_file == str(tmp_path / "blockfrost-mainnet.key")
    assert config.cardano[1].project_id_file == "/nonexistent/blockfrost.key"
