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

    def __init__(self, fail=(), error=None):
        self.payloads = []
        self.attempts = 0
        self.fail = set(fail)
        self.error = error or discord.DiscordError("webhook unreachable: URLError")

    def __call__(self, payload):
        self.attempts += 1
        if self.attempts in self.fail:
            raise self.error
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


def _headlines(posts):
    return [p["content"].split("\n")[1] for p in posts.payloads]


def test_pages_go_out_most_severe_first_and_ungrouped_before_grouped(tmp_path):
    store = cw.Store(str(tmp_path / "state.db"))
    store.add(_finding("alert", severity=rules.ALERT), now=1.0)
    store.add(rules.Finding(rules.CRITICAL, "flood", "headline flood", group="preprod signer X"), now=2.0)
    store.add(_finding("custody"), now=3.0)
    posts = Posts()
    assert cw.Pager(posts).flush(store, now=4.0) == 3
    assert _headlines(posts) == ["**headline custody**", "**headline flood**", "**headline alert**"]


def test_pending_findings_of_one_group_go_out_as_one_page(tmp_path):
    store = cw.Store(str(tmp_path / "state.db"))
    for i in range(40):
        store.add(rules.Finding(rules.ALERT, f"flood-{i}", f"flood {i}", group="preprod ReserveValidator"), now=1.0)
    store.add(rules.Finding(rules.CRITICAL, "flood-top", "flood top", group="preprod ReserveValidator"), now=2.0)
    store.add(_finding("custody"), now=3.0)
    posts = Posts()
    assert cw.Pager(posts).flush(store, now=4.0) == 42
    custody, aggregate = posts.payloads
    assert aggregate["content"].startswith("\U0001f6a8 **CRITICAL** @here")
    assert "**41 findings from preprod ReserveValidator**" in aggregate["content"]
    assert aggregate["content"].index("flood top") < aggregate["content"].index("flood 0")
    assert len(aggregate["content"]) <= 2000
    assert "headline custody" in custody["content"]
    assert cw.Pager(posts).flush(store, now=5.0) == 0


def test_a_rate_limited_webhook_is_left_alone_until_its_retry_after(tmp_path):
    store = cw.Store(str(tmp_path / "state.db"))
    store.add(_finding("a"), now=1.0)
    posts = Posts(fail={1}, error=discord.DiscordError("webhook answered HTTP 429", status=429, retry_after=30))
    pager = cw.Pager(posts)
    assert pager.flush(store, now=10.0) == 0
    assert pager.flush(store, now=39.0) == 0
    assert posts.attempts == 1
    assert pager.flush(store, now=40.0) == 1


def test_a_page_the_webhook_refuses_does_not_hold_back_the_rest_and_goes_out_as_its_headline(tmp_path):
    store = cw.Store(str(tmp_path / "state.db"))
    store.add(_finding("refused", details=("x",)), now=1.0)
    store.add(_finding("next", severity=rules.ALERT), now=2.0)
    posts = Posts(fail={1, 3, 4}, error=discord.DiscordError("webhook answered HTTP 400", status=400))
    pager = cw.Pager(posts)
    assert pager.flush(store, now=3.0) == 1
    assert _headlines(posts) == ["**headline next**"]
    pager.flush(store, now=4.0)
    pager.flush(store, now=5.0)
    assert pager.flush(store, now=6.0) == 1
    fallback = posts.payloads[-1]["content"]
    assert "headline refused" in fallback and "rejected" in fallback and "```" not in fallback
    assert store.unsent_pages() == []


def test_the_digest_counts_pages_still_waiting_for_delivery(config, tmp_path):
    store = cw.Store(str(tmp_path / "state.db"))
    posts = Posts(fail={1}, error=discord.DiscordError("webhook answered HTTP 429", status=429, retry_after=600))
    clock = [_at("2026-09-27T13:00:00")]
    store.add(_finding("waiting"), now=clock[0])
    _watch(config, store, [StubSource("cardano-mainnet")], posts, clock).cycle()
    digest = next(p["content"] for p in posts.payloads if "daily digest" in p["content"])
    assert "pages waiting for delivery: 1" in digest


def test_routine_findings_wait_for_the_digest(tmp_path):
    store = cw.Store(str(tmp_path / "state.db"))
    store.add(_finding("s", severity=rules.INFO, kind="surrender", amount=5), now=1.0)
    posts = Posts()
    assert cw.Pager(posts).flush(store, now=2.0) == 0
    assert posts.payloads == []


def test_a_critical_page_mentions_here_and_fits_one_discord_message():
    critical = cw.page_message([_finding(details=tuple("x" * 300 for _ in range(40)))])
    alert = cw.page_message([_finding(severity=rules.ALERT)])
    assert critical["content"].startswith("\U0001f6a8 **CRITICAL** @here")
    assert len(critical["content"]) <= 2000
    assert "@here" not in alert["content"]
    assert alert["allowed_mentions"] == {"parse": []}
    assert critical["allowed_mentions"] == {"parse": ["everyone"]}


@pytest.mark.parametrize("fence", ["````", "`````", "```"])
def test_text_from_the_chain_cannot_close_the_code_block(fence):
    payload = f"{fence}\n**RESOLVED: scheduled rehearsal, no action**\n{fence}"
    call = {"call_module": "Sudo", "call_function": "sudo", "call_args": [{"name": "call", "value": {
        "call_module": "System", "call_function": "remark", "call_args": [{"name": "remark", "value": payload}]}}]}
    [finding] = rules.classify_materios_block("materios-preprod", 9, [{"call": call}], None, None)
    content = cw.page_message([finding])["content"]
    assert content.count("`") == 6
    fenced = content.split("```")[1]
    assert "RESOLVED" in fenced and "\n**RESOLVED" not in content.replace(fenced, "")


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


def test_a_source_that_stays_stale_is_paged_critical_every_hour(config, tmp_path):
    store = cw.Store(str(tmp_path / "state.db"))
    posts = Posts()
    clock = [_at("2026-09-27T01:00:00")]
    watch = _watch(config, store, [StubSource("materios-preprod", fail=True)], posts, clock)
    watch.cycle()
    clock[0] += config.source_stale_seconds + 60
    watch.cycle()
    for _ in range(5):
        clock[0] += 600
        watch.cycle()
    assert len(posts.payloads) == 1
    clock[0] += 3600
    watch.cycle()
    assert len(posts.payloads) == 2
    assert all(p["content"].startswith("\U0001f6a8 **CRITICAL** @here") for p in posts.payloads)
    assert "materios-preprod stale for 12" in posts.payloads[1]["content"]


def test_the_digest_names_a_stale_source_instead_of_calling_the_watcher_alive(config, tmp_path):
    store = cw.Store(str(tmp_path / "state.db"))
    posts = Posts()
    clock = [_at("2026-09-27T12:00:00")]
    watch = _watch(config, store, [StubSource("materios-preprod", fail=True), StubSource("cardano-mainnet")],
                   posts, clock)
    watch.cycle()
    clock[0] = _at("2026-09-27T13:00:00")
    watch.cycle()
    digest = next(p["content"] for p in posts.payloads if "daily digest" in p["content"])
    assert "alive" not in digest
    assert "STALE: materios-preprod" in digest


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


class FindingSource(StubSource):
    """Stores a CRITICAL finding when polled, and records how many pages were out by then."""

    def __init__(self, name, store, posts):
        super().__init__(name)
        self.store, self.posts, self.pages_before = store, posts, None

    def poll(self, now):
        self.pages_before = len(self.posts.payloads)
        self.store.add(_finding(f"{self.name}:found"), now)
        return super().poll(now)


def test_pages_go_out_and_the_watchdog_is_pinged_after_each_source(config, tmp_path):
    store = cw.Store(str(tmp_path / "state.db"))
    posts = Posts()
    notified = []
    first, second = FindingSource("cardano-mainnet", store, posts), FindingSource("materios-preprod", store, posts)
    clock = [_at("2026-09-27T01:00:00")]
    cw.Watch(config, store, [first, second], posts, clock=lambda: clock[0], notify=notified.append).cycle()
    assert second.pages_before == 1
    assert notified.count("WATCHDOG=1") >= 2


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

    def __init__(self, head: int, blocks: dict[int, str], state_at=None, events_hex=None, extra=None):
        self.head = head
        self.blocks = blocks
        self.extra = extra or {}
        self.state_at = set(state_at or ())
        self.events_hex = events_hex
        self.sudo_key = "0x" + rules.account_bytes(SUDO_KEY).hex()
        self.metadata = _read("materios/metadata_spec238.hex.gz").decode()
        self.headers = json.loads(_read("materios/headers_spec238_upgrade.json"))
        self.calls = []
        self.connected = True
        self.connects = 0
        self.genesis = None

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
        extrinsics = json.loads(_read(f"materios/{name}"))["extrinsics"] + self.extra.get(number, [])
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
        if method == "chain_getBlockHash" and params[0] == 0 and self.genesis:
            return self.genesis
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


def test_events_that_cannot_be_decoded_do_not_hold_the_block_back(config, tmp_path):
    store = cw.Store(str(tmp_path / "state.db"))
    chain = FakeChain(head=1829226, blocks={1829226: "block_1829226.json"},
                      state_at={1829225, 1829226}, events_hex="0x04ff")
    source = _materios(config, store, chain)
    source.start_at(1829225)
    assert source.poll(1.0)
    [finding] = store.findings()
    assert finding.severity == rules.CRITICAL and "events unavailable" in finding.text
    assert store.get("cursor:materios-preprod") == "1829226"


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


def test_a_chain_reset_is_critical_and_watching_restarts_at_the_new_head(config, tmp_path):
    store = cw.Store(str(tmp_path / "state.db"))
    chain = FakeChain(head=5000, blocks={}, state_at={5000, 120})
    source = _materios(config, store, chain)
    source.poll(1.0)
    assert store.findings() == []

    chain.genesis = "0x" + "ab" * 32
    chain.head = 120
    chain.sudo_key = "0x" + rules.account_bytes("5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY").hex()
    assert source.poll(2.0)
    [finding] = store.findings()
    assert finding.severity == rules.CRITICAL
    assert "genesis changed" in finding.text and chain.genesis in finding.text
    assert store.get("cursor:materios-preprod") == "120"


def test_a_reset_chain_is_decoded_with_its_own_metadata_though_its_spec_version_repeats(config, tmp_path):
    store = cw.Store(str(tmp_path / "state.db"))
    chain = FakeChain(head=1000, blocks={}, state_at={999, 1000, 120, 121})
    source = _materios(config, store, chain)
    source.start_at(999)
    _drain(source)
    chain.genesis = "0x" + "ab" * 32
    chain.head = 120
    source.poll(2.0)
    chain.head = 121
    _drain(source, now=3.0)
    assert [m for m, _ in chain.calls].count("state_getMetadata") == 2
    assert not [k for k in store._db.execute("SELECT name FROM state").fetchall()
                if k[0].startswith(f"metadata:materios-preprod:{FakeChain.hash_of(0)}")]


def test_a_reset_chain_reaching_an_old_finding_height_is_still_paged(config, tmp_path):
    store = cw.Store(str(tmp_path / "state.db"))
    chain = FakeChain(head=1829210, blocks={1829210: "block_1829210.json"}, state_at={1829210})
    source = _materios(config, store, chain)
    source.start_at(1829209)
    _drain(source)
    cw.Pager(Posts()).flush(store, 1.0)

    chain.genesis = "0x" + "ab" * 32
    chain.head = 1829209
    source.poll(2.0)
    chain.head = 1829210
    _drain(source, now=3.0)
    posts = Posts()
    cw.Pager(posts).flush(store, 3.0)
    assert "genesis changed" in posts.text()
    assert "materios-preprod #1829210 extrinsic 2: Multisig.as_multi" in posts.text()


def _hostile_remark(chain) -> str:
    """A remark whose text reads as hex until its last character, encoded as a block carries it."""
    decoder = rules.RuntimeDecoder(chain.metadata)
    ext = decoder._config.create_scale_object("Extrinsic", metadata=decoder._metadata)
    text = "0x" + "a" * 199 + "z"
    return ext.encode({"call_module": "System", "call_function": "remark",
                       "call_args": {"remark": "0x" + text.encode().hex()}}).to_hex()


def test_a_hostile_argument_does_not_stall_the_privileged_calls_after_it(config, tmp_path):
    store = cw.Store(str(tmp_path / "state.db"))
    chain = FakeChain(head=1000, blocks={1000: "block_1587358.json"}, state_at={1000})
    chain.extra[999] = [_hostile_remark(chain)]
    source = _materios(config, store, chain)
    source.start_at(998)
    _drain(source)
    assert store.get("cursor:materios-preprod") == "1000"
    [finding] = [f for f in store.findings() if f.severity == rules.CRITICAL]
    assert "Sudo.set_key" in finding.text


def test_a_committee_inherent_that_cannot_be_read_pages_and_the_cursor_moves_on(config, tmp_path, monkeypatch):
    def unreadable(extrinsics):
        raise KeyError("validators")

    monkeypatch.setattr(rules, "committee_of", unreadable)
    store = cw.Store(str(tmp_path / "state.db"))
    chain = FakeChain(head=1000, blocks={1000: "block_2029707.json"}, state_at={1000})
    source = _materios(config, store, chain)
    source.start_at(999)
    _drain(source)
    assert store.get("cursor:materios-preprod") == "1000"
    [finding] = store.findings()
    assert finding.severity == rules.CRITICAL
    assert "committee inherent could not be classified" in finding.text and "KeyError" in finding.text


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

    def add_tx(self, name: str, address: str | None = None, height: int | None = None,
               tx_hash: str | None = None):
        doc = copy.deepcopy(_tx(name))
        if tx_hash is not None:
            doc["tx"]["hash"] = tx_hash
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
        if isinstance(value, list) and "page" in params:
            first = (params["page"] - 1) * params["count"]
            value = value[first:first + params["count"]]
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
    api.routes[f"/assets/policy/{policy}"] = [{"asset": unit, "quantity": "2"}]
    api.routes[f"/assets/{unit}"] = {"mint_or_burn_count": 2}
    api.routes[f"/assets/{unit}/history"] = [{"tx_hash": "0" * 64, "action": "minted", "amount": "1"},
                                             {"tx_hash": mint_hash, "action": "minted", "amount": "1"}]
    source.poll(_at("2026-09-27T01:02:00"))
    [finding] = store.findings()
    assert finding.severity == rules.CRITICAL and "minted" in finding.text
    assert store.get(f"cursor:cardano-mainnet:asset:{unit}") == "2"


def _large_policy_watch(config, tmp_path):
    """A watched policy with more assets than are worth reading one by one every poll."""
    store = cw.Store(str(tmp_path / "state.db"))
    network = _mainnet(config)
    api = FakeBlockfrost(tip=13_500_000, time=int(_at("2026-09-27T01:00:00")))
    source = cw.CardanoSource(network, api, store, stale_seconds=900)
    policy = network.policies[0].policy_id
    units = [network.pool.cmatra_unit] + [policy + f"{i:04x}" for i in range(cw.EXACT_POLICY_ASSETS)]
    api.routes[f"/assets/policy/{policy}"] = [{"asset": u, "quantity": "5"} for u in units]
    for u in units:
        api.routes[f"/assets/{u}"] = {"mint_or_burn_count": 1}
    source.poll(_at("2026-09-27T01:00:30"))
    return store, api, source, units[0]


def test_a_large_policy_whose_supply_is_unchanged_costs_only_its_listing(config, tmp_path):
    store, api, source, unit = _large_policy_watch(config, tmp_path)
    api.calls.clear()
    source.poll(_at("2026-09-27T01:01:30"))
    assert [c for c in api.calls if c[0].startswith("/assets/") and "/policy/" not in c[0]] == []


def test_a_mint_and_burn_that_cancel_out_under_a_large_policy_are_found_at_the_hourly_reconcile(config, tmp_path):
    store, api, source, unit = _large_policy_watch(config, tmp_path)
    mint_hash = api.add_tx("mint_v2", height=13_500_001)
    api.routes[f"/assets/{unit}"] = {"mint_or_burn_count": 2}
    api.routes[f"/assets/{unit}/history"] = [{"tx_hash": "0" * 64, "action": "minted", "amount": "5"},
                                             {"tx_hash": mint_hash, "action": "minted", "amount": "1"}]
    source.poll(_at("2026-09-27T01:01:30"))
    assert store.findings() == []
    api.routes["/blocks/latest"]["time"] = int(_at("2026-09-27T02:00:40"))
    source.poll(_at("2026-09-27T02:00:40"))
    [finding] = store.findings()
    assert finding.severity == rules.CRITICAL and "minted" in finding.text


def test_a_small_policy_has_every_mint_count_read_each_poll(config, tmp_path):
    store = cw.Store(str(tmp_path / "state.db"))
    network = _mainnet(config)
    api = FakeBlockfrost(tip=13_500_000, time=int(_at("2026-09-27T01:00:00")))
    source = cw.CardanoSource(network, api, store, stale_seconds=900)
    unit, policy = network.pool.cmatra_unit, network.policies[0].policy_id
    api.routes[f"/assets/policy/{policy}"] = [{"asset": unit, "quantity": "5"}]
    api.routes[f"/assets/{unit}"] = {"mint_or_burn_count": 1}
    source.poll(_at("2026-09-27T01:00:30"))
    mint_hash = api.add_tx("mint_v2", height=13_500_001)
    api.routes[f"/assets/{unit}"] = {"mint_or_burn_count": 2}
    api.routes[f"/assets/{unit}/history"] = [{"tx_hash": "0" * 64, "action": "minted", "amount": "5"},
                                             {"tx_hash": mint_hash, "action": "minted", "amount": "1"}]
    source.poll(_at("2026-09-27T01:01:30"))
    [finding] = store.findings()
    assert finding.severity == rules.CRITICAL


def test_surrenders_beyond_the_rate_table_supply_are_paged_once_per_new_count(config, tmp_path):
    store, network, api, source = _baselined(config, tmp_path)
    t2 = next(r for r in network.pool.redemptions if r.key == "T2_ADAM_PASS")
    held = [{"unit": t2.policy_id + name, "quantity": "1"} for name in sorted(t2.asset_names)[:96]]
    api.routes[f"/addresses/{network.pool.quarantine_address}"] = {"amount": held}
    source.poll(_at("2026-09-27T01:01:00"))
    source.poll(_at("2026-09-27T01:02:00"))
    [finding] = store.findings()
    assert finding.severity == rules.ALERT and "96 T2_ADAM_PASS surrendered" in finding.text


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


def test_a_backtest_reports_only_the_mints_inside_its_window(config, tmp_path):
    store, network, api, source = _baselined(config, tmp_path)
    unit = network.pool.cmatra_unit
    policy = network.policies[0].policy_id
    before = api.add_tx("mint_v2")
    inside = api.add_tx("mint_v2", height=13_500_005, tx_hash="ab" * 32)
    api.routes[f"/assets/policy/{policy}"] = [{"asset": unit, "quantity": "2"}]
    api.routes[f"/assets/{unit}"] = {"mint_or_burn_count": 2}
    api.routes[f"/assets/{unit}/history"] = [{"tx_hash": before, "action": "minted", "amount": "1"},
                                             {"tx_hash": inside, "action": "minted", "amount": "1"}]
    api.routes["/blocks/latest"]["height"] = 13_500_010
    report = cw.backtest([source], store, now=_at("2026-09-27T01:01:00"))
    assert [f.key for f in report] == [f"cardano-mainnet:{inside}"]
    assert report[0].severity == rules.CRITICAL


def test_a_flood_of_transactions_is_classified_in_bounded_polls(config, tmp_path):
    store, network, api, source = _baselined(config, tmp_path)
    for i in range(cw.MAX_TX_PER_POLL + 50):
        api.add_tx("surrender_agent", address=network.pool.address, height=13_500_001, tx_hash=f"{i:064x}")
    api.routes["/blocks/latest"]["height"] = 13_500_002
    assert not source.poll(_at("2026-09-27T01:01:00"))
    assert len(store.findings()) == cw.MAX_TX_PER_POLL
    assert store.get(f"cursor:cardano-mainnet:address:{network.pool.address}") == "13500000"
    assert source.poll(_at("2026-09-27T01:01:10"))
    assert len(store.findings()) == cw.MAX_TX_PER_POLL + 50
    assert store.get(f"cursor:cardano-mainnet:address:{network.pool.address}") == "13500002"


def test_a_transaction_the_classifier_cannot_read_pages_critical_once(config, tmp_path, monkeypatch):
    def unreadable(network, tx, utxos, redeemers):
        raise TypeError("unhashable type: 'list'")

    monkeypatch.setattr(rules, "classify_cardano_tx", unreadable)
    store, network, api, source = _baselined(config, tmp_path)
    tx_hash = api.add_tx("surrender_agent", address=network.pool.address, height=13_500_010)
    assert source.poll(_at("2026-09-27T01:01:00"))
    source.poll(_at("2026-09-27T01:02:00"))
    [finding] = store.findings()
    assert finding.severity == rules.CRITICAL
    assert finding.key == f"cardano-mainnet:{tx_hash}"
    assert "could not be classified" in finding.text and "TypeError" in finding.text


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
    assert cw.main(["run", "--config", str(path)]) == 2
    assert "DISCORD_WEBHOOK_URL" in capsys.readouterr().err


def test_a_failed_unit_is_paged_without_reading_the_config_that_may_have_failed_it(monkeypatch):
    posted = []
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "https://discord.test/api/webhooks/1/token")
    monkeypatch.setattr(cw.discord, "post_json", lambda url, payload: posted.append(payload))
    assert cw.main(["page-failure", "--unit", "custody-watch.service"]) == 0
    assert "custody-watch.service failed" in posted[0]["content"]
    assert "NOT being watched" in posted[0]["content"]
    assert posted[0]["allowed_mentions"] == {"parse": ["everyone"]}


def test_a_test_page_needs_only_the_webhook(monkeypatch):
    posted = []
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "https://discord.test/api/webhooks/1/token")
    monkeypatch.setattr(cw.discord, "post_json", lambda url, payload: posted.append(payload))
    assert cw.main(["test-page"]) == 0
    assert "custody-watch test page" in posted[0]["content"]
    assert posted[0]["allowed_mentions"] == {"parse": []}


def test_a_page_the_webhook_rejects_exits_nonzero(monkeypatch, capsys):
    def reject(url, payload):
        raise discord.DiscordError("webhook answered HTTP 403")
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "https://discord.test/api/webhooks/1/token")
    monkeypatch.setattr(cw.discord, "post_json", reject)
    assert cw.main(["page-failure", "--unit", "custody-watch.service"]) == 1
    assert "HTTP 403" in capsys.readouterr().err


def test_watching_and_replaying_require_a_config():
    for command in (["run"], ["backtest", "--state", "x.db"]):
        with pytest.raises(SystemExit) as exit_:
            cw.main(command)
        assert exit_.value.code == 2


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
