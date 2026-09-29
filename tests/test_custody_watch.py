"""The custody watcher's runtime: cursors, dedupe, paging, the daily digest,
source health, and both sources driven by real chain history.

Materios is served from the captured preprod blocks and runtime metadata;
Cardano from the captured Blockfrost transactions (see test_custody_rules).
"""

import copy
import dataclasses
import functools
import gzip
import http.server
import json
import socket
import threading
from datetime import datetime, timezone
from pathlib import Path

import pytest
from substrateinterface.exceptions import SubstrateRequestException
from substrateinterface.utils.hasher import blake2_128_concat, two_x64_concat, xxh128

from daemon import custody_rules as rules
from daemon import custody_watch as cw
from daemon import discord
from tests.test_custody_rules import (LEG_SIGNER, NORMAL_BLOCK_LENGTH, STRANGER, TODAYS_POOL, _count_walks,
                                      coverage_doc, filler_call, fillers_past_the_budget, nested_sudo_leg,
                                      signed_extrinsic)

FIX = Path(__file__).parent / "fixtures" / "custody"
SUDO_KEY = "5H2M5Dbt8hSfSCXS6hfEBPR1N21yh679finzcfMEwD62i7iP"
ALICE = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"
ROUTINE_BLOCK = "block_2034370_with_events.json"


def _read(name: str) -> bytes:
    raw = (FIX / name).read_bytes()
    return gzip.decompress(raw) if name.endswith(".gz") else raw


def _at(text: str) -> float:
    return datetime.fromisoformat(text).replace(tzinfo=timezone.utc).timestamp()


@pytest.fixture
def config():
    return rules.parse_config(json.loads((FIX / "config.json").read_text()))


def _finding(key="k", severity=rules.CRITICAL, kind="event", amount=0, details=("d",), authority=False):
    return rules.Finding(severity=severity, key=key, headline=f"headline {key}", details=details,
                         kind=kind, amount=amount, authority=authority)


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
    assert pager.flush(store, now=2.0 + 2 * cw.PAGE_INTERVAL) == 2
    assert pager.flush(store, now=2.0 + 4 * cw.PAGE_INTERVAL) == 0
    assert [p["content"].split("headline ")[1][0] for p in posts.payloads] == ["a", "b", "c"]


def _headlines(posts):
    return [p["content"].split("\n")[1] for p in posts.payloads]


def test_pages_that_go_alone_go_before_any_group_whatever_its_severity(tmp_path):
    # Only what an account anyone can be causes is grouped, so however severe a group is,
    # it waits behind every page that goes alone.
    store = cw.Store(str(tmp_path / "state.db"))
    store.add(_finding("alert", severity=rules.ALERT), now=1.0)
    store.add(rules.Finding(rules.CRITICAL, "flood", "headline flood", group="preprod signer X"), now=2.0)
    store.add(_finding("stale"), now=3.0)
    posts = Posts()
    pager = cw.Pager(posts)
    for second in range(0, 60, 6):
        pager.flush(store, now=4.0 + second)
    assert _headlines(posts) == ["**headline stale**", "**headline alert**", "**headline flood**"]


def _outsider_groups(store, second: int, now: float, outsiders: int) -> None:
    """One grouped CRITICAL from each of ``outsiders`` accounts, as any funded account can
    cause in every block."""
    for g in range(outsiders):
        store.add(rules.Finding(rules.CRITICAL, f"materios-preprod:{second}:{g}", f"outsider {g} #{second}",
                                group=f"materios-preprod signer {g}"), now)


@pytest.mark.parametrize("outsiders", [1, 2, 3, 5])
def test_grouped_pages_outsiders_cause_every_block_never_hold_back_a_page_that_goes_alone(tmp_path, outsiders):
    store = cw.Store(str(tmp_path / "state.db"))
    clock = [_at("2026-09-28T02:00:00")]
    pager = cw.Pager(DiscordModel(clock))
    for second in range(600):
        if second % 6 == 0:
            _outsider_groups(store, second, clock[0], outsiders)
        if second % 60 == 0 and second:
            store.add(rules.Finding(rules.ALERT, f"cardano-mainnet:coverage:{second}",
                                    "cardano-mainnet surrender pool covers 87.13%"), clock[0])
        pager.flush(store, clock[0])
        clock[0] += 1
    alone = [f for f in store.findings() if f.group is None]
    assert len(alone) == 9
    assert [f.sent_at for f in alone] == [f.found_at for f in alone]


def test_a_group_pages_its_first_finding_at_once_and_then_one_summary_per_window(tmp_path):
    store = cw.Store(str(tmp_path / "state.db"))
    posts = Posts()
    pager = cw.Pager(posts)
    started = _at("2026-09-28T02:00:00")
    for block in range(300):
        now = started + 6 * block
        store.add(rules.Finding(rules.ALERT, f"materios-preprod:{block}:3",
                                f"materios-preprod #{block} extrinsic 3: Recovery.create_recovery",
                                group="materios-preprod signer X"), now)
        pager.flush(store, now)
    assert _headlines(posts) == ["**materios-preprod #0 extrinsic 3: Recovery.create_recovery**",
                                 "**100 findings from materios-preprod signer X**",
                                 "**100 findings from materios-preprod signer X**"]


def test_groups_from_fresh_accounts_take_at_most_one_post_in_five(tmp_path):
    # Each block brings a group from an account never seen before.
    store = cw.Store(str(tmp_path / "state.db"))
    clock = [_at("2026-09-28T02:00:00")]
    sent = []
    pager = cw.Pager(lambda payload: sent.append(clock[0]))
    for second in range(300):
        if second % 6 == 0:
            store.add(rules.Finding(rules.ALERT, f"materios-preprod:{second}:3", f"fresh #{second}",
                                    group=f"materios-preprod signer {second}"), clock[0])
        pager.flush(store, clock[0])
        clock[0] += 1
    assert min(later - earlier for earlier, later in zip(sent, sent[1:])) >= 5 * cw.PAGE_INTERVAL


def test_a_flush_reads_only_what_it_may_send(tmp_path, monkeypatch):
    # A held group gathers findings for GROUP_WINDOW; reading them all back on every flush
    # would let accounts that raise a finding each block slow every cycle as they pile up.
    store = cw.Store(str(tmp_path / "state.db"))
    pager = cw.Pager(Posts())
    started = _at("2026-09-28T02:00:00")
    for block in range(100):
        _outsider_groups(store, block, started + 6 * block, 30)
        pager.flush(store, started + 6 * block)
    read = []
    rows = cw.Store._rows
    monkeypatch.setattr(cw.Store, "_rows", lambda self, *args, **kwargs: read.append(rows(self, *args, **kwargs))
                        or read[-1])
    pager.flush(store, started + 6 * 99 + 1)
    assert sum(len(r) for r in read) == 0


def _classified_cardano(name):
    def add(config, store, now):
        tx = _tx(name)
        store.add(rules.classify_cardano_tx(_mainnet(config), tx["tx"], tx["utxos"], tx["redeemers"]), now)
    return add


def _sudo_multisig_leg(config, store, now):
    block = json.loads(_read("materios/block_1829210.json"))
    [leg] = rules.classify_materios_block("materios-preprod", block["number"],
                                          _decoder_238().extrinsics(block["extrinsics"]), lambda: None, SUDO)
    store.add(leg, now)


def _polled_twice(prepare, change):
    """A Materios source polled at a head, then again after ``change`` at the next one."""
    def add(config, store, now):
        chain = FakeChain(head=1000, blocks={}, state_at={1000, 1001})
        prepare(chain)
        source = _materios(config, store, chain)
        source.poll(now)
        change(chain)
        chain.head = 1001
        source.poll(now)
    return add


def _replace_code(chain):
    chain.code_hash = "0x" + "11" * 32


AUTHORITY_CAUSED = {
    "custody outflow": _classified_cardano("custody_outflow"),
    "pool spend that is not a surrender": _classified_cardano("pool_rotate"),
    "mint under a watched policy": _classified_cardano("mint_v2"),
    "multisig leg of Sudo.Key": _sudo_multisig_leg,
    "Sudo.Key changed": _polled_twice(lambda chain: None, lambda chain: setattr(
        chain, "sudo_key", "0x" + rules.account_bytes(ALICE).hex())),
    "a friend vouched in the recovery of Sudo.Key": _polled_twice(
        lambda chain: (_recoverable_by(chain, SUDO, FRIENDS), _recovering(chain, SUDO, RESCUER, [])),
        lambda chain: _recovering(chain, SUDO, RESCUER, FRIENDS[:1])),
    "runtime code replaced": _polled_twice(lambda chain: None, _replace_code),
    "runtime environment digest": lambda config, store, now: store.add(
        rules.runtime_changed("materios-preprod", 1829227), now),
}


def _drained(tmp_path):
    """A pager whose bucket ten ALERTs, paging alone, have drained as far as they may."""
    store = cw.Store(str(tmp_path / "state.db"))
    for i in range(10):
        store.add(_finding(f"alert-{i}", severity=rules.ALERT), now=1.0)
    posts = Posts()
    pager = cw.Pager(posts)
    pager.flush(store, now=2.0)
    return store, posts, pager


@pytest.mark.parametrize("cause", sorted(AUTHORITY_CAUSED))
def test_a_token_is_kept_for_what_only_sudo_key_an_authority_or_a_custody_key_can_cause(config, tmp_path, cause):
    store, posts, pager = _drained(tmp_path)
    AUTHORITY_CAUSED[cause](config, store, 3.0)
    [page] = [f for f in store.unsent_pages() if not f.key.startswith("alert-")]
    pager.flush(store, now=3.0)
    assert f"**{page.headline}**" in posts.payloads[-1]["content"]


def test_what_anyone_else_causes_waits_for_the_bucket_to_refill(tmp_path):
    store, posts, pager = _drained(tmp_path)
    store.add(_finding("stale"), now=3.0)
    pager.flush(store, now=3.0)
    assert "headline stale" not in posts.text()
    pager.flush(store, now=3.0 + cw.PAGE_INTERVAL)
    assert "headline stale" in posts.payloads[-1]["content"]


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
    pager.flush(store, now=3.0 + cw.PAGE_INTERVAL)
    pager.flush(store, now=3.0 + 3 * cw.PAGE_INTERVAL)
    assert pager.flush(store, now=3.0 + 5 * cw.PAGE_INTERVAL) == 1
    fallback = posts.payloads[-1]["content"]
    assert "headline refused" in fallback and "rejected" in fallback and "```" not in fallback
    assert store.unsent_pages() == []


def test_a_refused_page_is_retried_on_a_doubling_delay_not_every_flush(tmp_path):
    # An authority's page, which the bucket's reserve never holds back, so only its
    # refusals pace it.
    store = cw.Store(str(tmp_path / "state.db"))
    store.add(_finding("refused", authority=True), now=1.0)
    posts = Posts(fail=set(range(1, 100)), error=discord.DiscordError("webhook answered HTTP 400", status=400))
    pager = cw.Pager(posts)
    attempts = []
    for second in range(64):
        before = posts.attempts
        pager.flush(store, now=100.0 + second)
        if posts.attempts > before:
            attempts.append(second)
    assert attempts == [0, 1, 3, 7, 15, 31, 63]


def test_a_webhook_that_refuses_every_post_is_asked_on_a_doubling_delay_and_loses_nothing(config, tmp_path):
    """A revoked or deleted webhook answers 401, 403 or 404 to every post. Asking it once per
    page per cycle would get the host's address banned by Discord's edge, silencing every
    watchdog that shares it; the pages themselves are not at fault and go out in full once
    the webhook is back."""
    store = cw.Store(str(tmp_path / "state.db"))
    for i in range(50):
        store.add(_finding(f"k{i}"), now=1.0)
    posts = Posts(fail=set(range(1, 10_000)), error=discord.DiscordError("webhook answered HTTP 401", status=401))
    clock = [_at("2026-09-27T12:55:00")]
    sources = [StubSource("materios-preprod"), StubSource("cardano-mainnet"), StubSource("cardano-preprod")]
    watch = _watch(config, store, sources, posts, clock)
    for _ in range(600):
        watch.cycle()
        clock[0] += 1.0
    assert posts.attempts <= 16

    posts.fail = set()
    for _ in range(int(50 * cw.PAGE_INTERVAL) + 61):
        watch.cycle()
        clock[0] += 1.0
    pages = [p["content"] for p in posts.payloads if "daily digest" not in p["content"]]
    assert len(pages) == 50 and not [p for p in pages if "rejected" in p]
    assert store.unsent_pages() == []
    assert any("daily digest" in p["content"] for p in posts.payloads)


def test_the_digest_counts_pages_still_waiting_for_delivery(config, tmp_path):
    store = cw.Store(str(tmp_path / "state.db"))
    posts = Posts(fail={1}, error=discord.DiscordError("webhook answered HTTP 400", status=400))
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


def _utf16_units(text: str) -> int:
    return len(text.encode("utf-16-le")) // 2


def test_a_page_and_the_digest_fit_discords_limit_however_it_counts_an_emoji(config, tmp_path):
    """Discord does not say whether an emoji outside the Basic Multilingual Plane counts as
    one character or two; cut to exactly 2000 code points, a page with one is refused if
    it counts two."""
    page = cw.page_message([_finding(details=tuple("x" * 300 for _ in range(40)))])["content"]
    assert _utf16_units(page) <= 2000
    store = cw.Store(str(tmp_path / "state.db"))
    for i in range(10):
        store.add(_finding(f"{i}" + "x" * 300, severity=rules.INFO), now=1.0)
    watch = _watch(config, store, [StubSource("cardano-mainnet")], Posts(), [1.0])
    digest = watch.digest_message(store.unsent_routine(), 1.0)["content"]
    assert "(truncated)" in digest and _utf16_units(digest) <= 2000


def test_a_deeply_nested_call_still_pages_within_discords_limit_naming_the_inner_call():
    call = {"call_module": "System", "call_function": "set_code", "call_args": [{"name": "code", "value": "0x00"}]}
    for _ in range(250):
        call = {"call_module": "Utility", "call_function": "batch", "call_args": [{"name": "calls", "value": [call]}]}
    call = {"call_module": "Sudo", "call_function": "sudo", "call_args": [{"name": "call", "value": call}]}
    [finding] = rules.classify_materios_block("materios-preprod", 9, [{"address": SUDO_KEY, "call": call}],
                                              lambda: None, rules.account_bytes(SUDO_KEY))
    for headline_only in (False, True):
        content = cw.page_message([finding], headline_only)["content"]
        assert len(content) <= 2000
        assert "Sudo.sudo" in content and "System.set_code" in content


@pytest.mark.parametrize("fence", ["````", "`````", "```"])
def test_text_from_the_chain_cannot_close_the_code_block(fence):
    payload = f"{fence}\n**RESOLVED: scheduled rehearsal, no action**\n{fence}"
    call = {"call_module": "Sudo", "call_function": "sudo", "call_args": [{"name": "call", "value": {
        "call_module": "System", "call_function": "remark", "call_args": [{"name": "remark", "value": payload}]}}]}
    [finding] = rules.classify_materios_block("materios-preprod", 9, [{"call": call}], lambda: None, None)
    content = cw.page_message([finding])["content"]
    assert content.count("`") == 6
    fenced = content.split("```")[1]
    assert "RESOLVED" in fenced and "\n**RESOLVED" not in content.replace(fenced, "")


# --- digest and source health -----------------------------------------------------


class StubSource:
    def __init__(self, name, fail=False, behind=False):
        self.name = name
        self.poll_seconds = 60
        self.position = "height 100"
        self.fail = fail
        self.behind = behind
        self.polls = 0

    def poll(self, now):
        self.polls += 1
        if self.fail:
            raise cw.SourceError(f"{self.name} unreachable")
        return not self.behind


def _watch(config, store, sources, posts, clock):
    return cw.Watch(config, store, sources, posts, clock=lambda: clock[0])


class DiscordModel:
    """One webhook shared by custody-watch and a co-tenant watchdog, under Discord's
    documented limit of 5 posts per 2 s per webhook and its 30 messages a minute per
    channel: a post past either is answered 429 with the wait."""

    def __init__(self, clock):
        self.clock = clock
        self.sent = []
        self.refused = []
        self.recent = []

    def __call__(self, payload, poster="custody-watch"):
        t = self.clock[0]
        self.recent = [s for s in self.recent if s > t - 60]
        last_two = [s for s in self.recent if s > t - 2]
        if len(last_two) >= 5 or len(self.recent) >= 30:
            self.refused.append((t, poster))
            wait = last_two[0] + 2 - t if len(last_two) >= 5 else self.recent[0] + 60 - t
            raise discord.DiscordError("webhook answered HTTP 429", 429, max(wait, 0.1))
        self.recent.append(t)
        self.sent.append((t, poster, payload))


class FloodSource(StubSource):
    """A block every 6 s carrying one ALERT from each of ``signers`` ordinary signers,
    grouped per signer as Materios findings are: what any funded accounts can send."""

    def __init__(self, name, store, signers):
        super().__init__(name)
        self.poll_seconds = 6
        self.store, self.signers, self.block = store, signers, 0

    def poll(self, now):
        self.block += 1
        for signer in range(self.signers):
            self.store.add(rules.Finding(
                rules.ALERT, f"{self.name}:{self.block}:{signer}",
                f"{self.name} #{self.block} extrinsic {signer}: Recovery.create_recovery",
                group=f"{self.name} signer {signer}"), now)
        return True


def _flood(config, tmp_path, signers, start, minutes):
    """Run the watcher through a flood, the co-tenant posting once every 2 minutes."""
    store = cw.Store(str(tmp_path / "state.db"))
    clock = [_at(start)]
    hook = DiscordModel(clock)
    watch = _watch(config, store, [FloodSource("materios-preprod", store, signers)], hook, clock)
    cotenant = []
    for second in range(minutes * 60):
        watch.cycle()
        if second % 120 == 60:
            try:
                hook({"content": "Finality Gap CRITICAL"}, poster="finality-watchdog")
                cotenant.append("delivered")
            except discord.DiscordError:
                cotenant.append("refused")
        clock[0] += 1
    return hook, cotenant


@pytest.mark.parametrize("signers", [5, 30])
def test_a_flood_of_grouped_pages_stays_under_discords_limits_and_the_cotenant_is_delivered(config, tmp_path,
                                                                                            signers):
    hook, cotenant = _flood(config, tmp_path, signers, "2026-09-28T02:00:00", minutes=20)
    ours = [t for t, poster, _ in hook.sent if poster == "custody-watch"]
    assert cotenant == ["delivered"] * 10
    assert hook.refused == []
    assert max(sum(1 for s in ours if t <= s < t + 60) for t in ours) <= cw.PAGE_BURST + 60 / cw.PAGE_INTERVAL
    pages = [p for _, poster, p in hook.sent if poster == "custody-watch"]
    assert pages and all("findings in" in p["content"] for p in pages)


def test_the_daily_digest_goes_out_through_a_flood(config, tmp_path):
    hook, _ = _flood(config, tmp_path, 5, "2026-09-28T12:55:00", minutes=30)
    digests = [p for _, _, p in hook.sent if "daily digest" in p["content"]]
    assert len(digests) == 1


def test_a_new_critical_page_goes_out_ahead_of_alerts_already_waiting(tmp_path):
    store = cw.Store(str(tmp_path / "state.db"))
    for i in range(6):
        store.add(_finding(f"alert-{i}", severity=rules.ALERT), now=1.0)
    posts = Posts()
    pager = cw.Pager(posts)
    assert pager.flush(store, now=2.0) < 6
    store.add(_finding("custody"), now=3.0)
    waiting = len(posts.payloads)
    assert pager.flush(store, now=12.0) >= 1
    assert _headlines(posts)[waiting] == "**headline custody**"


def test_many_pending_groups_go_out_as_one_summary(tmp_path):
    store = cw.Store(str(tmp_path / "state.db"))
    for group in range(5):
        for i in range(3):
            severity = rules.CRITICAL if (group, i) == (3, 1) else rules.ALERT
            store.add(rules.Finding(severity, f"g{group}-{i}", f"headline {group}.{i}",
                                    group=f"materios-preprod signer {group}"), now=1.0)
    posts = Posts()
    assert cw.Pager(posts).flush(store, now=2.0) == 15
    [summary] = posts.payloads
    assert summary["content"].startswith("\U0001f6a8 **CRITICAL** @here")
    assert "**15 findings in 5 groups**" in summary["content"]
    assert summary["content"].index("signer 3") < summary["content"].index("signer 0")
    assert store.unsent_pages() == []


def test_a_dedicated_webhook_is_read_from_the_file_the_config_names(tmp_path, monkeypatch):
    monkeypatch.delenv("DISCORD_WEBHOOK_URL", raising=False)
    (tmp_path / "custody-webhook").write_text("https://discord.test/api/webhooks/2/dedicated\n")
    doc = json.loads((FIX / "config.json").read_text())
    doc["discord_webhook_file"] = "custody-webhook"
    path = tmp_path / "config.json"
    path.write_text(json.dumps(doc))
    ran, posted = [], []
    monkeypatch.setattr(cw, "_run", lambda config, webhook: ran.append(webhook) or 0)
    monkeypatch.setattr(cw.discord, "post_json", lambda url, payload: posted.append(url))
    assert cw.main(["run", "--config", str(path)]) == 0
    assert cw.main(["test-page", "--config", str(path)]) == 0
    assert ran == posted == ["https://discord.test/api/webhooks/2/dedicated"]


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


def test_a_source_read_without_error_that_never_reaches_its_head_is_paged_stale_then_recovered(config, tmp_path):
    # Blocks that take longer to read than the chain takes to make them keep every poll
    # successful and leave the cursor further behind each time.
    store = cw.Store(str(tmp_path / "state.db"))
    posts = Posts()
    clock = [_at("2026-09-27T01:00:00")]
    source = StubSource("materios-preprod", behind=True)
    watch = _watch(config, store, [source], posts, clock)
    for _ in range(config.source_stale_seconds // 60 + 2):
        watch.cycle()
        clock[0] += 60
    [page] = posts.payloads
    assert page["content"].startswith("\U0001f6a8 **CRITICAL** @here")
    assert "materios-preprod stale for 1" in page["content"]
    assert "behind its head at height 100" in page["content"]
    assert "last caught up 2026-09-27 01:00:00Z" in page["content"]

    source.behind = False
    watch.cycle()
    assert "materios-preprod recovered" in posts.payloads[1]["content"]


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
CODE_KEY = "0x" + b":code".hex()
SPEC_238_CODE_HASH = "0xae5e94cef78cb63c58079c46b32b11e6f8c371ea9a701feeb5a4600c0e76edb4"
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
        # Storage beyond Sudo.Key and the events, by key; ``storage_at`` overrides it,
        # Sudo.Key included, at one block number.
        self.storage: dict[str, str] = {}
        self.storage_at: dict[int, dict[str, str | None]] = {}
        self.code_hash = SPEC_238_CODE_HASH
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

    def value(self, key: str, block_hash: str):
        at = self.storage_at.get(self.number_of(block_hash), {})
        if key in at:
            return at[key]
        return self.sudo_key if key == SUDO_STORAGE_KEY else self.storage.get(key)

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
        if method == "state_getMetadata":
            self._state(params[0] if params else None)
            return self.metadata
        if method == "state_getStorage":
            key, at = params
            self._state(at)
            if key == EVENTS_KEY:
                return self.events_hex
            if key == SUDO_STORAGE_KEY:
                return self.value(key, at)
        if method == "state_queryStorageAt":
            keys, at = params
            self._state(at)
            return [{"block": at, "changes": [[k, self.value(k, at)] for k in keys]}]
        if method == "state_getKeysPaged":
            prefix, count, start, at = params
            self._state(at)
            return sorted(k for k in self.storage if k.startswith(prefix) and k > start)[:count]
        if method == "state_getStorageHash":
            key, *at = params
            self._state(at[0] if at else None)
            assert key == CODE_KEY
            return self.code_hash
        raise AssertionError(f"unexpected RPC {method} {params}")


def _materios(config, store, chain):
    return cw.MateriosSource(config.materios, chain, store, config.source_stale_seconds)


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
                             "materios-preprod:1829227:4", "materios-preprod:1829227:runtime"}
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


def test_a_block_with_something_to_report_is_walked_once_and_its_events_read_once(config, tmp_path, monkeypatch):
    walked = _count_walks(monkeypatch)
    store = cw.Store(str(tmp_path / "state.db"))
    events = json.loads(_read(f"materios/{ROUTINE_BLOCK}"))["events"]
    chain = FakeChain(head=1829226, blocks={1829226: "block_1829226.json"},
                      state_at={1829225, 1829226}, events_hex=events)
    source = _materios(config, store, chain)
    source.start_at(1829225)
    _drain(source)
    [finding] = store.findings()
    assert "System.ExtrinsicSuccess" in finding.text
    assert len(walked) == len(json.loads(_read("materios/block_1829226.json"))["extrinsics"])
    assert [p for m, p in chain.calls if m == "state_getStorage" and p[0] == EVENTS_KEY] == [
        [EVENTS_KEY, FakeChain.hash_of(1829226)]]


def test_a_failure_to_read_a_blocks_events_fails_the_poll_and_the_block_is_read_again(config, tmp_path):
    store = cw.Store(str(tmp_path / "state.db"))
    chain = FakeChain(head=1829226, blocks={1829226: "block_1829226.json"}, state_at={1829225, 1829226})
    rpc = chain.rpc

    def dropped(method, params):
        if method == "state_getStorage" and params[0] == EVENTS_KEY:
            raise ConnectionError("socket closed")
        return rpc(method, params)

    chain.rpc = dropped
    source = _materios(config, store, chain)
    source.start_at(1829225)
    with pytest.raises(ConnectionError):
        source.poll(1.0)
    assert store.get("cursor:materios-preprod") == "1829225" and store.findings() == []


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
    loads = [p[1] for m, p in chain.calls if m == "state_getStorageHash" and p[1] != FakeChain.hash_of(1829228)]
    assert loads == [FakeChain.hash_of(1829225), FakeChain.hash_of(1829227)]


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


def test_a_sudo_leg_nested_deep_in_batches_pages_critical_naming_its_calls(config, tmp_path):
    store = cw.Store(str(tmp_path / "state.db"))
    chain = FakeChain(head=1000, blocks={}, state_at={1000}, extra={1000: [nested_sudo_leg(250)]})
    source = _materios(config, store, chain)
    source.start_at(999)
    posts = Posts()
    clock = [_at("2026-09-27T03:00:00")]
    _watch(config, store, [source], posts, clock).cycle()
    [page] = posts.payloads
    assert page["content"].startswith("\U0001f6a8 **CRITICAL** @here")
    assert page["allowed_mentions"] == {"parse": ["everyone"]}
    assert "System.authorize_upgrade**" in page["content"] and "Sudo.sudo" in page["content"]


def test_filler_smaller_than_an_authoritys_leg_leaves_the_legs_page_its_call_and_signer(config, tmp_path):
    # Any funded account can fill a block with signed extrinsics smaller than a multisig
    # leg; decoded smallest first against one budget, they would leave the leg unread.
    config = dataclasses.replace(config, materios=dataclasses.replace(
        config.materios, authority_accounts=(rules.render_account(LEG_SIGNER),)))
    fillers = fillers_past_the_budget(rules.RuntimeDecoder(_read("materios/metadata_spec238.hex.gz").decode()))
    store = cw.Store(str(tmp_path / "state.db"))
    chain = FakeChain(head=1829210, blocks={1829210: "block_1829210.json"}, state_at={1829210},
                      extra={1829210: fillers})
    source = _materios(config, store, chain)
    source.start_at(1829209)
    posts = Posts()
    _watch(config, store, [source], posts, [_at("2026-09-27T03:00:00")]).cycle()
    leg, undecoded = posts.payloads
    assert leg["content"].startswith("\U0001f6a8 **CRITICAL** @here")
    assert "Multisig.as_multi > Sudo.sudo > System.authorize_upgrade" in leg["content"]
    assert "multisig account is Sudo.Key" in leg["content"]
    assert f"signer {rules.render_account(LEG_SIGNER)}" in leg["content"]
    assert f"signer {rules.render_account(STRANGER)}" in undecoded["content"]


UTILITY_BATCH, REMARK_WITH_EVENT = bytes([8, 0]), bytes([0, 7])
REMARK_X = bytes([0, 0]) + rules._compact(1) + b"x"
SUDO_REMARK = bytes([6, 0]) + REMARK_X
AS_SUDO_KEY_REMARK = bytes([24, 0, 0]) + rules.account_bytes(SUDO_KEY) + REMARK_X
ATTEMPTERS = [bytes([0xB0 + i]) * 32 for i in range(6)]
OVERFLOW_AT = 7000
SUDO = rules.account_bytes(SUDO_KEY)
NOBODY = bytes([0x55]) * 32
# What an account can ask for on Sudo.Key's behalf, or to carry out what Root approved:
# Recovery.vouch_recovery(Sudo.Key, NOBODY), claim_recovery(Sudo.Key) and
# cancel_recovered(Sudo.Key), System.apply_authorized_upgrade(0x00), Treasury.payout(0).
VOUCH_FOR_NOBODY = bytes([24, 4, 0]) + SUDO + bytes([0]) + NOBODY
CLAIM_SUDO_KEY = bytes([24, 5, 0]) + SUDO
CANCEL_SUDO_KEY = bytes([24, 8, 0]) + SUDO
APPLY_UPGRADE = bytes([0, 11]) + rules._compact(1) + b"\x00"
PAYOUT_0 = bytes([9, 6]) + bytes(4)
OUTSIDER_ATTEMPTS = {"sudo": SUDO_REMARK, "as_recovered": AS_SUDO_KEY_REMARK, "vouch": VOUCH_FOR_NOBODY,
                     "claim": CLAIM_SUDO_KEY, "cancel": CANCEL_SUDO_KEY, "apply_authorized_upgrade": APPLY_UPGRADE,
                     "payout": PAYOUT_0}


def _events_overflow(chain, attempts, items=3200):
    """At OVERFLOW_AT, ``attempts`` ((call, signer), each failing) behind one routine
    Utility.batch of ``items`` remark_with_event, whose Remarked and ItemCompleted events
    take the block's events past DECODE_BUDGET: any funded account can send one."""
    batch = signed_extrinsic(UTILITY_BATCH + rules._compact(items) + (REMARK_WITH_EVENT + rules._compact(0)) * items,
                             bytes([0xA0]) * 32)
    chain.extra[OVERFLOW_AT] = [batch, *(signed_extrinsic(call, signer) for call, signer in attempts)]
    chain.events_hex = _batch_events(items, len(attempts))


DISPATCH_INFO = {"weight": {"ref_time": 1, "proof_size": 0}, "class": "Normal", "pays_fee": "Yes"}
SUCCESS = {"System": {"ExtrinsicSuccess": {"dispatch_info": DISPATCH_INFO}}}


def _record(index: int, event: dict) -> dict:
    return {"phase": {"ApplyExtrinsic": index}, "event": event, "topics": []}


def _encoded_events(records: list[dict]) -> str:
    decoder = rules.RuntimeDecoder(_read("materios/metadata_spec238.hex.gz").decode())
    return decoder._config.create_scale_object(decoder._events_type, metadata=decoder._metadata).encode(
        records).to_hex()


@functools.lru_cache(maxsize=None)
def _batch_events(items: int, failed: int) -> str:
    """System.Events of the routine block's three inherents, a batch of ``items``
    remark_with_event, then ``failed`` extrinsics that failed with Sudo's RequireSudo."""
    records = [_record(i, SUCCESS) for i in range(3)]
    for _ in range(items):
        records += [_record(3, {"System": {"Remarked": {"sender": "0x" + "a0" * 32, "hash": "0x" + "00" * 32}}}),
                    _record(3, {"Utility": "ItemCompleted"})]
    records += [_record(3, {"Utility": "BatchCompleted"}), _record(3, SUCCESS)]
    records += [_record(4 + i, {"System": {"ExtrinsicFailed": {
        "dispatch_error": {"Module": {"index": 6, "error": "0x00000000"}}, "dispatch_info": DISPATCH_INFO}}})
        for i in range(failed)]
    return _encoded_events(records)


@functools.lru_cache(maxsize=None)
def _succeeded(count: int) -> str:
    """System.Events in which each of a block's first ``count`` extrinsics succeeded."""
    return _encoded_events([_record(i, SUCCESS) for i in range(count)])


def _watch_overflow(config, tmp_path, attempts, state_at=(OVERFLOW_AT - 1, OVERFLOW_AT), items=3200):
    chain = FakeChain(head=OVERFLOW_AT + 1, blocks={}, state_at={*state_at, OVERFLOW_AT + 1})
    _events_overflow(chain, attempts, items)
    store = cw.Store(str(tmp_path / "state.db"))
    source = _materios(config, store, chain)
    source.start_at(OVERFLOW_AT - 1)
    posts = Posts()
    _watch(config, store, [source], posts, [_at("2026-09-28T02:00:00")]).cycle()
    return store, posts, chain


@pytest.mark.parametrize("call", list(OUTSIDER_ATTEMPTS.values()), ids=list(OUTSIDER_ATTEMPTS))
def test_failed_attempts_in_a_block_whose_events_overflow_are_proven_inert_from_its_state(config, tmp_path, call):
    store, posts, _ = _watch_overflow(config, tmp_path, [(call, signer) for signer in ATTEMPTERS])
    findings = store.findings()
    assert len(findings) == len(ATTEMPTERS)
    assert all(f.severity == rules.INFO and "events unavailable" in f.text for f in findings)
    assert all("cannot take effect from this origin" in f.text for f in findings)
    assert posts.payloads == []


def test_the_same_block_with_its_events_read_is_the_control(config, tmp_path):
    store, posts, _ = _watch_overflow(config, tmp_path, [(SUDO_REMARK, s) for s in ATTEMPTERS], items=100)
    assert [f.severity for f in store.findings()] == [rules.INFO] * len(ATTEMPTERS)
    assert all("dispatch failed" in f.text for f in store.findings())


def test_unverified_attempts_page_as_one_group_per_source_when_the_state_is_pruned_too(config, tmp_path):
    store, posts, _ = _watch_overflow(config, tmp_path, [(SUDO_REMARK, s) for s in ATTEMPTERS], state_at=())
    findings = store.findings()
    assert {f.severity for f in findings} == {rules.CRITICAL}
    assert {f.group for f in findings} == {"materios-preprod unverified"}
    [page] = posts.payloads
    assert f"**{len(ATTEMPTERS)} findings from materios-preprod unverified**" in page["content"]


def test_a_sudo_key_that_changed_inside_the_block_proves_nothing(config, tmp_path):
    # No events are served, so the attempt is unverified.
    signer = ATTEMPTERS[0]
    chain = FakeChain(head=OVERFLOW_AT, blocks={}, state_at={OVERFLOW_AT - 1, OVERFLOW_AT})
    chain.storage_at[OVERFLOW_AT - 1] = {SUDO_STORAGE_KEY: "0x" + signer.hex()}
    chain.extra[OVERFLOW_AT] = [signed_extrinsic(SUDO_REMARK, signer)]
    store = cw.Store(str(tmp_path / "state.db"))
    source = _materios(config, store, chain)
    source.start_at(OVERFLOW_AT - 1)
    source.poll(1.0)
    [finding] = store.findings()
    assert finding.severity == rules.CRITICAL and "Sudo.sudo" in finding.text


def test_an_attempt_by_the_proxy_of_the_account_it_acts_as_still_pages(config, tmp_path):
    signer = ATTEMPTERS[0]
    chain = FakeChain(head=OVERFLOW_AT, blocks={}, state_at={OVERFLOW_AT - 1, OVERFLOW_AT})
    proxy = "0x" + (xxh128(b"Recovery") + xxh128(b"Proxy") + blake2_128_concat(signer)).hex()
    chain.storage[proxy] = "0x" + rules.account_bytes(SUDO_KEY).hex()
    chain.extra[OVERFLOW_AT] = [signed_extrinsic(AS_SUDO_KEY_REMARK, signer)]
    store = cw.Store(str(tmp_path / "state.db"))
    source = _materios(config, store, chain)
    source.start_at(OVERFLOW_AT - 1)
    source.poll(1.0)
    first_read, finding = store.findings()
    assert f"Recovery.Proxy({rules.render_account(signer)}): acts as {SUDO_KEY}, Sudo.Key" in first_read.text
    assert finding.severity == rules.CRITICAL and "Recovery.as_recovered" in finding.text


FRIENDS = [bytes([0xF1 + i]) * 32 for i in range(5)]
RESCUER = bytes([0xE1]) * 32
RECOVERY = xxh128(b"Recovery")
VOUCH = bytes([24, 4, 0]) + SUDO + bytes([0]) + RESCUER
CLAIM = bytes([24, 5, 0]) + SUDO


@functools.lru_cache(maxsize=None)
def _decoder_238() -> rules.RuntimeDecoder:
    return rules.RuntimeDecoder(_read("materios/metadata_spec238.hex.gz").decode())


def _recovery_value(item: str, value) -> str:
    """``value`` encoded as Recovery's ``item`` holds it; scalecodec encodes a BoundedVec
    from its one field, so each list of friends is wrapped once more."""
    decoder = _decoder_238()
    function = decoder._metadata.get_metadata_pallet("Recovery").get_storage_function(item)
    return decoder._config.create_scale_object(function.get_value_type_string(),
                                               metadata=decoder._metadata).encode(value).to_hex()


def _recoverable_key(account: bytes) -> str:
    return "0x" + (RECOVERY + xxh128(b"Recoverable") + two_x64_concat(account)).hex()


def _active_key(lost: bytes, rescuer: bytes) -> str:
    return "0x" + (RECOVERY + xxh128(b"ActiveRecoveries") + two_x64_concat(lost) + two_x64_concat(rescuer)).hex()


def _proxy_key(rescuer: bytes) -> str:
    return "0x" + (RECOVERY + xxh128(b"Proxy") + blake2_128_concat(rescuer)).hex()


def _recoverable_by(chain, account: bytes, friends: list[bytes]) -> None:
    """``account`` recoverable as Sudo.Key is on the chain: 3 of ``friends``, 100,800 blocks."""
    chain.storage[_recoverable_key(account)] = _recovery_value("Recoverable", {
        "delay_period": 100_800, "deposit": 1, "friends": [["0x" + f.hex() for f in sorted(friends)]],
        "threshold": 3})


def _recovering(chain, lost: bytes, rescuer: bytes, vouched: list[bytes]) -> None:
    chain.storage[_active_key(lost, rescuer)] = _recovery_value("ActiveRecoveries", {
        "created": 7000, "deposit": 1, "friends": [["0x" + f.hex() for f in sorted(vouched)]]})


def _filler_like(call: bytes, signer: bytes = STRANGER) -> str:
    """A signed Utility.batch of remarks from ``signer`` whose call is exactly as long as ``call``."""
    empty, target = 3, len(call) - 2 - 1 - 3
    count, extra = divmod(target, empty)
    remark = bytes([0, 0]) + rules._compact(extra) + b"x" * extra
    filler = UTILITY_BATCH + rules._compact(count + 1) + (bytes([0, 0]) + rules._compact(0)) * count + remark
    assert len(filler) == len(call)
    return signed_extrinsic(filler, signer)


@pytest.mark.parametrize("signer, call, named", [(FRIENDS[0], VOUCH, "Recovery.vouch_recovery"),
                                                 (RESCUER, CLAIM, "Recovery.claim_recovery")],
                         ids=["friend", "rescuer"])
def test_what_a_friend_or_rescuer_of_sudo_key_signs_is_decoded_whatever_filler_the_block_holds(
        config, tmp_path, signer, call, named):
    chain = FakeChain(head=8000, blocks={}, state_at={8000})
    _recoverable_by(chain, SUDO, FRIENDS)
    _recovering(chain, SUDO, RESCUER, [])
    chain.extra[8000] = [_filler_like(call)] * 1500 + [signed_extrinsic(call, signer)]
    store = cw.Store(str(tmp_path / "state.db"))
    source = _materios(config, store, chain)
    source.start_at(7999)
    posts = Posts()
    clock = [_at("2026-09-28T02:00:00")]
    watch = _watch(config, store, [source], posts, clock)
    for _ in range(int(cw.GROUPED_INTERVAL) + 1):
        watch.cycle()
        clock[0] += 1
    [page] = [p["content"] for p in posts.payloads if named in p["content"]]
    assert page.startswith("\U0001f6a8 **CRITICAL** @here")
    assert SUDO_KEY in page and f"signer {rules.render_account(signer)}" in page


def test_the_first_read_of_the_recovery_of_sudo_key_goes_to_the_digest(config, tmp_path):
    chain = FakeChain(head=1000, blocks={}, state_at={1000})
    _recoverable_by(chain, SUDO, FRIENDS)
    store = cw.Store(str(tmp_path / "state.db"))
    _materios(config, store, chain).poll(1.0)
    [finding] = store.findings()
    assert finding.severity == rules.INFO
    assert f"Recovery.Recoverable({SUDO_KEY}, Sudo.Key)" in finding.text
    assert "threshold 3 of 5 friends, delay 100,800 blocks" in finding.text
    assert rules.render_account(FRIENDS[0]) in finding.text


STARTED = "materios-preprod recovery started"


def test_any_change_in_the_recovery_of_sudo_key_or_an_authority_pages_critical(config, tmp_path):
    # Only a recovery started with no friend's vouch yet, which any funded account can
    # start and which can do nothing until friends vouch, pages as an ALERT in a group;
    # every other change pages CRITICAL alone.
    authority = rules.account_bytes(ALICE)
    config = dataclasses.replace(config, materios=dataclasses.replace(config.materios, authority_accounts=(ALICE,)))
    chain = FakeChain(head=1000, blocks={}, state_at=set(range(1000, 1010)))
    _recoverable_by(chain, SUDO, FRIENDS)
    store = cw.Store(str(tmp_path / "state.db"))
    source = _materios(config, store, chain)
    source.poll(1.0)
    changes = [
        ("Recovery.ActiveRecoveries", STARTED, lambda: _recovering(chain, SUDO, RESCUER, [])),
        ("Recovery.ActiveRecoveries", None, lambda: _recovering(chain, SUDO, RESCUER, FRIENDS[:1])),
        ("Recovery.Proxy", None, lambda: chain.storage.__setitem__(_proxy_key(RESCUER), "0x" + SUDO.hex())),
        ("Recovery.Recoverable", None, lambda: _recoverable_by(chain, authority, FRIENDS)),
        ("Recovery.Recoverable", None, lambda: chain.storage.pop(_recoverable_key(SUDO))),
    ]
    for step, (item, group, change) in enumerate(changes, start=1):
        change()
        chain.head = 1000 + step
        source.poll(1.0 + step)
        [finding] = [f for f in store.findings() if f.key.endswith(f":recovery:{1000 + step}")]
        severity = rules.ALERT if group else rules.CRITICAL
        assert finding.severity == severity and finding.group == group, (step, finding.group)
        assert item in finding.text, (step, finding.text)
    assert "rescuer " + rules.render_account(RESCUER) in store.findings()[1].text
    assert f"vouched by {rules.render_account(FRIENDS[0])}" in store.findings()[2].text
    assert f"- Recovery.Recoverable({SUDO_KEY}, Sudo.Key)" in store.findings()[-1].text
    source.poll(9.0)
    assert len(store.findings()) == 1 + len(changes)


def test_recoveries_strangers_start_page_without_here_as_one_message_behind_the_pages_that_go_alone(config,
                                                                                                    tmp_path):
    # Starting a recovery of Sudo.Key costs any funded account a deposit, once a poll.
    chain = FakeChain(head=1000, blocks={}, state_at=set(range(1000, 1010)))
    _recoverable_by(chain, SUDO, FRIENDS)
    store = cw.Store(str(tmp_path / "state.db"))
    source = _materios(config, store, chain)
    source.poll(1.0)
    for step, stranger in enumerate(ATTEMPTERS[:4], start=1):
        _recovering(chain, SUDO, stranger, [])
        chain.head = 1000 + step
        source.poll(1.0 + step)
    _recovering(chain, SUDO, ATTEMPTERS[0], FRIENDS[:1])
    chain.head = 1005
    source.poll(6.0)
    started = [f for f in store.findings() if f.group == STARTED]
    assert len(started) == 4 and all(f.severity == rules.ALERT for f in started)
    posts = Posts()
    cw.Pager(posts).flush(store, 10.0)
    vouched, grouped = posts.payloads
    assert f"vouched by {rules.render_account(FRIENDS[0])}" in vouched["content"]
    assert grouped["content"].startswith("\u26a0\ufe0f **ALERT**")
    assert f"**4 findings from {STARTED}**" in grouped["content"]


ATT = bytes([0xC7]) * 32
LEG_AT = OVERFLOW_AT + 1


def _leg_signer_an_authority(config):
    return dataclasses.replace(config, materios=dataclasses.replace(
        config.materios, authority_accounts=(rules.render_account(LEG_SIGNER),)))


def test_a_stranger_recovering_sudo_key_is_no_authority_and_its_failed_attempts_hold_back_no_page(config, tmp_path):
    # Any funded account can start a recovery of Sudo.Key for a deposit. Its 200 failed
    # Sudo attempts are an outsider's, and the multisig leg in the next block goes at once.
    config = _leg_signer_an_authority(config)
    chain = FakeChain(head=LEG_AT, blocks={LEG_AT: "block_1829210.json"},
                      state_at={OVERFLOW_AT - 1, OVERFLOW_AT, LEG_AT})
    _recoverable_by(chain, SUDO, FRIENDS)
    _recovering(chain, SUDO, ATT, [])
    _events_overflow(chain, [(SUDO_REMARK, ATT)] * 200, items=1)
    store = cw.Store(str(tmp_path / "state.db"))
    source = _materios(config, store, chain)
    source.start_at(OVERFLOW_AT - 1)
    clock = [_at("2026-09-28T02:00:00")]
    posts = Posts()
    watch = _watch(config, store, [source], posts, clock)
    watch.cycle()
    assert any("System.authorize_upgrade" in p["content"] for p in posts.payloads)
    for _ in range(60):
        clock[0] += 1
        watch.cycle()
    assert not [p for p in posts.payloads if f"signer {rules.render_account(ATT)}" in p["content"]]
    assert all(f.severity == rules.INFO for f in store.findings() if rules.render_account(ATT) in f.text
               and ":recovery:" not in f.key)


def test_a_stranger_recovering_sudo_key_cannot_spend_the_budget_of_an_authoritys_leg(config, tmp_path):
    config = _leg_signer_an_authority(config)
    chain = FakeChain(head=LEG_AT, blocks={LEG_AT: "block_1829210.json"}, state_at={OVERFLOW_AT, LEG_AT},
                      extra={LEG_AT: [signed_extrinsic(SUDO_REMARK, ATT)] * 2400})
    _recoverable_by(chain, SUDO, FRIENDS)
    _recovering(chain, SUDO, ATT, [])
    store = cw.Store(str(tmp_path / "state.db"))
    source = _materios(config, store, chain)
    source.start_at(OVERFLOW_AT)
    _drain(source)
    [leg] = [f for f in store.findings() if f.key == f"materios-preprod:{LEG_AT}:2"]
    assert leg.severity == rules.CRITICAL and leg.group is None
    assert "Multisig.as_multi > Sudo.sudo > System.authorize_upgrade" in leg.text
    assert [f.group for f in store.findings() if ":undecoded" in f.key] == ["materios-preprod unclassifiable"]


def test_what_a_friend_of_sudo_key_signs_is_decoded_ahead_of_filler_from_a_stranger_recovering_it(config, tmp_path):
    chain = FakeChain(head=8000, blocks={}, state_at={8000})
    _recoverable_by(chain, SUDO, FRIENDS)
    _recovering(chain, SUDO, ATT, [])
    chain.extra[8000] = [_filler_like(VOUCH, ATT)] * 1500 + [signed_extrinsic(VOUCH, FRIENDS[0])]
    store = cw.Store(str(tmp_path / "state.db"))
    source = _materios(config, store, chain)
    source.start_at(7999)
    _drain(source)
    [vouch] = [f for f in store.findings() if "Recovery.vouch_recovery" in f.text]
    assert vouch.severity == rules.CRITICAL and f"signer {rules.render_account(FRIENDS[0])}" in vouch.text


def _force_batch(call: bytes) -> bytes:
    """Utility.force_batch([call]), which succeeds whether or not ``call`` does."""
    return bytes([8, 4]) + rules._compact(1) + call


def _as_proxy_of_sudo_key(chain, signer):
    chain.storage[_proxy_key(signer)] = "0x" + SUDO.hex()


AUTHORIZED_UPGRADE_KEY = "0x" + (xxh128(b"System") + xxh128(b"AuthorizedUpgrade")).hex()
SPEND_0_KEY = "0x" + (xxh128(b"Treasury") + xxh128(b"Spends") + two_x64_concat(bytes(4))).hex()
# Each call an outsider can ask for, and the state that lets it take effect: only Sudo.Key's
# recovery config, its friends' vouches, or Root's own approval can put it there.
ALLOWED_BY = {
    "vouch": (VOUCH_FOR_NOBODY, lambda chain, signer: _recoverable_by(chain, SUDO, [*FRIENDS[:4], signer])),
    "claim": (CLAIM_SUDO_KEY, lambda chain, signer: _recovering(chain, SUDO, signer, FRIENDS[:3])),
    "cancel": (CANCEL_SUDO_KEY, _as_proxy_of_sudo_key),
    "as_recovered": (AS_SUDO_KEY_REMARK, _as_proxy_of_sudo_key),
    "apply_authorized_upgrade": (APPLY_UPGRADE, lambda chain, signer: chain.storage.__setitem__(
        AUTHORIZED_UPGRADE_KEY, "0x" + "11" * 33)),
    "payout": (PAYOUT_0, lambda chain, signer: chain.storage.__setitem__(SPEND_0_KEY, "0x01")),
}


@pytest.mark.parametrize("allowed", [False, True], ids=["refused by the state", "allowed by the state"])
@pytest.mark.parametrize("name", sorted(ALLOWED_BY))
def test_what_an_outsider_asks_for_counts_only_where_the_blocks_state_allows_it(config, tmp_path, name, allowed):
    # force_batch succeeds whether or not the call it wraps does, so the extrinsic's events
    # show only that it was applied.
    call, allow = ALLOWED_BY[name]
    signer = ATTEMPTERS[0]
    chain = FakeChain(head=9001, blocks={}, state_at={9000, 9001}, events_hex=_succeeded(4))
    _recoverable_by(chain, SUDO, FRIENDS)
    if allowed:
        allow(chain, signer)
    chain.extra[9001] = [signed_extrinsic(_force_batch(call), signer)]
    store = cw.Store(str(tmp_path / "state.db"))
    source = _materios(config, store, chain)
    source.start_at(9000)
    _drain(source)
    [finding] = [f for f in store.findings() if f.key == "materios-preprod:9001:3"]
    if allowed:
        assert finding.severity == rules.CRITICAL
    else:
        assert finding.severity == rules.INFO and "cannot take effect from this origin" in finding.text


def _own_recovery_naming(friend: bytes) -> bytes:
    """Utility.batch_all(create_recovery([friend], 1, 0), remove_recovery()): the deposit is
    reserved and returned in one extrinsic, so it costs the signer fees alone."""
    create = bytes([24, 2]) + rules._compact(1) + friend + (1).to_bytes(2, "little") + bytes(4)
    return bytes([8, 2]) + rules._compact(2) + create + bytes([24, 7])


def test_an_outsider_naming_sudo_key_a_friend_of_its_own_recovery_never_pages_here(config, tmp_path):
    # A friend in the signer's own recovery config gains power over the signer's account,
    # never over Sudo.Key.
    first, signer = 9000, ATTEMPTERS[0]
    chain = FakeChain(head=first, blocks={}, state_at=set(range(first - 1, first + 11)), events_hex=_succeeded(4))
    _recoverable_by(chain, SUDO, FRIENDS)
    store = cw.Store(str(tmp_path / "state.db"))
    source = _materios(config, store, chain)
    source.start_at(first - 1)
    posts, clock = Posts(), [_at("2026-09-28T02:00:00")]
    watch = _watch(config, store, [source], posts, clock)
    for n in range(10):
        chain.head = first + n
        chain.extra[first + n] = [signed_extrinsic(_own_recovery_naming(SUDO), signer)]
        for _ in range(6):
            watch.cycle()
            clock[0] += 1
    created = [f for f in store.findings() if "Recovery.create_recovery" in f.text]
    assert len(created) == 10 and {f.severity for f in created} == {rules.ALERT}
    assert not [p for p in posts.payloads if "@here" in p["content"]]


def _remarks(count: int) -> bytes:
    """Utility.batch of ``count`` empty remarks; 20,000 of them are past the decode budget."""
    return UTILITY_BATCH + rules._compact(count) + (bytes([0, 0]) + rules._compact(0)) * count


PROXY = bytes([0xD7]) * 32


@pytest.mark.parametrize("signer, severity", [
    (ATTEMPTERS[0], rules.ALERT), (ATT, rules.ALERT), (FRIENDS[0], rules.CRITICAL), (RESCUER, rules.CRITICAL),
    (PROXY, rules.CRITICAL)], ids=["stranger", "rescuer no friend vouched for", "friend", "vouched rescuer", "proxy"])
def test_what_cannot_be_decoded_pages_critical_only_from_an_account_that_can_act_for_sudo_key(config, tmp_path,
                                                                                             signer, severity):
    chain = FakeChain(head=8000, blocks={}, state_at={7999, 8000})
    _recoverable_by(chain, SUDO, FRIENDS)
    _recovering(chain, SUDO, ATT, [])
    _recovering(chain, SUDO, RESCUER, FRIENDS[:3])
    _as_proxy_of_sudo_key(chain, PROXY)
    chain.extra[8000] = [signed_extrinsic(_remarks(20_000), signer)]
    store = cw.Store(str(tmp_path / "state.db"))
    source = _materios(config, store, chain)
    source.start_at(7999)
    _drain(source)
    [finding] = [f for f in store.findings() if ":undecoded" in f.key
                 and f"signer {rules.render_account(signer)}" in f.text]
    assert finding.severity == severity
    assert (finding.group is None) == (severity == rules.CRITICAL)


def _encoded(module: str, function: str, **args) -> bytes:
    """A call as the spec-238 runtime encodes it."""
    decoder = _decoder_238()
    call = decoder._config.create_scale_object("Call", metadata=decoder._metadata)
    return bytes.fromhex(call.encode({"call_module": module, "call_function": function, "call_args": args})
                         .to_hex()[2:])


FORCE_TRANSFER = {"call_module": "Balances", "call_function": "force_transfer",
                  "call_args": {"source": "0x" + SUDO.hex(), "dest": "0x" + NOBODY.hex(), "value": 10 ** 15}}
SUDO_FORCE_TRANSFER = _encoded("Sudo", "sudo", call=FORCE_TRANSFER)
AS_SUDO_KEY_FORCE_TRANSFER = _encoded("Recovery", "as_recovered", account="0x" + SUDO.hex(), call={
    "call_module": "Sudo", "call_function": "sudo", "call_args": {"call": FORCE_TRANSFER}})


def _watched_block(config, tmp_path, chain, number):
    store = cw.Store(str(tmp_path / "state.db"))
    source = _materios(config, store, chain)
    source.start_at(number - 1)
    posts = Posts()
    _watch(config, store, [source], posts, [_at("2026-09-28T02:00:00")]).cycle()
    return store, posts


@pytest.mark.parametrize("events", ["overflowing", "read"])
def test_what_a_rescuer_does_as_sudo_key_between_its_claim_and_its_cancel_pages_by_name(config, tmp_path, events):
    # Recovery.Proxy is empty at the block's parent and at the block, but a rescuer the
    # friends have vouched for can claim Sudo.Key inside the block and give it back.
    chain = FakeChain(head=OVERFLOW_AT, blocks={}, state_at={OVERFLOW_AT - 1, OVERFLOW_AT})
    _recoverable_by(chain, SUDO, FRIENDS)
    _recovering(chain, SUDO, RESCUER, FRIENDS[:3])
    attempts = [(CLAIM_SUDO_KEY, RESCUER), (AS_SUDO_KEY_FORCE_TRANSFER, RESCUER), (CANCEL_SUDO_KEY, RESCUER)]
    if events == "overflowing":
        _events_overflow(chain, attempts)
    else:
        chain.extra[OVERFLOW_AT] = [signed_extrinsic(call, signer) for call, signer in attempts]
        chain.events_hex = _succeeded(6)
    store, posts = _watched_block(config, tmp_path, chain, OVERFLOW_AT)
    [acted] = [f for f in store.findings() if "Balances.force_transfer" in f.text]
    assert acted.severity == rules.CRITICAL
    assert any(f"**{acted.headline}**" in p["content"] for p in posts.payloads)


def test_a_proven_proxys_act_as_sudo_key_pages_alone_by_name_however_many_outsiders_share_its_block(config,
                                                                                                  tmp_path):
    chain = FakeChain(head=9001, blocks={}, state_at={9000, 9001}, events_hex=_succeeded(3 + 16))
    _recoverable_by(chain, SUDO, FRIENDS)
    _as_proxy_of_sudo_key(chain, PROXY)
    chain.extra[9001] = ([signed_extrinsic(_own_recovery_naming(SUDO), bytes([0x90 + i]) * 32) for i in range(15)]
                         + [signed_extrinsic(AS_SUDO_KEY_FORCE_TRANSFER, PROXY)])
    store, posts = _watched_block(config, tmp_path, chain, 9001)
    [acted] = [f for f in store.findings() if "Balances.force_transfer" in f.text]
    assert acted.severity == rules.CRITICAL and acted.group is None
    [page] = [p["content"] for p in posts.payloads if "Balances.force_transfer" in p["content"]]
    assert page.startswith("\U0001f6a8 **CRITICAL** @here") and f"**{acted.headline}**" in page


@pytest.mark.parametrize("holder", ["set_key", "undecoded"])
def test_a_sudo_key_its_holder_may_have_handed_on_and_back_inside_a_block_proves_nothing(config, tmp_path, holder):
    # Sudo.Key is the same at the block's parent and at the block, but its holder acted in
    # the block, by a key change or by an extrinsic the watcher cannot read, so X may have
    # held the key in between and given it back.
    x = ATTEMPTERS[0]
    chain = FakeChain(head=OVERFLOW_AT, blocks={}, state_at={OVERFLOW_AT - 1, OVERFLOW_AT})
    handed = _encoded("Sudo", "set_key", new={"Id": "0x" + x.hex()}) if holder == "set_key" else _remarks(20_000)
    _events_overflow(chain, [(handed, SUDO), (SUDO_FORCE_TRANSFER, x)])
    store, posts = _watched_block(config, tmp_path, chain, OVERFLOW_AT)
    [acted] = [f for f in store.findings() if "Balances.force_transfer" in f.text]
    assert acted.severity == rules.CRITICAL and f"signer {rules.render_account(x)}" in acted.text


def test_an_authoritys_call_that_moves_neither_sudo_key_nor_a_proxy_leaves_the_proof_standing(config, tmp_path):
    # A committee operator is an authority account; its session keys change neither
    # Sudo.Key nor a Recovery.Proxy, so an outsider cannot time a failed Sudo call to it.
    config = _leg_signer_an_authority(config)
    chain = FakeChain(head=OVERFLOW_AT, blocks={}, state_at={OVERFLOW_AT - 1, OVERFLOW_AT})
    set_keys = _encoded("PalletSession", "purge_keys")
    _events_overflow(chain, [(set_keys, LEG_SIGNER), (SUDO_FORCE_TRANSFER, ATTEMPTERS[0])])
    store, posts = _watched_block(config, tmp_path, chain, OVERFLOW_AT)
    [attempt] = [f for f in store.findings() if "Balances.force_transfer" in f.text]
    assert attempt.severity == rules.INFO and "cannot take effect from this origin" in attempt.text


def test_a_sudo_key_no_holder_touched_in_the_block_still_proves_an_outsiders_attempt_inert(config, tmp_path):
    chain = FakeChain(head=OVERFLOW_AT, blocks={}, state_at={OVERFLOW_AT - 1, OVERFLOW_AT})
    _events_overflow(chain, [(_encoded("Sudo", "set_key", new={"Id": "0x" + NOBODY.hex()}), ATTEMPTERS[1]),
                             (SUDO_FORCE_TRANSFER, ATTEMPTERS[0])])
    store, posts = _watched_block(config, tmp_path, chain, OVERFLOW_AT)
    assert {f.severity for f in store.findings()} == {rules.INFO}
    assert posts.payloads == []


def test_a_proxy_acting_as_sudo_key_stays_watched_after_its_recovery_is_closed(config, tmp_path):
    chain = FakeChain(head=1000, blocks={}, state_at=set(range(1000, 1010)))
    _recoverable_by(chain, SUDO, FRIENDS)
    store = cw.Store(str(tmp_path / "state.db"))
    source = _materios(config, store, chain)
    source.poll(1.0)
    _recovering(chain, SUDO, RESCUER, FRIENDS[:3])
    chain.storage[_proxy_key(RESCUER)] = "0x" + SUDO.hex()
    chain.head = 1001
    source.poll(2.0)
    # as_recovered(Sudo.Key, close_recovery(RESCUER)) ends the recovery; the proxy stays.
    chain.storage.pop(_active_key(SUDO, RESCUER))
    chain.head = 1002
    source.poll(3.0)
    [closed] = [f for f in store.findings() if f.key.endswith(":recovery:1002")]
    rescuer = rules.render_account(RESCUER)
    assert len(closed.details) == 1
    assert closed.details[0].startswith(f"- Recovery.ActiveRecoveries(lost {SUDO_KEY}, Sudo.Key, rescuer {rescuer})")
    chain.storage.pop(_proxy_key(RESCUER))
    chain.head = 1003
    source.poll(4.0)
    [cancelled] = [f for f in store.findings() if f.key.endswith(":recovery:1003")]
    assert cancelled.details == (f"- Recovery.Proxy({rescuer}): acts as {SUDO_KEY}, Sudo.Key",)


def test_every_proxy_that_acts_as_sudo_key_is_watched_whatever_made_it(config, tmp_path):
    # Root's set_recovered, or a recovery older than the watcher, leaves a proxy with no
    # recovery under way.
    other = bytes([0xD1]) * 32
    chain = FakeChain(head=1000, blocks={}, state_at={1000, 1001})
    _recoverable_by(chain, SUDO, FRIENDS)
    chain.storage[_proxy_key(RESCUER)] = "0x" + SUDO.hex()
    chain.storage[_proxy_key(ATT)] = "0x" + other.hex()
    store = cw.Store(str(tmp_path / "state.db"))
    source = _materios(config, store, chain)
    source.poll(1.0)
    [first] = store.findings()
    assert f"Recovery.Proxy({rules.render_account(RESCUER)}): acts as {SUDO_KEY}, Sudo.Key" in first.text
    assert rules.render_account(ATT) not in first.text
    chain.storage[_proxy_key(RESCUER)] = "0x" + other.hex()
    chain.head = 1001
    source.poll(2.0)
    [moved] = [f for f in store.findings() if f.severity == rules.CRITICAL]
    assert moved.details == (f"- Recovery.Proxy({rules.render_account(RESCUER)}): acts as {SUDO_KEY}, Sudo.Key",)


def test_recovery_state_is_read_in_bounded_requests_and_decoded_only_when_it_changes(config, tmp_path, monkeypatch):
    chain = FakeChain(head=1000, blocks={}, state_at={1000, 1001})
    _recoverable_by(chain, SUDO, FRIENDS)
    _recovering(chain, SUDO, RESCUER, [])
    started = chain.storage[_active_key(SUDO, RESCUER)]
    for n in range(2500):
        chain.storage[_active_key(SUDO, n.to_bytes(4, "big") * 8)] = started
    store = cw.Store(str(tmp_path / "state.db"))
    source = _materios(config, store, chain)
    source.poll(1.0)
    decoded = []
    real = rules.RuntimeDecoder.storage
    monkeypatch.setattr(rules.RuntimeDecoder, "storage", lambda self, *a: decoded.append(a) or real(self, *a))
    chain.calls.clear()
    chain.head = 1001
    source.poll(2.0)
    reads = [len(params[0]) for method, params in chain.calls if method == "state_queryStorageAt"]
    assert sum(reads) >= 2502 and max(reads) <= cw.KEYS_PAGE
    assert decoded == []


def test_a_recovery_entry_the_runtime_cannot_decode_still_pages_as_its_hash(config, tmp_path):
    chain = FakeChain(head=1000, blocks={}, state_at={1000, 1001})
    _recoverable_by(chain, SUDO, FRIENDS)
    store = cw.Store(str(tmp_path / "state.db"))
    source = _materios(config, store, chain)
    source.poll(1.0)
    chain.storage[_recoverable_key(SUDO)] = "0x01"
    chain.head = 1001
    source.poll(2.0)
    [finding] = [f for f in store.findings() if f.severity == rules.CRITICAL]
    assert finding.details[0].startswith(f"~ Recovery.Recoverable({SUDO_KEY}, Sudo.Key): 1 bytes blake2_256 0x")
    assert "not decodable" in finding.details[0]


def test_a_runtime_environment_digest_pages_critical_on_its_own(config, tmp_path):
    store = cw.Store(str(tmp_path / "state.db"))
    chain = FakeChain(head=1829227, blocks={1829227: "block_2029707.json"}, state_at={1829226, 1829227})
    source = _materios(config, store, chain)
    source.start_at(1829226)
    _drain(source)
    [finding] = [f for f in store.findings() if f.severity == rules.CRITICAL]
    assert finding.key == "materios-preprod:1829227:runtime" and finding.group is None
    assert "runtime environment changed" in finding.headline


def test_runtime_code_replaced_without_an_upgrade_digest_pages_and_is_decoded_with_its_own_metadata(config, tmp_path):
    store = cw.Store(str(tmp_path / "state.db"))
    chain = FakeChain(head=1000, blocks={}, state_at=set(range(999, 1003)))
    source = _materios(config, store, chain)
    source.start_at(999)
    _drain(source)
    replaced = "0x" + "11" * 32
    chain.code_hash, chain.head = replaced, 1002
    _drain(source, now=2.0)
    [finding] = [f for f in store.findings() if f.severity == rules.CRITICAL]
    assert "runtime code changed" in finding.headline and finding.group is None
    assert SPEC_238_CODE_HASH in finding.text and replaced in finding.text
    assert [m for m, _ in chain.calls].count("state_getMetadata") == 2


def _custody_outflow_pending(config, tmp_path):
    store, network, api, cardano = _baselined(config, tmp_path)
    custody = next(a for a in network.addresses if a.label == "custody-a")
    api.add_tx("custody_outflow", address=custody.address, height=13_500_001)
    return store, cardano


def test_a_block_filled_to_its_length_limit_is_paged_and_passed_and_cardano_is_still_read(config, tmp_path):
    store, cardano = _custody_outflow_pending(config, tmp_path)
    filler = signed_extrinsic(filler_call("keys", NORMAL_BLOCK_LENGTH - 1024), STRANGER)
    chain = FakeChain(head=1001, blocks={1001: "block_1829210.json"}, state_at={1000, 1001},
                      extra={1001: [filler]})
    materios = _materios(config, store, chain)
    materios.start_at(1000)
    posts = Posts()
    _watch(config, store, [materios, cardano], posts, [_at("2026-09-27T01:01:00")]).cycle()
    assert store.get("cursor:materios-preprod") == "1001"
    text = posts.text()
    assert "System.authorize_upgrade" in text
    assert "materios-preprod #1001: 1 extrinsic could not be decoded" in text
    assert "outflow from custody-a" in text


def test_blocks_that_spend_the_decode_budget_end_the_poll_so_cardano_is_read_between_them(config, tmp_path,
                                                                                          monkeypatch):
    monkeypatch.setattr(rules, "DECODE_BUDGET", 300)
    store, cardano = _custody_outflow_pending(config, tmp_path)
    filler = signed_extrinsic(filler_call("keys", 2000), STRANGER)
    chain = FakeChain(head=1005, blocks={}, state_at=set(range(1000, 1006)),
                      extra={n: [filler] for n in range(1001, 1006)})
    materios = _materios(config, store, chain)
    materios.start_at(1000)
    posts = Posts()
    clock = [_at("2026-09-27T01:01:00")]
    watch = _watch(config, store, [materios, cardano], posts, clock)
    watch.cycle()
    assert store.get("cursor:materios-preprod") == "1001"
    assert "outflow from custody-a" in posts.text()
    for _ in range(4):
        clock[0] += 1.0
        watch.cycle()
    assert store.get("cursor:materios-preprod") == "1005"


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


def test_a_finalized_head_that_stops_advancing_is_a_source_failure(config, tmp_path):
    """A node that has stopped following the chain still answers every read; without this
    the watcher would report itself healthy while it watched nothing."""
    store = cw.Store(str(tmp_path / "state.db"))
    chain = FakeChain(head=1000, blocks={}, state_at={1000, 1001})
    source = _materios(config, store, chain)
    source.poll(1.0)
    source.poll(1.0 + config.source_stale_seconds)
    with pytest.raises(cw.SourceError, match="finalized head #1000 has not advanced for 15 min"):
        source.poll(2.0 + config.source_stale_seconds)
    chain.head = 1001
    assert source.poll(3.0 + config.source_stale_seconds)


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
            if params.get("order") == "desc":
                value = sorted(value, key=lambda row: (row["block_height"], row["tx_index"]), reverse=True)
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
    assert finding.group == "cardano-mainnet unclassifiable"


def test_an_address_blockfrost_has_never_seen_has_no_transactions(config, tmp_path):
    store, network, api, source = _baselined(config, tmp_path)
    assert source.poll(_at("2026-09-27T01:01:00"))
    assert store.findings() == []


def test_a_cardano_tip_older_than_the_stale_window_is_a_source_failure(config, tmp_path):
    store, network, api, source = _baselined(config, tmp_path)
    with pytest.raises(cw.SourceError, match="tip"):
        source.poll(_at("2026-09-27T01:30:00"))


@pytest.fixture
def covered():
    return rules.parse_config(coverage_doc())


def _covered_source(covered, tmp_path, balance, api_class=None):
    """cardano-mainnet with its pool holding ``balance`` cMATRA, quarantine holding what it
    held at the pin, and one surrender two days before the digest."""
    network = _mainnet(covered)
    store = cw.Store(str(tmp_path / "state.db"))
    api = (api_class or FakeBlockfrost)(tip=13_989_360, time=int(_at("2026-09-28T13:00:00")))
    api.routes[f"/addresses/{network.pool.address}"] = {"amount": [
        {"unit": "lovelace", "quantity": "1500000"}, {"unit": network.pool.cmatra_unit, "quantity": str(balance)}]}
    api.routes[f"/addresses/{network.pool.quarantine_address}"] = {"amount": [
        {"unit": u.unit, "quantity": str(u.quarantined)} for u in network.pool.coverage.units if u.quarantined]}
    api.add_tx("surrender_agent", address=network.pool.address, height=13_989_350)
    source = cw.CardanoSource(network, api, store, stale_seconds=900)
    source.start_at(13_989_360)
    return store, api, source


def test_the_daily_digest_reports_whether_the_pool_covers_what_is_outstanding(covered, tmp_path):
    store, api, source = _covered_source(covered, tmp_path, TODAYS_POOL)
    posts = Posts()
    _watch(covered, store, [source], posts, [_at("2026-09-28T13:00:05")]).cycle()
    [digest] = [p["content"] for p in posts.payloads]
    assert ("cardano-mainnet surrender pool: 333,944,276.732371 cMATRA against 344,302,945.315701 cMATRA "
            "outstanding at the pinned rates, 96.99% covered (10,358,668.583330 cMATRA short)") in digest
    assert "redeemed in the last 7 days: 1,056.778496 cMATRA; 61 days to the 2026-11-29 deadline" in digest


def test_a_pool_below_its_floor_pages_an_alert_once_a_day_and_the_digest_still_goes(covered, tmp_path):
    store, api, source = _covered_source(covered, tmp_path, 300_000_000_000_000)
    posts = Posts()
    clock = [_at("2026-09-28T13:00:05")]
    watch = _watch(covered, store, [source], posts, clock)
    for _ in range(3):
        watch.cycle()
        clock[0] += 60
    digest, page = posts.payloads
    assert "daily digest" in digest["content"] and "87.13% covered" in digest["content"]
    assert page["content"].startswith("⚠️ **ALERT**") and page["allowed_mentions"] == {"parse": []}
    assert "covers 87.13% of what is outstanding, below the 90% floor" in page["content"]


def test_the_weeks_pace_is_read_from_the_pools_newest_transactions(covered, tmp_path):
    # Anyone can pay dust into the pool. Read oldest first, 250 such payments early in the
    # week left the week's surrenders unread and the pace at nearly nothing.
    store, api, source = _covered_source(covered, tmp_path, TODAYS_POOL)
    pool = _mainnet(covered).pool
    now = _at("2026-09-28T13:00:05")
    listing = api.routes[f"/addresses/{pool.address}/transactions"]

    def paid(tx_hash: str, height: int, when: float, cmatra: int) -> None:
        listing.append({"tx_hash": tx_hash, "tx_index": 0, "block_height": height, "block_time": when})
        held = [{"address": pool.address, "amount": [{"unit": pool.cmatra_unit, "quantity": str(10 ** 15)}]}]
        api.routes[f"/txs/{tx_hash}/utxos"] = {"inputs": held if cmatra else [], "outputs": [
            {"address": pool.address, "amount": [{"unit": pool.cmatra_unit, "quantity": str(10 ** 15 - cmatra)}]}]}

    for i in range(250):
        paid(f"d{i:063x}", 13_960_000 + i, now - 6 * cw.DAY + i, 0)
    for i in range(6):
        paid(f"e{i:063x}", 13_989_300 + i, now - cw.DAY + i, 3_400_000_000_000)
    lines, _ = source.coverage(now)
    assert "redeemed in the last 7 days: at least 20,401,056.778496 cMATRA (57 transactions left unread)" in lines[1]


class FailingPoolRead(FakeBlockfrost):
    """Blockfrost failing every read of what the pool address holds."""

    def get(self, path, **params):
        if path == "/addresses/addr1w8s6rqdjlzm5he27v9s202p8vjumza8qfsmufm2f6dy68hg9mn27a":
            self.calls.append((path, params))
            raise cw.SourceError("blockfrost answered HTTP 500 for the pool")
        return super().get(path, **params)


def test_a_coverage_reading_that_fails_is_reported_in_the_digest_and_not_retried_that_day(covered, tmp_path):
    store, api, source = _covered_source(covered, tmp_path, TODAYS_POOL, FailingPoolRead)
    posts = Posts(fail={1})
    clock = [_at("2026-09-28T13:00:05")]
    watch = _watch(covered, store, [source], posts, clock)
    watch.cycle()
    clock[0] += 120
    watch.cycle()
    [digest] = [p["content"] for p in posts.payloads]
    assert "cardano-mainnet surrender pool coverage not read: SourceError: blockfrost answered HTTP 500" in digest
    assert "alive" in digest
    pool = _mainnet(covered).pool.address
    assert [c[0] for c in api.calls].count(f"/addresses/{pool}") == 1


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
    sources = [cw.MateriosSource(config.materios, chain, store, config.source_stale_seconds),
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
