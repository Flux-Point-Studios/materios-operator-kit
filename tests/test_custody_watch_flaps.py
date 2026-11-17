"""custody-watch says a source is unreadable however it fails, and says it once.

A source is blind when it has not been read to its head for ``source_blind_seconds``
and at least two polls failed since. Both are kept in the state file, so a watcher
that restarts while its source is down still pages. Counting from the last poll that
reached the head, not from the first failure of a run, a source that fails and reads
by turns without ever catching up is blind too, and the dead-man is not pinged for it.
One failed poll pages nothing, however slowly the source is polled. A source that
keeps reaching its head between failures is not blind: once at least half its polls
of the last 10 minutes have failed it raises one ALERT an hour.

Each scenario is run second by second on a fake clock.
"""
import logging

import pytest

from daemon import custody_rules as rules
from daemon import custody_watch as cw
from tests.test_custody_watch import FakeChain, Posts, StubSource, _at, _materios, config  # noqa: F401

T0 = _at("2026-11-16T18:00:00")
MATERIOS = "materios-preprod"
CARDANO = "cardano-mainnet"
BLIND = "custody-watch blind on"
RECOVERED = "recovered, watching again"
UNRELIABLE = "polls in the last 10 min"
FORMER_SUDO_KEY = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"


class Scripted(StubSource):
    """A source whose poll fails, reads part of its backlog or reaches its head by the
    second since T0: ``state(t)`` is "fail", "behind" or "ok"."""

    def __init__(self, name, clock, state, poll_seconds=6):
        super().__init__(name)
        self.poll_seconds = poll_seconds
        self.clock, self.state = clock, state

    def poll(self, now):
        self.polls += 1
        state = self.state(self.clock[0] - T0)
        if state == "fail":
            raise cw.SourceError(f"{self.name} unreachable")
        return state == "ok"


class Timeline:
    """One state file and one clock, across as many watchers as the scenario starts.
    ``pages`` holds each page's second since T0 and its text, ``pings`` the second
    of each dead-man ping."""

    def __init__(self, config, tmp_path):
        self.config = config
        self.db = str(tmp_path / "state.db")
        self.clock = [T0]
        self.posts = Posts()
        self.pages: list[tuple[int, str]] = []
        self.pings: list[int] = []
        store = cw.Store(self.db)
        store.put("digest:last-day", "2026-11-16")
        store.close()

    def run(self, sources, seconds):
        """A watcher over ``sources`` cycled once a second for ``seconds``."""
        store = cw.Store(self.db)
        watch = cw.Watch(self.config, store, sources, self.posts, clock=lambda: self.clock[0],
                         ping=lambda: self.pings.append(int(self.clock[0] - T0)))
        for _ in range(seconds):
            sent = len(self.posts.payloads)
            watch.cycle()
            self.pages += [(int(self.clock[0] - T0), p["content"]) for p in self.posts.payloads[sent:]]
            self.clock[0] += 1
        store.close()

    def of(self, text):
        return [(t, page) for t, page in self.pages if text in page]

    def criticals(self):
        return self.of("**CRITICAL**")


def _longest_gap(times, end):
    points = [*times, end]
    return max(b - a for a, b in zip(points, points[1:]))


def test_a_source_that_fails_two_thirds_of_the_time_yet_reaches_its_head_raises_one_alert_an_hour(config, tmp_path):
    timeline = Timeline(config, tmp_path)
    source = Scripted(MATERIOS, timeline.clock, lambda t: "fail" if t % 60 < 40 else "ok")
    timeline.run([source], 2 * 3600)

    assert timeline.criticals() == []
    first, second = timeline.of(UNRELIABLE)
    assert timeline.pages == [first, second]
    assert "**ALERT**" in first[1] and f"custody-watch: {MATERIOS} failed" in first[1]
    assert config.source_blind_seconds <= first[0] < 120
    assert 3600 <= second[0] - first[0] < 3660
    assert _longest_gap(timeline.pings, 2 * 3600) <= 120, "it is read to its head every minute: the dead-man is pinged"


def test_a_flapping_source_that_never_reaches_its_head_pages_blind_once_and_stops_the_dead_man(config, tmp_path):
    timeline = Timeline(config, tmp_path)

    def state(t):
        if t < 1:
            return "ok"
        return "fail" if (t - 6) % 60 < 40 else "behind"
    timeline.run([Scripted(MATERIOS, timeline.clock, state)], 3600)

    [blind] = timeline.of(BLIND)
    assert blind[0] == config.source_blind_seconds
    assert f"{BLIND} {MATERIOS} since 2026-11-16 18:00:00Z" in blind[1] and "@here" in blind[1]
    assert timeline.of(UNRELIABLE) == [], "a source paged as blind is not also paged as unreliable"
    assert [page for page in timeline.pages if page != blind] == timeline.of("stale for")
    assert timeline.pings == [0]


@pytest.mark.parametrize("read_before", [True, False], ids=["read-before", "never-read"])
def test_a_watcher_restarting_every_50_s_through_an_outage_still_pages_blind_once(config, tmp_path, read_before):
    timeline = Timeline(config, tmp_path)
    for _ in range(12):
        source = Scripted(MATERIOS, timeline.clock, lambda t: "ok" if read_before and t < 1 else "fail")
        timeline.run([source], 50)
        timeline.clock[0] += 30

    [blind] = timeline.pages
    assert BLIND in blind[1] and "**CRITICAL**" in blind[1]
    assert blind[0] < 2 * 80, "paged by the second start, not after an uninterrupted minute"
    assert timeline.pings == ([0] if read_before else [])


@pytest.mark.parametrize("poll_seconds", [60, 120, 300])
def test_one_failed_poll_pages_nothing_however_slowly_the_source_is_polled(config, tmp_path, poll_seconds):
    timeline = Timeline(config, tmp_path)
    source = Scripted(CARDANO, timeline.clock, lambda t: "fail" if 600 <= t < 601 else "ok", poll_seconds)
    timeline.run([source], 1500)

    assert timeline.pages == []
    assert _longest_gap(timeline.pings, 1500) <= poll_seconds + 60, (
        "the dead-man misses pings for one poll interval and one ping interval")


def test_a_slow_source_whose_first_poll_fails_pages_nothing(config, tmp_path):
    timeline = Timeline(config, tmp_path)
    timeline.run([Scripted(CARDANO, timeline.clock, lambda t: "fail" if t < 1 else "ok", poll_seconds=300)], 1000)
    assert timeline.pages == []


def test_two_failed_polls_of_a_slow_source_page_blind_from_when_it_was_last_read(config, tmp_path):
    timeline = Timeline(config, tmp_path)
    source = Scripted(CARDANO, timeline.clock, lambda t: "fail" if 600 <= t < 721 else "ok", poll_seconds=120)
    timeline.run([source], 1500)

    blind, recovered = timeline.pages
    assert blind[0] == 720 and f"{BLIND} {CARDANO} since 2026-11-16 18:08:00Z" in blind[1]
    assert recovered[0] == 840 and RECOVERED in recovered[1]


def test_a_source_polled_less_often_than_the_blind_threshold_is_blind_after_its_own_interval(config, tmp_path, caplog):
    """The threshold cannot be shorter than the time between two polls. The watcher
    says so when it starts, and restarts that make two polls fail early change nothing."""
    caplog.set_level(logging.WARNING)
    timeline = Timeline(config, tmp_path)

    def source():
        return Scripted(CARDANO, timeline.clock, lambda t: "ok" if t < 1 else "fail", poll_seconds=180)
    timeline.run([source()], 10)
    timeline.run([source()], 10)
    timeline.run([source()], 220)

    widened = [r.getMessage() for r in caplog.records if "source_blind_seconds" in r.getMessage()]
    assert len(widened) == 3 and all(CARDANO in line and "180" in line and "60" in line for line in widened)
    [blind] = timeline.pages
    assert blind[0] == 180 and BLIND in blind[1]


def test_a_source_down_for_two_thousand_polls_is_still_retried_once_a_minute(config, tmp_path):
    store = cw.Store(str(tmp_path / "state.db"))
    clock = [T0]
    source = StubSource(MATERIOS, fail=True)
    source.poll_seconds = 6
    watch = cw.Watch(config, store, [source], Posts(), clock=lambda: clock[0])
    for _ in range(2000):
        watch.cycle()
        clock[0] += cw.RETRY_CAP
    assert source.polls == 2000

    polls = source.polls
    clock[0] -= 1
    watch.cycle()
    assert source.polls == polls, "retried before the capped delay"


def test_an_outage_that_was_paged_does_not_raise_the_unreliable_alert_once_it_ends(config, tmp_path):
    timeline = Timeline(config, tmp_path)
    timeline.run([Scripted(MATERIOS, timeline.clock, lambda t: "fail" if 100 <= t < 500 else "ok")], 1500)

    blind, recovered = timeline.pages
    assert BLIND in blind[1] and RECOVERED in recovered[1]


def test_each_outage_pages_its_own_recovery_and_leaves_no_mark_behind(config, tmp_path):
    timeline = Timeline(config, tmp_path)

    def state(t):
        if 100 <= t < 300:
            return "fail"
        return "behind" if 1000 <= t < 2000 else "ok"
    timeline.run([Scripted(MATERIOS, timeline.clock, state)], 2100)

    first, second = timeline.of(RECOVERED)
    assert [page for _, page in timeline.pages] == [
        timeline.of(BLIND)[0][1], first[1], timeline.of("stale for")[0][1], second[1]]
    assert "unreadable since 2026-11-16 18:01:36Z" in first[1]
    assert "unreadable since 2026-11-16 18:16:36Z" in second[1]
    store = cw.Store(timeline.db)
    assert [store.get(f"health:{MATERIOS}:{mark}") for mark in ("blind-since", "stale-since", "failed")] == [None] * 3


def test_a_recovery_names_when_the_source_was_last_read_not_when_the_watcher_restarted(config, tmp_path):
    timeline = Timeline(config, tmp_path)
    timeline.run([Scripted(MATERIOS, timeline.clock, lambda t: "ok" if t < 1 else "fail")], 300)
    timeline.run([Scripted(MATERIOS, timeline.clock, lambda t: "fail" if t < 1300 else "ok")], 1100)

    assert len(timeline.of(BLIND)) == 1 and len(timeline.of("stale for")) == 1
    [recovered] = timeline.of(RECOVERED)
    assert "unreadable since 2026-11-16 18:00:00Z" in recovered[1]


def test_a_node_whose_finalized_head_is_behind_the_cursor_is_a_failed_poll(config, tmp_path):
    """A node resyncing from an older database answers every read, with a head the
    watcher is already past: nothing new is being read from it."""
    head = 1829300
    store = cw.Store(str(tmp_path / "state.db"))
    source = _materios(config, store, FakeChain(head=head, blocks={}, state_at={head}))
    source.start_at(head + cw.HEAD_SLACK_BLOCKS)
    assert source.poll(1.0), "a head a few blocks back is a node finalizing just behind another"

    cursor = head + cw.HEAD_SLACK_BLOCKS + 1
    source.start_at(cursor)
    with pytest.raises(cw.SourceError, match=f"finalized head #{head} is behind the cursor #{cursor}"):
        source.poll(7.0)


def test_a_node_resyncing_below_the_cursor_pages_blind_and_stops_the_dead_man(config, tmp_path):
    head = 1829300
    timeline = Timeline(config, tmp_path)
    store = cw.Store(timeline.db)
    chain = FakeChain(head=head, blocks={}, state_at={head})
    source = _materios(config, store, chain)
    source.start_at(head)
    watch = cw.Watch(config, store, [source], timeline.posts, clock=lambda: timeline.clock[0],
                     ping=lambda: timeline.pings.append(int(timeline.clock[0] - T0)))
    watch.cycle()
    chain.head = head - 500
    chain.state_at = {chain.head}
    chain.sudo_key = "0x" + rules.account_bytes(FORMER_SUDO_KEY).hex()
    for _ in range(120):
        timeline.clock[0] += 1
        watch.cycle()

    assert timeline.pings == [0]
    [page] = timeline.posts.payloads
    assert "Sudo.Key" not in page["content"], "an older block's Sudo.Key was compared with the one last read"
    assert f"{BLIND} {MATERIOS}" in page["content"] and "is behind the cursor" in page["content"]
