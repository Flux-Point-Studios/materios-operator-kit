"""custody-watch keeps watching through a lost websocket, and says so when it cannot.

On 2026-11-16 the Materios node's host was overloaded, custody-watch's socket to it
broke (BrokenPipeError on every poll from 18:01:48Z) and the watcher never dialled
again: it was blind for 53 minutes, through a runtime upgrade, and the dead-man had no
way to tell. A dead socket must be dropped and dialled again on a later poll, a poll
must never sleep through a reconnect backoff (the other sources wait behind it), a
source not read to its head for ``source_blind_seconds`` pages CRITICAL by name, and
the dead-man is not pinged while any source is failing. The read conn's background
reopen keeps its backoff: one thread, one dial per backoff step, however many reads
the write conn serves meanwhile.
"""
import dataclasses
import ssl
import threading

import pytest
from websocket import WebSocketConnectionClosedException

from daemon import custody_watch as cw
from tests.fake_chain import FakeWebSocket, kernel_timeout
from tests.test_custody_watch import FakeChain, Posts, StubSource, _at, _watch, config  # noqa: F401
from tests.test_read_conn_reopens import ReopenBackoff

HEAD = 1829300
DEAD_SOCKET_ERRORS = {
    "broken-pipe": lambda: BrokenPipeError(32, "Broken pipe"),
    "reset": lambda: ConnectionResetError(104, "Connection reset by peer"),
    "closed": lambda: WebSocketConnectionClosedException("socket is already closed."),
    "kernel-timeout": kernel_timeout,
    "tls-bad-record": lambda: ssl.SSLError(1, "[SSL: DECRYPTION_FAILED_OR_BAD_RECORD_MAC] decryption failed"),
}


class ChainSocket:
    """One websocket onto a custody ``FakeChain``; once ``failure`` is set every call
    raises it, as a dead socket does."""

    def __init__(self, ledger: FakeChain):
        self.ledger = ledger
        self.chain = "Materios Preprod v6"
        self.websocket = FakeWebSocket()
        self.failure: BaseException | None = None

    def rpc_request(self, method, params):
        if self.failure is not None:
            raise self.failure
        return {"jsonrpc": "2.0", "id": 1, "result": self.ledger.rpc(method, params)}

    @property
    def closed(self):
        return self.websocket.dropped


def _endpoint(ledger: FakeChain):
    """Opens a ``ChainSocket`` per dial into ``opened``; refuses while ``up`` is False."""
    def dial():
        dial.dials += 1
        if not dial.up:
            raise ConnectionRefusedError(111, "Connection refused")
        socket = ChainSocket(ledger)
        dial.opened.append(socket)
        return socket
    dial.up, dial.dials, dial.opened = True, 0, []
    return dial


def _production_source(config, tmp_path, reopen_sleeps=2.0):
    """The Materios source and the client ``build_sources`` gives it, dialling a fake
    endpoint. Its backoff records each sleep asked for inside a call instead of
    sleeping it; the background reopen's sleeps last ``reopen_sleeps`` seconds or
    until the backoff's ``wake`` is set."""
    store = cw.Store(str(tmp_path / "state.db"))
    [source] = cw.build_sources(dataclasses.replace(config, cardano=()), store, materios_rpc="ws://test-rpc:9945")
    ledger = FakeChain(head=HEAD, blocks={}, state_at={HEAD})
    endpoint = _endpoint(ledger)
    client = source._client
    client._create_substrate_interface = endpoint
    backoff = client._backoff_sleep = ReopenBackoff(reopen_sleeps)
    assert client.connect()
    return source, endpoint, backoff, store


def _store_past_the_digest(tmp_path):
    """A store whose daily digest for 2026-11-16 has gone out."""
    store = cw.Store(str(tmp_path / "state.db"))
    store.put("digest:last-day", "2026-11-16")
    return store


def _kill(endpoint, error):
    for socket in endpoint.opened:
        socket.failure = error


@pytest.mark.parametrize("error", DEAD_SOCKET_ERRORS.values(), ids=DEAD_SOCKET_ERRORS.keys())
def test_a_dead_socket_is_dialled_again_and_the_next_poll_reads_the_chain(config, tmp_path, error):
    source, endpoint, backoff, store = _production_source(config, tmp_path)
    assert source.poll(1.0)
    _kill(endpoint, error())
    endpoint.up = False
    with pytest.raises(Exception):
        source.poll(7.0)

    endpoint.up = True
    assert source.poll(13.0)
    assert store.get("cursor:materios-preprod") == str(HEAD)
    write, read = endpoint.opened[:2]
    assert write.closed and read.closed
    assert backoff.in_calls == []


def test_a_socket_that_dies_between_polls_is_replaced_within_the_poll(config, tmp_path):
    source, endpoint, _, _ = _production_source(config, tmp_path)
    _kill(endpoint, BrokenPipeError(32, "Broken pipe"))
    assert source.poll(1.0)
    assert len(endpoint.opened) > 2 and endpoint.opened[1].closed


def test_a_poll_dials_each_conn_at_most_once_and_never_sleeps_through_a_backoff(config, tmp_path):
    source, endpoint, backoff, _ = _production_source(config, tmp_path)
    _kill(endpoint, BrokenPipeError(32, "Broken pipe"))
    endpoint.up = False
    before = endpoint.dials
    for now in (1.0, 7.0, 13.0):
        with pytest.raises(Exception):
            source.poll(now)
    backoff.wake.set()
    source._client._read_reopen.join(5)
    assert backoff.in_calls == []
    assert endpoint.dials - before <= 3 * 2


def test_a_catch_up_poll_served_by_the_write_conn_starts_one_reopen_that_dials_once(
        config, tmp_path, caplog, monkeypatch):
    """The read conn is down, the write conn is not, and the node takes no new socket
    (at its connection limit). A poll 300 blocks behind makes 311 reads on the write
    conn. A reopen that ended after its one dial would be started again by each of
    them: 311 dials on as many threads, and three log lines apiece."""
    source, endpoint, backoff, store = _production_source(config, tmp_path, reopen_sleeps=120)
    assert source.poll(1.0)
    write, read = endpoint.opened[:2]
    read.failure = BrokenPipeError(32, "Broken pipe")
    endpoint.up = False
    with pytest.raises(BrokenPipeError):
        source.poll(7.0)
    write.ledger.head = HEAD + 300
    write.ledger.state_at = set(range(HEAD, HEAD + 301))
    reopens, start = [], threading.Thread.start

    def counted_start(thread):
        if thread.name == "substrate-read-reopen":
            reopens.append(thread)
        start(thread)

    monkeypatch.setattr(threading.Thread, "start", counted_start)
    dials = endpoint.dials
    caplog.clear()
    try:
        assert source.poll(13.0)
        assert backoff.asleep.wait(5), "the reopen never reached its backoff"
    finally:
        backoff.wake.set()

    assert store.get("cursor:materios-preprod") == str(HEAD + 300)
    assert len(reopens) == 1 and endpoint.dials - dials == 1
    assert caplog.text.count("the read conn is down") == 1
    assert len([r for r in caplog.records if r.name == "daemon.substrate_client"]) <= 3


def test_a_failing_source_is_retried_on_a_doubling_delay_capped_at_a_minute(config, tmp_path):
    store = cw.Store(str(tmp_path / "state.db"))
    clock = [_at("2026-11-16T18:01:48")]
    source = StubSource("materios-preprod", fail=True)
    source.poll_seconds = 6
    watch = _watch(config, store, [source], Posts(), clock)
    polled = []
    for _ in range(400):
        before = source.polls
        watch.cycle()
        if source.polls != before:
            polled.append(clock[0])
        clock[0] += 1
    gaps = [round(b - a) for a, b in zip(polled, polled[1:])]
    assert gaps[:5] == [6, 12, 24, 48, 60] and set(gaps[5:]) == {60}

    source.fail = False
    clock[0] += 60
    watch.cycle()
    for _ in range(3):
        before = source.polls
        clock[0] += 6
        watch.cycle()
        assert source.polls == before + 1

    source.fail = True
    for _ in range(2):
        before = source.polls
        clock[0] += 6
        watch.cycle()
        assert source.polls == before + 1, "the delay of an earlier run of failures was carried into this one"


def test_a_source_failing_past_the_blind_threshold_pages_critical_once_then_its_recovery(config, tmp_path):
    store = _store_past_the_digest(tmp_path)
    posts = Posts()
    clock = [_at("2026-11-16T18:01:42")]
    source = StubSource("materios-preprod")
    source.poll_seconds = 6
    watch = _watch(config, store, [source], posts, clock)
    watch.cycle()
    source.fail = True
    clock[0] = _at("2026-11-16T18:01:48")
    watch.cycle()
    while clock[0] < _at("2026-11-16T18:01:42") + config.source_blind_seconds - 1:
        clock[0] += 1
        watch.cycle()
    assert posts.payloads == []

    for _ in range(300):
        clock[0] += 1
        watch.cycle()
    [page] = posts.payloads
    assert page["content"].startswith("\U0001f6a8 **CRITICAL** @here")
    assert "custody-watch blind on materios-preprod since 2026-11-16 18:01:42Z" in page["content"]
    assert "unreachable" in page["content"]
    assert "4 polls failed since it was last read to its head" in page["content"]

    source.fail = False
    clock[0] += 60
    watch.cycle()
    assert len(posts.payloads) == 2
    assert "materios-preprod recovered" in posts.payloads[1]["content"]
    assert "unreadable since 2026-11-16 18:01:42Z" in posts.payloads[1]["content"]


def test_a_failure_shorter_than_the_blind_threshold_pages_nothing(config, tmp_path):
    store = _store_past_the_digest(tmp_path)
    posts = Posts()
    clock = [_at("2026-11-16T18:00:00")]
    source = StubSource("materios-preprod")
    source.poll_seconds = 6
    watch = _watch(config, store, [source], posts, clock)
    for second in range(600):
        source.fail = second % 120 < config.source_blind_seconds // 2
        watch.cycle()
        clock[0] += 1
    assert posts.payloads == []


def test_a_restart_of_a_blind_watcher_pages_the_recovery_it_owes(config, tmp_path):
    db = str(tmp_path / "state.db")
    clock = [_at("2026-11-16T18:00:00")]
    posts = Posts()
    _store_past_the_digest(tmp_path).close()
    watch = _watch(config, cw.Store(db), [StubSource("materios-preprod", fail=True)], posts, clock)
    for _ in range(config.source_blind_seconds + 30):
        watch.cycle()
        clock[0] += 1
    assert "blind on materios-preprod" in posts.text()

    store = cw.Store(db)
    _watch(config, store, [StubSource("materios-preprod")], posts, clock).cycle()
    assert "materios-preprod recovered" in posts.payloads[-1]["content"]


def test_the_blind_threshold_is_read_from_the_config_and_defaults_to_a_minute(config):
    from daemon import custody_rules as rules
    assert config.source_blind_seconds == 60
    doc = {"state_db": ":memory:", "source_blind_seconds": 120}
    assert rules.parse_config(doc).source_blind_seconds == 120
    with pytest.raises(ValueError, match="source_blind_seconds"):
        rules.parse_config({"state_db": ":memory:", "source_blind_seconds": 0})
