"""A node that stops answering without dropping its sockets costs a call its budget,
never more, and is dialled again.

On 2026-11-16 the node's host was overloaded. A node in that state keeps its TCP
connections (the kernel still acknowledges) and answers nothing, or accepts a new
connection and never answers its websocket upgrade. Neither a dial nor the drop of
a socket may wait on such a node: websocket-client's ``close()`` waits for the peer's
close frame behind the read lock that a call abandoned on the socket still holds, and
a dial given no timeout waits for a TCP handshake, an upgrade or its first read for
as long as the node stays silent. Either freezes the calling thread, which in
custody-watch is the only one: every source, the blind page and the dead-man ping,
until systemd's watchdog 15 minutes later.

These run the real websocket-client and substrate-interface against loopback sockets.
"""
import dataclasses
import threading
import time

import pytest

from daemon import custody_watch as cw
from daemon.config import DaemonConfig
from daemon.substrate_client import RPCTimeoutError, SubstrateClient
from tests.loopback_node import Caller, LoopbackNode, unanswered_port
from tests.test_custody_watch import FakeChain, config  # noqa: F401

HEAD = 1829300
BUDGET = 1.0
# What a call may take past its budgets on a loaded host.
SLACK = 3.0
HEALTH = {"peers": 3, "isSyncing": False, "shouldHavePeers": True}


@pytest.fixture
def budget(monkeypatch):
    monkeypatch.setenv("MATERIOS_RPC_TIMEOUT_SECS", str(BUDGET))


@pytest.fixture
def node():
    ledger = FakeChain(head=HEAD, blocks={}, state_at={HEAD})
    node = LoopbackNode(lambda method, params: HEALTH if method == "system_health" else ledger.rpc(method, params))
    yield node
    node.close()


def _custody_source(config, tmp_path, url, on):
    """The Materios source ``build_sources`` gives the watcher, made on the thread
    that polls it."""
    def build():
        store = cw.Store(str(tmp_path / "state.db"))
        [source] = cw.build_sources(dataclasses.replace(config, cardano=()), store, materios_rpc=url)
        return source
    source, _ = on(build)
    return source


@pytest.fixture
def abandoned():
    """The calls this test left reading a socket, once they have had 5 s to leave it."""
    before = set(threading.enumerate())

    def still_reading():
        until = time.monotonic() + 5.0
        while True:
            calls = [t.name for t in set(threading.enumerate()) - before if t.name.startswith("substrate-rpc-")]
            if not calls or time.monotonic() >= until:
                return calls
            time.sleep(0.05)
    return still_reading


def test_sockets_that_go_silent_cost_each_poll_its_budget_and_are_replaced(config, tmp_path, budget, node, abandoned):
    on = Caller()
    source = _custody_source(config, tmp_path, node.url, on)
    assert on(lambda: source.poll(1.0))[0] is True
    node.silence()

    for now in (7.0, 13.0):
        outcome, seconds = on(lambda: source.poll(now))
        assert isinstance(outcome, RPCTimeoutError), outcome
        assert seconds < 2 * BUDGET + SLACK, "a call's budget and a dial's bound a poll"

    outcome, seconds = on(lambda: source.poll(19.0))
    assert outcome is True, "the poll after the second timeout reads a new socket"
    assert len(node.sockets) == 3
    assert abandoned() == [], "the call abandoned on the silent socket was never woken"


def test_a_node_that_never_answers_the_upgrade_costs_each_poll_its_dials_budgets(config, tmp_path, budget, node):
    on = Caller()
    source = _custody_source(config, tmp_path, node.url, on)
    assert on(lambda: source.poll(1.0))[0] is True
    node.answers_upgrades = False
    node.drop_sockets()

    for now in (7.0, 13.0, 19.0):
        outcome, seconds = on(lambda: source.poll(now))
        assert isinstance(outcome, Exception), outcome
        assert seconds < 2 * BUDGET + SLACK, "each conn's dial is bounded by the RPC budget"

    node.answers_upgrades = True
    outcome, _ = on(lambda: source.poll(25.0))
    assert outcome is True


def test_a_dial_whose_first_read_is_never_answered_ends_within_the_budget(budget, node):
    node.answers_calls = False
    client = SubstrateClient(DaemonConfig(rpc_url=node.url))
    connected, seconds = Caller()(client.connect)
    assert connected is False
    assert seconds < 2 * BUDGET + SLACK, "the write and the read dial each end at the RPC budget"


def test_a_dial_that_tcp_never_answers_ends_within_the_budget(budget):
    port, held = unanswered_port()
    client = SubstrateClient(DaemonConfig(rpc_url=f"ws://127.0.0.1:{port}"))
    connected, seconds = Caller()(client.connect)
    assert connected is False
    assert seconds < 2 * BUDGET + SLACK, "the write and the read dial each end at the RPC budget"
    del held


def test_the_cert_daemons_keepalive_replaces_a_silent_write_socket_without_waiting_for_its_close(
        budget, node, abandoned):
    on = Caller()
    client = SubstrateClient(DaemonConfig(rpc_url=node.url))
    assert on(client.connect)[0] is True
    assert on(client.keepalive)[0] is None
    node.silence()

    for _ in range(client._reconnect_after_timeouts):
        outcome, seconds = on(client.keepalive)
        assert isinstance(outcome, RPCTimeoutError), outcome
        assert seconds < 2 * BUDGET + SLACK

    outcome, seconds = on(client.keepalive)
    assert outcome is None, "the keepalive after the replace runs on a new socket"
    assert len(node.sockets) == 3
    assert abandoned() == [], "the call abandoned on the silent socket was never woken"


def test_a_dialled_socket_waits_as_long_as_a_call_does(budget, node):
    """The dial's timeout is lifted once the node has answered: a submit waits for
    its inclusion on the socket for as long as the call's own budget allows."""
    client = SubstrateClient(DaemonConfig(rpc_url=node.url))
    assert client.connect()
    assert client._write.si.websocket.gettimeout() is None and client._read.si.websocket.gettimeout() is None
