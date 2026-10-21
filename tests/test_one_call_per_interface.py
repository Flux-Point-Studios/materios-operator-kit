"""One call at a time on each SubstrateInterface (#72).

substrate-interface 1.8.1's websocket transport has no lock. Calls on one
interface share a single reply queue, and each reads the next frame off the
socket, which may be another call's reply. A call whose reply was taken waits
in ``recv()`` for a frame that never comes, until the bounded call's budget
runs out. ``_call`` admits one call at a time onto each interface. A call
that timed out leaves its worker on the socket, and the next call waits for
that worker to leave or for the interface to be replaced, never sharing the
socket with it.
"""
import json
import queue
import random
import threading
import time

from types import SimpleNamespace

import pytest
from substrateinterface import SubstrateInterface
from websocket import WebSocketConnectionClosedException

from daemon import substrate_client
from daemon.substrate_client import RPCTimeoutError
from tests.fake_chain import connected_client, dialling_client

HEALTH = {"peers": 3, "isSyncing": False}


class NodeSocket:
    """A websocket onto a node that answers each JSON-RPC request after a
    random 0-10 ms, so the replies to concurrent requests arrive in random
    order. An answer carries its request's params as its result."""

    connected = True

    def __init__(self, rng: random.Random):
        self.rng = rng
        self.frames: queue.Queue = queue.Queue()

    def send(self, text):
        request = json.loads(text)
        answer = json.dumps({"jsonrpc": "2.0", "id": request["id"], "result": request["params"]})
        threading.Timer(self.rng.uniform(0, 0.01), self.frames.put, [answer]).start()

    def recv(self):
        frame = self.frames.get()
        if frame is None:
            self.frames.put(None)
            raise WebSocketConnectionClosedException("socket is already closed.")
        return frame

    def close(self):
        self.connected = False
        self.frames.put(None)


def test_concurrent_reads_on_one_interface_each_get_their_own_reply():
    """The library's own rpc_request over a node that answers out of order:
    three readers at once, 30 times. Unserialized, a round stalls (a reader
    waits out its budget) more often than not."""
    rng = random.Random(72)
    client = dialling_client(lambda: SubstrateInterface(websocket=NodeSocket(rng), auto_discover=False))
    client._rpc_timeout_secs = 1.0

    for round_number in range(30):
        start = threading.Barrier(3)
        outcomes = {}

        def read(tag):
            start.wait(5)
            try:
                outcomes[tag] = client.rpc("probe_echo", [tag])
            except Exception as error:  # noqa: BLE001
                outcomes[tag] = error

        readers = [threading.Thread(target=read, args=(f"{round_number}-{n}",)) for n in range(3)]
        for reader in readers:
            reader.start()
        for reader in readers:
            reader.join(10)

        assert outcomes == {tag: [tag] for tag in outcomes} and len(outcomes) == 3, (
            f"round {round_number}: {outcomes}")


def wedge(si, method):
    """``si`` holds a call to ``method`` until the returned event is set or
    ``si`` is closed; every other call it answers at once."""
    leave = threading.Event()
    entered = threading.Event()
    answer, close = si.rpc_request, si.close

    def rpc_request(name, params):
        if name != method:
            return answer(name, params)
        si.calls.append(name)
        entered.set()
        leave.wait(10)
        return {"jsonrpc": "2.0", "id": 1, "result": "late"}

    si.rpc_request = rpc_request
    si.close = lambda: (close(), leave.set())
    return entered, leave


def test_a_call_that_timed_out_keeps_the_next_call_off_its_socket_until_it_leaves():
    client, endpoint = connected_client()
    client._rpc_timeout_secs = 0.2
    client._reconnect_after_timeouts = 10
    read = endpoint.opened[1]
    _, leave = wedge(read, "wedged")

    with pytest.raises(RPCTimeoutError):
        client.rpc("wedged", [])
    with pytest.raises(RPCTimeoutError, match="waiting behind"):
        client.rpc("queued", [])
    assert read.calls == ["wedged"], "a call reached a socket that a timed-out call was still reading"

    leave.set()
    assert client.rpc("system_health", []) == HEALTH
    time.sleep(0.3)
    assert read.calls == ["wedged", "system_health"], "a call that had timed out ran later"
    assert client._read.si is read and len(endpoint.opened) == 2


@pytest.mark.parametrize("close_wakes_the_worker", [True, False], ids=["close-wakes", "close-does-not-wake"])
def test_a_call_queued_on_a_replaced_interface_runs_on_the_replacement(close_wakes_the_worker):
    """A wedged call times out and its socket is replaced. A call that was
    queued behind it moves to the replacement within its own budget, whether
    or not closing the old socket woke the worker still reading it
    (websocket-client's close skips the shutdown that wakes a reader when it
    has already marked the socket closed)."""
    client, endpoint = connected_client()
    client._rpc_timeout_secs = 1.0
    client._reconnect_after_timeouts = 1
    read = endpoint.opened[1]
    entered, leave = wedge(read, "wedged")
    if not close_wakes_the_worker:
        read.close = lambda: setattr(read, "closed", True)
    outcome = {}

    def wedged():
        try:
            client.rpc("wedged", [])
        except RPCTimeoutError as error:
            outcome["wedged"] = error

    first = threading.Thread(target=wedged)
    first.start()
    assert entered.wait(5)
    time.sleep(0.3)
    started = time.monotonic()
    assert client.rpc("queued", []) == HEALTH
    elapsed = time.monotonic() - started
    first.join(5)
    leave.set()

    replacement = endpoint.opened[2]
    assert isinstance(outcome["wedged"], RPCTimeoutError)
    assert read.calls == ["wedged"] and replacement.calls == ["queued"]
    assert client._read.si is replacement
    assert elapsed < client._rpc_timeout_secs, f"the queued call waited out its budget ({elapsed:.2f}s)"


def test_calls_waiting_for_an_interface_run_in_the_order_they_arrived():
    """The socket goes to the longest waiter. A caller that asks again as soon
    as its call returns queues behind the others instead of starving them, so
    a wait is bounded by the calls ahead of it."""
    client, endpoint = connected_client()
    read = endpoint.opened[1]
    entered, leave = wedge(read, "first")

    def first_then_again():
        client.rpc("first", [])
        client.rpc("again", [])

    callers = [threading.Thread(target=first_then_again)]
    callers[0].start()
    assert entered.wait(5)
    for name in ("w1", "w2", "w3"):
        callers.append(threading.Thread(target=client.rpc, args=(name, [])))
        callers[-1].start()
        time.sleep(0.1)
    leave.set()
    for caller in callers:
        caller.join(10)

    assert read.calls == ["first", "w1", "w2", "w3", "again"]


def test_a_call_granted_the_socket_past_its_budget_never_runs(monkeypatch):
    """Its caller has given up on it by then: a call that reached the socket
    after its budget would run with nobody waiting for its result."""
    client, endpoint = connected_client()
    client._rpc_timeout_secs = 100
    clock = [0.0]
    monkeypatch.setattr(substrate_client, "time", SimpleNamespace(monotonic=lambda: clock[0], time=time.time))
    monkeypatch.setattr(substrate_client, "REPLACED_CHECK_SECS", 10)
    read = endpoint.opened[1]
    entered, leave = wedge(read, "first")
    outcome = {}

    def late():
        try:
            outcome["late"] = client.rpc("late", [])
        except RPCTimeoutError as error:
            outcome["late"] = error

    first = threading.Thread(target=client.rpc, args=("first", []))
    first.start()
    assert entered.wait(5)
    waiting = threading.Thread(target=late)
    waiting.start()
    time.sleep(0.2)
    clock[0] = 101.0
    leave.set()
    waiting.join(5)
    first.join(5)

    assert isinstance(outcome["late"], RPCTimeoutError)
    assert read.calls == ["first"]


def test_a_call_whose_worker_never_started_leaves_the_interface_free(monkeypatch):
    client, endpoint = connected_client()
    client._rpc_timeout_secs = 0.5
    start = threading.Thread.start

    def refuse_workers(thread):
        if thread.name.startswith("substrate-rpc-"):
            raise RuntimeError("can't start new thread")
        return start(thread)

    monkeypatch.setattr(threading.Thread, "start", refuse_workers)
    with pytest.raises(RuntimeError):
        client.rpc("system_health", [])
    monkeypatch.undo()

    assert client.rpc("system_health", []) == HEALTH
