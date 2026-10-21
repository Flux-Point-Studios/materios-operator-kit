"""A read conn left without an interface is reopened in the background (#70).

A failed connect, or a replace that gave up, leaves the read conn empty, and
reads then run on the write conn behind its submits (#69). Nothing else
reopens the read conn while the write conn stays healthy: the poll loop
reconnects only on an error, and a read served by the write conn raises none.
A read that finds the read conn empty is served by the write conn at once and
starts a background reopen, which retries with the conn's reconnect backoff.
The degraded mode is logged at error level while it lasts. The reopen holds
the conn's replace lock only while it dials, never through its backoff.
"""
import threading
import time

import pytest
from websocket import WebSocketConnectionClosedException

from daemon import substrate_client
from daemon.config import DaemonConfig
from daemon.substrate_client import SubstrateClient
from tests.fake_chain import FakeChain, dialling_client, fake_endpoint

HEALTH = {"peers": 3, "isSyncing": False}
# The longest a reopen backoff sleep waits for its wake.
SLEEP_CAP = 2.0


class ReopenBackoff:
    """A ``_backoff_sleep`` that returns at once except on the background
    reopen's thread, where each sleep waits for ``wake``, at most SLEEP_CAP.
    ``asleep`` is set while the reopen sleeps, and ``slept`` counts the sleeps
    it has finished."""

    def __init__(self):
        self.asleep, self.wake = threading.Event(), threading.Event()
        self.slept = 0

    def __call__(self, _delay):
        if threading.current_thread().name != "substrate-read-reopen":
            return
        self.asleep.set()
        self.wake.wait(SLEEP_CAP)
        self.asleep.clear()
        self.slept += 1


def read_conn_left_empty(how):
    """A client whose read conn ``how`` left empty, the endpoint now down,
    its write conn's interface, and the endpoint."""
    endpoint = fake_endpoint(FakeChain())
    if how == "connect":
        def write_then_refuse():
            if endpoint.opened:
                raise ConnectionRefusedError(111, "Connection refused")
            return endpoint()

        client = dialling_client(write_then_refuse)
        client._create_substrate_interface = endpoint
        endpoint.up = False
    else:
        client = dialling_client(endpoint)
        endpoint.opened[1].failure = BrokenPipeError(32, "Broken pipe")
        endpoint.up = False
        with pytest.raises(BrokenPipeError):
            client.rpc("system_health", [])
    assert client._read.si is None
    return client, endpoint.opened[0], endpoint


def settled(client):
    """Waits for the background reopen, if one is running, to finish."""
    reopen = getattr(client, "_read_reopen", None)
    if reopen is not None:
        reopen.join(5)


@pytest.mark.parametrize("how", ["connect", "replace"])
def test_a_read_conn_left_empty_is_reopened_and_reads_leave_the_write_conn(how, caplog):
    client, write, endpoint = read_conn_left_empty(how)

    assert client.rpc("system_health", []) == HEALTH
    settled(client)
    assert client._read.si is None, "the endpoint is still down"
    assert "the read conn is down: reads run on the write conn" in caplog.text

    endpoint.up = True
    assert client.rpc("system_health", []) == HEALTH
    settled(client)

    reopened = client._read.si
    assert reopened is endpoint.opened[-1] and reopened is not write
    assert client.rpc("system_health", []) == HEALTH
    assert write.calls == ["system_health", "system_health"], "reads stayed on the write conn"
    assert reopened.calls == ["system_health"]
    assert "force-replace[read]: new SubstrateInterface online" in caplog.text


def test_a_read_never_waits_for_the_reopen():
    client, write, endpoint = read_conn_left_empty("replace")
    connect = client._create_substrate_interface

    def slow_connect():
        time.sleep(1)
        return connect()

    client._create_substrate_interface = slow_connect
    endpoint.up = True

    started = time.monotonic()
    assert client.rpc("system_health", []) == HEALTH
    assert time.monotonic() - started < 0.5, "a read waited for the read conn's reopen"
    settled(client)
    assert client._read.si is endpoint.opened[-1]


def test_reads_on_the_old_socket_never_wait_out_the_reopens_backoff(monkeypatch):
    """The poll loop's reconnect, its read dial refused, empties the read conn
    while one read is on the conn's socket and two more wait for it. A fresh
    read runs on the write conn and starts the reopen. The old socket dies
    while the reopen sleeps between attempts. Each of the three takes the
    conn's replace lock, finds its socket gone and raises at once. A reopen
    that held the lock through its backoff kept them, the event loop's own
    reads among them, for its whole cycle: up to 240 s at the defaults. Here
    the queued reads hear of the replacement only as the socket's holder
    leaves, after the reopen has started."""
    monkeypatch.setattr(substrate_client, "REPLACED_CHECK_SECS", 10)
    endpoint = fake_endpoint(FakeChain())
    client = dialling_client(endpoint)
    backoff = client._backoff_sleep = ReopenBackoff()
    on_socket, dies = threading.Event(), threading.Event()

    def dying_socket(method, params):
        on_socket.set()
        dies.wait(5)
        raise BrokenPipeError(32, "Broken pipe")

    endpoint.opened[1].rpc_request = dying_socket
    outcomes = {}

    def read(tag):
        try:
            outcomes[tag] = client.rpc("system_health", [])
        except Exception as error:  # noqa: BLE001
            outcomes[tag] = (type(error), backoff.slept)

    readers = [threading.Thread(target=read, args=("on the socket",))]
    readers[0].start()
    assert on_socket.wait(5)
    readers += [threading.Thread(target=read, args=(f"queued {n}",)) for n in range(2)]
    for reader in readers[1:]:
        reader.start()
    time.sleep(0.2)

    def write_dial_only():
        if len(endpoint.opened) == 2:
            return endpoint()
        raise ConnectionRefusedError(111, "Connection refused")

    client._create_substrate_interface = write_dial_only
    assert client.connect() and client._read.si is None
    assert client.rpc("system_health", []) == HEALTH
    assert backoff.asleep.wait(5), "the reopen never reached its backoff"
    assert client.rpc("system_health", []) == HEALTH
    dies.set()
    for reader in readers:
        reader.join(3 * SLEEP_CAP)
    backoff.wake.set()
    client._read_reopen.join(3 * SLEEP_CAP)

    assert outcomes == {
        "on the socket": (BrokenPipeError, 0),
        "queued 0": (WebSocketConnectionClosedException, 0),
        "queued 1": (WebSocketConnectionClosedException, 0),
    }, "(error, backoff sleeps the reopen had finished): a read waited out the reopen's backoff"
    assert endpoint.opened[2].calls == ["system_health", "system_health"]


def test_the_reopen_never_dials_alongside_another_replace_of_the_read_conn():
    """The poll loop's reconnect hands the read conn a new socket while the
    reopen sleeps between attempts. That socket dies under a read, whose
    replace is still dialling when the reopen wakes. The reopen waits for the
    replace to finish and, finding the conn reopened, stops: the conn is
    dialled once, and keeps the replacement its read is answered on."""
    client, _, endpoint = read_conn_left_empty("replace")
    backoff = client._backoff_sleep = ReopenBackoff()
    assert client.rpc("system_health", []) == HEALTH
    assert backoff.asleep.wait(5), "the reopen never reached its backoff"
    endpoint.up = True
    assert client.connect()
    client._read.si.failure = BrokenPipeError(32, "Broken pipe")
    dialled, dialling, overlap = [], threading.Event(), threading.Event()

    def slow_dial():
        dialled.append(threading.current_thread().name)
        if not dialling.is_set():
            dialling.set()
            overlap.wait(0.5)
        else:
            overlap.set()
        return endpoint()

    client._create_substrate_interface = slow_dial
    answers = []
    reader = threading.Thread(target=lambda: answers.append(client.rpc("system_health", [])), name="poll-read")
    reader.start()
    assert dialling.wait(5), "the read's replace never dialled"
    backoff.wake.set()
    reader.join(5)
    client._read_reopen.join(5)

    assert dialled == ["poll-read"] and not overlap.is_set()
    assert answers == [HEALTH] and client._read.si is endpoint.opened[-1]
    assert client._read.si.calls == ["system_health"]


def test_a_client_that_never_connected_dials_nothing():
    """Fixtures that wire only the write conn: reads run there, and nothing dials."""
    endpoint = fake_endpoint(FakeChain())
    client = SubstrateClient(DaemonConfig(rpc_url="ws://test-rpc:9944"))
    client._create_substrate_interface = endpoint
    client.substrate = endpoint()

    assert client.rpc("system_health", []) == HEALTH
    settled(client)
    assert len(endpoint.opened) == 1 and client._read.si is None


def test_the_give_up_says_what_each_conn_does_next(caplog):
    client, _, endpoint = read_conn_left_empty("replace")
    endpoint.opened[0].failure = BrokenPipeError(32, "Broken pipe")
    with pytest.raises(BrokenPipeError):
        client._call("rpc_request", "system_health", [], conn=client._write)

    assert ("force-replace[read]: gave up after 2 attempts; reads run on the write conn until a "
            "background reopen succeeds") in caplog.text
    assert "force-replace[write]: gave up after 2 attempts; the next call on it reconnects first" in caplog.text
