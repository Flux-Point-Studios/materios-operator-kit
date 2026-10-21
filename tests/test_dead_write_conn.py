"""A websocket whose peer or proxy has dropped it raises on every call
(BrokenPipeError on send, a closed-connection error on receive) until its
SubstrateInterface is replaced, and substrate-interface never replaces it by
itself after a failed send. The 2026-10-20 incident: sat230's write conn, behind
Cloudflare, failed 9 of 9 receipts with BrokenPipeError for 34 minutes while
its read conn kept polling, until a container restart.

A transport-dead failure replaces only the conn it hit and retries the call
once on the fresh interface; anything else is the node's verdict and
propagates untouched. Imports stay within what df30747 already had, so the
receipt scenario runs against that tree as the positive control.
"""
import asyncio
import errno
import json
import ssl
import threading
import time

import pytest
from substrateinterface.exceptions import SubstrateRequestException
from websocket import WebSocketConnectionClosedException

from daemon import health_server
from daemon.substrate_client import RPCTimeoutError
from tests.fake_chain import (
    CALL_NOT_EXPECTED, RECEIPT_ID, FakeChain, attest_daemon, connected_client, fake_endpoint, health,
    kernel_timeout,
)


TRANSPORT_FAILURES = [
    BrokenPipeError(32, "Broken pipe"),
    ConnectionResetError(104, "Connection reset by peer"),
    ConnectionAbortedError(103, "Software caused connection abort"),
    WebSocketConnectionClosedException("Connection is closed"),
    ssl.SSLEOFError(8, "EOF occurred in violation of protocol"),
    OSError(errno.EPIPE, "Broken pipe"),
    OSError(errno.ECONNRESET, "Connection reset by peer"),
    # A send or recv on a socket another call closed (plain or TLS alike).
    OSError(errno.EBADF, "Bad file descriptor"),
    OSError(errno.ENOTCONN, "Transport endpoint is not connected"),
    OSError(errno.ENOTSOCK, "Socket operation on non-socket"),
    kernel_timeout(),
]
VERDICTS = [
    SubstrateRequestException(CALL_NOT_EXPECTED),
    SubstrateRequestException({"code": -32000, "message": "Client error: Execution failed"}),
    ValueError("Parameter 'claimed_hash' not specified"),
]


def test_a_dead_write_socket_is_replaced_and_the_receipt_attests(tmp_path):
    ledger = FakeChain()
    ledger.add_receipt(RECEIPT_ID, block=100)
    endpoint = fake_endpoint(ledger)
    daemon = attest_daemon(ledger, endpoint, tmp_path)
    write, read = endpoint.opened
    write.failure = BrokenPipeError(32, "Broken pipe")

    asyncio.run(daemon._process_block_range(100))

    assert [receipt for _, _, receipt in ledger.included] == [RECEIPT_ID]
    assert daemon.last_processed_block == 100
    assert write.closed and daemon.client._write.si is endpoint.opened[2]
    assert daemon.client._read.si is read and not read.closed


@pytest.mark.parametrize("failure", TRANSPORT_FAILURES, ids=lambda e: type(e).__name__)
def test_a_transport_failure_replaces_only_its_conn_and_retries_once(failure):
    client, endpoint = connected_client()
    write, read = endpoint.opened
    write.failure = failure

    assert health(client) == {"peers": 3, "isSyncing": False}

    fresh = endpoint.opened[2]
    assert client._write.si is fresh and write.closed
    assert write.calls == ["system_health"] and fresh.calls == ["system_health"]
    assert client._read.si is read and read.calls == [] and not read.closed


@pytest.mark.parametrize("verdict", VERDICTS, ids=lambda e: f"{type(e).__name__}-{str(e)[:24]}")
def test_a_node_verdict_is_not_a_transport_failure(verdict):
    client, endpoint = connected_client()
    write = endpoint.opened[0]
    write.failure = verdict

    with pytest.raises(type(verdict)) as raised:
        health(client)

    assert raised.value is verdict
    assert len(endpoint.opened) == 2 and client._write.si is write and not write.closed
    assert write.calls == ["system_health"]


def test_a_second_transport_failure_surfaces_after_one_retry():
    client, endpoint = connected_client()
    endpoint.opened[0].failure = BrokenPipeError(32, "Broken pipe")
    endpoint.failure = BrokenPipeError(32, "Broken pipe")

    with pytest.raises(BrokenPipeError):
        health(client)

    write, read, *fresh = endpoint.opened
    assert write.calls == ["system_health"]
    assert [si.calls for si in fresh] == [["system_health"], []], "retried exactly once"
    assert client._write.si is fresh[-1], "a conn that died on the retry is replaced too"
    assert client._read.si is read and read.calls == []


def test_a_dead_read_socket_replaces_the_read_conn_only():
    client, endpoint = connected_client()
    write, read = endpoint.opened
    read.failure = ConnectionResetError(104, "Connection reset by peer")

    assert client.rpc("system_health", []) == {"peers": 3, "isSyncing": False}

    fresh = endpoint.opened[2]
    assert client._read.si is fresh and read.closed
    assert client._write.si is write and write.calls == [] and not write.closed
    client.rpc("system_health", [])
    assert fresh.calls == ["system_health", "system_health"]


def test_calls_on_a_socket_that_dies_run_one_at_a_time_and_replace_it_once():
    """One call at a time runs on a socket (#72). The call on it finds it
    dead and replaces it. The call waiting behind it never shares the socket
    with it, and does not replace the replacement: it runs there."""
    client, endpoint = connected_client()
    write = endpoint.opened[0]
    in_flight, release, on_socket, most_on_socket = threading.Event(), threading.Event(), [], []

    def dies_while_the_second_call_waits(method, params):
        write.calls.append(method)
        on_socket.append(method)
        most_on_socket.append(len(on_socket))
        in_flight.set()
        if len(write.calls) == 1:
            release.wait(5)
        on_socket.pop()
        raise BrokenPipeError(32, "Broken pipe")

    write.rpc_request = dies_while_the_second_call_waits
    results = []
    callers = [threading.Thread(target=lambda: results.append(health(client))) for _ in range(2)]
    callers[0].start()
    assert in_flight.wait(5)
    callers[1].start()
    time.sleep(0.2)
    release.set()
    for caller in callers:
        caller.join(timeout=10)

    assert results == [{"peers": 3, "isSyncing": False}] * 2
    assert max(most_on_socket) == 1, "two calls were on the socket at once"
    assert len(endpoint.opened) == 3, "one replacement, not one per call"
    assert client._write.si is endpoint.opened[2]
    assert endpoint.opened[2].calls == ["system_health"] * 2


def test_a_call_arriving_during_a_timeout_replace_waits_for_it():
    client, endpoint = connected_client()
    client._rpc_timeout_secs = 0.1
    client._reconnect_after_timeouts = 1
    endpoint.opened[0].rpc_request = lambda method, params: time.sleep(5)
    connect, release, connecting = client._create_substrate_interface, threading.Event(), []

    def slow_connect():
        connecting.append(threading.current_thread().name)
        release.wait(5)
        return connect()

    client._create_substrate_interface = slow_connect
    outcomes = {}

    def call(name):
        try:
            outcomes[name] = health(client)
        except RPCTimeoutError as error:
            outcomes[name] = error

    wedged = threading.Thread(target=call, args=("wedged",), name="wedged")
    wedged.start()
    deadline = time.monotonic() + 5
    while not connecting and time.monotonic() < deadline:
        time.sleep(0.01)
    arriving = threading.Thread(target=call, args=("arriving",), name="arriving")
    arriving.start()
    deadline = time.monotonic() + 2
    while len(connecting) < 2 and time.monotonic() < deadline:
        time.sleep(0.01)
    release.set()
    wedged.join(10)
    arriving.join(10)

    assert connecting == ["wedged"], "the arriving call waited for the replace in flight"
    assert isinstance(outcomes["wedged"], RPCTimeoutError)
    assert outcomes["arriving"] == {"peers": 3, "isSyncing": False}
    assert len(endpoint.opened) == 3 and client._write.si is endpoint.opened[2]


def test_a_conn_whose_replacement_failed_reconnects_on_its_next_call():
    client, endpoint = connected_client()
    write = endpoint.opened[0]
    write.failure = BrokenPipeError(32, "Broken pipe")
    endpoint.up = False

    with pytest.raises(BrokenPipeError):
        health(client)
    assert client._write.si is None

    endpoint.up = True
    assert health(client) == {"peers": 3, "isSyncing": False}
    assert client._write.si is endpoint.opened[2]


def test_a_peer_close_frame_is_a_dead_socket():
    """websocket-client answers a close frame by marking itself closed and
    returning "", which substrate-interface fails to parse as JSON."""
    client, endpoint = connected_client()
    write = endpoint.opened[0]
    write.websocket.connected = False
    write.failure = json.JSONDecodeError("Expecting value", "", 0)

    assert health(client) == {"peers": 3, "isSyncing": False}
    assert client._write.si is endpoint.opened[2] and write.closed


def test_a_json_error_on_an_open_websocket_is_not_a_transport_failure():
    client, endpoint = connected_client()
    write = endpoint.opened[0]
    write.failure = json.JSONDecodeError("Expecting value", "", 0)

    with pytest.raises(json.JSONDecodeError):
        health(client)
    assert len(endpoint.opened) == 2 and client._write.si is write


def test_a_transport_replacement_is_logged_and_counted(caplog):
    client, endpoint = connected_client()
    endpoint.opened[0].failure = BrokenPipeError(32, "Broken pipe")
    before = health_server._metrics.get("ws_transport_replacements_total", 0)

    health(client)

    assert health_server._metrics.get("ws_transport_replacements_total", 0) == before + 1
    assert "force-replace[write]: transport error BrokenPipeError in 'rpc_request'" in caplog.text


def test_a_kernel_timed_out_socket_is_one_replacement_and_no_rpc_timeout():
    client, endpoint = connected_client()
    endpoint.opened[0].failure = kernel_timeout()
    timeouts = health_server._metrics.get("ws_rpc_timeouts_total", 0)
    replacements = health_server._metrics.get("ws_transport_replacements_total", 0)

    assert health(client) == {"peers": 3, "isSyncing": False}

    assert len(endpoint.opened) == 3 and client._write.si is endpoint.opened[2]
    assert health_server._metrics.get("ws_transport_replacements_total", 0) == replacements + 1
    assert health_server._metrics.get("ws_rpc_timeouts_total", 0) == timeouts
    assert client._write.consecutive_timeouts == 0


def test_a_timeout_on_a_socket_already_replaced_is_not_a_strike_against_its_replacement():
    """A call wedged on the write socket outlasts the budget after the poll
    loop's reconnect replaced that socket. The timeout is the old socket's:
    the replacement's timeout streak stays clear, so its own first timeout
    does not replace it."""
    client, endpoint = connected_client()
    client._rpc_timeout_secs = 1.0
    client._reconnect_after_timeouts = 2
    write = endpoint.opened[0]
    release = threading.Event()

    def wedged_then_dead(method, params):
        write.calls.append(method)
        if method == "state_getStorage":
            release.wait(10)
        raise kernel_timeout()

    write.rpc_request = wedged_then_dead
    outcome = {}

    def wedged_read():
        try:
            client._call("rpc_request", "state_getStorage", ["0x00"], conn=client._write)
        except RPCTimeoutError as error:
            outcome["wedged"] = error

    reader = threading.Thread(target=wedged_read)
    reader.start()
    deadline = time.monotonic() + 5
    while "state_getStorage" not in write.calls and time.monotonic() < deadline:
        time.sleep(0.005)
    assert client.connect()
    assert health(client) == {"peers": 3, "isSyncing": False}
    reader.join(10)
    release.set()

    fresh = endpoint.opened[2]
    assert isinstance(outcome["wedged"], RPCTimeoutError)
    assert client._write.si is fresh and client._write.consecutive_timeouts == 0

    fresh.rpc_request = lambda method, params: time.sleep(10)
    with pytest.raises(RPCTimeoutError):
        health(client)
    assert client._write.si is fresh and len(endpoint.opened) == 4
