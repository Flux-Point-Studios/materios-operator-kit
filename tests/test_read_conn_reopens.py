"""A read conn left without an interface is reopened in the background (#70).

A failed connect, or a replace that gave up, leaves the read conn empty, and
reads then run on the write conn behind its submits (#69). Nothing else
reopens the read conn while the write conn stays healthy: the poll loop
reconnects only on an error, and a read served by the write conn raises none.
A read that finds the read conn empty is served by the write conn at once and
starts a background reopen, which retries with the conn's reconnect backoff.
The degraded mode is logged at error level while it lasts.
"""
import time

import pytest

from daemon.config import DaemonConfig
from daemon.substrate_client import SubstrateClient
from tests.fake_chain import FakeChain, dialling_client, fake_endpoint

HEALTH = {"peers": 3, "isSyncing": False}


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
