"""Regression tests for the substrate-client WS retry shell (task #41).

The 2026-05-08 incident was a poll loop that wedged inside
`websocket.recv()` and stayed there for 11+ hours while
`substrate_connected=true` was reported to consumers. The fix shipped
in `daemon/substrate_client.py` does three things; these tests pin all
three.

  1. `connect()` passes `ws_options={'timeout': N}` to SubstrateInterface
     so the underlying socket has a recv timeout (the actual wedge was a
     blocking recv with no timeout).
  2. Every public RPC method goes through `_call_with_retry`, which on a
     transient WS error drops the substrate handle, reconnects once, and
     retries the same call.
  3. `connected` is recent-success-based (NOT `substrate is not None`),
     so a freshly-built client is NOT connected, and a successful call
     bumps the freshness.

These tests run the real `SubstrateClient` class against a fully-mocked
SubstrateInterface so they exercise the wrapper logic, not the network.
"""

from __future__ import annotations

import socket
import threading
import time
from unittest import mock

import pytest

from daemon.config import DaemonConfig
from daemon.substrate_client import SubstrateClient, _WS_TRANSIENT


def _build_client(config_overrides=None):
    """Build a SubstrateClient with a Keypair that doesn't need a real chain.

    `Keypair.create_from_uri("//Alice")` is a deterministic well-known dev
    keypair; no RPC is touched in the constructor.
    """
    cfg = DaemonConfig()
    cfg.signer_uri = "//Alice"
    cfg.rpc_url = "ws://test.invalid:9944"
    cfg.tx_max_retries = 3
    if config_overrides:
        for k, v in config_overrides.items():
            setattr(cfg, k, v)
    return SubstrateClient(cfg)


# ─── (1) connect() passes ws_options{timeout} ─────────────────────────────


def test_connect_passes_ws_options_timeout():
    """The fix MUST forward `ws_recv_timeout` into ws_options. Without
    this, the underlying socket has no recv timeout and the wedge from
    the 2026-05-08 incident is silently re-introduced."""
    client = _build_client({"ws_recv_timeout": 17})
    captured: dict = {}

    class FakeSubstrate:
        chain = "Materios Test"

        def __init__(self, *_, **kwargs):
            captured["kwargs"] = kwargs

        def close(self):
            pass

    with mock.patch("daemon.substrate_client.SubstrateInterface", FakeSubstrate):
        ok = client.connect()
    assert ok is True
    assert captured["kwargs"].get("ws_options") == {"timeout": 17}


def test_connect_default_timeout_is_30s():
    """Default ws_recv_timeout (30s) is the production default and must
    not silently become 0 or None — those would re-create the wedge."""
    client = _build_client()
    assert client._ws_recv_timeout == 30


# ─── (2) connected property is recent-success-based ──────────────────────


def test_connected_false_before_first_call():
    """A SubstrateClient that has never made a successful RPC must NOT
    report connected. This guards against the original bug where
    `substrate is not None` lied about liveness."""
    client = _build_client()
    assert client.substrate is None
    assert client.connected is False


def test_connected_true_after_successful_connect_and_false_after_freshness_lapses():
    client = _build_client({"ws_connected_freshness": 30})

    class FakeSubstrate:
        chain = "Materios Test"

        def close(self):
            pass

    with mock.patch("daemon.substrate_client.SubstrateInterface", return_value=FakeSubstrate()):
        client.connect()

    assert client.connected is True
    # Backdate the freshness stamp by more than the threshold.
    client._last_ok_at = time.monotonic() - 31
    assert client.connected is False


def test_connected_false_when_substrate_is_none_even_if_recent():
    client = _build_client()
    client._last_ok_at = time.monotonic()
    client.substrate = None
    assert client.connected is False


# ─── (3) _call_with_retry reconnects on transient WS errors ──────────────


def test_retry_shell_reconnects_after_socket_timeout():
    """The exact wedge mode: socket.timeout raised mid-call. Retry shell
    must drop the handle, reconnect, and rerun the call once."""
    client = _build_client()
    call_count = {"n": 0}
    connect_count = {"n": 0}

    def flaky_call():
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise socket.timeout("recv timed out")
        return 42

    def fake_connect():
        connect_count["n"] += 1
        client.substrate = mock.Mock()  # any non-None handle
        client._last_ok_at = time.monotonic()
        return True

    with mock.patch.object(client, "connect", side_effect=fake_connect):
        result = client._call_with_retry(flaky_call)

    assert result == 42
    assert call_count["n"] == 2
    # First connect was triggered because substrate was None at start;
    # second connect was triggered by the retry path.
    assert connect_count["n"] == 2


def test_retry_shell_propagates_after_two_failures():
    """If both attempts fail, the second exception is raised. The poll
    loop's outer except handles it; we don't loop forever inside."""
    client = _build_client()

    def always_broken():
        raise socket.timeout("dead")

    fake_handle = mock.Mock()
    with mock.patch.object(
        client,
        "connect",
        side_effect=lambda: (setattr(client, "substrate", fake_handle), setattr(client, "_last_ok_at", time.monotonic()))[0] is None or True,
    ):
        with pytest.raises(socket.timeout):
            client._call_with_retry(always_broken)


def test_retry_shell_does_not_retry_substrate_request_exception():
    """SubstrateRequestException = chain-side error (e.g. unknown storage
    key). Retrying would mask real bugs and hammer the chain — propagate
    immediately. Also: that path must STILL bump _last_ok_at because the
    WS itself worked."""
    from substrateinterface.exceptions import SubstrateRequestException

    client = _build_client()
    client.substrate = mock.Mock()
    client._last_ok_at = 0.0

    call_count = {"n": 0}

    def chain_side_error():
        call_count["n"] += 1
        raise SubstrateRequestException({"code": -32602, "message": "unknown storage key"})

    with pytest.raises(SubstrateRequestException):
        client._call_with_retry(chain_side_error)
    assert call_count["n"] == 1  # NOT retried
    assert client._last_ok_at > 0  # WS round-trip succeeded — freshness bumped


def test_retry_shell_handles_all_listed_transient_types():
    """Every exception type in `_WS_TRANSIENT` must trigger the retry
    path. Adding a new transient type without listing it here would
    silently cause that error class to skip retry — which is the bug
    pattern we're trying to prevent."""
    client = _build_client()

    for exc_cls in _WS_TRANSIENT:
        # Build a fresh client for each so state doesn't leak.
        client = _build_client()
        call_count = {"n": 0}

        def flaky():
            call_count["n"] += 1
            if call_count["n"] == 1:
                # Every listed transient must be raise-able with a string.
                raise exc_cls(f"simulated {exc_cls.__name__}")
            return "ok"

        with mock.patch.object(
            client,
            "connect",
            side_effect=lambda: (setattr(client, "substrate", mock.Mock()), setattr(client, "_last_ok_at", time.monotonic()))[0] is None or True,
        ):
            result = client._call_with_retry(flaky)
        assert result == "ok", f"retry did not recover for {exc_cls.__name__}"
        assert call_count["n"] == 2, f"retry did not re-execute for {exc_cls.__name__}"


# ─── (4) public method wrappers go through retry ─────────────────────────


def test_get_best_block_number_uses_retry_shell():
    """The original wedge fired in `get_best_block_number` at line 873
    of cert_daemon's poll loop. Its underlying call MUST go through
    `_call_with_retry`, not raw substrate access."""
    client = _build_client()
    captured = {"reached_inner": 0}

    def fake_connect():
        client.substrate = mock.Mock()
        client.substrate.get_block_header = lambda: {"header": {"number": 999}}
        client._last_ok_at = time.monotonic()
        return True

    with mock.patch.object(client, "connect", side_effect=fake_connect):
        # First call: substrate is None → connect() → call → bump
        result = client.get_best_block_number()
    assert result == 999

    # Now simulate a wedge: replace get_block_header with a flaky one
    # that fails once then succeeds — the wrapper must reconnect+retry.
    flaky_calls = {"n": 0}

    def flaky_header():
        flaky_calls["n"] += 1
        if flaky_calls["n"] == 1:
            raise socket.timeout("simulated wedge")
        return {"header": {"number": 1000}}

    client.substrate.get_block_header = flaky_header
    with mock.patch.object(client, "connect", side_effect=fake_connect):
        # connect() resets substrate, but our flaky_header is bound to
        # the OLD substrate. So we re-attach after connect by patching.
        original_connect = fake_connect
        def reconnect_keeping_flaky():
            ok = original_connect()
            client.substrate.get_block_header = flaky_header
            return ok
        with mock.patch.object(client, "connect", side_effect=reconnect_keeping_flaky):
            result = client.get_best_block_number()
    assert result == 1000
    assert flaky_calls["n"] == 2


def test_lock_serializes_concurrent_calls():
    """Multiple threads calling RPCs must NOT interleave WS sends — the
    transport keys responses by request id and concurrent send/recv
    pairs race the response queue. The lock guarantees serialization."""
    client = _build_client()
    overlap = {"max_concurrent": 0, "current": 0, "lock": threading.Lock()}

    def slow_call():
        with overlap["lock"]:
            overlap["current"] += 1
            overlap["max_concurrent"] = max(overlap["max_concurrent"], overlap["current"])
        time.sleep(0.05)
        with overlap["lock"]:
            overlap["current"] -= 1
        return 1

    def fake_connect():
        client.substrate = mock.Mock()
        client._last_ok_at = time.monotonic()
        return True

    threads = []
    with mock.patch.object(client, "connect", side_effect=fake_connect):
        # Pre-establish the connection so all threads contend on the
        # call lock, not the connect lock.
        client.connect()
        for _ in range(8):
            t = threading.Thread(target=lambda: client._call_with_retry(slow_call))
            t.start()
            threads.append(t)
        for t in threads:
            t.join()

    assert overlap["max_concurrent"] == 1, (
        f"expected serialized RPCs (max_concurrent=1) but observed "
        f"{overlap['max_concurrent']} — the retry shell lock is broken"
    )
