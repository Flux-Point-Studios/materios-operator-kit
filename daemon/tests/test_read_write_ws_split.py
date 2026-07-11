"""Read/write WebSocket split (task #500).

The attestation poll loop reads (``get_events`` / ``get_block_hash``) share a
single ``SubstrateInterface`` socket with the extrinsic writes
(``submit_extrinsic`` with ``wait_for_inclusion``). A burst of submits holds
that one socket for seconds at a time, so the next poll read blocks until the
30s budget kills it, forcing a reconnect loop — the daemon never processes new
receipts and produces no certs.

The fix gives reads their own dedicated connection so a wedged write socket
cannot block a read. These tests pin that guarantee, the per-connection
timeout isolation, and the backward-compat fallback (a client with only the
write conn wired — every pre-existing fixture — still routes reads through it).
"""

import threading
import time

import pytest

from daemon.substrate_client import RPCTimeoutError, SubstrateClient


def _bare_client(timeout=0.3):
    """A SubstrateClient with wrapper state but no real websockets."""
    c = SubstrateClient.__new__(SubstrateClient)
    c._ensure_wrapper_state()
    c._rpc_timeout_secs = timeout
    c._reconnect_after_timeouts = 2
    c._reconnect_max_attempts = 1
    c._backoff_sleep = lambda *_a, **_k: None
    # Force-replace must never dial a real node in a unit test.
    c._create_substrate_interface = lambda: _FakeSI()
    return c


class _FakeSI:
    """Records which methods were invoked; per-method behaviour is injectable."""

    def __init__(self):
        self.calls = []
        self._behaviours = {}

    def set(self, method, fn):
        self._behaviours[method] = fn

    def __getattr__(self, name):
        # Only reached for names not set in __dict__.
        def _method(*args, **kwargs):
            self.calls.append(name)
            fn = self._behaviours.get(name)
            if fn is not None:
                return fn(*args, **kwargs)
            return {"header": {"number": 42}}

        return _method


def test_read_dispatches_to_read_conn_not_write():
    c = _bare_client()
    read_si, write_si = _FakeSI(), _FakeSI()
    c._read.si = read_si
    c.substrate = write_si  # property -> write conn

    c.get_best_block_number()  # a read: get_block_header

    assert "get_block_header" in read_si.calls
    assert "get_block_header" not in write_si.calls


def test_write_dispatches_to_write_conn_not_read():
    c = _bare_client()
    read_si, write_si = _FakeSI(), _FakeSI()
    c._read.si = read_si
    c.substrate = write_si

    c.compose_call(call_module="X", call_function="y", call_params={})

    assert "compose_call" in write_si.calls
    assert "compose_call" not in read_si.calls


def test_substrate_property_aliases_write_conn():
    c = _bare_client()
    si = _FakeSI()
    c.substrate = si
    assert c._write.si is si
    assert c.substrate is si


def test_read_falls_back_to_write_when_read_conn_unset():
    """Pre-split fixtures wire only ``self.substrate``; reads must still work."""
    c = _bare_client()
    write_si = _FakeSI()
    c.substrate = write_si
    c._read.si = None  # read conn never opened

    c.get_best_block_number()

    assert "get_block_header" in write_si.calls


def test_read_timeout_isolated_from_write_streak():
    c = _bare_client(timeout=0.2)
    read_si, write_si = _FakeSI(), _FakeSI()
    read_si.set("get_block_header", lambda *a, **k: time.sleep(5))  # wedge read
    c._read.si = read_si
    c.substrate = write_si

    with pytest.raises(RPCTimeoutError):
        c.get_best_block_number()

    # The read streak advanced; the write conn's streak is untouched.
    assert c._read.consecutive_timeouts >= 1
    assert c._write.consecutive_timeouts == 0


def test_hung_write_does_not_block_read():
    """The headline guarantee: a submit that holds the write socket for
    seconds must not delay a poll-loop read on the read socket."""
    c = _bare_client(timeout=3.0)
    read_si, write_si = _FakeSI(), _FakeSI()
    write_si.set("submit_extrinsic", lambda *a, **k: time.sleep(2.0) or {"ok": 1})
    read_si.set("get_block_header", lambda *a, **k: {"header": {"number": 99}})
    c._read.si = read_si
    c.substrate = write_si

    # Hold the write socket in the background.
    t = threading.Thread(
        target=lambda: c._call("submit_extrinsic"), daemon=True
    )
    t.start()
    time.sleep(0.1)  # ensure the write is in flight

    started = time.monotonic()
    n = c.get_best_block_number()
    elapsed = time.monotonic() - started

    assert n == 99
    assert elapsed < 1.0, f"read blocked behind the hung write ({elapsed:.2f}s)"
