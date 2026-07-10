"""Tests for `SubstrateClient`'s bounded-timeout + auto-reconnect wrapper.

Context: tasks #288 (poll-loop wedge) and #156 (evidence_submitter
submit_extrinsic wedge). substrate-interface 1.x's blocking
``websocket-client`` socket recv enters a state where any recv blocks
forever — past the timeout passed to it — once the connection has been
idle for a while (multi-minute extrinsic phases, intermittent network
glitch). No exception is raised. The Python call sits indefinitely and
the cert-daemon's outer watchdog has to docker-restart the container.

The fix:

  1. Every RPC call is wrapped in a ``ThreadPoolExecutor.submit() +
     future.result(timeout=...)``. If the call exceeds the timeout, the
     calling thread sees ``RPCTimeoutError`` (a daemon-side wrapper around
     ``concurrent.futures.TimeoutError``).
  2. After ``MATERIOS_RPC_RECONNECT_AFTER_TIMEOUTS`` (default 2) consecutive
     timeouts, the wrapper drops the current ``SubstrateInterface``,
     closes its websocket, and creates a new instance.
  3. If the reconnect itself fails, exponential backoff: 1s, 2s, 4s, 8s,
     capped at 30s.
  4. Successful RPCs reset the consecutive-timeout counter AND bump
     ``health_server`` so ``/ready`` last_poll_age reflects the latest
     successful chain interaction.

Tests confirm each contract above. The seam is at the SubstrateInterface
constructor (we patch ``SubstrateClient._create_substrate_interface`` so
no real WS is dialled), and at ``time.sleep`` so backoff tests run fast.
"""

from __future__ import annotations

import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from daemon import substrate_client as sc_module
from daemon.config import DaemonConfig
from daemon.substrate_client import (
    RPCTimeoutError,
    SubstrateClient,
)


# --- helpers ---------------------------------------------------------------


class _FakeSI:
    """Simulates a SubstrateInterface for the wrapper tests.

    Each public method on a real ``SubstrateInterface`` is mocked here as a
    plain attribute that the test wires up via ``set_method``. The
    ``close()`` + ``websocket`` attributes are mimicked so the wrapper's
    force-replace path can poke them.
    """

    def __init__(self, *, chain: str = "Materios", url: str = "ws://x"):
        self.chain = chain
        self.url = url
        self.websocket = MagicMock()
        self.close_calls = 0
        self._closed = False

    def close(self):
        self.close_calls += 1
        self._closed = True

    def set_method(self, name, fn):
        setattr(self, name, fn)


def _make_client(*, rpc_timeout_secs: float = 0.2, reconnect_after: int = 2) -> SubstrateClient:
    """Build a SubstrateClient with a stub config + the WSGuard knobs pinned
    to test-friendly values."""
    config = DaemonConfig()
    config.rpc_url = "ws://test-rpc:9944"
    client = SubstrateClient(config)
    # Pin the WSGuard knobs explicitly so test behavior is independent of
    # the env-var defaults the production wrapper picks up.
    client._rpc_timeout_secs = rpc_timeout_secs
    client._reconnect_after_timeouts = reconnect_after
    client._backoff_sleep = MagicMock()
    return client


# --- 1. happy path: bounded call succeeds, counter stays at 0 -------------


def test_call_returns_value_when_underlying_method_returns_promptly():
    """A method that returns in <timeout returns its value; no reconnect; counter
    stays at 0 and the success bumps the consecutive_reconnect_failures back
    to 0 too (it was already 0)."""
    client = _make_client()
    fake = _FakeSI()
    fake.set_method("foo", lambda: 42)
    client.substrate = fake
    assert client._call("foo") == 42
    assert client._write.consecutive_timeouts == 0
    assert client._write.consecutive_reconnect_failures == 0


def test_call_passes_args_and_kwargs_through_to_underlying_method():
    client = _make_client()
    fake = _FakeSI()
    captured = {}

    def foo(a, b, *, c):
        captured["args"] = (a, b)
        captured["c"] = c
        return "ok"

    fake.set_method("foo", foo)
    client.substrate = fake
    assert client._call("foo", 1, 2, c="three") == "ok"
    assert captured == {"args": (1, 2), "c": "three"}


# --- 2. timeout: blocking call exceeds budget, wrapper raises -------------


def test_call_raises_RPCTimeoutError_when_underlying_method_blocks_past_budget():
    """The blocking call should not hold the calling thread past the timeout
    budget. The executor thread is left to leak (real-world: it dies when the
    OS reaps the socket; in tests we don't care that the executor thread is
    still ``sleep()``-ing — we care that the calling thread returns inside
    the budget)."""
    client = _make_client(rpc_timeout_secs=0.1)
    fake = _FakeSI()

    def slow():
        # Sleeps far past the timeout budget — the real wedge sleeps
        # forever; we use 60s as a "forever stand-in" so test framework
        # doesn't hang if the wrapper is broken.
        time.sleep(60)

    fake.set_method("slow", slow)
    client.substrate = fake

    start = time.monotonic()
    with pytest.raises(RPCTimeoutError):
        client._call("slow")
    elapsed = time.monotonic() - start
    # Allow generous slack so test stays robust under CI load — what matters
    # is "well under 60s". 5s is a safe ceiling on every CI we use; the
    # actual wrapper aborts at ~0.1s.
    assert elapsed < 5.0, f"wrapper held caller for {elapsed:.2f}s; should have aborted at ~0.1s"
    assert client._write.consecutive_timeouts == 1


def test_underlying_exception_propagates_as_itself_not_RPCTimeoutError():
    """A non-timeout failure (e.g. SubstrateRequestException) must surface as
    the original exception type so existing callers' ``except`` clauses
    still match. The wrapper only converts the TimeoutError class."""
    from substrateinterface.exceptions import SubstrateRequestException

    client = _make_client()
    fake = _FakeSI()

    def bad():
        raise SubstrateRequestException("nope")

    fake.set_method("bad", bad)
    client.substrate = fake
    with pytest.raises(SubstrateRequestException):
        client._call("bad")
    # Real exceptions should NOT count as timeouts.
    assert client._write.consecutive_timeouts == 0


# --- 3. reconnect on consecutive timeouts ---------------------------------


def test_consecutive_timeouts_trigger_force_replace_of_substrate_interface():
    """After 2 consecutive timeouts the wrapper drops the current SI, closes
    its websocket, and creates a new one (which the 3rd attempt uses)."""
    client = _make_client(rpc_timeout_secs=0.05, reconnect_after=2)

    # Track which SI is "current" — we'll switch the slow→fast behavior after
    # reconnect to assert the wrapper actually replaced the underlying.
    state = {"si_count": 0}
    old_fakes = []

    def factory():
        state["si_count"] += 1
        fake = _FakeSI()
        if state["si_count"] == 1:
            # First SI hangs forever
            fake.set_method("foo", lambda: time.sleep(60))
        else:
            fake.set_method("foo", lambda: "ok-after-reconnect")
        old_fakes.append(fake)
        return fake

    with patch.object(client, "_create_substrate_interface", side_effect=factory):
        client.substrate = factory()  # first instance via factory
        # 1st call: times out
        with pytest.raises(RPCTimeoutError):
            client._call("foo")
        assert client._write.consecutive_timeouts == 1
        assert state["si_count"] == 1  # no reconnect yet
        # 2nd call: times out, hits reconnect threshold
        with pytest.raises(RPCTimeoutError):
            client._call("foo")
        # The wrapper should have closed the first SI and created a new one
        # BEFORE returning from the timeout (so the next call uses the new SI).
        assert old_fakes[0].close_calls == 1, "first SI should have been closed"
        assert state["si_count"] == 2, "second SI should have been created"
        # Counter resets after the reconnect.
        assert client._write.consecutive_timeouts == 0
        # 3rd call on the new SI succeeds
        assert client._call("foo") == "ok-after-reconnect"


def test_single_timeout_does_not_trigger_reconnect():
    """Reconnect only fires at the threshold, not on every timeout."""
    client = _make_client(rpc_timeout_secs=0.05, reconnect_after=3)
    fake = _FakeSI()
    fake.set_method("foo", lambda: time.sleep(60))
    client.substrate = fake
    with patch.object(client, "_create_substrate_interface") as ctor:
        with pytest.raises(RPCTimeoutError):
            client._call("foo")
        assert ctor.call_count == 0
        assert fake.close_calls == 0
        with pytest.raises(RPCTimeoutError):
            client._call("foo")
        assert ctor.call_count == 0
        # Still under threshold.
        assert client._write.consecutive_timeouts == 2


def test_success_resets_consecutive_timeout_counter():
    """A successful call after a partial-failure streak clears the counter so
    the next timeout starts a fresh streak (otherwise we'd reconnect after
    one timeout, then one success, then one more timeout — way too eager)."""
    client = _make_client(rpc_timeout_secs=0.1, reconnect_after=2)
    fake = _FakeSI()
    state = {"slow_next": True}

    def maybe_slow():
        if state["slow_next"]:
            state["slow_next"] = False
            time.sleep(60)
        return "ok"

    fake.set_method("foo", maybe_slow)
    client.substrate = fake
    with pytest.raises(RPCTimeoutError):
        client._call("foo")
    assert client._write.consecutive_timeouts == 1
    assert client._call("foo") == "ok"
    assert client._write.consecutive_timeouts == 0


# --- 4. reconnect-failure backoff -----------------------------------------


def test_reconnect_failures_use_exponential_backoff_with_30s_cap():
    """If the new SubstrateInterface fails to construct, the wrapper retries
    with exponential backoff: 1s, 2s, 4s, 8s, 16s, then capped at 30s. The
    gauge tracks the current streak; metric counters tick."""
    client = _make_client(rpc_timeout_secs=0.01, reconnect_after=2)

    # Underlying call always wedges so we keep accumulating timeouts.
    def wedged(*_a, **_kw):
        time.sleep(60)

    fake = _FakeSI()
    fake.set_method("foo", wedged)
    client.substrate = fake

    # Constructor raises until the 7th attempt succeeds. That gives us 6
    # backoff sleeps to observe: 1, 2, 4, 8, 16, 30 (cap).
    attempts = {"n": 0}

    def factory():
        attempts["n"] += 1
        if attempts["n"] < 7:
            raise OSError(f"connect-failed-{attempts['n']}")
        # 7th attempt succeeds — fake stops wedging
        new_fake = _FakeSI()
        new_fake.set_method("foo", lambda: "ok")
        return new_fake

    with patch.object(client, "_create_substrate_interface", side_effect=factory):
        # First timeout — counter goes to 1, no reconnect yet.
        with pytest.raises(RPCTimeoutError):
            client._call("foo")
        # Second timeout — triggers reconnect. The reconnect retries (with
        # backoff) until the 7th constructor call succeeds. The wrapper
        # should still raise RPCTimeoutError for this CALL (we don't retry
        # the user's call inside the reconnect loop — caller decides).
        with pytest.raises(RPCTimeoutError):
            client._call("foo")

    # 6 failed reconnect attempts → 6 backoff sleeps. Values:
    #   sleep(1), sleep(2), sleep(4), sleep(8), sleep(16), sleep(30)  (capped)
    # We compare against a known list; the wrapper uses
    # min(30, 2 ** (failures - 1)).
    actual_sleeps = [c.args[0] for c in client._backoff_sleep.call_args_list]
    assert actual_sleeps == [1, 2, 4, 8, 16, 30], (
        f"expected exp-backoff sleeps [1,2,4,8,16,30], got {actual_sleeps}"
    )
    # After successful reconnect, the failure streak is back to 0.
    assert client._write.consecutive_reconnect_failures == 0


def test_reconnect_failure_gauge_surfaces_streak():
    """During an ongoing reconnect-failure streak, the gauge should reflect
    the current streak length so /ready / /metrics can show the operator
    that the daemon is trying."""
    client = _make_client(rpc_timeout_secs=0.01, reconnect_after=2)

    def wedged(*_a, **_kw):
        time.sleep(60)

    fake = _FakeSI()
    fake.set_method("foo", wedged)
    client.substrate = fake

    # Constructor permanently fails — wrapper bails after some bounded
    # number of attempts and surfaces the streak.
    def factory():
        raise OSError("perma-fail")

    # Cap the wrapper's reconnect attempts at 4 so the test terminates.
    client._reconnect_max_attempts = 4

    with patch.object(client, "_create_substrate_interface", side_effect=factory):
        with pytest.raises(RPCTimeoutError):
            client._call("foo")  # counter=1
        with pytest.raises(RPCTimeoutError):
            client._call("foo")  # counter=2 → reconnect → 4 failures

    assert client._write.consecutive_reconnect_failures == 4


# --- 5. metrics + health bump on every successful call --------------------


def test_successful_call_bumps_health_last_poll_ts():
    """Per the watchdog comment in cert-daemon-liveness-watchdog.sh, the
    health bump must fire on EVERY successful RPC, not just outer loop
    ticks. We patch health_server.update_metrics and assert it was called
    with a fresh last_poll_timestamp."""
    client = _make_client()
    fake = _FakeSI()
    fake.set_method("foo", lambda: "ok")
    client.substrate = fake

    with patch("daemon.health_server.update_metrics") as upd:
        assert client._call("foo") == "ok"

    assert upd.call_count >= 1, "health_server.update_metrics should fire on success"
    found = False
    for c in upd.call_args_list:
        if "last_poll_timestamp" in c.kwargs:
            found = True
            assert isinstance(c.kwargs["last_poll_timestamp"], (int, float))
            assert c.kwargs["last_poll_timestamp"] > 0
    assert found, "expected at least one update_metrics(last_poll_timestamp=...) call"


def test_timeout_increments_rpc_timeouts_total():
    """Each timeout bumps the daemon's rpc_timeouts_total counter so the
    metrics surface shows the trend."""
    client = _make_client(rpc_timeout_secs=0.05)
    fake = _FakeSI()
    fake.set_method("foo", lambda: time.sleep(60))
    client.substrate = fake

    with patch("daemon.health_server.increment_metric") as inc:
        with pytest.raises(RPCTimeoutError):
            client._call("foo")
        with pytest.raises(RPCTimeoutError):
            client._call("foo")

    timeout_calls = [c for c in inc.call_args_list if c.args and c.args[0] == "ws_rpc_timeouts_total"]
    assert len(timeout_calls) == 2, (
        f"expected 2 increments of ws_rpc_timeouts_total, got {len(timeout_calls)}: "
        f"{inc.call_args_list}"
    )


def test_force_reconnect_increments_force_reconnects_counter():
    """A successful force-replace ticks the ws_force_reconnects_total counter."""
    client = _make_client(rpc_timeout_secs=0.01, reconnect_after=2)

    def wedged(*_a, **_kw):
        time.sleep(60)

    fake = _FakeSI()
    fake.set_method("foo", wedged)
    client.substrate = fake

    def factory():
        new_fake = _FakeSI()
        new_fake.set_method("foo", lambda: "ok")
        return new_fake

    with patch.object(client, "_create_substrate_interface", side_effect=factory):
        with patch("daemon.health_server.increment_metric") as inc:
            with pytest.raises(RPCTimeoutError):
                client._call("foo")  # 1
            with pytest.raises(RPCTimeoutError):
                client._call("foo")  # 2 → reconnect

    force_calls = [c for c in inc.call_args_list if c.args and c.args[0] == "ws_force_reconnects_total"]
    assert len(force_calls) == 1, (
        f"expected 1 increment of ws_force_reconnects_total, got {len(force_calls)}: "
        f"{inc.call_args_list}"
    )


# --- 6. env-var defaults --------------------------------------------------


def test_env_var_overrides_for_timeout_and_reconnect_threshold(monkeypatch):
    """Defaults come from env vars so an operator can tune the budgets without
    a re-deploy. Test the env-var → field plumbing on construction."""
    monkeypatch.setenv("MATERIOS_RPC_TIMEOUT_SECS", "45")
    monkeypatch.setenv("MATERIOS_RPC_RECONNECT_AFTER_TIMEOUTS", "5")
    config = DaemonConfig()
    config.rpc_url = "ws://test:9944"
    client = SubstrateClient(config)
    assert client._rpc_timeout_secs == 45.0
    assert client._reconnect_after_timeouts == 5


def test_invalid_env_var_falls_back_to_default(monkeypatch):
    monkeypatch.setenv("MATERIOS_RPC_TIMEOUT_SECS", "not-a-number")
    config = DaemonConfig()
    config.rpc_url = "ws://test:9944"
    client = SubstrateClient(config)
    assert client._rpc_timeout_secs == 30.0  # default


# --- 7. apply uniformly: every wrapper method goes through _call ----------


def test_get_finalized_head_number_routes_through_call():
    """Spot-check ONE existing wrapper method to confirm it now uses
    self._call (not bare self.substrate.foo). If the dispatch were left
    unwrapped, a wedged WS would still hang this entry point."""
    client = _make_client(rpc_timeout_secs=0.05)
    fake = _FakeSI()
    fake.set_method("get_chain_finalised_head", lambda: time.sleep(60))
    fake.set_method("get_block_header", lambda *_a, **_kw: {"header": {"number": 100}})
    client.substrate = fake
    with pytest.raises(RPCTimeoutError):
        client.get_finalized_head_number()


def test_get_best_block_number_routes_through_call():
    client = _make_client(rpc_timeout_secs=0.05)
    fake = _FakeSI()
    fake.set_method("get_block_header", lambda *_a, **_kw: time.sleep(60))
    client.substrate = fake
    with pytest.raises(RPCTimeoutError):
        client.get_best_block_number()


def test_get_genesis_hash_routes_through_call():
    client = _make_client(rpc_timeout_secs=0.05)
    fake = _FakeSI()
    fake.set_method("get_block_hash", lambda *_a, **_kw: time.sleep(60))
    client.substrate = fake
    with pytest.raises(RPCTimeoutError):
        client.get_genesis_hash()


def test_no_bare_substrate_calls_in_substrate_client_outside_wrapper():
    """Static grep: outside ``_call`` and the wrapper-construction helper,
    ``substrate_client.py`` must not call ``self.substrate.<method>`` directly
    — every site has to go through the bounded-timeout wrapper.

    We inspect the module source rather than running the daemon because
    the wedge happens on a method we may not exercise in any one test, but
    a bare call site is a wedge waiting to fire.
    """
    import re
    from pathlib import Path

    src = Path(sc_module.__file__).read_text()
    # Strip comments + docstrings (very loose — we only care about the
    # imperative bodies). Comments starting with `#` are filtered line by
    # line; triple-quoted strings are dropped by a non-greedy regex.
    src_nodoc = re.sub(r'"""[\s\S]*?"""', "", src)
    lines = [ln for ln in src_nodoc.splitlines() if not ln.strip().startswith("#")]

    offenders = []
    for i, line in enumerate(lines, 1):
        if "self.substrate." in line and "self._call" not in line:
            # Allowed call sites:
            #   - reading the `.substrate` attribute itself for None-checks
            #   - the wrapper that constructs / replaces the SI
            #   - direct attribute access (.chain, .url, .websocket) is fine —
            #     those are reads, not RPC calls.
            if "self.substrate is None" in line or "self.substrate =" in line:
                continue
            # Property access (not method call): `self.substrate.chain`,
            # `self.substrate.websocket`. The bug we're fixing is the
            # blocking ``recv`` inside RPC method calls, not attribute
            # reads. We accept ANY ``self.substrate.<X>`` that's NOT
            # followed by ``(`` (i.e. not a call).
            m = re.search(r"self\.substrate\.(\w+)(\s*\()?", line)
            if m and m.group(2) is None:
                continue
            offenders.append((i, line.strip()))

    assert not offenders, (
        "bare self.substrate.<method>(...) call sites still in substrate_client.py "
        f"(must route through self._call): {offenders[:5]}"
    )
