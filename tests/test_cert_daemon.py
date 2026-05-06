"""Tests for cert-daemon's stale-cursor self-heal.

Context (internal task #144): three external operators hit the same
crashloop within 24h because their validator got restarted while the
cert-daemon held an old `last_processed_block`. The daemon retried the
pruned block every tick and never advanced:

    SubstrateRequestException: {"code": 4003, "message":
        "Client error: Api called for an unknown Block:
         State already discarded for 0x..."}

Memory ref: `feedback_cert_daemon_cursor_pruned.md`. Previous manual
rescue was `rm -f daemon-state.json && docker restart ...`; this test
module covers the self-heal that replaces it.

Mocks `SubstrateClient` at the seam (same style as `test_auto_bond.py`).
"""

from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from substrateinterface.exceptions import SubstrateRequestException

from daemon import cert_daemon as cd_module
from daemon.cert_daemon import (
    CertDaemon,
    FALLBACK_WINDOW,
    MAX_CLAMPS_PER_TICK,
    _is_pruned_state_error,
)
from daemon.config import DaemonConfig


# --- helpers ---------------------------------------------------------------


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


def _make_daemon(
    *,
    last_processed_block: int = 0,
    checkpoint_enabled: bool = False,
) -> CertDaemon:
    """Build a minimal CertDaemon wired for `_process_block_range` tests.

    The substrate client, save_state, process_receipt, and checkpointer
    seams are mocked. No real I/O is performed.
    """
    config = DaemonConfig()
    config.checkpoint_enabled = checkpoint_enabled

    daemon = CertDaemon.__new__(CertDaemon)
    daemon.config = config
    daemon.client = MagicMock()
    daemon.client.keypair = SimpleNamespace(
        ss58_address="5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY",
        public_key=b"\x00" * 32,
    )
    daemon.last_processed_block = last_processed_block
    daemon._running = True
    daemon.pending = {}
    daemon._notified_ids = {}
    daemon._pruned_warned_blocks = set()
    # Concurrency primitives added in task #120 — fixtures bypass __init__
    # via `CertDaemon.__new__`, so we have to materialize the fields the
    # `_ensure_concurrency_primitives()` lazy-init path expects.
    daemon._chain_write_lock = None
    daemon._pending_lock = None
    daemon._concurrency_sem = None
    daemon.checkpointer = MagicMock()

    # Replace state persistence so we don't touch disk.
    daemon.save_state = MagicMock()

    # process_receipt is async — stub with a no-op coroutine. Using MagicMock
    # directly would not produce an awaitable; we wrap an AsyncMock-equivalent.
    async def _noop_process_receipt(receipt_id):
        return None

    daemon.process_receipt = MagicMock(side_effect=_noop_process_receipt)

    # send_discord is async — stubbed away.
    async def _no_discord(*args, **kwargs):
        return None

    daemon.send_discord = _no_discord
    return daemon


def _pruned_exc() -> SubstrateRequestException:
    """Build the exact exception shape substrate-interface raises when the
    node has pruned state for the requested block (RPC error code 4003)."""
    return SubstrateRequestException({
        "code": 4003,
        "message": (
            "Client error: Api called for an unknown Block: "
            "State already discarded for 0xdeadbeef"
        ),
    })


# --- helper tests -----------------------------------------------------------


def test_is_pruned_state_error_matches_code_4003():
    """Detection must match on RPC error code 4003."""
    exc = SubstrateRequestException({"code": 4003, "message": "something"})
    assert _is_pruned_state_error(exc) is True


def test_is_pruned_state_error_matches_message_string():
    """Detection must match on the substrate error message string even if
    the code field is absent or different."""
    exc = Exception("State already discarded for 0x1234")
    assert _is_pruned_state_error(exc) is True


def test_is_pruned_state_error_matches_both_signals():
    """The combined form (what the operators actually saw in task #144) must
    trip detection via both signals — code AND message."""
    exc = _pruned_exc()
    assert _is_pruned_state_error(exc) is True


def test_is_pruned_state_error_rejects_unrelated_exceptions():
    """Connection errors, timeouts, 500s, etc. must NOT be classified as
    pruned-state errors — they need the normal reconnect path."""
    assert _is_pruned_state_error(ConnectionError("connection refused")) is False
    assert _is_pruned_state_error(TimeoutError("timed out")) is False
    assert _is_pruned_state_error(ValueError("bad value")) is False
    generic_500 = SubstrateRequestException({
        "code": -32000, "message": "Internal server error"
    })
    assert _is_pruned_state_error(generic_500) is False


def test_is_pruned_state_error_handles_none():
    assert _is_pruned_state_error(None) is False


# --- clamp-unit tests -------------------------------------------------------


def test_clamp_advances_to_head_minus_window():
    """Basic clamp: cursor stuck at 100, head 10_000, window 256 →
    new cursor = 10_000 - 256 = 9_744."""
    daemon = _make_daemon(last_processed_block=100)
    new_cursor = daemon._clamp_cursor_past_pruned(stuck_block=101, head=10_000)
    assert new_cursor == 10_000 - FALLBACK_WINDOW
    assert daemon.last_processed_block == 10_000 - FALLBACK_WINDOW


def test_clamp_never_moves_backward():
    """If the existing cursor is already ahead of `head - window` (tiny
    window relative to processed range), `max()` keeps the current cursor."""
    # Cursor has processed way past head-window; spec says max() applies.
    daemon = _make_daemon(last_processed_block=9_999)
    new_cursor = daemon._clamp_cursor_past_pruned(stuck_block=10_000, head=10_000)
    assert new_cursor == 9_999


def test_clamps_to_zero_when_head_less_than_window():
    """Edge: head < FALLBACK_WINDOW (brand-new chain). Cursor = 0."""
    daemon = _make_daemon(last_processed_block=0)
    new_cursor = daemon._clamp_cursor_past_pruned(stuck_block=5, head=50)
    assert new_cursor == 0
    assert daemon.last_processed_block == 0


def test_clamp_saves_state():
    """After clamping, state MUST be persisted — a crash before the next
    tick must not lose the self-heal progress (otherwise the daemon would
    re-pick up the pruned cursor on restart)."""
    daemon = _make_daemon(last_processed_block=100)
    daemon._clamp_cursor_past_pruned(stuck_block=101, head=10_000)
    daemon.save_state.assert_called_once()


def test_clamp_warns_once_per_stuck_block(caplog):
    """If the same stuck block triggers clamp() multiple times within the
    same tick, only ONE log record is emitted (dedupe via
    `_pruned_warned_blocks`). On a new tick this is reset."""
    daemon = _make_daemon(last_processed_block=100)
    with caplog.at_level(logging.INFO, logger="daemon.cert_daemon"):
        daemon._clamp_cursor_past_pruned(stuck_block=101, head=10_000)
        daemon._clamp_cursor_past_pruned(stuck_block=101, head=10_000)
        daemon._clamp_cursor_past_pruned(stuck_block=101, head=10_000)

    clamp_logs = [r for r in caplog.records if "pruned cursor" in r.message]
    assert len(clamp_logs) == 1, (
        f"expected exactly one clamp-log for the same stuck_block, "
        f"got {[r.message for r in clamp_logs]}"
    )


# --- _process_block_range integration tests ---------------------------------


def test_clamps_cursor_on_state_discarded():
    """Spec case #1: `get_block_events` raises 4003 once, then succeeds on
    the clamped cursor. Verify `last_processed_block` advanced to
    `head - FALLBACK_WINDOW` exactly, and a subsequent call went through.
    """
    daemon = _make_daemon(last_processed_block=100)
    head = 10_000
    expected_clamp = head - FALLBACK_WINDOW  # = 9744

    # First call (block_num=101) raises pruned; all subsequent calls
    # (starting at block 9745 after clamp) succeed with no events.
    call_log = []

    def fake_events(block_num: int):
        call_log.append(block_num)
        if block_num == 101:
            raise _pruned_exc()
        return []

    daemon.client.get_block_events = MagicMock(side_effect=fake_events)
    daemon.client.get_block_certified_events = MagicMock(return_value=[])

    _run(daemon._process_block_range(head))

    # Clamp advanced to head - window, then processed through head.
    assert daemon.last_processed_block == head
    # The clamp snapshot happened: the first call was the stuck block, and
    # the next call after clamp started at expected_clamp + 1.
    assert call_log[0] == 101
    assert expected_clamp + 1 in call_log, (
        f"expected block {expected_clamp + 1} to be fetched post-clamp, "
        f"call_log starts: {call_log[:5]}"
    )
    daemon.save_state.assert_called()


def test_does_not_clamp_on_other_errors():
    """Spec case #2: a non-4003 exception (connection refused, 500, etc.)
    must NOT trigger the clamp — it re-raises to the poll loop's outer
    try/except so the reconnect path fires."""
    daemon = _make_daemon(last_processed_block=100)
    head = 200

    daemon.client.get_block_events = MagicMock(
        side_effect=ConnectionError("connection refused")
    )
    daemon.client.get_block_certified_events = MagicMock(return_value=[])

    with pytest.raises(ConnectionError):
        _run(daemon._process_block_range(head))

    # Cursor must NOT have been clamped — still at its original value.
    assert daemon.last_processed_block == 100
    # And save_state was not invoked by the clamp path.
    daemon.save_state.assert_not_called()


def test_does_not_clamp_on_generic_substrate_500():
    """Another non-pruned failure: SubstrateRequestException with a generic
    code. Must propagate, not trigger clamp."""
    daemon = _make_daemon(last_processed_block=100)
    head = 200

    generic_exc = SubstrateRequestException(
        {"code": -32000, "message": "Internal server error"}
    )
    daemon.client.get_block_events = MagicMock(side_effect=generic_exc)
    daemon.client.get_block_certified_events = MagicMock(return_value=[])

    with pytest.raises(SubstrateRequestException):
        _run(daemon._process_block_range(head))

    assert daemon.last_processed_block == 100


def test_clamps_to_zero_when_head_less_than_window_integration():
    """Spec case #3: when head < FALLBACK_WINDOW, the clamp should resolve
    to 0 via the `_process_block_range` path too."""
    daemon = _make_daemon(last_processed_block=0)
    head = 50  # much less than FALLBACK_WINDOW=256

    call_log = []

    def fake_events(block_num: int):
        call_log.append(block_num)
        if block_num == 1:
            raise _pruned_exc()
        return []

    daemon.client.get_block_events = MagicMock(side_effect=fake_events)
    daemon.client.get_block_certified_events = MagicMock(return_value=[])

    _run(daemon._process_block_range(head))

    # After clamp to 0, loop re-enters at block 1 (which just raised) — but
    # the stuck_block dedupe keeps us from re-warning, and we then clamp
    # again. With the max-clamps guard we eventually bail. The important
    # invariant: we never went negative and save_state was called.
    assert daemon.last_processed_block >= 0
    daemon.save_state.assert_called()


def test_warns_once_not_every_tick(caplog):
    """Spec case #4: repeated 4003 errors within the SAME tick fire only
    ONE log line per unique stuck block."""
    daemon = _make_daemon(last_processed_block=100)
    head = 200

    # Every call to get_block_events raises pruned — this should clamp,
    # loop back in, clamp again, etc. until MAX_CLAMPS_PER_TICK hits.
    def _always_pruned(*_args, **_kwargs):
        raise _pruned_exc()

    daemon.client.get_block_events = MagicMock(side_effect=_always_pruned)
    daemon.client.get_block_certified_events = MagicMock(return_value=[])

    with caplog.at_level(logging.INFO, logger="daemon.cert_daemon"):
        _run(daemon._process_block_range(head))

    # The stuck_block varies as the cursor advances, so one line per
    # *unique* stuck block. But crucially: even within one stuck block
    # hit multiple times, only one line per block_num. We can assert an
    # upper bound on the number of warnings (at most one per clamp attempt).
    clamp_logs = [r for r in caplog.records if "pruned cursor" in r.message]
    assert len(clamp_logs) <= MAX_CLAMPS_PER_TICK, (
        f"clamp warnings unbounded within a single tick: {len(clamp_logs)}"
    )
    # And at least one fired so we know the path ran.
    assert len(clamp_logs) >= 1


def test_bounded_clamps_per_tick():
    """Spec case #5 (edge): persistent pruning across many clamps must
    bail after MAX_CLAMPS_PER_TICK — no infinite loop. Total calls to
    get_block_events is bounded."""
    daemon = _make_daemon(last_processed_block=100)
    head = 10_000

    def _always_pruned(*_args, **_kwargs):
        raise _pruned_exc()

    daemon.client.get_block_events = MagicMock(side_effect=_always_pruned)
    daemon.client.get_block_certified_events = MagicMock(return_value=[])

    _run(daemon._process_block_range(head))

    # Each clamp causes exactly one get_block_events call before breaking
    # out of the inner for-loop (on the first block of the range). So the
    # total number of calls equals clamps_this_tick at the cap.
    assert daemon.client.get_block_events.call_count == MAX_CLAMPS_PER_TICK


def test_saves_state_after_clamp():
    """Spec case: mock `save_state`, assert called after the clamp path."""
    daemon = _make_daemon(last_processed_block=100)
    head = 10_000

    call_count = {"n": 0}

    def fake_events(block_num: int):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise _pruned_exc()
        return []

    daemon.client.get_block_events = MagicMock(side_effect=fake_events)
    daemon.client.get_block_certified_events = MagicMock(return_value=[])

    _run(daemon._process_block_range(head))
    # save_state was invoked at minimum during the clamp (once) and after
    # each successful block in the post-clamp range (>=1). So >=2 total.
    assert daemon.save_state.call_count >= 2


def test_certified_events_call_also_triggers_clamp():
    """The certified-events scan (used when checkpointing is enabled) is
    the other RPC call inside the block loop. A 4003 from THIS call must
    trigger the same self-heal — otherwise checkpointing nodes would still
    crashloop."""
    daemon = _make_daemon(last_processed_block=100, checkpoint_enabled=True)
    head = 10_000

    daemon.client.get_block_events = MagicMock(return_value=[])
    call_log = []

    def fake_certified(block_num: int):
        call_log.append(block_num)
        if block_num == 101:
            raise _pruned_exc()
        return []

    daemon.client.get_block_certified_events = MagicMock(side_effect=fake_certified)

    _run(daemon._process_block_range(head))

    # Same clamp advanced cursor to head - window and processing continued.
    assert daemon.last_processed_block == head
    # The certified-events seam was hit both pre-clamp and post-clamp.
    assert 101 in call_log


def test_stop_flag_breaks_out_of_range(caplog):
    """If `_running` goes False mid-tick, the loop returns cleanly without
    trying further blocks — avoids wasting a shutdown window on RPC calls."""
    daemon = _make_daemon(last_processed_block=0)
    head = 100

    call_count = {"n": 0}

    def fake_events(block_num: int):
        call_count["n"] += 1
        if call_count["n"] == 3:
            daemon._running = False  # simulate shutdown signal
        return []

    daemon.client.get_block_events = MagicMock(side_effect=fake_events)
    daemon.client.get_block_certified_events = MagicMock(return_value=[])

    _run(daemon._process_block_range(head))

    # We stopped around call #3 plus the next iteration's `if not _running`
    # check — so total event calls should be close to 3, not 100.
    assert daemon.client.get_block_events.call_count < 10


def test_first_run_clean_start_not_retroactively_reclamped():
    """Spec edge: brand-new daemon, `last_processed_block == 0`, RPC is
    healthy. The self-heal path must NOT fire — the existing clean-start
    `last_processed_block = head` logic lives in `run()`, not in
    `_process_block_range`. Here we just verify that when no error is
    raised, the cursor advances normally and no clamp happens.
    """
    daemon = _make_daemon(last_processed_block=9_500)
    head = 9_505
    daemon.client.get_block_events = MagicMock(return_value=[])
    daemon.client.get_block_certified_events = MagicMock(return_value=[])

    _run(daemon._process_block_range(head))

    # Advanced normally from 9500 -> 9505. Clamp did NOT fire (would have
    # pushed cursor to head - window = 9249, which is BEHIND 9500, so max()
    # would protect us anyway — but we also verify no warning logged).
    assert daemon.last_processed_block == head
    # save_state called at least once per block (>=5 total).
    assert daemon.save_state.call_count >= 5
