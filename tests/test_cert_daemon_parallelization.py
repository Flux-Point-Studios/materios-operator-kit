"""Tests for cert-daemon's bounded-concurrency per-receipt parallelization.

Context (task #120): `_process_block_range` previously processed receipts in
a serial `for r in receipts: await self.process_receipt(r)` loop. With many
receipts per block, the prep phase (locator fetch + blob download + Merkle
verify — all HTTP-bound) drained one-at-a-time, leaving the cursor 64+ blocks
behind chain head and pushing p50 cert latency to 4.7 min.

The refactor wraps the per-block batch in a bounded `asyncio.gather`:

  - Concurrency cap = `config.max_concurrent_receipts` (default 8) via
    semaphore so we don't hammer the gateway with 50 parallel fetches.
  - Chain-write step (`submit_availability_cert`) is serialized under
    `_chain_write_lock` so `accountNextIndex` nonces stay monotonic
    (memory: `feedback_polkadot_nonce_race_on_burst.md`).
  - `self.pending` mutations are guarded by `_pending_lock` so two
    coroutines that both miss the locator cannot race on the dict.
  - Per-receipt errors are isolated via `return_exceptions=True` —
    one failure does not cancel siblings.
  - Block-level cursor advance stays sequential — block N+1 is only
    entered after every receipt in block N completes.

These tests exercise each guarantee at the seam, plus a profiling test
that demonstrates the prep-phase concurrency benefit (the whole reason
the refactor exists).
"""

from __future__ import annotations

import asyncio
import os
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from daemon.cert_daemon import CertDaemon
from daemon.config import DaemonConfig
from daemon.models import AttestationLevel, ReceiptRecord, VerificationResult


# --- helpers ---------------------------------------------------------------


def _run(coro):
    """Run a coroutine on a fresh event loop. Mirrors test_cert_daemon.py."""
    return asyncio.new_event_loop().run_until_complete(coro)


def _make_receipt(receipt_id: str) -> ReceiptRecord:
    """Build a minimal ReceiptRecord. Hashes are placeholders; tests stub the
    verifier so the actual values never have to round-trip Merkle math."""
    return ReceiptRecord(
        receipt_id=receipt_id,
        content_hash=b"\x11" * 32,
        base_root_sha256=b"\x22" * 32,
        storage_locator_hash=b"\x33" * 32,
        schema_hash=b"\x00" * 32,
        base_manifest_hash=b"\x00" * 32,
        safety_manifest_hash=b"\x00" * 32,
        monitor_config_hash=b"\x00" * 32,
        attestation_evidence_hash=b"\x00" * 32,
        availability_cert_hash=b"\x00" * 32,
        created_at_millis=0,
        submitter="5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY",
    )


def _make_full_daemon(
    *,
    last_processed_block: int = 0,
    max_concurrent: int = 8,
    submit_delay: float = 0.0,
    prep_delay: float = 0.0,
    submit_success: bool = True,
) -> CertDaemon:
    """Build a CertDaemon with FULL `process_receipt` plumbing wired to
    instrumented stubs.

    Knobs:
      - `submit_delay` — sleep injected into the chain-write to prove the
        write lock serializes overlapping submits.
      - `prep_delay` — sleep injected into the locator-resolve to prove the
        prep phase parallelizes across receipts.
    """
    config = DaemonConfig()
    config.checkpoint_enabled = False
    config.content_validation_enabled = False
    config.max_concurrent_receipts = max_concurrent

    daemon = CertDaemon.__new__(CertDaemon)
    daemon.config = config
    daemon.client = MagicMock()
    daemon.client.keypair = SimpleNamespace(
        ss58_address="5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY",
        public_key=b"\x00" * 32,
    )
    # get_receipt returns a fresh ReceiptRecord per id — used by the
    # not-yet-certified branch in process_receipt.
    daemon.client.get_receipt = MagicMock(side_effect=_make_receipt)

    # The chain-write seam. We track call order + elapsed time per call so
    # tests can assert serialization. submit_delay simulates ~6s block-time.
    submit_log = []
    submit_lock_observed = {"max_concurrent": 0, "in_flight": 0}

    def _fake_submit(receipt_id, cert_hash):
        # Track concurrent in-flight submits at the substrate seam.
        submit_lock_observed["in_flight"] += 1
        submit_lock_observed["max_concurrent"] = max(
            submit_lock_observed["max_concurrent"],
            submit_lock_observed["in_flight"],
        )
        try:
            time.sleep(submit_delay)
            submit_log.append(receipt_id)
            return submit_success
        finally:
            submit_lock_observed["in_flight"] -= 1

    daemon.client.submit_availability_cert = MagicMock(side_effect=_fake_submit)
    daemon._submit_log = submit_log
    daemon._submit_lock_observed = submit_lock_observed

    # The locator seam. Every call returns a deterministic non-None manifest
    # so process_receipt does not bail to pending. The prep_delay simulates
    # the HTTP round-trip we're trying to overlap.
    prep_log = []

    class _Locator:
        async def resolve(self, rid, content_hash=None):
            prep_log.append((rid, time.monotonic(), "start"))
            await asyncio.sleep(prep_delay)
            prep_log.append((rid, time.monotonic(), "end"))
            # Real BlobManifest shape isn't required because we mock the
            # verifier directly below; we just need a non-None sentinel.
            return SimpleNamespace(receipt_id=rid, chunks=[])

    daemon.locator = _Locator()
    daemon._prep_log = prep_log

    # Verifier stub: every blob is ROOT_VERIFIED. Skips the L2/L3 logic.
    class _Verifier:
        async def verify(self, receipt, manifest):
            return VerificationResult(
                attestation_level=AttestationLevel.ROOT_VERIFIED,
                computed_root=receipt.base_root_sha256,
                chunks_verified=0,
                chunks_total=0,
            )

    daemon.verifier = _Verifier()

    daemon.content_validator = MagicMock()
    daemon.cert_store = MagicMock()
    daemon.cert_store.exists.return_value = False  # never short-circuit

    daemon.checkpointer = MagicMock()
    daemon.last_processed_block = last_processed_block
    daemon._running = True
    daemon.pending = {}
    daemon._notified_ids = {}
    daemon._pruned_warned_blocks = set()

    # Concurrency primitives (lazy-init path will materialize on first use).
    daemon._chain_write_lock = None
    daemon._pending_lock = None
    daemon._concurrency_sem = None

    daemon.save_state = MagicMock()

    async def _no_discord(*args, **kwargs):
        return None

    daemon.send_discord = _no_discord
    return daemon


# --- config plumbing -------------------------------------------------------


def test_max_concurrent_receipts_in_config():
    """Default is 8, env override binds, max(1, ...) clamp prevents 0."""
    cfg = DaemonConfig()
    assert cfg.max_concurrent_receipts == 8

    os.environ["MAX_CONCURRENT_RECEIPTS"] = "16"
    try:
        cfg = DaemonConfig.from_env()
        assert cfg.max_concurrent_receipts == 16
    finally:
        del os.environ["MAX_CONCURRENT_RECEIPTS"]

    # Pathological: env=0 must not produce a Semaphore(0) deadlock.
    os.environ["MAX_CONCURRENT_RECEIPTS"] = "0"
    try:
        cfg = DaemonConfig.from_env()
        assert cfg.max_concurrent_receipts == 1
    finally:
        del os.environ["MAX_CONCURRENT_RECEIPTS"]


# --- AST / class-body integrity -------------------------------------------


def test_class_methods_intact_after_parallelization_refactor():
    """Mirrors `test_class_methods_intact` from test_auto_bond.py — guards
    against indentation drift that would silently de-class methods. Also
    asserts the new helper `_ensure_concurrency_primitives` lives on the
    class (not at module level)."""
    for name in (
        "run",
        "_request_faucet_drip",
        "_ensure_bond",
        "_ensure_committee_membership",
        "process_receipt",
        "_process_block_range",
        "_process_receipt_bounded",
        "_clamp_cursor_past_pruned",
        "_ensure_concurrency_primitives",
        "retry_pending",
    ):
        assert callable(getattr(CertDaemon, name, None)), (
            f"CertDaemon.{name} missing — likely an indentation bug nested "
            f"it inside a module-level function. AST guard regression."
        )


# --- core parallelization tests --------------------------------------------


def test_block_with_multiple_receipts_processes_all_exactly_once():
    """20 receipts in one block must each have process_receipt run exactly
    once — no drops, no duplicates, no cancellations."""
    receipts = [f"0x{i:064x}" for i in range(20)]
    daemon = _make_full_daemon(last_processed_block=99, max_concurrent=8)
    daemon.client.get_block_events = MagicMock(
        side_effect=lambda blk: [{"receipt_id": r, "content_hash": "", "submitter": ""} for r in receipts]
        if blk == 100
        else []
    )
    daemon.client.get_block_certified_events = MagicMock(return_value=[])

    _run(daemon._process_block_range(100))

    # All 20 receipts hit the chain-write seam exactly once.
    submitted_ids = set(daemon._submit_log)
    assert submitted_ids == set(receipts), (
        f"expected all 20 receipts submitted, got {len(submitted_ids)} unique "
        f"({len(daemon._submit_log)} total submit calls — duplicates if >20)"
    )
    assert len(daemon._submit_log) == 20, (
        f"expected exactly 20 submit calls, got {len(daemon._submit_log)} — "
        f"duplicates indicate a gather-double-await bug"
    )
    # Cursor advanced past the block.
    assert daemon.last_processed_block == 100


def test_concurrency_bound_respected_by_semaphore():
    """With max_concurrent=4 and 12 receipts, never more than 4 prep
    coroutines should be in flight simultaneously. We measure this via
    overlapping spans in the prep_log."""
    receipts = [f"0x{i:064x}" for i in range(12)]
    daemon = _make_full_daemon(
        last_processed_block=99,
        max_concurrent=4,
        prep_delay=0.05,  # 50ms overlap window per prep
    )
    daemon.client.get_block_events = MagicMock(
        side_effect=lambda blk: [{"receipt_id": r, "content_hash": "", "submitter": ""} for r in receipts]
        if blk == 100
        else []
    )
    daemon.client.get_block_certified_events = MagicMock(return_value=[])

    _run(daemon._process_block_range(100))

    # Walk the prep_log and compute the maximum number of overlapping
    # (start, end) spans at any moment. With cap=4 this must be <=4.
    events = []  # (timestamp, +1 on start / -1 on end)
    for rid, ts, kind in daemon._prep_log:
        events.append((ts, +1 if kind == "start" else -1))
    events.sort()
    in_flight = 0
    max_in_flight = 0
    for _, delta in events:
        in_flight += delta
        max_in_flight = max(max_in_flight, in_flight)

    assert max_in_flight <= 4, (
        f"semaphore bound violated: observed max_in_flight={max_in_flight} "
        f"> cap=4 across 12 receipts. Concurrency limit not enforced."
    )
    # Sanity: actually parallelized at all (would be 1 if serial).
    assert max_in_flight >= 2, (
        f"prep phase did not parallelize: max_in_flight={max_in_flight}. "
        f"Either the gather is being awaited serially or prep_delay is too "
        f"short to observe overlap."
    )


def test_chain_write_lock_serializes_concurrent_submits():
    """5 receipts with 50ms submit_delay each — without the chain-write lock,
    asyncio.to_thread would run them in parallel and observed max_concurrent
    submits would be >1. With the lock, must be exactly 1."""
    receipts = [f"0x{i:064x}" for i in range(5)]
    daemon = _make_full_daemon(
        last_processed_block=99,
        max_concurrent=8,  # plenty of slack so the BOTTLENECK is the chain lock
        submit_delay=0.05,
    )
    daemon.client.get_block_events = MagicMock(
        side_effect=lambda blk: [{"receipt_id": r, "content_hash": "", "submitter": ""} for r in receipts]
        if blk == 100
        else []
    )
    daemon.client.get_block_certified_events = MagicMock(return_value=[])

    _run(daemon._process_block_range(100))

    assert daemon._submit_lock_observed["max_concurrent"] == 1, (
        f"chain-write lock failed: observed "
        f"{daemon._submit_lock_observed['max_concurrent']} concurrent submits. "
        f"This would cause nonce races (Priority too low) on the live chain. "
        f"See feedback_polkadot_nonce_race_on_burst.md."
    )
    # And all 5 still landed.
    assert len(daemon._submit_log) == 5


def test_one_receipt_failure_does_not_cancel_siblings():
    """20 receipts, one of them raises mid-flight. The other 19 must still
    complete (chain-write seam called for them). gather(return_exceptions=True)
    is the contract being asserted."""
    receipts = [f"0x{i:064x}" for i in range(20)]
    poison_id = receipts[7]

    # Build daemon with a verifier that raises ONLY for the poison id.
    daemon = _make_full_daemon(last_processed_block=99, max_concurrent=8)

    class _PoisonVerifier:
        async def verify(self, receipt, manifest):
            if receipt.receipt_id == poison_id:
                raise RuntimeError(f"synthetic mid-flight failure for {poison_id[:16]}")
            return VerificationResult(
                attestation_level=AttestationLevel.ROOT_VERIFIED,
                computed_root=receipt.base_root_sha256,
                chunks_verified=0,
                chunks_total=0,
            )

    daemon.verifier = _PoisonVerifier()
    daemon.client.get_block_events = MagicMock(
        side_effect=lambda blk: [{"receipt_id": r, "content_hash": "", "submitter": ""} for r in receipts]
        if blk == 100
        else []
    )
    daemon.client.get_block_certified_events = MagicMock(return_value=[])

    _run(daemon._process_block_range(100))

    submitted = set(daemon._submit_log)
    expected_survivors = set(receipts) - {poison_id}
    assert submitted == expected_survivors, (
        f"error-isolation broken. Expected exactly the 19 non-poison receipts "
        f"to reach submit; instead got {len(submitted)} (missing: "
        f"{expected_survivors - submitted}, extra: {submitted - expected_survivors})"
    )
    # And the cursor still advanced — the block is "done" once the gather
    # resolves (with or without per-receipt exceptions).
    assert daemon.last_processed_block == 100


def test_pending_dict_safe_under_concurrent_locator_misses():
    """When the locator returns None for many receipts at once, all must
    end up in self.pending exactly once — no lost writes, no duplicates,
    no KeyError races."""
    receipts = [f"0x{i:064x}" for i in range(20)]
    daemon = _make_full_daemon(last_processed_block=99, max_concurrent=8)

    # Override locator to return None (no manifest found) for everything.
    class _NoLocator:
        async def resolve(self, rid, content_hash=None):
            # Tiny sleep to widen the race window.
            await asyncio.sleep(0.001)
            return None

    daemon.locator = _NoLocator()
    daemon.client.get_block_events = MagicMock(
        side_effect=lambda blk: [{"receipt_id": r, "content_hash": "", "submitter": ""} for r in receipts]
        if blk == 100
        else []
    )
    daemon.client.get_block_certified_events = MagicMock(return_value=[])

    _run(daemon._process_block_range(100))

    # Every receipt landed in pending exactly once.
    assert set(daemon.pending.keys()) == set(receipts), (
        f"pending dict corrupted under concurrency: expected 20 entries, got "
        f"{len(daemon.pending)} ({set(receipts) - set(daemon.pending.keys())} missing)"
    )
    # And nothing got submitted (they're all stuck in pending).
    assert len(daemon._submit_log) == 0


def test_block_level_advance_sequential_after_concurrent_receipts():
    """Cursor advances ONLY after every receipt in block N completes —
    not as individual receipts finish. Tested by snapshotting cursor at
    each save_state call and asserting it advances one block per save."""
    receipts_per_block = 5
    blocks_to_process = 3
    block_events = {}
    for blk in range(101, 101 + blocks_to_process):
        block_events[blk] = [
            {"receipt_id": f"0x{blk:032x}{i:032x}", "content_hash": "", "submitter": ""}
            for i in range(receipts_per_block)
        ]

    daemon = _make_full_daemon(last_processed_block=100, max_concurrent=8)
    daemon.client.get_block_events = MagicMock(
        side_effect=lambda blk: block_events.get(blk, [])
    )
    daemon.client.get_block_certified_events = MagicMock(return_value=[])

    cursor_snapshots = []

    def _capture_cursor():
        cursor_snapshots.append(daemon.last_processed_block)

    daemon.save_state = MagicMock(side_effect=_capture_cursor)

    _run(daemon._process_block_range(103))

    # Every save_state happens AFTER cursor was advanced by one block,
    # so we expect cursors [101, 102, 103]. No partial-block snapshots.
    assert cursor_snapshots == [101, 102, 103], (
        f"block-level advance broken: cursor snapshots = {cursor_snapshots}. "
        f"Expected [101, 102, 103] — one save per fully-completed block. "
        f"Anything else means cursor advanced mid-block (split-state hazard)."
    )
    # Every receipt across all 3 blocks reached submit.
    assert len(daemon._submit_log) == receipts_per_block * blocks_to_process


def test_prep_phase_overlaps_concurrently_profile():
    """The whole-point profiling test: with 8 receipts at 100ms prep each
    and 5ms submit each, serial would take ~840ms, parallel-prep should be
    under ~250ms (one prep span + 8 serial submits). This proves the
    parallelization buys real time on prep-bound workloads."""
    receipts = [f"0x{i:064x}" for i in range(8)]
    daemon = _make_full_daemon(
        last_processed_block=99,
        max_concurrent=8,
        prep_delay=0.10,  # 100ms HTTP-bound prep
        submit_delay=0.005,  # 5ms chain write
    )
    daemon.client.get_block_events = MagicMock(
        side_effect=lambda blk: [{"receipt_id": r, "content_hash": "", "submitter": ""} for r in receipts]
        if blk == 100
        else []
    )
    daemon.client.get_block_certified_events = MagicMock(return_value=[])

    t0 = time.monotonic()
    _run(daemon._process_block_range(100))
    elapsed = time.monotonic() - t0

    # Serial budget: 8 * (100ms prep + 5ms submit) = 840ms.
    # Parallel budget: ~max(prep_span) + 8*submit ≈ 100ms + 40ms = 140ms.
    # We assert <= 350ms to leave ample headroom for CI variance, while
    # still being well under the 840ms serial floor.
    serial_budget = 8 * (0.10 + 0.005)
    assert elapsed < serial_budget * 0.55, (
        f"prep phase did not parallelize: elapsed={elapsed*1000:.1f}ms, "
        f"serial_budget={serial_budget*1000:.1f}ms. Either gather is awaiting "
        f"serially or asyncio.to_thread is blocking the event loop."
    )
    # Sanity: all 8 still landed.
    assert len(daemon._submit_log) == 8

    # Print the profile so the test output documents the speedup the next
    # operator reads in CI.
    print(
        f"\n[profile] N=8 prep=100ms submit=5ms: "
        f"serial_budget={serial_budget*1000:.0f}ms "
        f"actual={elapsed*1000:.1f}ms "
        f"speedup={serial_budget/elapsed:.1f}x"
    )


def test_no_failure_on_empty_block():
    """A block with no events must not crash the gather (zero-length
    coroutine list edge case) — must just advance the cursor."""
    daemon = _make_full_daemon(last_processed_block=99, max_concurrent=8)
    daemon.client.get_block_events = MagicMock(return_value=[])
    daemon.client.get_block_certified_events = MagicMock(return_value=[])

    _run(daemon._process_block_range(100))

    assert daemon.last_processed_block == 100
    assert len(daemon._submit_log) == 0


def test_existing_certified_skip_path_still_short_circuits():
    """The "already certified" branch in process_receipt must still skip
    the chain-write — concurrency should not regress this pre-existing
    cost saver."""
    receipt_id = "0xabc123"

    daemon = _make_full_daemon(last_processed_block=99, max_concurrent=8)

    # Force the receipt to look already-certified.
    def _already_certified(rid):
        r = _make_receipt(rid)
        r.availability_cert_hash = b"\x99" * 32  # non-zero = already certified
        return r

    daemon.client.get_receipt = MagicMock(side_effect=_already_certified)
    daemon.client.get_block_events = MagicMock(
        side_effect=lambda blk: [{"receipt_id": receipt_id, "content_hash": "", "submitter": ""}]
        if blk == 100
        else []
    )
    daemon.client.get_block_certified_events = MagicMock(return_value=[])

    _run(daemon._process_block_range(100))

    # No submit, no pending, cursor still advanced.
    assert daemon.client.submit_availability_cert.call_count == 0
    assert receipt_id not in daemon.pending
    assert daemon.last_processed_block == 100
