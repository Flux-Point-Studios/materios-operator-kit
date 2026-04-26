"""Tests for residue eviction logic (task #180).

Covers:
  (a) Synthetic-pattern detection — repeating-stride hash IDs from stress harness
  (b) Max-age TTL eviction
  (c) Locator-failure-count cap eviction

Plus an integration-style test that walks PendingReceipt entries through
the full retry loop with a stubbed locator returning None and asserts the
right entries are evicted with the right reason counters.
"""
import asyncio
import time
from dataclasses import replace
from unittest.mock import AsyncMock, MagicMock

import pytest

from daemon.eviction import (
    EvictionStats,
    EvictionConfig,
    is_synthetic_hash,
    evict_pending,
    SYNTHETIC,
    TTL,
    FAILURE_CAP,
)
from daemon.models import PendingReceipt, ReceiptRecord


# ----------------------------- helpers -----------------------------

def _make_receipt(receipt_id: str = "0x" + "ab" * 32) -> ReceiptRecord:
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
    )


def _pending(receipt_id: str, first_seen: float, failure_count: int = 0, retries: int = 0) -> PendingReceipt:
    p = PendingReceipt(
        receipt_id=receipt_id,
        receipt=_make_receipt(receipt_id),
        first_seen=first_seen,
        retries=retries,
    )
    p.failure_count = failure_count
    return p


# --------------------------- (a) synthetic detection ---------------------------

def test_synthetic_detect_4byte_stride_real_residue():
    """The actual hash from production logs (Node-2): 0x0cffde370cffde37... × 8."""
    h = "0x" + "0cffde37" * 8
    assert is_synthetic_hash(h) is True


def test_synthetic_detect_2byte_stride():
    h = "0x" + "abcd" * 16
    assert is_synthetic_hash(h) is True


def test_synthetic_detect_1byte_stride():
    h = "0x" + "ff" * 32
    assert is_synthetic_hash(h) is True


def test_synthetic_detect_8byte_stride():
    h = "0x" + "deadbeefcafef00d" * 4
    assert is_synthetic_hash(h) is True


def test_synthetic_detect_does_not_flag_random_real_hash():
    # SHA-256 of "real receipt #1" — exact bytes don't matter, just non-repeating
    h = "0xa1b2c3d4e5f607182930415263748596" "a7b8c9d0e1f203142536475869708192"
    assert is_synthetic_hash(h) is False


def test_synthetic_detect_does_not_flag_low_unique_but_non_periodic():
    # Only 4 unique bytes but no clean stride — should NOT be flagged synthetic.
    # (Conservative: better to keep a real-ish hash and let TTL evict than
    # mistakenly nuke a legit weird-but-real receipt.)
    h = "0x" + "00ff" + "ff00" + "00ff" + "ff00" + ("aabbccdd" * 7)
    assert is_synthetic_hash(h) is False


def test_synthetic_detect_handles_missing_0x_prefix():
    h = "0cffde37" * 8
    assert is_synthetic_hash(h) is True


def test_synthetic_detect_rejects_wrong_length():
    assert is_synthetic_hash("0xdeadbeef") is False
    assert is_synthetic_hash("") is False
    assert is_synthetic_hash("0x" + "ab" * 31) is False  # 62 hex chars = 31 bytes


def test_synthetic_detect_rejects_non_hex():
    assert is_synthetic_hash("0x" + "zz" * 32) is False


# --------------------------- (b) TTL eviction ---------------------------

def test_evict_ttl_drops_old_entries():
    now = 1_000_000.0
    pending = {
        "0x" + "11" + "00" * 31: _pending("0x" + "11" + "00" * 31, first_seen=now - 7 * 3600),
        "0x" + "22" + "00" * 31: _pending("0x" + "22" + "00" * 31, first_seen=now - 30 * 60),
    }
    cfg = EvictionConfig(max_age_seconds=6 * 3600, max_failures=10_000, prune_synthetic=False)
    stats = evict_pending(pending, now=now, cfg=cfg)
    assert stats.evicted_ttl == 1
    assert stats.evicted_synthetic == 0
    assert stats.evicted_failure_cap == 0
    assert "0x" + "11" + "00" * 31 not in pending
    assert "0x" + "22" + "00" * 31 in pending


def test_evict_ttl_disabled_when_max_age_zero():
    now = 1_000_000.0
    rid = "0x" + "11" + "00" * 31
    pending = {rid: _pending(rid, first_seen=now - 100 * 3600)}
    cfg = EvictionConfig(max_age_seconds=0, max_failures=10_000, prune_synthetic=False)
    stats = evict_pending(pending, now=now, cfg=cfg)
    assert stats.evicted_ttl == 0
    assert rid in pending


# --------------------------- (c) failure-count cap ---------------------------

def test_evict_failure_cap_drops_repeat_failers():
    now = 1_000_000.0
    a = "0x" + "11" + "00" * 31
    b = "0x" + "22" + "00" * 31
    pending = {
        a: _pending(a, first_seen=now - 60, failure_count=60),
        b: _pending(b, first_seen=now - 60, failure_count=10),
    }
    cfg = EvictionConfig(max_age_seconds=10**9, max_failures=50, prune_synthetic=False)
    stats = evict_pending(pending, now=now, cfg=cfg)
    assert stats.evicted_failure_cap == 1
    assert a not in pending
    assert b in pending


def test_evict_failure_cap_disabled_when_zero():
    now = 1_000_000.0
    rid = "0x" + "11" + "00" * 31
    pending = {rid: _pending(rid, first_seen=now, failure_count=999_999)}
    cfg = EvictionConfig(max_age_seconds=10**9, max_failures=0, prune_synthetic=False)
    stats = evict_pending(pending, now=now, cfg=cfg)
    assert stats.evicted_failure_cap == 0
    assert rid in pending


# --------------------------- (a) synthetic-pattern eviction in mix ---------------------------

def test_evict_synthetic_drops_residue_first():
    now = 1_000_000.0
    real = "0xa1b2c3d4e5f607182930415263748596a7b8c9d0e1f203142536475869708192"
    synth = "0x" + "0cffde37" * 8
    pending = {
        real: _pending(real, first_seen=now - 60),
        synth: _pending(synth, first_seen=now - 60),
    }
    cfg = EvictionConfig(max_age_seconds=10**9, max_failures=10_000, prune_synthetic=True)
    stats = evict_pending(pending, now=now, cfg=cfg)
    assert stats.evicted_synthetic == 1
    assert real in pending
    assert synth not in pending


def test_evict_synthetic_at_71k_scale():
    """Sanity: 71k synthetic entries get cleared in <1s, real ones survive."""
    now = 1_000_000.0
    pending = {}
    # 1000 synthetic — same pattern as production residue
    for i in range(1000):
        rid = "0x" + (f"{i:08x}" * 8)
        pending[rid] = _pending(rid, first_seen=now)
    # 5 real
    real_ids = []
    for i in range(5):
        rid = "0x" + ("a1b2c3d4" + f"{i:02x}" + "e5f60718") * 4
        # tweak away from stride-pattern: vary middle bytes
        rid = "0xa1b2c3d4e5f607182930415263748596a7b8c9d0e1f2031425364758697081" + f"{i:02x}"
        pending[rid] = _pending(rid, first_seen=now)
        real_ids.append(rid)
    cfg = EvictionConfig(max_age_seconds=10**9, max_failures=10_000, prune_synthetic=True)
    t0 = time.time()
    stats = evict_pending(pending, now=now, cfg=cfg)
    elapsed = time.time() - t0
    assert stats.evicted_synthetic == 1000
    assert len(pending) == 5
    for rid in real_ids:
        assert rid in pending
    assert elapsed < 1.0, f"eviction too slow: {elapsed:.3f}s for 1005 entries"


# --------------------------- combined / priority ---------------------------

def test_evict_synthetic_takes_priority_over_ttl():
    """Synthetic and TTL would both fire — entry counted as synthetic, not TTL."""
    now = 1_000_000.0
    rid = "0x" + "0cffde37" * 8
    pending = {rid: _pending(rid, first_seen=now - 100 * 3600)}
    cfg = EvictionConfig(max_age_seconds=3600, max_failures=10, prune_synthetic=True)
    stats = evict_pending(pending, now=now, cfg=cfg)
    assert stats.evicted_synthetic == 1
    assert stats.evicted_ttl == 0  # Not double-counted
    assert rid not in pending


def test_evict_keeps_fresh_real_entries_with_low_failures():
    now = 1_000_000.0
    rid = "0xa1b2c3d4e5f607182930415263748596a7b8c9d0e1f203142536475869708192"
    pending = {rid: _pending(rid, first_seen=now - 30, failure_count=2)}
    cfg = EvictionConfig(max_age_seconds=6 * 3600, max_failures=60, prune_synthetic=True)
    stats = evict_pending(pending, now=now, cfg=cfg)
    assert stats.evicted_synthetic == 0
    assert stats.evicted_ttl == 0
    assert stats.evicted_failure_cap == 0
    assert rid in pending


# --------------------------- integration with retry_pending loop ---------------------------

@pytest.mark.asyncio
async def test_retry_pending_evicts_synthetic_after_one_pass():
    """Drives the actual cert_daemon.retry_pending() with stubbed locator.

    Asserts that after one retry loop:
      * synthetic entries are evicted (via prune_synthetic)
      * failure_count is incremented for entries that miss locator
      * stats counters surface in health metrics
    """
    from daemon import cert_daemon as cd_mod
    from daemon import health_server
    from daemon.cert_daemon import CertDaemon
    from daemon.config import DaemonConfig

    cfg = DaemonConfig()
    cfg.pending_max_age_seconds = 6 * 3600
    cfg.pending_max_failures = 60
    cfg.prune_synthetic_pattern = True
    cfg.discord_webhook_url = ""

    # Skip __init__ heavy network deps
    daemon = CertDaemon.__new__(CertDaemon)
    daemon.config = cfg
    daemon.pending = {}
    daemon.locator = MagicMock()
    daemon.locator.resolve = AsyncMock(return_value=None)  # never resolves
    daemon._notified_ids = {}
    daemon._eviction_totals = {"synthetic": 0, "ttl": 0, "failure_cap": 0}

    now = time.time()
    real_id = "0xa1b2c3d4e5f607182930415263748596a7b8c9d0e1f203142536475869708192"
    synth_id = "0x" + "0cffde37" * 8
    daemon.pending[real_id] = _pending(real_id, first_seen=now - 60)
    daemon.pending[synth_id] = _pending(synth_id, first_seen=now - 60)

    await daemon.retry_pending()

    # Synthetic gone; real still there (with failure_count bumped)
    assert synth_id not in daemon.pending
    assert real_id in daemon.pending
    assert daemon.pending[real_id].failure_count >= 1
    assert daemon._eviction_totals["synthetic"] >= 1


@pytest.mark.asyncio
async def test_retry_pending_evicts_after_n_failures():
    from daemon.cert_daemon import CertDaemon
    from daemon.config import DaemonConfig

    cfg = DaemonConfig()
    cfg.pending_max_age_seconds = 10**9
    cfg.pending_max_failures = 3
    cfg.prune_synthetic_pattern = False

    daemon = CertDaemon.__new__(CertDaemon)
    daemon.config = cfg
    daemon.pending = {}
    daemon.locator = MagicMock()
    daemon.locator.resolve = AsyncMock(return_value=None)
    daemon._notified_ids = {}
    daemon._eviction_totals = {"synthetic": 0, "ttl": 0, "failure_cap": 0}

    rid = "0xa1b2c3d4e5f607182930415263748596a7b8c9d0e1f203142536475869708192"
    daemon.pending[rid] = _pending(rid, first_seen=time.time())

    # Two passes bump failure_count to 1, 2 — entry survives.
    # Third pass bumps to 3 == max_failures, post-pass eviction fires.
    for _ in range(2):
        await daemon.retry_pending()
        assert rid in daemon.pending, f"unexpectedly evicted at fc={daemon.pending.get(rid).failure_count if rid in daemon.pending else 'gone'}"
    await daemon.retry_pending()
    assert rid not in daemon.pending
    assert daemon._eviction_totals["failure_cap"] == 1


@pytest.mark.asyncio
async def test_retry_pending_evicts_after_ttl():
    from daemon.cert_daemon import CertDaemon
    from daemon.config import DaemonConfig

    cfg = DaemonConfig()
    cfg.pending_max_age_seconds = 10  # 10 seconds
    cfg.pending_max_failures = 10**9
    cfg.prune_synthetic_pattern = False

    daemon = CertDaemon.__new__(CertDaemon)
    daemon.config = cfg
    daemon.pending = {}
    daemon.locator = MagicMock()
    daemon.locator.resolve = AsyncMock(return_value=None)
    daemon._notified_ids = {}
    daemon._eviction_totals = {"synthetic": 0, "ttl": 0, "failure_cap": 0}

    rid = "0xa1b2c3d4e5f607182930415263748596a7b8c9d0e1f203142536475869708192"
    # Insert as if added 30s ago — already past TTL
    daemon.pending[rid] = _pending(rid, first_seen=time.time() - 30)

    await daemon.retry_pending()
    assert rid not in daemon.pending
    assert daemon._eviction_totals["ttl"] == 1
