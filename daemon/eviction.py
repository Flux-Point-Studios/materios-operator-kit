"""Pending-receipt eviction (task #180).

Three eviction reasons, applied in priority order per call:

  1. ``synthetic``   — receipt-id matches a low-entropy / repeating-stride
                       pattern characteristic of stress-test harness output.
                       (See :func:`is_synthetic_hash`.)
  2. ``ttl``         — entry has been pending for longer than
                       ``EvictionConfig.max_age_seconds`` and still has no
                       resolvable locator. The TPS test residue on Node-2
                       (71k entries on 2026-04-25) is precisely this:
                       backing blobs never existed, so locator returns 404
                       forever. We cap the loop at a sane wall-clock age.
  3. ``failure_cap`` — N consecutive locator-lookup failures
                       (``EvictionConfig.max_failures``). Catches receipts
                       whose backing gateway is permanently gone before
                       the TTL would fire.

Synthetic-pattern wins over TTL/failure-cap so the stats counters
are meaningful (a stress-test residue entry that's both old and synthetic
gets counted as ``synthetic``, not ``ttl``).

This module is pure (no I/O, no async) so it can be unit-tested without
a substrate node, gateway, or container.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable

# Reason tags — stable string constants (used in logs, status, tests).
SYNTHETIC = "synthetic"
TTL = "ttl"
FAILURE_CAP = "failure_cap"


@dataclass
class EvictionStats:
    evicted_synthetic: int = 0
    evicted_ttl: int = 0
    evicted_failure_cap: int = 0

    @property
    def total(self) -> int:
        return self.evicted_synthetic + self.evicted_ttl + self.evicted_failure_cap


@dataclass
class EvictionConfig:
    """Eviction thresholds.

    All three knobs admit a "disable" sentinel:
      * max_age_seconds=0  → TTL eviction off
      * max_failures=0     → failure-cap eviction off
      * prune_synthetic=False → synthetic-pattern eviction off

    Defaults match production cert-daemon config (6h TTL, 60-failure cap,
    synthetic prune ON).
    """
    max_age_seconds: int = 6 * 3600
    max_failures: int = 60
    prune_synthetic: bool = True


# ----------------------------- synthetic detection -----------------------------

# Cheap up-front guards: a real receipt id is `0x` + 64 hex chars (32 bytes).
_HEX_CHARS = set("0123456789abcdefABCDEF")


def _normalize(receipt_id: str) -> str | None:
    """Return the lower-case 64-char hex body, or None if not a valid 32-byte id."""
    if not isinstance(receipt_id, str):
        return None
    body = receipt_id.removeprefix("0x").removeprefix("0X")
    if len(body) != 64:
        return None
    if any(c not in _HEX_CHARS for c in body):
        return None
    return body.lower()


def is_synthetic_hash(receipt_id: str) -> bool:
    """True if ``receipt_id`` looks like a stress-test synthetic hash.

    Detection rule (conservative — false-negatives preferred over
    false-positives, since false-positives nuke real receipts):

      * The 32-byte hex body must repeat as N copies of a stride S where
        S ∈ {1, 2, 4, 8, 16} bytes. (32-byte hashes from real cryptographic
        digests will essentially never satisfy this.)

    That alone catches the harness's `(byte4)*8` pattern (production
    residue: ``0cffde37`` × 8) plus the simpler all-zero / all-FF /
    `cafebabe` × 8 / etc shapes that any future harness might use.

    The earlier "low unique byte count" heuristic was deliberately
    discarded: fewer than 4 unique bytes in a real SHA-256 happens
    P ≈ 10^-30; conversely, hand-rolled "random-looking" test fixtures
    sometimes only use 4-8 unique chars and would trigger false positives.
    Strict periodicity is the cleanest discriminator.
    """
    body = _normalize(receipt_id)
    if body is None:
        return False
    # body is 64 hex chars = 32 bytes.
    # Try strides in bytes: 1, 2, 4, 8, 16.
    for stride_bytes in (1, 2, 4, 8, 16):
        stride_chars = stride_bytes * 2
        head = body[:stride_chars]
        # body == head * (64 / stride_chars)
        if body == head * (64 // stride_chars):
            return True
    return False


# ----------------------------- eviction core -----------------------------

def evict_pending(
    pending: Dict[str, object],
    *,
    now: float,
    cfg: EvictionConfig,
) -> EvictionStats:
    """Evict residue from the ``pending`` dict in-place.

    ``pending`` maps receipt_id → ``PendingReceipt`` (duck-typed: needs
    ``first_seen: float`` and ``failure_count: int`` attrs). We only iterate
    a snapshot of keys so concurrent inserts elsewhere are safe.

    Returns counters by reason. Each evicted entry is counted exactly
    once, with priority synthetic > ttl > failure_cap.
    """
    stats = EvictionStats()
    if not pending:
        return stats

    # snapshot to avoid "dict changed during iteration" if caller mutates
    # the dict from another coroutine (we don't, but cheap defense).
    keys: Iterable[str] = list(pending.keys())

    for rid in keys:
        entry = pending.get(rid)
        if entry is None:
            continue

        # 1. Synthetic — fastest check first
        if cfg.prune_synthetic and is_synthetic_hash(rid):
            del pending[rid]
            stats.evicted_synthetic += 1
            continue

        # 2. TTL
        if cfg.max_age_seconds > 0:
            first_seen = getattr(entry, "first_seen", 0.0)
            if now - first_seen > cfg.max_age_seconds:
                del pending[rid]
                stats.evicted_ttl += 1
                continue

        # 3. Failure-count cap
        if cfg.max_failures > 0:
            failure_count = getattr(entry, "failure_count", 0)
            if failure_count >= cfg.max_failures:
                del pending[rid]
                stats.evicted_failure_cap += 1
                continue

    return stats
