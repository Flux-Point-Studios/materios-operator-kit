import asyncio
import hashlib
import json
import logging
import os
import time
from typing import Optional
import aiohttp

from daemon.config import DaemonConfig
from daemon.models import AttestationLevel, PendingReceipt
from daemon.substrate_client import SubstrateClient
from daemon.locator_registry import LocatorRegistry
from daemon.blob_verifier import BlobVerifier
from daemon.cert_builder import scale_cert_encode
from daemon.cert_store import CertStore
from daemon.checkpoint import CardanoCheckpointer
from daemon.content_validator import ContentValidator
from daemon.evidence_submitter import (
    EvidenceSubmitter,
    maybe_create_evidence_submitter,
)
from daemon import health_server
from daemon.health_server import drain_notifications

logger = logging.getLogger(__name__)


# Minimum verification level required to attest a receipt on-chain.
#
# Set to ROOT_VERIFIED (3) — the verifier MUST have computed the Merkle
# root from the fetched chunks AND confirmed it matches `base_root_sha256`
# on the receipt. Anything below that is insufficient evidence:
#
#   FETCHED (1)        — chunks downloaded but no integrity check
#   HASH_VERIFIED (2)  — each chunk's SHA-256 matches the manifest's
#                        declared hash, but the manifest is supplied by
#                        the (potentially malicious) blob-gateway. A
#                        malicious gateway can serve any (data, declared
#                        hash) pair that's internally consistent but
#                        unrelated to the on-chain commitment.
#   ROOT_VERIFIED (3)  — `merkle_root(chunk_hashes) == receipt.base_root_sha256`,
#                        the only level that proves the data corresponds to
#                        the on-chain commitment.
#
# Bug fixed by task #184: the gate previously accepted HASH_VERIFIED, so a
# Merkle-root mismatch (already logged as CRITICAL by the verifier) still
# resulted in `cert_store.save()` + `submit_availability_cert()`. The cert
# body itself (post PR #5) is purely a hash of on-chain fields, so the
# off-chain pre-attestation gate is the ONLY mechanism stopping a wrong-
# content blob from getting an availability cert. That gate MUST require
# ROOT_VERIFIED. See `tests/test_blob_verifier_merkle_gate.py` for the
# live evidence + repro.
MIN_ATTESTATION_LEVEL_TO_ATTEST: AttestationLevel = AttestationLevel.ROOT_VERIFIED


def _default_fallback_window() -> int:
    """Read CERT_DAEMON_PRUNED_FALLBACK_WINDOW once at import time.

    Defaults to 256 — roughly matching substrate's default state-pruning
    distance. If an operator runs a node with a deeper pruning window (or
    an archive node), they can bump this via env so the self-heal clamps
    further back and processes more historical events before giving up.
    """
    try:
        return max(0, int(os.environ.get("CERT_DAEMON_PRUNED_FALLBACK_WINDOW", "256")))
    except ValueError:
        return 256


# Module-level so tests can monkeypatch without spinning up the whole daemon.
FALLBACK_WINDOW: int = _default_fallback_window()

# Cap on clamp iterations per single poll tick. Prevents an unbounded
# clamp-retry loop if the fallback window is itself behind pruning (very deep
# pruning, chain tip jumping faster than we can poll, etc.) — at the cap we
# break out of the tick and let the next tick recompute head from scratch.
MAX_CLAMPS_PER_TICK: int = 5


def _is_pruned_state_error(exc) -> bool:
    """Detect the substrate "state pruned" error in whatever form the client
    raises it. Checks both the RPC error code (4003) and the message string
    ("State already discarded") for robustness across substrate-interface
    versions. Accepts any exception or nested dict/list payload.
    """
    if exc is None:
        return False
    # Unwrap SubstrateRequestException / similar that stash the JSON-RPC
    # error dict on `args[0]`.
    for arg in getattr(exc, "args", ()):
        if _is_pruned_state_error_payload(arg):
            return True
    return _is_pruned_state_error_payload(str(exc))


def _is_pruned_state_error_payload(payload) -> bool:
    if payload is None:
        return False
    if isinstance(payload, dict):
        if payload.get("code") == 4003:
            return True
        msg = payload.get("message")
        if isinstance(msg, str) and "State already discarded" in msg:
            return True
        # Recurse into nested error envelopes.
        for v in payload.values():
            if _is_pruned_state_error_payload(v):
                return True
        return False
    if isinstance(payload, (list, tuple)):
        return any(_is_pruned_state_error_payload(item) for item in payload)
    if isinstance(payload, str):
        return "State already discarded" in payload or "'code': 4003" in payload
    return False


class CertDaemon:
    def __init__(self, config: DaemonConfig):
        self.config = config
        self.client = SubstrateClient(config)
        self.locator = LocatorRegistry(config)
        self.verifier = BlobVerifier(config)
        self.content_validator = ContentValidator(config)
        self.cert_store = CertStore(config)
        self.checkpointer = CardanoCheckpointer(
            config,
            state_file=os.path.join(config.data_dir, "checkpoint-state.json"),
        )
        self.pending: dict[str, PendingReceipt] = {}
        self.last_processed_block: int = 0
        self._running = True
        self._notified_ids: dict[str, float] = {}  # receipt_id -> timestamp for dedupe
        # Tracks pruned-block numbers we've already warned about in the
        # current tick so we log the self-heal message ONCE per stuck block,
        # not on every retry. Cleared at the top of each poll tick.
        self._pruned_warned_blocks: set[int] = set()
        # Concurrency primitives for parallelized per-receipt processing
        # (task #120). Lazy-initialized on first use because asyncio.Lock /
        # asyncio.Semaphore must be constructed under a running event loop
        # in older Python versions, and `__init__` may run before the loop
        # is alive. See `_ensure_concurrency_primitives()`.
        #
        #   _chain_write_lock — serializes the on-chain submit step so the
        #     extrinsic nonce stays monotonic. Without this, two parallel
        #     submits would both fetch the same `accountNextIndex` and the
        #     second would die with "Priority too low" (per
        #     `feedback_polkadot_nonce_race_on_burst.md`).
        #   _pending_lock — guards `self.pending` against concurrent
        #     mutation across receipt-processing coroutines. Without this,
        #     two coroutines that both fail locator-resolution would race
        #     on the same dict key.
        #   _concurrency_sem — bounds the number of in-flight
        #     `process_receipt` coroutines per block to
        #     `config.max_concurrent_receipts` (default 8). Prevents
        #     unbounded fan-out that would hammer the blob gateway.
        self._chain_write_lock: Optional[asyncio.Lock] = None
        self._pending_lock: Optional[asyncio.Lock] = None
        self._concurrency_sem: Optional[asyncio.Semaphore] = None
        # Task #143 — TEE evidence submitter. Lazy-initialised after the
        # event loop is alive (in `run()`), since it depends on the same
        # `_chain_write_lock` the receipt path uses for nonce safety.
        self.evidence_submitter: Optional[EvidenceSubmitter] = None

    def _ensure_concurrency_primitives(self):
        """Lazy-init asyncio Lock / Semaphore inside the running event loop.

        Idempotent — only constructs if the field is still None. Called from
        `_process_block_range` and `process_receipt`'s pending-mutation paths
        so callers from any entry point (poll loop, push notification,
        retry_pending) all share the same primitives.
        """
        if self._chain_write_lock is None:
            self._chain_write_lock = asyncio.Lock()
        if self._pending_lock is None:
            self._pending_lock = asyncio.Lock()
        if self._concurrency_sem is None:
            self._concurrency_sem = asyncio.Semaphore(
                max(1, self.config.max_concurrent_receipts)
            )

    def stop(self):
        self._running = False
        # Best-effort: signal the evidence submitter loop to exit on its
        # next sleep boundary. Safe if it was never started.
        try:
            if self.evidence_submitter is not None:
                self.evidence_submitter.stop()
        except Exception:  # noqa: BLE001
            pass

    def load_state(self):
        if os.path.exists(self.config.state_file):
            try:
                with open(self.config.state_file) as f:
                    state = json.load(f)
                self.last_processed_block = state.get("last_processed_block", 0)
                self._stored_chain_genesis = state.get("chain_genesis", "")
                logger.info(
                    f"Loaded state: last_processed_block={self.last_processed_block} "
                    f"chain_genesis={self._stored_chain_genesis[:16] or '(none)'}"
                )
            except Exception as e:
                logger.warning(f"Failed to load state: {e}")
                self._stored_chain_genesis = ""
        else:
            self._stored_chain_genesis = ""

    def verify_chain_genesis_or_wipe(self):
        """Self-healing check: compare the chain's live genesis hash against
        whatever we had persisted. On mismatch (typical cause: a chain reset
        on the network), wipe local state so we don't report stale
        `last_processed_block` values in heartbeats or re-process dead blocks.
        Safe to wipe — blobs+certs are hash-keyed and re-fetch on demand.

        Retries the RPC a few times before giving up. If we return without a
        `_live_chain_genesis` attribute set, the caller should re-invoke this
        before processing any receipts (v5-reset lesson: on initial call the
        substrate client may have connected but not yet initialized metadata,
        causing get_genesis_hash to raise; old code silently accepted that and
        left stale state in place, preventing self-heal).
        """
        live = None
        for attempt in range(6):
            try:
                live = self.client.get_genesis_hash()
                break
            except Exception as e:
                if attempt < 5:
                    logger.info(
                        f"RPC not ready for genesis check (attempt {attempt+1}/6): {e}; retrying in 3s"
                    )
                    time.sleep(3)
                else:
                    logger.warning(
                        f"Could not query live chain genesis after 6 attempts: {e}; "
                        f"state check deferred until poll loop can confirm live genesis"
                    )
                    return
        stored = getattr(self, "_stored_chain_genesis", "") or ""
        if stored and stored != live:
            logger.warning(
                f"Chain genesis mismatch — stored={stored[:16]}... live={live[:16]}... "
                f"Wiping local daemon state (last_processed_block={self.last_processed_block}) "
                f"to avoid stale reports. Typical cause: network-wide chain reset."
            )
            self.last_processed_block = 0
            # Drop any pending receipts tracked against the old chain
            self.pending.clear()
            self._notified_ids.clear()
            # Also clear checkpoint state so we don't try to anchor old leaves
            try:
                cp_state = os.path.join(self.config.data_dir, "checkpoint-state.json")
                if os.path.exists(cp_state):
                    os.remove(cp_state)
                    logger.info(f"Removed stale checkpoint state: {cp_state}")
            except Exception as e:
                logger.warning(f"Failed to remove checkpoint state: {e}")
        self._live_chain_genesis = live

        # As of spec-219, both cert construction (`scale_cert_encode`) and the
        # checkpoint pipeline (`checkpoint.py::flush`) read
        # `self._live_chain_genesis` directly. `config.chain_id` is no longer
        # consulted on the hot path — env is decorative documentation only.
        # See feedback_cert_daemon_chain_id_must_be_set.md for the bug class
        # this migration eliminates.

    def save_state(self):
        try:
            state = {
                "last_processed_block": self.last_processed_block,
                "chain_genesis": getattr(self, "_live_chain_genesis", "")
                or getattr(self, "_stored_chain_genesis", ""),
            }
            tmp = self.config.state_file + ".tmp"
            with open(tmp, "w") as f:
                json.dump(state, f)
            os.replace(tmp, self.config.state_file)
        except Exception as e:
            logger.error(f"Failed to save state: {e}")

    async def _process_block_range(self, head: int):
        """Process every block in `(last_processed_block, head]`, self-healing
        past `State already discarded` (RPC 4003) errors from the node's
        pruning window.

        Design:
          - Each block's RPC calls are wrapped in a try/except. A 4003 from
            ANY call in the block (event fetch, certified-event fetch) is
            treated as "cursor is past pruning" and triggers a clamp.
          - Receipts WITHIN a block are processed concurrently via a bounded
            `asyncio.gather` (cap = `config.max_concurrent_receipts`, default
            8) so HTTP-bound prep (locator + blob fetch + Merkle) overlaps.
            Per-receipt failures are isolated — `return_exceptions=True` keeps
            the rest of the batch progressing instead of cancelling siblings.
          - The on-chain submit step inside `process_receipt` is serialized
            by a chain-write lock so the extrinsic nonce stays monotonic
            (per `feedback_polkadot_nonce_race_on_burst.md`).
          - Block-level completion remains SEQUENTIAL — block N+1 is only
            entered after every receipt in block N completes (success or
            error). This keeps the cursor-advance contract: once
            `last_processed_block = N`, every receipt in (prior, N] has been
            seen exactly once.
          - After a clamp, we re-enter the outer loop with the new cursor.
            If the clamped cursor is ALSO past pruning, we clamp again, up
            to `MAX_CLAMPS_PER_TICK` — then bail and let the next tick
            recompute `head` from scratch.
          - Non-4003 exceptions on the per-block RPC propagate to the poll
            loop's outer try/except so reconnection logic still fires.

        Broken out of `run()` so the self-heal behavior is unit-testable
        without spinning up the full daemon lifecycle.
        """
        self._ensure_concurrency_primitives()
        clamps_this_tick = 0
        processed_all = False
        while not processed_all and clamps_this_tick < MAX_CLAMPS_PER_TICK:
            processed_all = True  # set False if we clamp and need to retry
            for block_num in range(self.last_processed_block + 1, head + 1):
                if not self._running:
                    return
                try:
                    events = self.client.get_block_events(block_num)
                    if events:
                        # Bounded-concurrency batch of per-receipt coroutines.
                        # `return_exceptions=True` ensures one failed receipt
                        # (gateway 500, blob verify error, content rejection,
                        # transient RPC glitch) does not cancel the others.
                        results = await asyncio.gather(
                            *(self._process_receipt_bounded(e["receipt_id"]) for e in events),
                            return_exceptions=True,
                        )
                        for ev, res in zip(events, results):
                            if isinstance(res, BaseException):
                                logger.warning(
                                    f"process_receipt failed for "
                                    f"{ev['receipt_id'][:16]}... in block {block_num}: "
                                    f"{type(res).__name__}: {res}"
                                )
                    self.last_processed_block = block_num
                    health_server.increment_metric("blocks_processed_total")

                    # Scan for AvailabilityCertified events for checkpointing
                    if self.config.checkpoint_enabled:
                        certified = self.client.get_block_certified_events(block_num)
                        for cert_event in certified:
                            cert_hash_bytes = bytes.fromhex(cert_event["cert_hash"].removeprefix("0x"))
                            self.checkpointer.add_cert(cert_event["receipt_id"], cert_hash_bytes, block_num)

                    self.save_state()
                except Exception as per_block_exc:
                    if _is_pruned_state_error(per_block_exc):
                        self._clamp_cursor_past_pruned(block_num, head)
                        clamps_this_tick += 1
                        processed_all = False
                        break
                    # Non-pruned error — re-raise to the poll loop's outer
                    # try/except so the existing reconnect path fires.
                    raise

    async def _process_receipt_bounded(self, receipt_id: str):
        """Run `process_receipt` under the per-block concurrency semaphore.

        Acquiring the semaphore here (rather than inside `process_receipt`)
        keeps the bound applied ONLY to the parallel-batch entrypoint —
        callers from `retry_pending` and the push-notification path stay
        single-threaded against `self.pending` and don't need to compete for
        a slot. The shared chain-write lock and pending lock still apply
        in `process_receipt` regardless of caller, so nonce / dict-mutation
        safety is preserved across all entry points.
        """
        assert self._concurrency_sem is not None  # set by _ensure_concurrency_primitives
        async with self._concurrency_sem:
            return await self.process_receipt(receipt_id)

    def _clamp_cursor_past_pruned(self, stuck_block: int, head: int) -> int:
        """Advance `last_processed_block` past the node's state-pruning window
        when we hit a `State already discarded` (RPC code 4003) error.

        Crashloop context: if a validator was restarted (runtime upgrade,
        chain reset recovery, OOM kill) while the cert-daemon held an old
        `last_processed_block`, the node will have since pruned state for
        that block. Every poll tick then re-requests the same pruned block,
        gets a 4003, reconnects, and loops forever. Manual rescue used to be
        `docker exec ... rm -f daemon-state.json && docker restart ...` —
        this method makes the daemon self-heal instead.

        Behavior:
          - Advance cursor to `max(current, head - FALLBACK_WINDOW)` so we
            never move backward.
          - Clamp to `0` if `head < FALLBACK_WINDOW` (very new chain).
          - Log a single INFO warning per stuck block per tick (dedupe via
            `self._pruned_warned_blocks`).
          - Persist state so a crash before the next tick doesn't reintroduce
            the same stuck cursor.

        Returns the new `last_processed_block` so the caller can break out
        of its inner block-range loop and let the next tick pick up from
        the clamped cursor.
        """
        new_cursor = max(self.last_processed_block, head - FALLBACK_WINDOW)
        if new_cursor < 0:
            new_cursor = 0
        if stuck_block not in self._pruned_warned_blocks:
            self._pruned_warned_blocks.add(stuck_block)
            logger.info(
                f"pruned cursor at block {stuck_block} — clamping forward to "
                f"head-{FALLBACK_WINDOW} (head={head}, new_cursor={new_cursor})"
            )
        self.last_processed_block = new_cursor
        self.save_state()
        return new_cursor

    async def send_discord(self, message: str, level: str = "info"):
        if not self.config.discord_webhook_url:
            return
        prefix = {"info": "\u2139\ufe0f", "warning": "\u26a0\ufe0f", "critical": "\ud83d\udea8"}.get(level, "")
        payload = {"content": f"{prefix} **materios-cert-daemon**: {message}"}
        try:
            async with aiohttp.ClientSession() as session:
                await session.post(
                    self.config.discord_webhook_url,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=5),
                )
        except Exception as e:
            logger.warning(f"Discord notification failed: {e}")

    def get_cardano_epoch(self) -> int:
        """Get current Cardano epoch from Ogmios /health endpoint."""
        try:
            import requests as _requests
            resp = _requests.get(
                f"{self.config.ogmios_url}/health",
                headers={"Accept": "application/json"},
                timeout=5,
            )
            data = resp.json()
            epoch = data.get("currentEpoch")
            if epoch is not None:
                return int(epoch)
            # fallback: derive from lastKnownTip slot
            tip = data.get("lastKnownTip", {})
            slot = tip.get("slot", 0)
            if slot > 0:
                return slot // 432000  # preprod epoch length
            return 0
        except Exception as e:
            logger.warning(f"Failed to get Cardano epoch from Ogmios: {e}")
            return 0

    async def process_receipt(self, receipt_id: str):
        # Concurrency primitives are required for the pending / chain-write
        # locks below regardless of which entrypoint called us (parallel
        # batch, push notification, retry_pending). Idempotent.
        self._ensure_concurrency_primitives()
        receipt = self.client.get_receipt(receipt_id)
        if receipt is None:
            logger.warning(f"Receipt {receipt_id} not found on chain")
            return

        # Skip if already certified
        if receipt.availability_cert_hash != b'\x00' * 32:
            logger.info(f"Receipt {receipt_id} already certified, skipping")
            return

        # Skip if we already have a cert stored
        if self.cert_store.exists(receipt_id):
            logger.info(f"Cert already stored for {receipt_id}, skipping")
            return

        # Resolve blob locations (try receipt_id first, then content_hash fallback)
        content_hash_hex = receipt.content_hash.hex() if isinstance(receipt.content_hash, bytes) else str(receipt.content_hash)
        manifest = await self.locator.resolve(receipt_id, content_hash=content_hash_hex)
        if manifest is None:
            logger.info(f"No locator found for {receipt_id}, adding to pending")
            async with self._pending_lock:
                if receipt_id not in self.pending:
                    self.pending[receipt_id] = PendingReceipt(
                        receipt_id=receipt_id,
                        receipt=receipt,
                        first_seen=time.time(),
                    )
            return

        # Verify blobs
        verification = await self.verifier.verify(receipt, manifest)
        logger.info(
            f"Verification for {receipt_id}: level={verification.attestation_level.name}, "
            f"chunks={verification.chunks_verified}/{verification.chunks_total}"
        )

        if verification.attestation_level < MIN_ATTESTATION_LEVEL_TO_ATTEST:
            # Hard reject. Includes the Merkle-root mismatch case
            # (level=HASH_VERIFIED but blob_verifier already logged CRITICAL),
            # the chunk-hash mismatch case, and the unfetchable-chunk case.
            # In all three the data does NOT match the on-chain commitment,
            # so we MUST NOT save the cert or submit an availability tx.
            logger.error(
                f"Verification REJECTED for {receipt_id}: "
                f"level={verification.attestation_level.name} "
                f"(need {MIN_ATTESTATION_LEVEL_TO_ATTEST.name}); "
                f"errors={verification.errors}"
            )
            health_server.increment_metric("verification_failures_total")
            await self.send_discord(
                f"Verification rejected for `{receipt_id[:16]}...` "
                f"(level={verification.attestation_level.name}): "
                f"{verification.errors[0] if verification.errors else 'unknown'}",
                "warning",
            )
            return

        # Content validation gate (if enabled)
        if self.config.content_validation_enabled:
            validation = self.content_validator.validate(
                verification.chunk_data_list, receipt_id
            )
            if not validation.valid:
                logger.warning(
                    f"Content validation FAILED for {receipt_id}: {validation.errors}"
                )
                health_server.increment_metric("content_validation_failures_total")
                await self.send_discord(
                    f"Content rejected for `{receipt_id[:16]}...`: {validation.errors[0]}",
                    "warning",
                )
                return  # Don't certify — receipt stays with zero cert hash

        # Source the chain_id from the LIVE genesis we queried via RPC, not
        # from the env-set config.chain_id. Operator env can drift stale
        # across chain resets (2026-05-11 preprod: 5 receipts stranded because
        # our 3 daemons had a v5 hash cached in env while external attestors
        # used the correct v6 hash → CertHashMismatch every cross-attest).
        # The live RPC value is the single source of truth.
        live_chain_id = getattr(self, "_live_chain_genesis", None)
        if not live_chain_id:
            # Should be impossible by the time process_receipt runs (the poll
            # loop calls verify_chain_genesis_or_wipe until _live_chain_genesis
            # is set). Defensive: refuse to attest rather than silently fall
            # back to a potentially-wrong value.
            logger.error(
                f"Cannot attest {receipt_id[:16]}…: _live_chain_genesis not "
                f"set yet (substrate RPC has not provided the genesis hash). "
                f"Skipping — will retry on next poll."
            )
            return

        # Build cert (spec-219 SCALE-canonical: byte-identical to runtime's
        # `canonical_cert_hash(receipt_id)`). The runtime now verifies the
        # claim against its own computation on every attest_availability_cert
        # — drift = CertHashMismatch + BadAttestStrike + (at threshold)
        # auto-slash. See design doc spec-219 §3.
        cert_bytes = scale_cert_encode(
            chain_genesis=live_chain_id,
            receipt_id=receipt_id,
            content_hash=receipt.content_hash,
            base_root_sha256=receipt.base_root_sha256,
            storage_locator_hash=receipt.storage_locator_hash,
        )
        cert_hash = hashlib.sha256(cert_bytes).digest()

        # Store cert to filesystem
        self.cert_store.save(receipt_id, cert_bytes)

        # Submit on-chain. Two concerns:
        #
        #   (1) Nonce ordering — substrate-interface fetches `accountNextIndex`
        #       on every submit. Two concurrent submits on the same signing
        #       key would race, the second dying with "Priority too low" (per
        #       memory `feedback_polkadot_nonce_race_on_burst.md`). We
        #       serialize the submit step under `_chain_write_lock` so nonces
        #       stay monotonic across the parallelized batch.
        #
        #   (2) Event-loop blocking — `submit_extrinsic(wait_for_inclusion=True)`
        #       is synchronous and blocks ~6-12s per call (one block time).
        #       Calling it directly under `await asyncio.gather(...)` would
        #       freeze the event loop and starve every sibling coroutine of
        #       its prep-phase HTTP I/O. `asyncio.to_thread` shunts the
        #       blocking call to a worker thread so other coroutines keep
        #       making progress on locator/blob fetches while one submit is
        #       pending. The lock keeps the chain-write path itself serial.
        async with self._chain_write_lock:
            outcome = await asyncio.to_thread(
                self.client.submit_availability_cert, receipt_id, cert_hash
            )
        if outcome.success:
            health_server.increment_metric("certs_submitted_total")
            async with self._pending_lock:
                self.pending.pop(receipt_id, None)
            await self.send_discord(
                f"Cert submitted for `{receipt_id[:16]}...` (L{verification.attestation_level})",
                "info",
            )
        elif outcome.bad_attest_strike:
            # spec-219: runtime rejected our cert_hash as non-canonical.
            # Strike is permanent for this receipt — retry would re-strike
            # against the same canonical value. Drop from pending so we
            # stop wasting submits; operator must fix the input drift
            # (chain_id, locator-hash, base-root) before any new receipts
            # can succeed.
            health_server.increment_metric("bad_attest_strikes_total")
            async with self._pending_lock:
                self.pending.pop(receipt_id, None)
            if outcome.auto_slashed:
                health_server.increment_metric("auto_slashed_total")
                await self.send_discord(
                    f"AUTO-SLASHED for bad attest on `{receipt_id[:16]}...` — "
                    f"strikes={outcome.strikes}, "
                    f"amount={outcome.slashed_amount}. "
                    f"Signer ejected from committee; re-bond required after "
                    f"fixing cert-builder input drift.",
                    "critical",
                )
            else:
                claimed_hex = outcome.claimed.hex()[:16] if outcome.claimed else "?"
                canonical_hex = outcome.canonical.hex()[:16] if outcome.canonical else "?"
                await self.send_discord(
                    f"BadAttestStrike on `{receipt_id[:16]}...` — "
                    f"strikes={outcome.strikes}, claimed=`{claimed_hex}...` "
                    f"vs canonical=`{canonical_hex}...`. "
                    f"Check chain_id / cert_builder inputs.",
                    "critical",
                )
        else:
            health_server.increment_metric("verification_failures_total")
            await self.send_discord(
                f"Failed to submit cert tx for `{receipt_id[:16]}...` after {self.config.tx_max_retries} retries",
                "critical",
            )

    async def retry_pending(self):
        self._ensure_concurrency_primitives()
        now = time.time()
        to_remove = []
        # Snapshot under lock so a concurrent push-notification or batched
        # process_receipt cannot mutate the dict mid-iteration. The actual
        # locator-resolve work happens OUTSIDE the lock to avoid serializing
        # all retries behind one HTTP call.
        async with self._pending_lock:
            snapshot = list(self.pending.items())
        for receipt_id, pending in snapshot:
            content_hash_hex = pending.receipt.content_hash.hex() if isinstance(pending.receipt.content_hash, bytes) else str(pending.receipt.content_hash)
            manifest = await self.locator.resolve(receipt_id, content_hash=content_hash_hex)
            if manifest is None:
                pending.retries += 1
                if now - pending.first_seen > self.config.pending_alert_seconds and pending.retries % 60 == 0:
                    await self.send_discord(
                        f"Receipt `{receipt_id[:16]}...` pending for {int((now - pending.first_seen) / 60)}min, no locator found",
                        "warning",
                    )
                continue

            # Found locator — process it
            await self.process_receipt(receipt_id)
            to_remove.append(receipt_id)

        if to_remove:
            async with self._pending_lock:
                for rid in to_remove:
                    self.pending.pop(rid, None)

    async def _ensure_bond(self):
        """Ensure the daemon's signer has `AttestorBonds[addr] >= BondRequirement`.

        Called on startup (before the first `join_committee` attempt) and as a
        defensive fallback if a later `join_committee` dispatch still reports
        `InsufficientBond` (e.g. governance raised the requirement after we
        last bonded).

        Idempotent:
          - `current_bond >= required` → skip (logs "already bonded ...")
          - `required == 0` → skip (preprod bootstrap / upgrade grace window)
          - `free_matra < delta` → warn (with faucet URL hint) and return;
            does NOT submit a tx, does NOT crash. Daemon still tries to join.
          - Otherwise → submit `bond(required - current)` and log the tx hash.
        """
        try:
            my_address = self.client.keypair.ss58_address
            required = self.client.get_bond_requirement()
            if required == 0:
                logger.info("OrinqReceipts.BondRequirement is 0, skipping auto-bond")
                return

            current = self.client.get_attestor_bond(my_address)
            if current >= required:
                logger.info(
                    f"already bonded {current} / {required} MATRA base units, "
                    f"skipping auto-bond"
                )
                return

            delta = required - current
            free = self.client.get_free_balance(my_address)
            if free < delta:
                # Not enough free MATRA to cover the bond delta. Warn clearly
                # and continue — the join_committee attempt that follows will
                # fail with InsufficientBond, but that's expected and loud.
                logger.warning(
                    f"Insufficient free MATRA to auto-bond: need {delta} more "
                    f"base units (have {free}). Request more MATRA from the "
                    f"faucet at {self.config.blob_base_url or '<blob-gateway>'}"
                    f"/faucet/drip and restart the daemon, or wait for an "
                    f"automatic faucet drip. Continuing anyway so the operator "
                    f"can see the join_committee error in the logs."
                )
                await self.send_discord(
                    f"Auto-bond skipped for `{my_address[:16]}...`: need "
                    f"{delta} more MATRA base units, have {free}",
                    "warning",
                )
                return

            logger.info(
                f"Auto-bonding {delta} base units to reach BondRequirement "
                f"({current} + {delta} = {required})"
            )
            try:
                success, tx_hash = self.client.submit_bond(delta)
            except Exception as e:
                # Defensive: never let a transient RPC glitch kill startup.
                logger.warning(f"Auto-bond submit raised {type(e).__name__}: {e}")
                return

            if success:
                logger.info(f"Auto-bond submitted successfully, tx {tx_hash}")
                await self.send_discord(
                    f"Auto-bonded {delta} MATRA base units "
                    f"(`{my_address[:16]}...`)",
                    "info",
                )
            else:
                logger.warning(
                    "Auto-bond submit returned failure; continuing to "
                    "join_committee anyway so the error surfaces."
                )
        except Exception as e:
            # Top-level safety net — never crash daemon startup on bond errors.
            logger.warning(f"_ensure_bond failed with {type(e).__name__}: {e}")

    async def _ensure_committee_membership(self):
        """Check if this daemon's signer is in the attestation committee. If not, join.

        Before the first join attempt we call `_ensure_bond()` to satisfy the
        `BondRequirement` (introduced when Component 8 of the v5.1 tokenomics
        landed). If the chain still reports `InsufficientBond` after the
        initial bond (e.g. governance raised the floor mid-flight), we re-run
        `_ensure_bond()` and retry the join exactly ONCE more — never an
        infinite loop on the same failure mode.
        """
        try:
            my_address = self.client.keypair.ss58_address
            committee = self.client.substrate.query("OrinqReceipts", "CommitteeMembers")

            # Check if we're already a member (handle different SS58 prefixes)
            my_pubkey = self.client.keypair.public_key.hex()
            is_member = False
            for member in committee or []:
                member_str = str(member)
                try:
                    from substrateinterface.utils.ss58 import ss58_decode
                    if ss58_decode(member_str) == my_pubkey:
                        is_member = True
                        break
                except Exception:
                    if member_str == my_address:
                        is_member = True
                        break

            if is_member:
                logger.info(f"Already in attestation committee as {my_address}")
                return

            # Ensure bond meets BondRequirement before attempting join. This
            # handles the first-time onboarding case where the attestor has
            # MATRA but has never called bond().
            await self._ensure_bond()

            logger.info(f"Not in committee — calling join_committee as {my_address}")

            bond_retry_used = False
            for attempt in range(2):  # at most: original attempt + 1 bond-fallback
                call = self.client.substrate.compose_call(
                    call_module="OrinqReceipts",
                    call_function="join_committee",
                    call_params={},
                )
                extrinsic = self.client.substrate.create_signed_extrinsic(
                    call=call,
                    keypair=self.client.keypair,
                )
                receipt = self.client.substrate.submit_extrinsic(
                    extrinsic, wait_for_inclusion=True
                )

                if receipt.is_success:
                    logger.info(f"Successfully joined attestation committee!")
                    await self.send_discord(
                        f"Joined attestation committee as `{my_address[:16]}...`", "info"
                    )
                    return

                error = receipt.error_message or "unknown error"
                logger.warning(f"join_committee failed: {error}")

                # Defensive re-bond path: if the chain says InsufficientBond
                # after we already ran _ensure_bond, governance likely raised
                # the floor. Re-run once and retry. Never loop beyond this.
                if _is_insufficient_bond_error(error) and not bond_retry_used:
                    bond_retry_used = True
                    logger.info(
                        "join_committee returned InsufficientBond — re-running "
                        "auto-bond once and retrying."
                    )
                    await self._ensure_bond()
                    continue

                # Common non-bond failure: can't pay fees (no MOTRA).
                if "pay" in str(error).lower() or "fee" in str(error).lower():
                    await self._request_faucet_drip(my_address)
                return  # any other failure: bail, poll loop will retry later

        except Exception as e:
            logger.warning(f"Committee membership check failed: {e}")

    async def _request_faucet_drip(self, address: str):
        """Request a MATRA airdrop from the gateway faucet to pay for join_committee."""
        gateway_url = self.config.blob_base_url
        if not gateway_url:
            return
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{gateway_url}/faucet/drip",
                    json={"address": address},
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    data = await resp.json()
                    if resp.status == 200:
                        logger.info(f"Faucet drip received: {data.get('amount')} MATRA")
                        # Wait for MOTRA to generate, then retry join
                        await asyncio.sleep(30)
                        await self._ensure_committee_membership()
                    else:
                        logger.warning(f"Faucet request failed: {data.get('error', resp.status)}")
        except Exception as e:
            logger.warning(f"Faucet request error: {e}")

    async def run(self):
        await self.send_discord("Daemon starting up", "info")
        self.load_state()

        if not self.client.connect():
            await self.send_discord("Failed to connect to substrate node", "critical")
            return

        # Self-heal across chain resets: if the chain's genesis hash differs
        # from what we persisted last run, wipe local state so heartbeats and
        # poll offsets don't report the dead chain's numbers.
        self.verify_chain_genesis_or_wipe()

        health_server.update_metrics(substrate_connected=True)

        # Auto-join the attestation committee if not already a member
        await self._ensure_committee_membership()
        self._last_committee_check = time.time()

        # Task #143 — fire up the TEE evidence submitter. Reuses the
        # receipt-path's chain-write lock so submit_evidence and
        # attest_availability_cert never race on the signer's nonce. Soft-
        # disabled when the gateway URL or submitter token are unset (older
        # deploys that don't yet act as evidence submitters).
        self._ensure_concurrency_primitives()
        self.evidence_submitter = maybe_create_evidence_submitter(
            self.config, self.client, self._chain_write_lock
        )
        if self.evidence_submitter is not None:
            self.evidence_submitter.start()

        logger.info(f"Starting poll loop, interval={self.config.poll_interval}s")

        while self._running:
            notifications = []  # defined outside try so adaptive sleep can see it
            try:
                # Retry committee join every 60s if initial attempt failed
                if time.time() - self._last_committee_check > 60:
                    self._last_committee_check = time.time()
                    await self._ensure_committee_membership()

                # If the startup genesis check deferred (RPC not ready), retry
                # it here on every poll tick until we've confirmed live genesis.
                # Without this, a daemon that started while substrate was still
                # connecting (or that survived a chain reset mid-run) would
                # process against stale `_stored_chain_genesis` forever.
                if not getattr(self, "_live_chain_genesis", None):
                    self.verify_chain_genesis_or_wipe()

                # Drain push notifications from gateway
                notifications = drain_notifications()
                now_ts = time.time()

                # Clean expired entries from dedupe set (>5 min old)
                expired = [rid for rid, ts in self._notified_ids.items() if now_ts - ts > 300]
                for rid in expired:
                    del self._notified_ids[rid]

                # Process new notifications
                for notif in notifications:
                    receipt_id = notif.get("receiptId", "")
                    if receipt_id and receipt_id not in self._notified_ids:
                        self._notified_ids[receipt_id] = now_ts
                        logger.info(f"Push notification for {receipt_id[:16]}...")
                        try:
                            await self.process_receipt(receipt_id)
                        except Exception as e:
                            logger.warning(f"Error processing notified receipt {receipt_id[:16]}...: {e}")

                head = self.client.get_best_block_number()

                if self.last_processed_block == 0:
                    # On first run, start from current head block (don't replay history)
                    self.last_processed_block = head
                    self.save_state()
                    logger.info(f"First run, starting from block {head}")

                # Reset per-tick dedupe set so we warn once per stuck block
                # per tick, not once ever (a clamp that advances past one
                # pruned block should still warn if a LATER block is also
                # pruned on a future tick).
                self._pruned_warned_blocks.clear()

                # Process outstanding blocks through `_process_block_range`,
                # which wraps each block's RPC calls in a try/except so a
                # `State already discarded` error from the node's pruning
                # window triggers a self-heal clamp instead of an infinite
                # retry loop.
                await self._process_block_range(head)

                # Retry pending receipts
                await self.retry_pending()

                # Periodic L1 checkpoint flush
                if self.config.checkpoint_enabled and self.checkpointer.should_flush():
                    live_genesis = getattr(self, "_live_chain_genesis", None)
                    if not live_genesis:
                        logger.warning(
                            "Checkpoint flush deferred: _live_chain_genesis "
                            "not yet set. Will retry next interval."
                        )
                    else:
                        self.checkpointer.flush(
                            current_best_block=head,
                            live_chain_genesis=live_genesis,
                        )

                # Track finalized head for health metrics
                try:
                    finalized = self.client.get_finalized_head_number()
                    finality_gap = head - finalized
                    health_server.update_metrics(
                        finalized_head=finalized,
                        finality_gap=finality_gap,
                    )
                except Exception:
                    pass  # Don't fail main loop if finalized head query fails

                health_server.update_metrics(
                    last_processed_block=self.last_processed_block,
                    last_poll_timestamp=time.time(),
                    pending_receipts=len(self.pending),
                )

            except Exception as e:
                logger.error(f"Poll loop error: {e}", exc_info=True)
                health_server.update_metrics(substrate_connected=False)
                # Try to reconnect
                try:
                    self.client.connect()
                    health_server.update_metrics(substrate_connected=True)
                except Exception:
                    await self.send_discord(f"Connection lost: {e}", "critical")

            # Adaptive polling: fast when work pending, idle when not
            has_pending = len(self.pending) > 0 or len(notifications) > 0
            interval = self.config.poll_interval_fast if has_pending else self.config.poll_interval_idle
            await asyncio.sleep(interval)

        await self.send_discord("Daemon shutting down", "info")


def _is_insufficient_bond_error(error) -> bool:
    """Detect the InsufficientBond dispatch error in whatever form
    substrate-interface hands us (dict, str, SCALE-decoded enum).

    Module-level helper deliberately placed after the class so the class body
    stays intact — the prior placement nested `_request_faucet_drip` and `run`
    inside this helper (indentation pitfall), which would crash the daemon at
    runtime even though pytest mocks didn't catch it. See the
    `test_class_methods_intact` regression test.
    """
    if error is None:
        return False
    if isinstance(error, dict):
        if error.get("name") == "InsufficientBond":
            return True
        # Nested {"Module": {"error": "InsufficientBond", ...}} form.
        for v in error.values():
            if _is_insufficient_bond_error(v):
                return True
        return False
    return "InsufficientBond" in str(error)
