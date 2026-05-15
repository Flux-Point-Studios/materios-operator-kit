"""Cert-daemon module that attests Cardano-tx confirmation for the
spec-221 / task #267 ``pallet-intent-settlement::attest_expire_policy``
extrinsic (operator-kit task #284, sister PR materios-intent-settlement
#34 ``feat/expire-policy-mirror-b-plus-d`` rev ``01952c69...``).

Why this exists
---------------
Pre-spec-221 the ``expire_policy_mirror`` path on pallet-intent-settlement
accepted a single committee member's word with NO domain tag and NO
falsifiable Cardano evidence — any colluding signer could prematurely
flip any intent to ``Expired`` (mis-sec P0, see materios-intent-settlement
PR #34 §1.2 audit narrative). Spec-221 (LIVE on Materios preprod since
2026-05-15 block ``0x23fe4d9e...``) shipped the fix: split the path into

  * permissionless ``request_expire_policy(intent_id, cardano_tx_hash,
    attestation_evidence: ExpiryEvidence)`` — anyone observes a Cardano
    Expire redeemer tx and pins the request in
    ``PolicyExpireRequests<intent_id -> ExpiryRequestRecord>``.
  * committee ``attest_expire_policy(intent_id, signatures)`` — M-of-N
    sigs over the canonical EXPP digest.

Without the daemon-side autonomous signer the spec-221 path is live on
chain but has no committee actor — anyone can post a request_expire_policy
with valid Cardano evidence but no daemon will produce sigs, so manual
signing is the only path (same gap shape #266 had for settle before
this module's sibling ``settle_claim_attestor`` shipped).

This module closes that gap for the expire path. It polls Materios for
pending ``PolicyExpireRequests`` rows, independently observes each
requested Cardano tx via Ogmios+Kupo (reusing the post-#280
``CardanoTxObserver`` infrastructure), verifies the seven facts in the
request match what the cert-daemon's own Cardano follower sees, and
signs the canonical ``EXPP`` digest with the existing cert-daemon
sr25519 committee key. Each attestor's sig is a falsifiable claim about
the Cardano chain — same property the STCA path established.

The seven facts every attestor verifies before signing
------------------------------------------------------
1. ``cardano_tx_hash`` exists on Cardano (Kupo finds at least one match)
2. ``observed_at_depth >= MinFinalityDepth`` Cardano blocks (cert-daemon
   uses its OWN independently observed depth, not the requester's claim)
3. ``observed_slot`` matches the slot Kupo reports for the tx
4. The Cardano network's genesis hash matches
   ``mainchain_genesis_hash`` (preprod vs mainnet domain separation,
   same network-magic lookup the settle path uses)
5. The Materios ``chain_id`` is the LIVE genesis hash (not env) —
   confirmed via ``substrate_client.get_genesis_hash``
6. The on-chain Intent for ``intent_id`` exists and is NOT already
   terminal (Settled / Expired). Already-Expired is treated as
   idempotent skip per pallet contract (same shape as settle's
   AlreadySettled).
7. The requester-supplied ``policy_id_witness`` agrees with the
   chain-state-resolved policy id (``intent.kind.product_id`` for
   ``BuyPolicy``, ``intent.kind.policy_id`` for ``RequestPayout``,
   N/A for ``RefundCredit`` — those intents are not Cardano-side
   policies and never expire via this path).

Mapping to PR #34
-----------------
- Mechanism: ``Hybrid B + D`` (memo §2.3, same as STCA).
- Sig payload: ``EXPP`` (PR #34 §3.2). Exact 176-byte preimage
  (4B tag + 172B content) built by :func:`build_expp_preimage` below.
  Hashed with blake2_256 and signed sr25519 (matches
  ``CommitteePubkey``/``CommitteeSig`` types reused from
  ``ensure_threshold_signatures``).
- Extrinsic: ``IntentSettlement::attest_expire_policy(intent_id,
  signatures)``.
- Refusal logic: if any of the seven facts disagree, REFUSE to sign
  and log the specific mismatch — same auditability property the STCA
  path established.
- Reuse: same sr25519 committee key, same ``CardanoTxObserver``, same
  ``chain_write_lock``, same network-magic genesis table. The compounding
  leverage of one TEE+chain identity backing three orthogonal attestation
  types (TEE evidence, settle_claim, expire_policy).

EXPP preimage layout (PR #34 §3.2, pallet
``expire_policy_attested_payload`` byte order frozen):

::

    blake2_256(
        b"EXPP"                                  # 4B tag
        || materios_chain_id (32B)
        || intent_id (32B)
        || policy_id (32B)                       # chain-state-derived
        || cardano_tx_hash (32B)
        || observed_at_depth (LE u32, 4B)
        || observed_slot (LE u64, 8B)
        || mainchain_genesis_hash (32B)
    )

Total preimage = 4 + 32 + 32 + 32 + 32 + 4 + 8 + 32 = 176 bytes.
The 172-byte ``body`` (without the 4-byte tag) is the post-tag content.
Pinned fixture G (PR #34, this module's parity vector):
    digest = ``0x773fa47732e9af0d07dc6e7acb81e8d6c4c94e4f93f5f1ba8d5ff92da34defd6``
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
from dataclasses import dataclass
from typing import Any, Optional

import aiohttp

# Reuse the Cardano follower built for the settle path. We do NOT
# duplicate observer / network-identity / address-extraction code —
# the EXPP path has the same Cardano-side observation needs (tip,
# genesis, tx slot, tx depth, network magic) modulo the per-output
# amount-summing step which we don't need (the expire path doesn't
# bind a beneficiary or an amount; only the tx-existence + slot +
# depth facts are required).
from daemon.settle_claim_attestor import (
    CARDANO_MAINNET_GENESIS_HASH,
    CARDANO_PREPROD_GENESIS_HASH,
    CARDANO_PREVIEW_GENESIS_HASH,
    CardanoTxObservation,
    CardanoTxObserver,
    NETWORK_MAGIC_TO_GENESIS_HASH,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# EXPP domain tag + preimage builder.
#
# PINNED to pallet-intent-settlement::expire_policy_attested_payload (PR #34
# §3.2). Byte order, frozen:
#     b"EXPP" (4B) || materios_chain_id (32B) || intent_id (32B)
#         || policy_id (32B) || cardano_tx_hash (32B)
#         || observed_at_depth (LE u32, 4B) || observed_slot (LE u64, 8B)
#         || mainchain_genesis_hash (32B)
# = 4 + 32 + 32 + 32 + 32 + 4 + 8 + 32 = 176 bytes total preimage,
#   172-byte content body (everything after the 4-byte tag).
#
# Bumping any field requires a ``settlement_version`` bump in the
# pallet-side digest pre-image, which propagates here. Drift between
# this module and the pallet's ``expire_policy_attested_payload`` is a
# CertHashMismatch-class bug — the parity-vector test
# (``TestExppFixtureG``) guards against it.
# ---------------------------------------------------------------------------
TAG_EXPP: bytes = b"EXPP"
EXPP_PREIMAGE_LEN: int = 176
EXPP_CONTENT_LEN: int = 172  # for documentation/test alignment


def build_expp_preimage(
    chain_id: bytes,
    intent_id: bytes,
    policy_id: bytes,
    cardano_tx_hash: bytes,
    observed_at_depth: int,
    observed_slot: int,
    mainchain_genesis_hash: bytes,
) -> bytes:
    """Build the exact byte-stream EXPP preimage per PR #34 §3.2.

    Every byte-length and integer-width assumption is enforced — a
    malformed pre-image silently producing a different hash than the
    pallet expects would be a CertHashMismatch-class bug (see
    ``feedback_sdk_runtime_arg_drift_recurrence.md``).

    Args:
        chain_id: 32-byte Materios genesis hash (live RPC value, NOT env).
        intent_id: 32-byte IntentId.
        policy_id: 32-byte PolicyId, chain-state-derived from
            ``Intents[intent_id]`` per ``resolve_intent_policy_id``:
            ``BuyPolicy.product_id`` / ``RequestPayout.policy_id``.
        cardano_tx_hash: 32-byte Cardano transaction hash of the
            Expire-redeemer tx.
        observed_at_depth: u32 Cardano block depth of the tx (must be
            ``>= MinFinalityDepth``). The attestor commits to its OWN
            observed depth, NOT the requester's claim.
        observed_slot: u64 Cardano slot of the tx.
        mainchain_genesis_hash: 32-byte Cardano network genesis hash
            (pins preprod vs mainnet domain separation).

    Returns:
        The 176-byte preimage ready to be passed to blake2_256.

    Raises:
        ValueError: any byte-length or integer-range invariant violated.
    """
    if len(chain_id) != 32:
        raise ValueError(f"chain_id must be 32 bytes, got {len(chain_id)}")
    if len(intent_id) != 32:
        raise ValueError(f"intent_id must be 32 bytes, got {len(intent_id)}")
    if len(policy_id) != 32:
        raise ValueError(f"policy_id must be 32 bytes, got {len(policy_id)}")
    if len(cardano_tx_hash) != 32:
        raise ValueError(
            f"cardano_tx_hash must be 32 bytes, got {len(cardano_tx_hash)}"
        )
    if len(mainchain_genesis_hash) != 32:
        raise ValueError(
            f"mainchain_genesis_hash must be 32 bytes, got "
            f"{len(mainchain_genesis_hash)}"
        )
    if not 0 <= observed_at_depth < 2**32:
        raise ValueError(
            f"observed_at_depth out of u32 range: {observed_at_depth}"
        )
    if not 0 <= observed_slot < 2**64:
        raise ValueError(f"observed_slot out of u64 range: {observed_slot}")

    out = bytearray(EXPP_PREIMAGE_LEN)
    pos = 0
    out[pos:pos + 4] = TAG_EXPP
    pos += 4
    out[pos:pos + 32] = chain_id
    pos += 32
    out[pos:pos + 32] = intent_id
    pos += 32
    out[pos:pos + 32] = policy_id
    pos += 32
    out[pos:pos + 32] = cardano_tx_hash
    pos += 32
    out[pos:pos + 4] = observed_at_depth.to_bytes(4, "little")
    pos += 4
    out[pos:pos + 8] = observed_slot.to_bytes(8, "little")
    pos += 8
    out[pos:pos + 32] = mainchain_genesis_hash
    pos += 32
    if pos != EXPP_PREIMAGE_LEN:
        # Static guard — every branch above adds a fixed width, so this
        # should be unreachable. Kept so silent drift triggers a loud
        # error instead of a wrong hash.
        raise AssertionError(
            f"EXPP preimage builder wrote {pos} bytes, expected "
            f"{EXPP_PREIMAGE_LEN}"
        )
    return bytes(out)


def compute_expp_digest(preimage: bytes) -> bytes:
    """Hash the EXPP preimage with blake2_256 (same hasher as the
    pallet's ``domain_hash``). Returns 32 bytes."""
    if len(preimage) != EXPP_PREIMAGE_LEN:
        raise ValueError(
            f"preimage must be {EXPP_PREIMAGE_LEN} bytes, got {len(preimage)}"
        )
    return hashlib.blake2b(preimage, digest_size=32).digest()


# ---------------------------------------------------------------------------
# Pending request envelope from the pallet.
# ---------------------------------------------------------------------------


@dataclass
class PendingExpiryRequest:
    """A row from ``PolicyExpireRequests``. SCALE-decoded by
    ``SubstrateClient`` and handed to the dispatcher.

    Mirrors ``ExpiryRequestRecord<T>`` (PR #34 §3.5) flattened with the
    ``ExpiryEvidence`` fields inlined for ergonomic attribute access.
    The ``policy_id_witness`` field is the requester's claim about which
    Cardano policy expired; the dispatcher cross-checks it against the
    chain-state-resolved policy id from ``Intents[intent_id]`` before
    signing.
    """
    intent_id: bytes              # 32B
    requester: str                # ss58 address
    submitted_block: int
    # ExpiryEvidence fields, inlined
    cardano_tx_hash: bytes        # 32B
    observed_at_depth: int        # u32
    observed_slot: int            # u64
    mainchain_genesis_hash: bytes  # 32B
    policy_id_witness: bytes      # 32B (PolicyId = H256)


# ---------------------------------------------------------------------------
# Refusal-reason taxonomy.
#
# Each refusal is logged with one of these tags so an operator (and an
# external auditor) can rapidly grep journalctl for the specific safety
# property that fired. The strings are STABLE — keep them in sync with
# PR #34's seven-fact list.
#
# Variant naming mirrors the STCA path where the facts overlap (1-5);
# the EXPP-specific facts (6-7) get their own variants.
# ---------------------------------------------------------------------------
class RefusalReason:
    TX_NOT_FOUND = "tx_not_found"                       # fact 1
    FINALITY_BELOW_MIN = "finality_below_min"           # fact 2
    SLOT_MISMATCH = "slot_mismatch"                     # fact 3
    GENESIS_MISMATCH = "genesis_mismatch"               # fact 4
    CHAIN_ID_UNAVAILABLE = "chain_id_unavailable"       # fact 5
    INTENT_NOT_FOUND = "intent_not_found"               # fact 6a
    INTENT_ALREADY_TERMINAL = "intent_already_terminal"  # fact 6b
    POLICY_ID_WITNESS_MISMATCH = "policy_id_witness_mismatch"  # fact 7
    OBSERVER_UNAVAILABLE = "observer_unavailable"       # tooling, not safety
    DEPTH_OBSERVATION_UNAVAILABLE = "depth_observation_unavailable"
    DEPTH_UNDERSHOOT = "depth_undershoot"  # task #287: reality < pinned


@dataclass
class AttestationVerdict:
    """Outcome of one attestation attempt against one pending request.

    Either ``signed=True`` (signature published) or ``signed=False``
    with ``refusal_reason`` set. A refusal is NOT an error — it's the
    central safety property of PR #34 §2.4.

    ``intent_already_terminal`` (already-Expired) is a STRUCTURED
    SUCCESS-EQUIVALENT: the pallet treats it as idempotent and consumes
    the pending request on the next attest_expire_policy. The dispatcher
    surfaces it as a refusal so the operator sees the skip in journalctl
    but does NOT retry the request (same legacy contract as the STCA
    path's AlreadySettled handling).
    """
    intent_id: bytes
    signed: bool
    refusal_reason: Optional[str] = None
    refusal_detail: Optional[str] = None
    extrinsic_hash: Optional[str] = None


# ---------------------------------------------------------------------------
# ExpirePolicyAttestor: glues the observer to a chain-write path.
# ---------------------------------------------------------------------------


class ExpirePolicyAttestor:
    """Top-level dispatcher for the expire_policy attestation type.

    Operational shape: a single background asyncio task started by
    ``CertDaemon.run()`` after substrate is connected. Each tick:
      1. Reads pending ``PolicyExpireRequests`` from chain.
      2. For each row, fetches the on-chain Intent (status + kind)
         and resolves the canonical policy_id from chain state.
      3. Observes the requested Cardano tx via Ogmios+Kupo.
      4. Cross-checks the seven facts; if any disagree, REFUSE.
      5. Otherwise builds the EXPP preimage, signs blake2_256(preimage)
         with the existing cert-daemon sr25519 key, and submits
         ``IntentSettlement::attest_expire_policy(intent_id,
         [(my_pubkey, sig)])``.

    Concurrency: bounded by ``max_concurrent`` (default 8, same cap as
    settle path per memo §6 OQ#10) via a Semaphore so a request-storm
    can't fan out unbounded. Chain submission is serialized under the
    SHARED ``chain_write_lock`` so nonce stays monotonic against the
    receipt-cert, TEE-evidence, AND settle-claim paths (memory
    ``feedback_polkadot_nonce_race_on_burst.md``).
    """

    DEFAULT_MAX_CONCURRENT: int = 8

    def __init__(
        self,
        *,
        config: Any,
        substrate_client: Any,
        chain_write_lock: asyncio.Lock,
        observer: CardanoTxObserver,
        min_finality_depth: int,
        poll_interval: int = 12,
        max_concurrent: Optional[int] = None,
        # Task #286: gateway-mediated peer-sig aggregator. Same contract
        # as `SettleClaimAttestor` (see substrate_client docstring).
        aggregator: Optional[Any] = None,
        min_signer_threshold: Optional[int] = None,
    ):
        self.config = config
        self.client = substrate_client
        self._chain_write_lock = chain_write_lock
        self.observer = observer
        # ``MinFinalityDepth`` value comes from the pallet's runtime
        # config (PR #34 §3.4). We read it via SubstrateClient at startup
        # so a governance bump is picked up automatically.
        self.min_finality_depth = max(1, int(min_finality_depth))
        self._poll_interval = max(3, int(poll_interval))
        sem_value = max(
            1,
            int(
                max_concurrent
                if max_concurrent is not None
                else self.DEFAULT_MAX_CONCURRENT
            ),
        )
        self._sem = asyncio.Semaphore(sem_value)
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self.aggregator = aggregator
        if min_signer_threshold is None and aggregator is not None:
            try:
                min_signer_threshold = substrate_client.get_min_signer_threshold()
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    f"expire_attestor: MinSignerThreshold query failed "
                    f"({type(e).__name__}); defaulting to 2"
                )
                min_signer_threshold = 2
        self.min_signer_threshold = max(1, int(min_signer_threshold or 1))

    async def process_one(
        self, request: PendingExpiryRequest, live_chain_id: bytes
    ) -> AttestationVerdict:
        """Verify + sign one pending request.

        Returns an ``AttestationVerdict``. Refusals log the specific
        fact mismatch at WARNING (audit-trail). Successful sigs log
        at INFO.
        """
        async with self._sem:
            return await self._process_one_locked(request, live_chain_id)

    async def _process_one_locked(
        self, request: PendingExpiryRequest, live_chain_id: bytes
    ) -> AttestationVerdict:
        verdict = AttestationVerdict(intent_id=request.intent_id, signed=False)
        cardano_tx_hex = request.cardano_tx_hash.hex()

        # Fact 4: genesis pin. Short-circuit before talking to Cardano
        # at all if the request was clearly built for a different network.
        observer_genesis: Optional[bytes] = None
        async with aiohttp.ClientSession() as session:
            observer_genesis = await self.observer.get_genesis_hash(session)
        if observer_genesis is None:
            verdict.refusal_reason = RefusalReason.OBSERVER_UNAVAILABLE
            verdict.refusal_detail = "ogmios_genesis_hash_unavailable"
            self._log_refusal(verdict)
            return verdict
        if observer_genesis != request.mainchain_genesis_hash:
            verdict.refusal_reason = RefusalReason.GENESIS_MISMATCH
            verdict.refusal_detail = (
                f"request_genesis={request.mainchain_genesis_hash.hex()[:16]}... "
                f"vs observer_genesis={observer_genesis.hex()[:16]}..."
            )
            self._log_refusal(verdict)
            return verdict

        # Fact 6: intent presence + state. Hydrate from chain state and
        # short-circuit if the intent is missing or already terminal.
        # Already-Expired is idempotent skip per pallet contract (same
        # legacy semantic the pallet preserves in `attest_expire_policy`).
        try:
            intent_status = await asyncio.to_thread(
                self.client.get_intent_status, request.intent_id
            )
        except Exception as e:  # noqa: BLE001
            verdict.refusal_reason = RefusalReason.OBSERVER_UNAVAILABLE
            verdict.refusal_detail = (
                f"failed_to_fetch_intent: {type(e).__name__}: {e}"
            )
            self._log_refusal(verdict)
            return verdict
        if intent_status is None:
            verdict.refusal_reason = RefusalReason.INTENT_NOT_FOUND
            verdict.refusal_detail = "no_intent_for_id"
            self._log_refusal(verdict)
            return verdict
        # IntentStatus enum order: Pending=0, Attested=1, Vouchered=2,
        # Settled=3, Expired=4, Refunded=5. Terminal states = Settled,
        # Expired, Refunded. The pallet treats already-Expired as
        # idempotent skip (consume pending request, return Ok), so we
        # mirror that contract here — refuse with a structured reason but
        # do NOT retry on the next tick.
        if intent_status in ("Settled", "Expired", "Refunded"):
            verdict.refusal_reason = RefusalReason.INTENT_ALREADY_TERMINAL
            verdict.refusal_detail = f"intent_status={intent_status}"
            self._log_refusal(verdict)
            return verdict

        # Fact 7: policy_id witness vs chain-state-resolved policy id.
        # We fetch the resolved id from the intent's kind directly —
        # never trust the requester's witness for the digest pre-image.
        try:
            resolved_policy_id = await asyncio.to_thread(
                self.client.get_policy_id_for_intent, request.intent_id
            )
        except Exception as e:  # noqa: BLE001
            verdict.refusal_reason = RefusalReason.OBSERVER_UNAVAILABLE
            verdict.refusal_detail = (
                f"failed_to_resolve_policy_id: {type(e).__name__}: {e}"
            )
            self._log_refusal(verdict)
            return verdict
        if resolved_policy_id is None:
            # RefundCredit intents have no Cardano-side policy; the
            # pallet rejects with IntentNotEligibleForExpiry. Same
            # taxonomy on the daemon side.
            verdict.refusal_reason = RefusalReason.POLICY_ID_WITNESS_MISMATCH
            verdict.refusal_detail = "intent_kind_has_no_policy_id"
            self._log_refusal(verdict)
            return verdict
        if resolved_policy_id != request.policy_id_witness:
            verdict.refusal_reason = RefusalReason.POLICY_ID_WITNESS_MISMATCH
            verdict.refusal_detail = (
                f"chain_resolved_policy_id={resolved_policy_id.hex()[:16]}... "
                f"vs request.policy_id_witness="
                f"{request.policy_id_witness.hex()[:16]}..."
            )
            self._log_refusal(verdict)
            return verdict

        # Observe the Cardano tx. We pass a zero hash for the
        # beneficiary because the expire path doesn't bind to a
        # beneficiary — observer.observe still resolves tip + genesis +
        # tx slot + tx block_no for the depth check, which is all we
        # need. The observer's per-output amount-summing loop iterates
        # against the zero hash (no addresses match) and reports
        # matched_address_lovelace = 0, which we don't read.
        obs = await self.observer.observe(
            cardano_tx_hex, b"\x00" * 28
        )

        if obs.mismatches and "kupo_no_matches_for_tx" in obs.mismatches:
            verdict.refusal_reason = RefusalReason.TX_NOT_FOUND
            verdict.refusal_detail = ", ".join(obs.mismatches)
            self._log_refusal(verdict)
            return verdict

        # Slot + depth must both be resolvable; the matched-lovelace
        # field is intentionally NOT read (expire path doesn't bind to
        # an amount).
        if obs.observed_slot is None:
            verdict.refusal_reason = RefusalReason.OBSERVER_UNAVAILABLE
            verdict.refusal_detail = (
                ", ".join(obs.mismatches) or "incomplete_observation_no_slot"
            )
            self._log_refusal(verdict)
            return verdict

        # Fact 3: slot matches.
        if obs.observed_slot != request.observed_slot:
            verdict.refusal_reason = RefusalReason.SLOT_MISMATCH
            verdict.refusal_detail = (
                f"observed_slot={obs.observed_slot} "
                f"vs evidence_slot={request.observed_slot}"
            )
            self._log_refusal(verdict)
            return verdict

        # Fact 2: finality depth — verify reality has caught up to the
        # depth the requester pinned. Task #287: the pallet rebuilds the
        # EXPP preimage from `request.observed_at_depth` (pinned at
        # request_expire_policy time), so the daemon signs over that
        # pinned value below, NOT a fresh obs.depth. This block is the
        # independent reality check that gates whether signing is safe.
        if obs.depth is None:
            verdict.refusal_reason = RefusalReason.DEPTH_OBSERVATION_UNAVAILABLE
            verdict.refusal_detail = (
                f"tip_block_no={obs.cardano_tip_block_no} "
                f"tx_block_no={obs.tx_block_no}"
            )
            self._log_refusal(verdict)
            return verdict
        if obs.depth < int(request.observed_at_depth):
            verdict.refusal_reason = RefusalReason.DEPTH_UNDERSHOOT
            verdict.refusal_detail = (
                f"observed_depth={obs.depth} < "
                f"request_observed_at_depth={request.observed_at_depth} "
                f"(reality has not caught up to pinned depth)"
            )
            self._log_refusal(verdict)
            return verdict
        if obs.depth < self.min_finality_depth:
            verdict.refusal_reason = RefusalReason.FINALITY_BELOW_MIN
            verdict.refusal_detail = (
                f"observed_depth={obs.depth} < "
                f"min_finality_depth={self.min_finality_depth}"
            )
            self._log_refusal(verdict)
            return verdict

        # All seven facts agree. Build the EXPP preimage, sign, submit.
        # Task #287: preimage commits to REQUEST-pinned depth + slot,
        # matching what the pallet rebuilds at verify time. Daemon's
        # `obs` fields are independent verification only.
        preimage = build_expp_preimage(
            chain_id=live_chain_id,
            intent_id=request.intent_id,
            policy_id=resolved_policy_id,
            cardano_tx_hash=request.cardano_tx_hash,
            observed_at_depth=int(request.observed_at_depth),
            observed_slot=int(request.observed_slot),
            mainchain_genesis_hash=request.mainchain_genesis_hash,
        )
        digest = compute_expp_digest(preimage)
        sig_bytes = self.client.keypair.sign(digest)
        pubkey_bytes = self.client.keypair.public_key

        # Task #286: gateway-mediated envelope assembly. Same shape as
        # settle_claim_attestor; falls back to 1-sig submit on chains
        # where MinSignerThreshold == 1.
        if self.aggregator is not None and self.min_signer_threshold > 1:
            async with aiohttp.ClientSession() as session:
                envelope = await self.aggregator.assemble_envelope(
                    session,
                    kind="expire",
                    key=request.intent_id,
                    digest=digest,
                    my_pubkey=pubkey_bytes,
                    my_sig=sig_bytes,
                )
            if len(envelope) < self.min_signer_threshold:
                logger.info(
                    f"expire_attestor: {len(envelope)}/{self.min_signer_threshold} "
                    f"sigs assembled for intent_id="
                    f"{request.intent_id.hex()[:16]}... — deferring submit "
                    f"until more peers share. Next tick will retry."
                )
                verdict.refusal_reason = RefusalReason.OBSERVER_UNAVAILABLE
                verdict.refusal_detail = (
                    f"awaiting_peer_sigs:{len(envelope)}/"
                    f"{self.min_signer_threshold}"
                )
                return verdict
            logger.info(
                f"expire_attestor: assembled {len(envelope)}/"
                f"{self.min_signer_threshold} sigs for "
                f"{request.intent_id.hex()[:16]}... — submitting envelope"
            )
            async with self._chain_write_lock:
                try:
                    ext_hash = await asyncio.to_thread(
                        self.client.submit_attest_expire_policy_envelope,
                        request.intent_id,
                        envelope,
                    )
                except Exception as e:  # noqa: BLE001
                    logger.warning(
                        f"expire_attestor: submit_attest_expire_policy_envelope "
                        f"raised for {request.intent_id.hex()[:16]}...: "
                        f"{type(e).__name__}: {e}"
                    )
                    verdict.refusal_reason = RefusalReason.OBSERVER_UNAVAILABLE
                    verdict.refusal_detail = f"submit_raised: {type(e).__name__}"
                    return verdict
        else:
            # 1-sig fallback (test chains).
            async with self._chain_write_lock:
                try:
                    ext_hash = await asyncio.to_thread(
                        self.client.submit_attest_expire_policy,
                        request.intent_id,
                        pubkey_bytes,
                        sig_bytes,
                    )
                except Exception as e:  # noqa: BLE001
                    logger.warning(
                        f"expire_attestor: submit_attest_expire_policy raised "
                        f"for {request.intent_id.hex()[:16]}...: "
                        f"{type(e).__name__}: {e}"
                    )
                    verdict.refusal_reason = RefusalReason.OBSERVER_UNAVAILABLE
                    verdict.refusal_detail = f"submit_raised: {type(e).__name__}"
                    return verdict

        if not ext_hash:
            logger.warning(
                f"expire_attestor: submit_attest_expire_policy returned "
                f"empty hash for {request.intent_id.hex()[:16]}... — "
                f"retryable"
            )
            verdict.refusal_reason = RefusalReason.OBSERVER_UNAVAILABLE
            verdict.refusal_detail = "submit_no_hash"
            return verdict

        verdict.signed = True
        verdict.extrinsic_hash = ext_hash
        logger.info(
            f"expire_attestor: attest_expire_policy OK for "
            f"intent_id={request.intent_id.hex()[:16]}... "
            f"cardano_tx={cardano_tx_hex[:16]}... "
            f"depth={obs.depth} ext_hash={ext_hash[:18]}..."
        )
        return verdict

    def _log_refusal(self, verdict: AttestationVerdict) -> None:
        """Log a refusal at WARNING with the fact tag + structured
        detail. This is the audit-trail surface — operators grep
        journalctl for ``expire_attestor: REFUSE`` to find any
        attestor that disagreed with a request."""
        logger.warning(
            f"expire_attestor: REFUSE attest_expire_policy for "
            f"intent_id={verdict.intent_id.hex()[:16]}... "
            f"reason={verdict.refusal_reason} "
            f"detail={verdict.refusal_detail}"
        )

    async def _tick(self, live_chain_id: bytes) -> None:
        """Single poll tick — fetch pending requests, process each
        under the concurrency semaphore."""
        try:
            requests = await asyncio.to_thread(
                self.client.list_pending_expiry_requests
            )
        except Exception as e:  # noqa: BLE001
            logger.warning(
                f"expire_attestor: list_pending_expiry_requests raised "
                f"{type(e).__name__}: {e}"
            )
            return
        if not requests:
            return
        logger.info(
            f"expire_attestor: {len(requests)} pending expiry "
            f"request(s) — processing under sem cap "
            f"{self._sem._value}"  # type: ignore[attr-defined]
        )
        # SubstrateClient.list_pending_expiry_requests returns a list[dict]
        # (deliberately loose — see its docstring). Convert to the
        # PendingExpiryRequest dataclass here at the dispatcher boundary
        # before process_one consumes it via attribute access. A malformed
        # row (missing field) is logged and skipped rather than killing the
        # batch.
        coros = []
        for r in requests:
            try:
                req = PendingExpiryRequest(**r)
            except TypeError as e:
                logger.warning(
                    f"expire_attestor: skipping malformed pending request row "
                    f"(intent_id={r.get('intent_id', b'').hex()[:16] if isinstance(r, dict) else '?'}...): "
                    f"{type(e).__name__}: {e}"
                )
                continue
            coros.append(self.process_one(req, live_chain_id))
        await asyncio.gather(*coros, return_exceptions=True)

    async def _run_forever(self) -> None:
        while self._running:
            live_chain_id_hex = await asyncio.to_thread(
                self.client.get_genesis_hash
            )
            if not live_chain_id_hex:
                logger.info(
                    "expire_attestor: substrate genesis not yet available "
                    "— skipping tick"
                )
            else:
                try:
                    chain_id = bytes.fromhex(
                        live_chain_id_hex.removeprefix("0x")
                    )
                except ValueError:
                    logger.warning(
                        f"expire_attestor: malformed live genesis "
                        f"{live_chain_id_hex!r} — skipping tick"
                    )
                    chain_id = b""
                if len(chain_id) == 32:
                    try:
                        await self._tick(chain_id)
                    except Exception as e:  # noqa: BLE001
                        logger.error(
                            f"expire_attestor: tick raised "
                            f"{type(e).__name__}: {e}",
                            exc_info=True,
                        )
            try:
                await asyncio.sleep(self._poll_interval)
            except asyncio.CancelledError:
                break

    def start(self) -> None:
        """Schedule the background loop on the running event loop.
        Idempotent — a second ``start()`` does NOT spawn two loops."""
        if self._task is not None and not self._task.done():
            return
        self._running = True
        self._task = asyncio.create_task(self._run_forever())
        logger.info(
            f"expire_attestor: started "
            f"(poll_interval={self._poll_interval}s, "
            f"min_finality_depth={self.min_finality_depth}, "
            f"max_concurrent={self._sem._value})"  # type: ignore[attr-defined]
        )

    def stop(self) -> None:
        """Signal the loop to exit at its next sleep boundary."""
        self._running = False
        if self._task is not None and not self._task.done():
            self._task.cancel()


# ---------------------------------------------------------------------------
# Factory: build the attestor from env vars + DaemonConfig.
# ---------------------------------------------------------------------------


def maybe_create_expire_policy_attestor(
    config: Any,
    substrate_client: Any,
    chain_write_lock: asyncio.Lock,
) -> Optional[ExpirePolicyAttestor]:
    """Construct an ``ExpirePolicyAttestor`` if its required deps are wired.

    Soft-disable contract: if ``OGMIOS_URL`` (already in DaemonConfig)
    OR ``KUPO_URL`` is unset, return None. Older deploys without a Kupo
    follower stay on the receipt-cert + TEE-evidence + settle_claim
    paths only.

    Optional env (mirror the SETTLE_ATTESTOR_* knobs so an operator can
    tune the two attestors independently):

        EXPIRE_ATTESTOR_POLL_INTERVAL — seconds between polls (default 12).
        EXPIRE_ATTESTOR_MAX_CONCURRENT — concurrency cap (default 8).
        EXPIRE_ATTESTOR_MIN_FINALITY_DEPTH — fallback when the pallet
            constant cannot be read (default 15).
    """
    ogmios_url = (getattr(config, "ogmios_url", "") or "").strip()
    kupo_url = (
        getattr(config, "kupo_url", "") or os.environ.get("KUPO_URL", "") or ""
    ).strip()
    if not ogmios_url or not kupo_url:
        logger.info(
            "expire_attestor: not configured "
            f"(ogmios_url={'set' if ogmios_url else 'missing'}, "
            f"kupo_url={'set' if kupo_url else 'missing'}) — "
            f"expire_policy attestation will not run on this node."
        )
        return None
    try:
        poll_interval = int(
            os.environ.get("EXPIRE_ATTESTOR_POLL_INTERVAL", "12")
        )
    except ValueError:
        poll_interval = 12
    try:
        max_concurrent = int(
            os.environ.get(
                "EXPIRE_ATTESTOR_MAX_CONCURRENT",
                str(ExpirePolicyAttestor.DEFAULT_MAX_CONCURRENT),
            )
        )
    except ValueError:
        max_concurrent = ExpirePolicyAttestor.DEFAULT_MAX_CONCURRENT
    try:
        fallback_min_finality = int(
            os.environ.get("EXPIRE_ATTESTOR_MIN_FINALITY_DEPTH", "15")
        )
    except ValueError:
        fallback_min_finality = 15
    # Pull MinFinalityDepth from the pallet's runtime metadata if
    # available; fall back to env. The pallet exposes it as a
    # ``Get<u32>`` constant at ``IntentSettlement.MinFinalityDepth`` —
    # the SAME constant the settle path uses.
    min_finality_depth = fallback_min_finality
    try:
        mfd = substrate_client.get_min_finality_depth()
        if isinstance(mfd, int) and mfd > 0:
            min_finality_depth = mfd
    except Exception as e:  # noqa: BLE001
        logger.info(
            f"expire_attestor: could not read MinFinalityDepth from "
            f"runtime ({type(e).__name__}); using fallback "
            f"{fallback_min_finality}"
        )
    observer = CardanoTxObserver(
        ogmios_url=ogmios_url,
        kupo_url=kupo_url,
    )
    # Task #286: gateway-mediated peer-sig aggregator (same contract as
    # settle path). Disabled when blob_gateway_url is unset.
    aggregator = None
    gateway_url = (getattr(config, "blob_gateway_url", "") or "").strip()
    if gateway_url:
        from daemon.multisig_aggregator import MultisigAggregator
        aggregator = MultisigAggregator(gateway_url=gateway_url)
        logger.info(
            f"expire_attestor: multisig aggregator wired (gateway={gateway_url})"
        )
    else:
        logger.warning(
            "expire_attestor: BLOB_GATEWAY_URL unset — aggregator disabled. "
            "Submits will use 1-sig envelope; pallet rejects with "
            "InsufficientSignatures on MinSignerThreshold >= 2 chains."
        )
    return ExpirePolicyAttestor(
        config=config,
        substrate_client=substrate_client,
        chain_write_lock=chain_write_lock,
        observer=observer,
        min_finality_depth=min_finality_depth,
        poll_interval=poll_interval,
        max_concurrent=max_concurrent,
        aggregator=aggregator,
    )


# Re-export shared constants from settle_claim_attestor so callers that
# only depend on the expire path don't need to reach into the sibling
# module for the network-magic table.
__all__ = [
    "TAG_EXPP",
    "EXPP_PREIMAGE_LEN",
    "EXPP_CONTENT_LEN",
    "build_expp_preimage",
    "compute_expp_digest",
    "PendingExpiryRequest",
    "RefusalReason",
    "AttestationVerdict",
    "ExpirePolicyAttestor",
    "maybe_create_expire_policy_attestor",
    "CARDANO_MAINNET_GENESIS_HASH",
    "CARDANO_PREPROD_GENESIS_HASH",
    "CARDANO_PREVIEW_GENESIS_HASH",
    "NETWORK_MAGIC_TO_GENESIS_HASH",
]
