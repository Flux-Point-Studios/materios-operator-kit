"""Cert-daemon module that prosecutes fraudulent ``SettlementEvidence``
posts via the spec-225 ``slash_bad_settlement_evidence`` extrinsic
(task #84-watcher).

Why this exists
---------------
Spec-225 (live on Materios preprod 2026-05-15 block hash
``0x14389692...``) shipped three permissionless bond-and-slash
extrinsics on ``pallet-intent-settlement``:

  * ``post_settlement_bond(claim_id, amount)`` — requester opt-in bond
    against their ``request_settle`` evidence.
  * ``release_settlement_bond(claim_id)`` — refund the bond after the
    post-attest delay if no fraud was prosecuted.
  * ``slash_bad_settlement_evidence(claim_id, fraud_proof, signatures)``
    — permissionless slash: a watcher proves the requester's evidence
    is fraudulent; M-of-N committee co-signs the canonical FRAU
    digest; the bond gets repatriated (watcher share + treasury
    share).

Without a daemon-side autonomous watcher, the slash path is on chain
but no automated party calls ``slash_bad_settlement_evidence``. This
module closes that gap. It polls Materios for bonded
``ClaimSettlementRequests`` rows, independently observes each Cardano
tx via the existing Ogmios+Kupo follower (same infrastructure the
settle and expire attestors use), classifies the evidence as fraud
when it disagrees with reality, signs the canonical FRAU digest with
the cert-daemon sr25519 committee key, and submits the slash via the
gateway-mediated multisig aggregator (M-sig envelope).

Fraud classification (per task brief)
-------------------------------------
For each ``ClaimSettlementRequests`` row with ``bond_amount > 0``:

1. **Look up the Cardano tx** referenced by
   ``evidence.cardano_tx_hash`` via Ogmios+Kupo (Kupo for tx
   enumeration, Ogmios for genesis hash + tip + chain-sync block
   height). If the tx does NOT exist at the claimed depth → fraud
   variant ``TxNotFound``.

2. **If the tx exists**, parse its outputs. For each output whose
   payment-key hash matches ``evidence.beneficiary_addr_hash``, sum
   the lovelace. Compare:
     * Beneficiary mismatch — the tx pays a DIFFERENT 28-byte
       payment-key hash. Surface the actual hash via
       ``WrongBeneficiary { actual_payment_hash }``.
     * Amount mismatch — the tx pays the expected beneficiary but at
       a different amount. Surface the actual amount via
       ``WrongAmount { actual_lovelace }``.

3. **Honest evidence** (tx exists, beneficiary matches, amount matches)
   — no slash. The settle attestor's normal STCA path handles the
   close.

Priority order: ``TxNotFound`` takes precedence over the
field-mismatch variants. If the tx isn't on chain we can't
meaningfully check fields against it.

Transient-error safety
----------------------
A Cardano L1 query that times out or returns an unrecognised shape is
NOT classified as ``TxNotFound`` — that would be a false positive
that slashes an honest requester. The watcher logs the transient at
WARNING and skips the row for the current tick; the next poll cycle
retries. This is the same fail-safe-on-tooling-error contract the
settle attestor's ``OBSERVER_UNAVAILABLE`` refusal expresses.

FraudProof SCALE encoding (byte-pinned)
---------------------------------------
The pallet's ``slash_bad_settlement_evidence_payload`` hashes:

::

    blake2_256(b"FRAU" || materios_chain_id (32B)
                       || claim_id (32B)
                       || scale(fraud_proof))

``scale(fraud_proof)`` is the SCALE encoding of the
``FraudProof`` enum, source variant order
(``types.rs`` lines 448-464):

   * ``WrongAmount { actual_lovelace: u128 }`` — discriminant 0
   * ``TxNotFound`` — discriminant 1
   * ``WrongBeneficiary { actual_payment_hash: [u8; 28] }`` —
     discriminant 2

Encoding by variant:

   * ``WrongAmount`` → ``[0x00] || u128_le_bytes(actual_lovelace)``
     (17 bytes total: 1B discriminant + 16B u128 LE).
   * ``TxNotFound`` → ``[0x01]`` (1 byte).
   * ``WrongBeneficiary`` →
     ``[0x02] || actual_payment_hash (28B)`` (29 bytes total).

We hand-roll the encoder because each variant body is < 30 bytes
and the byte layout is fully specified by SCALE's spec. The unit
test ``test_fraud_proof_scale_encoding`` pins the discriminants +
field layout byte-exact so a future variant reorder in the pallet
will turn the test red before it could ever ship a wrong-discriminant
slash.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
from dataclasses import dataclass
from typing import Any, Optional, Union

import aiohttp

# Reuse the Cardano follower built for the settle path. The slash
# watcher has the SAME L1 observation needs (tip, genesis, tx slot,
# tx depth, per-output amounts + beneficiary payment-hash) so we use
# the same observer rather than duplicate the Ogmios+Kupo plumbing.
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
# FRAU domain tag + FraudProof SCALE encoder.
#
# PINNED to pallet-intent-settlement::slash_bad_settlement_evidence_payload
# (lib.rs lines 599-633) and the FraudProof enum (types.rs lines 448-464).
# Byte layout, frozen:
#
#     blake2_256(b"FRAU" (4B) || materios_chain_id (32B)
#                              || claim_id (32B)
#                              || scale(fraud_proof))
#
# scale(fraud_proof) widths by variant:
#     WrongAmount       : 1B discriminant (0x00) + 16B u128 LE = 17B
#     TxNotFound        : 1B discriminant (0x01)               =  1B
#     WrongBeneficiary  : 1B discriminant (0x02) + 28B hash    = 29B
#
# Drift between this module and the pallet is a slash-mis-prosecution
# class bug — the parity-vector tests (test_fraud_proof_scale_encoding
# + test_frau_preimage) guard against it. A reorder in types.rs would
# turn the discriminant-pinning test red before it could ever ship.
# ---------------------------------------------------------------------------
TAG_FRAU: bytes = b"FRAU"

# FraudProof variant discriminants — pinned to the Rust enum order in
# `pallets/intent-settlement/src/types.rs` lines 448-464. SCALE assigns
# discriminants by SOURCE DECLARATION ORDER (NOT alphabetical), so the
# order below is load-bearing — do NOT reorder without checking the
# pallet source AND bumping the parity-vector test.
FRAUD_DISCRIMINANT_WRONG_AMOUNT: int = 0
FRAUD_DISCRIMINANT_TX_NOT_FOUND: int = 1
FRAUD_DISCRIMINANT_WRONG_BENEFICIARY: int = 2

# Cardano preprod runs at 1 slot/sec. The classifier MUST NOT promote
# Kupo's empty-match response to FraudProof::TxNotFound until the local
# Kupo follower is demonstrably caught up to the requested
# observation depth + a safety margin. 20 slots ≈ 20 seconds covers
# any per-RPC indexer lag on a healthy follower; the operator's
# auto-recover watchdog catches longer outages on a different
# timescale (sec-review round-1 Vuln 3).
KUPO_SYNC_SAFETY_MARGIN_SLOTS: int = 20


# ---------------------------------------------------------------------------
# FraudProof Python representation.
#
# We use a small typed dataclass hierarchy rather than a string-enum so
# the encoder + dispatcher can pattern-match on the variant + body in
# one shape. Three variants exactly mirror the Rust enum.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class WrongAmount:
    """Fraud variant: the stored evidence claimed
    ``stored.amount_lovelace`` but the Cardano tx actually paid
    ``actual_lovelace`` to the beneficiary.

    Internally consistent iff
    ``actual_lovelace != stored.amount_lovelace`` (the pallet
    enforces this, but we ALSO enforce it in the classifier so a
    daemon bug can't ship a no-op proof).
    """
    actual_lovelace: int


@dataclass(frozen=True)
class TxNotFound:
    """Fraud variant: the requester claimed ``cardano_tx_hash`` exists
    on Cardano at the claimed depth, but the committee's independent
    observation says it does not. Always internally consistent — the
    proof IS the absence of the tx, which the M-of-N sig attests.
    """


@dataclass(frozen=True)
class WrongBeneficiary:
    """Fraud variant: the stored evidence claimed
    ``stored.beneficiary_addr_hash`` but the Cardano tx actually
    paid a different 28-byte payment-key hash (``actual_payment_hash``).

    Internally consistent iff
    ``actual_payment_hash != stored.beneficiary_addr_hash``.
    """
    actual_payment_hash: bytes


FraudProof = Union[WrongAmount, TxNotFound, WrongBeneficiary]


def encode_fraud_proof(proof: FraudProof) -> bytes:
    """SCALE-encode a :class:`FraudProof` byte-exact to the Rust pallet's
    encoding.

    Layout per variant (see module docstring for the full byte map):

      * ``WrongAmount(actual_lovelace)`` → ``[0x00, ...u128_le_bytes]``
        (17 bytes total: 1B discriminant + 16B little-endian u128).
      * ``TxNotFound()`` → ``[0x01]`` (1 byte: discriminant only).
      * ``WrongBeneficiary(actual_payment_hash)`` →
        ``[0x02, ...payment_hash_bytes]`` (29 bytes: 1B discriminant +
        28B payment-key hash, NOT length-prefixed because the pallet
        type is the fixed-width Rust array ``[u8; 28]``).

    Raises:
        ValueError: a variant body violates its width / range invariant
            (e.g. ``actual_payment_hash`` not 28 bytes,
            ``actual_lovelace`` out of u128 range).
        TypeError: ``proof`` is not one of the three FraudProof
            variants.
    """
    if isinstance(proof, WrongAmount):
        amount = int(proof.actual_lovelace)
        if not 0 <= amount < 2**128:
            raise ValueError(
                f"WrongAmount.actual_lovelace out of u128 range: {amount}"
            )
        return bytes([FRAUD_DISCRIMINANT_WRONG_AMOUNT]) + amount.to_bytes(
            16, "little"
        )
    if isinstance(proof, TxNotFound):
        return bytes([FRAUD_DISCRIMINANT_TX_NOT_FOUND])
    if isinstance(proof, WrongBeneficiary):
        if len(proof.actual_payment_hash) != 28:
            raise ValueError(
                f"WrongBeneficiary.actual_payment_hash must be 28 bytes, "
                f"got {len(proof.actual_payment_hash)}"
            )
        return (
            bytes([FRAUD_DISCRIMINANT_WRONG_BENEFICIARY])
            + bytes(proof.actual_payment_hash)
        )
    raise TypeError(
        f"encode_fraud_proof: unknown FraudProof variant {type(proof).__name__}"
    )


def build_frau_preimage(
    chain_id: bytes,
    claim_id: bytes,
    fraud_proof: FraudProof,
) -> bytes:
    """Build the FRAU preimage that the pallet's
    ``slash_bad_settlement_evidence_payload`` will rebuild at verify
    time.

    Layout: ``b"FRAU" (4B) || chain_id (32B) || claim_id (32B) ||
    scale(fraud_proof)``.

    Total length:
      * 4 + 32 + 32 + 17 = 85 bytes for WrongAmount
      * 4 + 32 + 32 + 1  = 69 bytes for TxNotFound
      * 4 + 32 + 32 + 29 = 97 bytes for WrongBeneficiary

    Raises:
        ValueError: any byte-length invariant violated on chain_id /
            claim_id, or any FraudProof variant invariant violated.
    """
    if len(chain_id) != 32:
        raise ValueError(f"chain_id must be 32 bytes, got {len(chain_id)}")
    if len(claim_id) != 32:
        raise ValueError(f"claim_id must be 32 bytes, got {len(claim_id)}")
    encoded_proof = encode_fraud_proof(fraud_proof)
    return TAG_FRAU + chain_id + claim_id + encoded_proof


def compute_frau_digest(preimage: bytes) -> bytes:
    """Hash the FRAU preimage with blake2_256 (same hasher as the
    pallet's ``domain_hash``). Returns 32 bytes.

    No length assertion: unlike STCA/EXPP the FRAU preimage is
    variant-width (69/85/97 bytes); the right invariant lives in
    :func:`build_frau_preimage` which validates the components.
    """
    return hashlib.blake2b(preimage, digest_size=32).digest()


# ---------------------------------------------------------------------------
# Pending settlement-request envelope as seen by the watcher.
#
# Mirrors :class:`daemon.settle_claim_attestor.PendingSettlementRequest`
# minus the chain-state ``voucher_digest`` (which the slash path
# doesn't need — the slash digest binds only chain_id + claim_id +
# fraud_proof). The dispatcher converts the dicts returned by
# ``substrate_client.list_pending_settlement_requests`` into this
# dataclass at the boundary, exactly the same pattern the settle and
# expire dispatchers use.
# ---------------------------------------------------------------------------


@dataclass
class PendingBondedRequest:
    """A row from ``ClaimSettlementRequests`` with ``bond_amount > 0``
    — i.e. a slashable settlement request.

    Fields mirror the settle path's ``PendingSettlementRequest`` so the
    dispatcher boundary stays uniform: dicts in,
    ``PendingBondedRequest(**row)`` out, classifier consumes via
    attribute access.

    The ``voucher_digest`` field is consumed by the settle attestor but
    NOT by the slash watcher. The pallet's slash dispatch binds the
    digest to (chain_id, claim_id, fraud_proof) only — the voucher is
    irrelevant. We accept the field for shape uniformity with the
    settle path's dict shape but ignore it.

    The ``bond_amount`` field is the slash trigger. Rows with
    ``bond_amount == 0`` are pre-#84 records or unbonded post-#84
    records; the pallet rejects ``slash_bad_settlement_evidence`` on
    those with ``BondNotReserved`` so we filter them out before the
    classifier runs.
    """
    claim_id: bytes              # 32B
    requester: str               # ss58 address
    submitted_block: int
    settled_direct: bool
    # SettlementEvidence
    cardano_tx_hash: bytes       # 32B
    observed_at_depth: int       # u32
    observed_slot: int           # u64
    beneficiary_addr_hash: bytes  # 28B
    amount_lovelace: int         # u64
    mainchain_genesis_hash: bytes  # 32B
    # Chain-state-derived; settle path needs it, slash path ignores.
    voucher_digest: bytes        # 32B
    # The slash trigger — present in dicts emitted by the post-#84
    # substrate_client helper; legacy rows from older clients default
    # to 0 and the dispatcher skips them.
    bond_amount: int = 0


# ---------------------------------------------------------------------------
# Classification taxonomy.
#
# Mirrors the settle path's ``RefusalReason`` but for the SLASH path
# the verdict is a fraud variant, not a refusal. We keep one stable
# tag per classifier outcome so an operator can grep journalctl for
# the specific safety property that fired.
# ---------------------------------------------------------------------------


class ClassifierOutcome:
    """Stable string tags emitted as ``verdict.outcome`` in logs.

    NOT_FRAUD / TRANSIENT_L1_ERROR / OBSERVER_UNAVAILABLE /
    KUPO_BEHIND_REQUEST_DEPTH are non-slash outcomes — the watcher
    moves on without dispatching. The remaining three are the SLASH
    outcomes and correspond directly to the pallet's FraudProof
    variants.
    """
    NOT_FRAUD = "not_fraud"
    TRANSIENT_L1_ERROR = "transient_l1_error"
    OBSERVER_UNAVAILABLE = "observer_unavailable"
    # Kupo follower hasn't caught up to request.observed_slot +
    # KUPO_SYNC_SAFETY_MARGIN_SLOTS yet. An empty Kupo match list in
    # this case is NOT proof the tx is absent from chain — it could
    # also be that the local Kupo follower is mid-resync (after an
    # operator restart, ~10-20 min on preprod). We defer rather than
    # promote to TxNotFound (sec-review round-1 Vuln 3).
    KUPO_BEHIND_REQUEST_DEPTH = "kupo_behind_request_depth"
    SLASH_TX_NOT_FOUND = "slash_tx_not_found"
    SLASH_WRONG_AMOUNT = "slash_wrong_amount"
    SLASH_WRONG_BENEFICIARY = "slash_wrong_beneficiary"


@dataclass
class SlashVerdict:
    """Outcome of one slash-watcher tick against one bonded request.

    Either ``fraud_proof`` is set (the watcher classified the evidence
    as fraudulent and tried to dispatch the slash) or it is None (no
    fraud, or transient error, or observer down). ``extrinsic_hash``
    is populated iff the slash dispatch landed on chain.
    """
    claim_id: bytes
    outcome: str
    fraud_proof: Optional[FraudProof] = None
    detail: Optional[str] = None
    extrinsic_hash: Optional[str] = None


# ---------------------------------------------------------------------------
# Fraud classifier — operates on one (request, observation) pair.
# ---------------------------------------------------------------------------


def classify_fraud(
    request: PendingBondedRequest,
    observation: CardanoTxObservation,
    *,
    kupo_checkpoint_slot: Optional[int] = None,
) -> Optional[FraudProof]:
    """Decide whether a bonded settlement request's evidence is
    fraudulent given the cert-daemon's independent Cardano
    observation.

    Returns:
        * A :class:`FraudProof` instance when the observation
          contradicts the evidence — the watcher will sign the
          corresponding FRAU digest and submit a slash.
        * ``None`` when (a) the evidence agrees with reality (honest
          evidence), or (b) the observation is incomplete / transient
          (cannot conclude — defer to next tick), or (c) the local
          Kupo follower hasn't caught up to the requested observation
          depth + safety margin (sec-review round-1 Vuln 3).
          The caller distinguishes (a/b/c) via the observation's
          ``mismatches`` list and the
          ``kupo_behind_request_depth`` marker tag.

    Priority order:
        1. Kupo-sync gate: if ``kupo_checkpoint_slot`` is supplied and
           is below ``request.observed_slot +
           KUPO_SYNC_SAFETY_MARGIN_SLOTS``, AND the observation
           carries ``kupo_no_matches_for_tx``, append
           ``kupo_behind_request_depth`` to the observation
           mismatches and return None. The follower being behind
           means "empty matches" doesn't prove the tx is absent —
           it could still appear once the follower catches up.
        2. ``TxNotFound`` (Kupo found nothing for cardano_tx_hash AND
           the follower is caught up) — takes precedence over the
           field-mismatch variants because if the tx doesn't exist
           there's no concrete output to field-check.
        3. ``WrongBeneficiary`` (tx exists but no output pays the
           claimed beneficiary; we use a different
           candidate-payment-hash from the observer).
        4. ``WrongAmount`` (tx exists, pays the claimed beneficiary,
           but a different lovelace total).
        5. ``None`` (honest evidence).

    Safety contract: transient errors (Ogmios down, Kupo 500,
    chain-sync timeout, Kupo follower mid-resync) MUST NOT be
    classified as ``TxNotFound``. The observer emits
    ``kupo_no_matches_for_tx`` ONLY on a successful lookup that
    returned an empty match list — but that empty list is only
    proof-of-absence when the follower is demonstrably caught up.
    Any other mismatch tag (including the new
    ``kupo_behind_request_depth``) means incomplete observation;
    the classifier returns ``None`` and the dispatcher defers.
    """
    # Slot 1: TxNotFound has highest priority IFF the follower is
    # demonstrably caught up. The settle attestor uses the EXACT same
    # `kupo_no_matches_for_tx` tag as its tx-existence proof; we
    # mirror that contract — but the slash path is one-sided
    # (false-positive slashes burn an honest requester) so we gate
    # the promotion on a local Kupo sync check first.
    if "kupo_no_matches_for_tx" in observation.mismatches:
        if kupo_checkpoint_slot is None or kupo_checkpoint_slot < (
            request.observed_slot + KUPO_SYNC_SAFETY_MARGIN_SLOTS
        ):
            # Follower below the safety threshold (or health probe
            # failed entirely → kupo_checkpoint_slot is None). Surface
            # the new mismatch tag so the dispatcher's logging path
            # makes the defer reason visible to operators.
            if "kupo_behind_request_depth" not in observation.mismatches:
                observation.mismatches.append("kupo_behind_request_depth")
            return None
        return TxNotFound()

    # Any OTHER mismatch tag = transient / incomplete observation.
    # Fall through to None so the dispatcher logs + defers.
    if observation.mismatches:
        return None

    # Sanity: the observation must have populated the four fields the
    # classifier reads. If any are None, the observation is incomplete
    # — defer rather than classify.
    if (
        observation.matched_address_lovelace is None
        or observation.beneficiary_addr_blake2_224 is None
    ):
        return None

    # Slot 2: WrongBeneficiary. The observer was asked to filter
    # outputs by `request.beneficiary_addr_hash` — if it summed 0
    # lovelace to that hash but the Cardano tx did exist (Kupo
    # returned at least one match), the tx must pay SOMEONE ELSE.
    # We need to know the "actual" payment hash to surface to the
    # pallet. The observer stub records the payment hash it FILTERED
    # by — that's `request.beneficiary_addr_hash`, not what the tx
    # actually paid. So we look it up separately on the observation:
    # the `actual_beneficiary_hash` field is populated by
    # `CardanoSlashObserver` (see below) which enumerates ALL
    # outputs, not just matches.
    actual_hash = getattr(observation, "actual_beneficiary_hash", None)
    if (
        observation.matched_address_lovelace == 0
        and isinstance(actual_hash, (bytes, bytearray))
        and len(actual_hash) == 28
        and bytes(actual_hash) != request.beneficiary_addr_hash
    ):
        # Internal-consistency mirror of the pallet's
        # FraudProofInvalid check — never ship a no-op proof where
        # actual == stored.
        return WrongBeneficiary(actual_payment_hash=bytes(actual_hash))

    # Slot 3: WrongAmount. Tx exists, beneficiary matches (the
    # observer summed nonzero lovelace), but the total != evidence.
    if observation.matched_address_lovelace != request.amount_lovelace:
        # Pallet expects u128. Cast through int() so the u64 / u128
        # boundary stays explicit.
        return WrongAmount(
            actual_lovelace=int(observation.matched_address_lovelace)
        )

    # Slot 4: honest evidence. No slash.
    return None


# ---------------------------------------------------------------------------
# Slash-aware observation. Extends the settle observer's contract with
# one extra field: the "actual" beneficiary payment hash that the tx
# paid (if any). The settle attestor doesn't need it because amount
# mismatch is sufficient evidence for refusal; the slash path needs
# it to fill the `WrongBeneficiary.actual_payment_hash` slot.
# ---------------------------------------------------------------------------


@dataclass
class SlashObservation(CardanoTxObservation):
    """Extends :class:`CardanoTxObservation` with the slash-specific
    ``actual_beneficiary_hash`` field.

    The base observer's :meth:`observe` only reports the SUM of
    lovelace going to the filtered beneficiary — it doesn't capture
    OTHER payment hashes in the tx. The slash path needs to know
    what the tx ACTUALLY paid if it didn't pay the expected
    beneficiary (so it can fill ``WrongBeneficiary.actual_payment_hash``).

    Populated by :meth:`CardanoSlashObserver.observe` only.
    """
    actual_beneficiary_hash: Optional[bytes] = None


def _extract_kupo_checkpoint_slot(payload: Any) -> Optional[int]:
    """Pull the slot number out of a Kupo ``/health`` JSON payload.

    Kupo 2.x emits ``most_recent_checkpoint`` as either a scalar slot
    number or a sub-dict carrying ``slot_no``. Older builds emit
    ``most_recent_checkpoint.slot_no``; some forks emit
    ``most_recent_checkpoint`` directly as an integer. We parse both
    defensively (sec-review round-1 Vuln 3).

    Returns the slot number on success, None on any unexpected shape
    (the caller treats None as "can't prove caught-up" → defer).
    """
    if not isinstance(payload, dict):
        return None
    checkpoint = payload.get("most_recent_checkpoint")
    # Form 1: bare scalar.
    if isinstance(checkpoint, int):
        return checkpoint
    # Form 2: sub-dict carrying slot_no.
    if isinstance(checkpoint, dict):
        slot = checkpoint.get("slot_no")
        if isinstance(slot, int):
            return slot
    # Some forks expose a top-level `most_recent_node_tip` sub-dict
    # with the same shape — fall back to that if the canonical key is
    # missing or wrong-shape.
    tip = payload.get("most_recent_node_tip")
    if isinstance(tip, int):
        return tip
    if isinstance(tip, dict):
        slot = tip.get("slot_no")
        if isinstance(slot, int):
            return slot
    return None


class CardanoSlashObserver(CardanoTxObserver):
    """Slash-watcher's Cardano observer.

    Inherits :class:`CardanoTxObserver` (Ogmios + Kupo plumbing) and
    overrides :meth:`observe` to ALSO surface the "actual" payment-key
    hash of the first output we couldn't match to the expected
    beneficiary. That hash fills the ``WrongBeneficiary`` slot.

    Everything else (tip / genesis / depth / slot resolution) is
    identical to the parent — we deliberately don't duplicate that
    logic.
    """

    async def kupo_checkpoint_slot(self) -> Optional[int]:
        """Return the Kupo follower's most-recent-checkpoint slot number.

        Used by the slash watcher's classifier (sec-review round-1
        Vuln 3) to gate the ``TxNotFound`` promotion: an empty Kupo
        ``/matches`` response is only proof-of-absence if the
        follower is demonstrably caught up to the requested observation
        depth + a safety margin (see
        :data:`KUPO_SYNC_SAFETY_MARGIN_SLOTS`). Otherwise the follower
        might be mid-resync after an operator restart and we'd burn an
        honest requester for a false-positive ``TxNotFound``.

        Returns ``None`` on any error (Kupo unreachable, non-200
        response, unexpected JSON shape). The dispatcher treats None
        as "can't prove caught-up" → defer (NOT slash). Same fail-safe
        contract as :meth:`_first_payment_hash` returning None.

        Kupo 2.x exposes ``/health`` with a JSON body containing
        ``most_recent_checkpoint`` as either a scalar slot number or a
        sub-dict carrying ``slot_no``. We parse both shapes
        defensively to stay forward-compatible.
        """
        url = f"{self.kupo_url}/health"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url, timeout=self._timeout,
                ) as resp:
                    if resp.status != 200:
                        body_preview = (await resp.text())[:200]
                        logger.warning(
                            f"slash_watcher: Kupo /health HTTP "
                            f"{resp.status}: {body_preview}"
                        )
                        return None
                    payload = await resp.json()
        except Exception as e:  # noqa: BLE001
            logger.warning(
                f"slash_watcher: Kupo /health raised "
                f"{type(e).__name__}: {e}"
            )
            return None
        return _extract_kupo_checkpoint_slot(payload)

    async def observe(  # type: ignore[override]
        self,
        cardano_tx_hash_hex: str,
        expected_beneficiary_blake2_224: bytes,
    ) -> SlashObservation:
        """Same contract as :meth:`CardanoTxObserver.observe`, with
        one addition: if the tx exists but no output pays the
        expected beneficiary, the FIRST non-matching output's
        payment-key hash is captured in
        :attr:`SlashObservation.actual_beneficiary_hash`.

        That hash is what the slash dispatcher passes to
        ``WrongBeneficiary``. We deliberately use the FIRST
        non-matching output rather than any heuristic about "the
        biggest output" — the pallet doesn't require that the
        watcher pick a specific payee, only that the alleged
        ``actual_payment_hash`` differs from the stored evidence
        (FraudProofInvalid check). Picking the first deterministic
        candidate keeps the M-of-N sig assembly trivial: every
        watcher derives the same FRAU digest from the same chain
        state.
        """
        # Reuse the parent class's logic by composition. We can't
        # super().observe() because the parent returns a plain
        # CardanoTxObservation (not the slash subclass). Instead we
        # call it, then enrich.
        base = await CardanoTxObserver.observe(
            self, cardano_tx_hash_hex, expected_beneficiary_blake2_224,
        )
        slash_obs = SlashObservation(tx_hash_hex=base.tx_hash_hex)
        slash_obs.cardano_tip_block_no = base.cardano_tip_block_no
        slash_obs.tx_block_no = base.tx_block_no
        slash_obs.observed_slot = base.observed_slot
        slash_obs.matched_address_lovelace = base.matched_address_lovelace
        slash_obs.beneficiary_addr_blake2_224 = base.beneficiary_addr_blake2_224
        slash_obs.mainchain_genesis_hash = base.mainchain_genesis_hash
        slash_obs.mismatches = list(base.mismatches)

        # Defensive pre-validation of Kupo output shapes (sec-review
        # round-1 Vuln 2). The parent observer's amount-sum treats
        # `value.coins` as 0 when it's not an int (e.g. stringly-typed
        # `"5000000"` from some Kupo forks). That silently understates
        # `matched_address_lovelace` and would mis-trigger the
        # WrongBeneficiary slash path here. Surface the shape error as
        # a NEW mismatch tag so the dispatcher defers rather than
        # slashes.
        #
        # We do a SEPARATE Kupo fetch (cheap, no chain-write side
        # effects) so we can inspect the raw output shape without
        # touching the settle attestor's amount-sum code path. If any
        # output for the expected beneficiary has a non-int
        # `value.coins`, append the new mismatch tag and short-circuit
        # the actual-hash lookup.
        if (
            "kupo_no_matches_for_tx" not in slash_obs.mismatches
            and slash_obs.matched_address_lovelace == 0
        ):
            shape_error = await self._kupo_response_has_shape_error(
                cardano_tx_hash_hex,
                expected_beneficiary_blake2_224,
            )
            if shape_error:
                slash_obs.mismatches.append("kupo_response_shape_error")
                return slash_obs

        # If the tx exists (no kupo_no_matches_for_tx) AND no
        # lovelace went to the expected beneficiary, walk the outputs
        # ONE more time to surface the actual payment-hash. We do a
        # second Kupo fetch rather than thread the matches list back
        # from the parent class — the parent's logic isn't designed
        # to expose them, and a second tx lookup is cheap relative
        # to the slash flow's other I/O.
        needs_actual_hash = (
            "kupo_no_matches_for_tx" not in slash_obs.mismatches
            and slash_obs.matched_address_lovelace == 0
        )
        if needs_actual_hash:
            slash_obs.actual_beneficiary_hash = await self._first_payment_hash(
                cardano_tx_hash_hex,
                expected_beneficiary_blake2_224,
            )

        return slash_obs

    async def _kupo_response_has_shape_error(
        self,
        cardano_tx_hash_hex: str,
        expected_beneficiary_blake2_224: bytes,
    ) -> bool:
        """Pre-validate Kupo output shapes before downstream code makes
        a slash decision (sec-review round-1 Vuln 2).

        Returns True if ANY output that pays the expected beneficiary
        has a non-int ``value.coins`` (e.g. stringly-typed from older
        Kupo versions / forks). In that case the parent observer would
        silently treat the output as 0 lovelace and the slash path
        would mis-trigger WrongBeneficiary on an honest requester.

        Network / decode failures return False (the parent observer
        will surface its own mismatch tags on the same path) — we only
        care about the specific stringly-typed-coins shape here.
        """
        try:
            async with aiohttp.ClientSession() as session:
                matches = await self._kupo_get(
                    session,
                    "/matches/*",
                    {"transaction_id": cardano_tx_hash_hex},
                )
        except Exception as e:  # noqa: BLE001
            logger.warning(
                f"slash_watcher: _kupo_response_has_shape_error lookup "
                f"raised {type(e).__name__}: {e}"
            )
            return False
        if not matches:
            return False
        from daemon.cardano_address import (
            extract_payment_hash_from_cardano_address,
        )
        for m in matches:
            if not isinstance(m, dict):
                continue
            address = m.get("address")
            if not isinstance(address, str):
                continue
            try:
                addr_hash = extract_payment_hash_from_cardano_address(
                    address,
                )
            except ValueError:
                continue
            if addr_hash != expected_beneficiary_blake2_224:
                continue
            value = m.get("value") or {}
            if not isinstance(value, dict):
                # Whole `value` field isn't a dict — the parent
                # observer's `value.coins` access would have silently
                # short-circuited. Surface as shape error.
                return True
            coins = value.get("coins")
            if coins is None:
                # Some Kupo builds use `lovelace` instead of `coins`.
                # Mirror the parent observer's fallback.
                coins = value.get("lovelace")
            if coins is None:
                # No coin field at all — also a shape mismatch the
                # parent silently treats as 0.
                return True
            if not isinstance(coins, int) or isinstance(coins, bool):
                # Stringly-typed `coins` (the headline bug) or
                # boolean (accidental Truthy coercion). Either way,
                # the parent observer silently treats it as 0 and the
                # slash path mis-triggers.
                return True
        return False

    async def _first_payment_hash(
        self,
        cardano_tx_hash_hex: str,
        expected_beneficiary_blake2_224: bytes,
    ) -> Optional[bytes]:
        """Return a NON-BENEFICIARY 28-byte payment-key hash via Kupo.

        We deliberately FILTER OUT any output whose computed payment
        hash equals ``expected_beneficiary_blake2_224`` before picking
        the first remaining hash (sec-review round-1 Vuln 2). Without
        this filter, a Kupo response shape error or a stringly-typed
        ``value.coins`` could let the parent observer report
        ``matched_address_lovelace == 0`` against a tx that actually
        paid the beneficiary correctly — and we'd then surface the
        keeper's CHANGE-output hash as the "actual" payee, slashing
        an honest requester for ``WrongBeneficiary(change_hash)``.

        Determinism: Kupo doesn't guarantee stable match ordering, so
        we sort by ``output_index`` ascending (CIP-0019 outputs are
        contiguously indexed by tx-construction order) with a stable
        secondary sort by ``address`` to break ties when
        ``output_index`` is missing or duplicated. This pins
        cross-watcher determinism on the (rare but valid)
        legitimate-slash path so the M-of-N envelope can converge.

        Returns ``None`` on any error (no non-beneficiary output,
        decode failure, Kupo unreachable). The dispatcher treats None
        as "can't construct WrongBeneficiary proof" and defers the
        slash to the next tick rather than fabricating a hash —
        :class:`SlashWatcher._process_one_locked` will tag the row
        with :attr:`ClassifierOutcome.OBSERVER_UNAVAILABLE` so an
        operator can grep journalctl for the deferred decision.
        """
        # Reuse the parent's address-extraction helper.
        from daemon.cardano_address import (
            extract_payment_hash_from_cardano_address,
        )
        try:
            async with aiohttp.ClientSession() as session:
                matches = await self._kupo_get(
                    session,
                    "/matches/*",
                    {"transaction_id": cardano_tx_hash_hex},
                )
        except Exception as e:  # noqa: BLE001
            logger.warning(
                f"slash_watcher: _first_payment_hash Kupo lookup raised "
                f"{type(e).__name__}: {e}"
            )
            return None
        if not matches:
            return None
        # Deterministic ordering: sort by output_index ascending, then
        # address (stable secondary sort so a missing/duplicate
        # output_index doesn't break cross-watcher convergence).
        # Non-dict entries sort last via a high sentinel — they're
        # filtered out in the loop below anyway.

        def _sort_key(entry: Any) -> tuple:
            if not isinstance(entry, dict):
                return (2**31, "")
            oi = entry.get("output_index")
            if not isinstance(oi, int):
                oi = 2**31 - 1
            addr = entry.get("address") if isinstance(
                entry.get("address"), str
            ) else ""
            return (oi, addr)

        try:
            sorted_matches = sorted(matches, key=_sort_key)
        except Exception as e:  # noqa: BLE001
            logger.warning(
                f"slash_watcher: _first_payment_hash sort raised "
                f"{type(e).__name__}: {e} — falling back to unsorted "
                f"iteration"
            )
            sorted_matches = list(matches)
        for m in sorted_matches:
            if not isinstance(m, dict):
                continue
            address = m.get("address")
            if not isinstance(address, str):
                continue
            try:
                candidate_hash = extract_payment_hash_from_cardano_address(
                    address,
                )
            except ValueError:
                # Skip non-CIP-0019 type-0/6 addresses (script /
                # enterprise / type-7 etc). Move on to the next
                # output. None at the end if NONE decode.
                continue
            # CRITICAL: filter out the expected beneficiary. Without
            # this, a Kupo shape error could surface the beneficiary's
            # own payment hash as "actual" and the dispatcher would
            # then defer via the FraudProofInvalid-style internal-
            # consistency check in classify_fraud (actual == stored
            # short-circuits to no-slash). But if there's ALSO a
            # change output, we'd return that hash here and slash an
            # honest requester for WrongBeneficiary(change_hash).
            if candidate_hash == expected_beneficiary_blake2_224:
                continue
            return candidate_hash
        return None


# ---------------------------------------------------------------------------
# SlashWatcher dispatcher — glues the observer to a chain-write path
# via the gateway-mediated multisig aggregator.
# ---------------------------------------------------------------------------


class SlashWatcher:
    """Top-level dispatcher for the slash watcher.

    Operational shape: a single background asyncio task started by
    ``CertDaemon.run()`` after substrate is connected, alongside the
    settle and expire attestor loops. Each tick:

      1. Reads ``ClaimSettlementRequests`` from chain and filters to
         rows with ``bond_amount > 0`` (the slash trigger).
      2. For each bonded row, fetches the on-chain genesis hash and
         independently observes the requested Cardano tx via
         Ogmios+Kupo.
      3. Runs the classifier. None → no slash. Otherwise builds the
         FRAU preimage, signs blake2_256(preimage) with the
         cert-daemon sr25519 key, assembles an M-sig envelope via
         the gateway aggregator (channel namespace ``slash``), and
         submits ``IntentSettlement::slash_bad_settlement_evidence``.

    Concurrency: bounded by ``max_concurrent`` (default 8, matching
    the settle/expire paths per memo §6 OQ#10) via a Semaphore. Chain
    submission is serialized under the SHARED ``chain_write_lock`` so
    the slash signer's nonce stays monotonic against the
    receipt-cert, TEE-evidence, settle, and expire submission paths
    (memory ``feedback_polkadot_nonce_race_on_burst.md``).

    Idempotency: a successful slash removes the
    ``ClaimSettlementRequests`` row, so the next tick's iteration
    naturally skips it. We do NOT keep a local "already slashed"
    cache — chain state is the source of truth.
    """

    DEFAULT_MAX_CONCURRENT: int = 8

    def __init__(
        self,
        *,
        config: Any,
        substrate_client: Any,
        chain_write_lock: asyncio.Lock,
        observer: CardanoSlashObserver,
        poll_interval: int = 12,
        max_concurrent: Optional[int] = None,
        aggregator: Optional[Any] = None,
        min_signer_threshold: Optional[int] = None,
    ):
        self.config = config
        self.client = substrate_client
        self._chain_write_lock = chain_write_lock
        self.observer = observer
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
                    f"slash_watcher: MinSignerThreshold query failed "
                    f"({type(e).__name__}); defaulting to 2"
                )
                min_signer_threshold = 2
        self.min_signer_threshold = max(1, int(min_signer_threshold or 1))

    async def process_one(
        self,
        request: PendingBondedRequest,
        live_chain_id: bytes,
    ) -> SlashVerdict:
        """Classify + sign + submit slash for one bonded request.

        Returns a :class:`SlashVerdict` describing the outcome. The
        dispatcher writes a structured log line per outcome (warn on
        slash, info on no-fraud, warn on transient).
        """
        async with self._sem:
            return await self._process_one_locked(request, live_chain_id)

    async def _process_one_locked(
        self,
        request: PendingBondedRequest,
        live_chain_id: bytes,
    ) -> SlashVerdict:
        verdict = SlashVerdict(
            claim_id=request.claim_id,
            outcome=ClassifierOutcome.NOT_FRAUD,
        )

        # Pre-flight: bond-presence gate. The pallet rejects with
        # BondNotReserved on bond_amount==0 anyway, but skipping here
        # saves the L1 round-trip + the M-sig envelope dance.
        if request.bond_amount <= 0:
            verdict.outcome = ClassifierOutcome.NOT_FRAUD
            verdict.detail = "unbonded_skip"
            return verdict

        # Observe the Cardano tx. The observer returns either a
        # populated SlashObservation or an observation with mismatch
        # tags. We pass the EXPECTED beneficiary hash so the
        # observer's match-summing filters correctly; the
        # actual_beneficiary_hash field surfaces the divergent payee
        # when match-sum == 0.
        cardano_tx_hex = request.cardano_tx_hash.hex()
        try:
            observation = await self.observer.observe(
                cardano_tx_hex, request.beneficiary_addr_hash,
            )
        except Exception as e:  # noqa: BLE001
            # Transient L1 error — log loudly, defer to next tick. We
            # deliberately do NOT classify as TxNotFound because that
            # would slash an honest requester if Ogmios is just down.
            logger.warning(
                f"slash_watcher: observer raised for "
                f"{request.claim_id.hex()[:16]}...: {type(e).__name__}: "
                f"{e} — treating as transient, deferring to next tick"
            )
            verdict.outcome = ClassifierOutcome.TRANSIENT_L1_ERROR
            verdict.detail = f"observer_raised: {type(e).__name__}"
            return verdict

        # The observation must distinguish between three states:
        #
        #   (a) "tx not on chain" — Kupo successfully looked up and
        #       returned an empty match list. The observer signals
        #       this with the `kupo_no_matches_for_tx` mismatch tag.
        #       This is the TxNotFound proof's CANDIDATE trigger —
        #       but only after the sync gate below confirms the
        #       follower is caught up.
        #   (b) "tx exists, here's the data" — no mismatch tags,
        #       fields populated. The classifier compares fields and
        #       returns Wrong* or None.
        #   (c) "tooling failed" — anything else (kupo timeout,
        #       ogmios unreachable, malformed response,
        #       kupo_response_shape_error). The observer surfaces
        #       non-`kupo_no_matches_for_tx` mismatch tags
        #       (`ogmios_tip_unavailable`, `kupo_match_missing_slot`,
        #       `kupo_response_shape_error`, etc.) — we MUST NOT
        #       classify those as TxNotFound.
        if (
            observation.mismatches
            and "kupo_no_matches_for_tx" not in observation.mismatches
        ):
            # State (c) — defer. Log so an operator can see why the
            # slash didn't progress.
            logger.info(
                f"slash_watcher: observation incomplete for claim "
                f"{request.claim_id.hex()[:16]}... — "
                f"mismatches={observation.mismatches}. Will retry next tick."
            )
            verdict.outcome = ClassifierOutcome.OBSERVER_UNAVAILABLE
            verdict.detail = ", ".join(observation.mismatches)
            return verdict

        # Vuln 3: gate `TxNotFound` promotion on the local Kupo
        # follower being demonstrably caught up. Probe the
        # `/health` endpoint (if the observer exposes the helper)
        # and feed the slot to the classifier; an unreachable or
        # behind follower causes the classifier to defer rather than
        # promote an empty match list to a slash.
        kupo_checkpoint_slot: Optional[int] = None
        if "kupo_no_matches_for_tx" in observation.mismatches:
            checkpoint_getter = getattr(
                self.observer, "kupo_checkpoint_slot", None,
            )
            if checkpoint_getter is not None:
                try:
                    kupo_checkpoint_slot = await checkpoint_getter()
                except Exception as e:  # noqa: BLE001
                    logger.warning(
                        f"slash_watcher: kupo_checkpoint_slot probe "
                        f"raised for claim_id="
                        f"{request.claim_id.hex()[:16]}...: "
                        f"{type(e).__name__}: {e} — deferring slash"
                    )
                    kupo_checkpoint_slot = None

        proof = classify_fraud(
            request, observation,
            kupo_checkpoint_slot=kupo_checkpoint_slot,
        )
        # The classifier appends `kupo_behind_request_depth` to the
        # observation mismatches when it defers due to the sync gate.
        # Surface that explicitly so the operator sees the right
        # reason in journalctl (NOT a generic OBSERVER_UNAVAILABLE).
        if (
            proof is None
            and "kupo_behind_request_depth" in observation.mismatches
        ):
            logger.info(
                f"slash_watcher: deferring TxNotFound slash for claim "
                f"{request.claim_id.hex()[:16]}... — Kupo follower "
                f"checkpoint_slot={kupo_checkpoint_slot} below "
                f"request.observed_slot={request.observed_slot} + margin "
                f"({KUPO_SYNC_SAFETY_MARGIN_SLOTS}). Will retry next "
                f"tick."
            )
            verdict.outcome = ClassifierOutcome.KUPO_BEHIND_REQUEST_DEPTH
            verdict.detail = (
                f"kupo_checkpoint_slot={kupo_checkpoint_slot} "
                f"vs request.observed_slot={request.observed_slot}"
            )
            return verdict
        if proof is None:
            # State (b) with honest evidence — no slash. The settle
            # attestor's STCA path handles the close.
            verdict.outcome = ClassifierOutcome.NOT_FRAUD
            verdict.detail = (
                f"observed_to_beneficiary={observation.matched_address_lovelace} "
                f"vs evidence={request.amount_lovelace}"
            )
            return verdict

        # Tag the outcome by variant for journalctl grep.
        if isinstance(proof, TxNotFound):
            verdict.outcome = ClassifierOutcome.SLASH_TX_NOT_FOUND
            verdict.detail = "kupo_returned_no_matches"
        elif isinstance(proof, WrongAmount):
            verdict.outcome = ClassifierOutcome.SLASH_WRONG_AMOUNT
            verdict.detail = (
                f"actual_lovelace={proof.actual_lovelace} "
                f"vs evidence={request.amount_lovelace}"
            )
        elif isinstance(proof, WrongBeneficiary):
            verdict.outcome = ClassifierOutcome.SLASH_WRONG_BENEFICIARY
            verdict.detail = (
                f"actual_payment_hash={proof.actual_payment_hash.hex()[:16]}... "
                f"vs evidence={request.beneficiary_addr_hash.hex()[:16]}..."
            )
        verdict.fraud_proof = proof

        # Build + sign the FRAU digest.
        preimage = build_frau_preimage(
            chain_id=live_chain_id,
            claim_id=request.claim_id,
            fraud_proof=proof,
        )
        digest = compute_frau_digest(preimage)
        sig_bytes = self.client.keypair.sign(digest)
        pubkey_bytes = self.client.keypair.public_key

        logger.warning(
            f"slash_watcher: SLASH detected for "
            f"claim_id={request.claim_id.hex()[:16]}... "
            f"variant={verdict.outcome} detail={verdict.detail} — "
            f"assembling M-sig envelope"
        )

        # Assemble + submit. Two paths matching the settle / expire
        # contract:
        #
        #   (1) Aggregator wired + threshold > 1: gateway-mediated
        #       M-sig envelope. Channel namespace ``slash``;
        #       per-claim_id key (so a peer that classifies the same
        #       fraud signs the same digest and lands in the same
        #       envelope).
        #   (2) Aggregator unwired OR threshold == 1: 1-sig submit.
        #       Only useful on test chains; production chains have
        #       MinSignerThreshold >= 2 and the pallet rejects 1-sig
        #       envelopes with InsufficientSignatures /
        #       FraudThresholdNotMet.
        if self.aggregator is not None and self.min_signer_threshold > 1:
            # Wrap the aggregator call in a structured try/except so a
            # future channel-namespace rejection (or any other
            # ValueError-class bug) logs LOUDLY with claim_id + kind +
            # exception class instead of dying silently inside
            # `asyncio.gather(..., return_exceptions=True)` at the
            # _tick boundary. See sec-review round-1 Vuln 1 — pre-fix
            # the aggregator's `_url` whitelist only accepted "settle"
            # and "expire" so every slash raised ValueError at this
            # call site and the exception was swallowed by gather().
            try:
                async with aiohttp.ClientSession() as session:
                    envelope = await self.aggregator.assemble_envelope(
                        session,
                        kind="slash",
                        key=request.claim_id,
                        digest=digest,
                        my_pubkey=pubkey_bytes,
                        my_sig=sig_bytes,
                    )
            except Exception as e:  # noqa: BLE001
                logger.error(
                    f"slash_watcher: aggregator.assemble_envelope FAILED "
                    f"for claim_id={request.claim_id.hex()[:16]}... "
                    f"kind='slash' — {type(e).__name__}: {e!r}. "
                    f"Slash deferred to next tick. If this is a "
                    f"namespace-rejection ValueError, the gateway-side "
                    f"MULTISIG_KINDS whitelist needs 'slash' added "
                    f"(blob-gateway src/multisig_sigs_store.ts).",
                    exc_info=True,
                )
                verdict.detail = f"aggregator_raised: {type(e).__name__}"
                return verdict
            if len(envelope) < self.min_signer_threshold:
                logger.info(
                    f"slash_watcher: {len(envelope)}/{self.min_signer_threshold} "
                    f"sigs assembled for claim_id="
                    f"{request.claim_id.hex()[:16]}... — deferring submit "
                    f"until more peers share. Next tick will retry."
                )
                verdict.detail = (
                    f"awaiting_peer_sigs:{len(envelope)}/"
                    f"{self.min_signer_threshold}"
                )
                return verdict
            logger.info(
                f"slash_watcher: assembled {len(envelope)}/"
                f"{self.min_signer_threshold} sigs for "
                f"{request.claim_id.hex()[:16]}... — submitting slash"
            )
            async with self._chain_write_lock:
                try:
                    ext_hash = await asyncio.to_thread(
                        self.client.submit_slash_bad_settlement_evidence,
                        request.claim_id,
                        proof,
                        envelope,
                    )
                except Exception as e:  # noqa: BLE001
                    logger.warning(
                        f"slash_watcher: submit_slash_envelope raised "
                        f"for {request.claim_id.hex()[:16]}...: "
                        f"{type(e).__name__}: {e}"
                    )
                    verdict.detail = f"submit_raised: {type(e).__name__}"
                    return verdict
        else:
            async with self._chain_write_lock:
                try:
                    ext_hash = await asyncio.to_thread(
                        self.client.submit_slash_bad_settlement_evidence,
                        request.claim_id,
                        proof,
                        [(pubkey_bytes, sig_bytes)],
                    )
                except Exception as e:  # noqa: BLE001
                    logger.warning(
                        f"slash_watcher: submit_slash_1sig raised for "
                        f"{request.claim_id.hex()[:16]}...: "
                        f"{type(e).__name__}: {e}"
                    )
                    verdict.detail = f"submit_raised: {type(e).__name__}"
                    return verdict

        if not ext_hash:
            logger.warning(
                f"slash_watcher: submit_slash_bad_settlement_evidence "
                f"returned empty hash for {request.claim_id.hex()[:16]}... — "
                f"retryable next tick"
            )
            verdict.detail = "submit_no_hash"
            return verdict

        verdict.extrinsic_hash = ext_hash
        logger.warning(
            f"slash_watcher: SLASH SUBMITTED for "
            f"claim_id={request.claim_id.hex()[:16]}... "
            f"variant={verdict.outcome} ext_hash={ext_hash[:18]}..."
        )
        return verdict

    async def _tick(self, live_chain_id: bytes) -> None:
        """Single poll tick — fetch pending requests, filter to
        bonded rows, process each under the concurrency semaphore.
        """
        try:
            requests = await asyncio.to_thread(
                self.client.list_pending_settlement_requests
            )
        except Exception as e:  # noqa: BLE001
            logger.warning(
                f"slash_watcher: list_pending_settlement_requests raised "
                f"{type(e).__name__}: {e}"
            )
            return
        if not requests:
            return

        # Convert dicts → PendingBondedRequest at the boundary
        # (identical pattern to the settle / expire attestors). A
        # malformed row is skipped + logged rather than killing the
        # batch.
        bonded: list[PendingBondedRequest] = []
        for r in requests:
            if not isinstance(r, dict):
                logger.warning(
                    f"slash_watcher: skipping non-dict row {type(r).__name__}"
                )
                continue
            try:
                # PendingBondedRequest accepts an optional bond_amount
                # field. Older clients (pre-#84) don't supply it — we
                # default to 0 and the dispatcher's unbonded-skip
                # gate filters them out before classifier runs.
                req = PendingBondedRequest(**r)
            except TypeError as e:
                logger.warning(
                    f"slash_watcher: skipping malformed pending row "
                    f"(claim_id={r.get('claim_id', b'').hex()[:16] if isinstance(r.get('claim_id'), bytes) else '?'}...): "
                    f"{type(e).__name__}: {e}"
                )
                continue
            if req.bond_amount <= 0:
                # Filter early: unbonded rows are not slashable. The
                # settle attestor handles them via the STCA path.
                continue
            bonded.append(req)

        if not bonded:
            return
        logger.info(
            f"slash_watcher: {len(bonded)} bonded settlement "
            f"request(s) — processing under sem cap "
            f"{self._sem._value}"  # type: ignore[attr-defined]
        )
        coros = [
            self.process_one(req, live_chain_id) for req in bonded
        ]
        results = await asyncio.gather(*coros, return_exceptions=True)
        # Surface any per-row exception explicitly — `gather` would
        # otherwise swallow them and the operator would never see the
        # failure (sec-review round-1 Vuln 1). Pair each exception with
        # the originating claim_id for journalctl forensics.
        for req, result in zip(bonded, results):
            if isinstance(result, BaseException):
                logger.error(
                    f"slash_watcher: process_one raised for "
                    f"claim_id={req.claim_id.hex()[:16]}... "
                    f"— {type(result).__name__}: {result!r}",
                    exc_info=result,
                )

    async def _run_forever(self) -> None:
        while self._running:
            live_chain_id_hex = await asyncio.to_thread(
                self.client.get_genesis_hash
            )
            if not live_chain_id_hex:
                logger.info(
                    "slash_watcher: substrate genesis not yet available "
                    "— skipping tick"
                )
            else:
                try:
                    chain_id = bytes.fromhex(
                        live_chain_id_hex.removeprefix("0x")
                    )
                except ValueError:
                    logger.warning(
                        f"slash_watcher: malformed live genesis "
                        f"{live_chain_id_hex!r} — skipping tick"
                    )
                    chain_id = b""
                if len(chain_id) == 32:
                    try:
                        await self._tick(chain_id)
                    except Exception as e:  # noqa: BLE001
                        logger.error(
                            f"slash_watcher: tick raised "
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
            f"slash_watcher: started "
            f"(poll_interval={self._poll_interval}s, "
            f"max_concurrent={self._sem._value}, "  # type: ignore[attr-defined]
            f"min_signer_threshold={self.min_signer_threshold})"
        )

    def stop(self) -> None:
        """Signal the loop to exit at its next sleep boundary."""
        self._running = False
        if self._task is not None and not self._task.done():
            self._task.cancel()


# ---------------------------------------------------------------------------
# Factory: build the slash watcher from env vars + DaemonConfig.
# ---------------------------------------------------------------------------


def maybe_create_slash_watcher(
    config: Any,
    substrate_client: Any,
    chain_write_lock: asyncio.Lock,
) -> Optional[SlashWatcher]:
    """Construct a :class:`SlashWatcher` if its required deps are wired.

    Soft-disable contract (matches the settle / expire attestor
    factories): if ``ogmios_url`` or ``KUPO_URL`` is unset, return
    None. The slash path needs the same Ogmios + Kupo follower as
    the settle path — without it we can't observe Cardano txs
    independently and we MUST NOT slash blindly.

    Optional env (mirror the SETTLE_/EXPIRE_ attestor knobs so an
    operator can tune the three loops independently):

        SLASH_WATCHER_POLL_INTERVAL — seconds between polls (default 12).
        SLASH_WATCHER_MAX_CONCURRENT — concurrency cap (default 8).
    """
    ogmios_url = (getattr(config, "ogmios_url", "") or "").strip()
    kupo_url = (
        getattr(config, "kupo_url", "") or os.environ.get("KUPO_URL", "") or ""
    ).strip()
    if not ogmios_url or not kupo_url:
        logger.info(
            "slash_watcher: not configured "
            f"(ogmios_url={'set' if ogmios_url else 'missing'}, "
            f"kupo_url={'set' if kupo_url else 'missing'}) — "
            f"slash watcher will not run on this node."
        )
        return None
    try:
        poll_interval = int(
            os.environ.get("SLASH_WATCHER_POLL_INTERVAL", "12")
        )
    except ValueError:
        poll_interval = 12
    try:
        max_concurrent = int(
            os.environ.get(
                "SLASH_WATCHER_MAX_CONCURRENT",
                str(SlashWatcher.DEFAULT_MAX_CONCURRENT),
            )
        )
    except ValueError:
        max_concurrent = SlashWatcher.DEFAULT_MAX_CONCURRENT
    observer = CardanoSlashObserver(
        ogmios_url=ogmios_url,
        kupo_url=kupo_url,
    )
    # Gateway-mediated multisig aggregator (same contract as settle
    # / expire path). Disabled when blob_gateway_url is unset; the
    # slash path then falls back to 1-sig submit, which only works
    # on test chains with MinSignerThreshold == 1.
    aggregator = None
    gateway_url = (getattr(config, "blob_gateway_url", "") or "").strip()
    if gateway_url:
        from daemon.multisig_aggregator import MultisigAggregator
        aggregator = MultisigAggregator(gateway_url=gateway_url)
        logger.info(
            f"slash_watcher: multisig aggregator wired (gateway={gateway_url})"
        )
    else:
        logger.warning(
            "slash_watcher: BLOB_GATEWAY_URL unset — aggregator disabled. "
            "Submits will use 1-sig envelope; pallet rejects with "
            "FraudThresholdNotMet on MinSignerThreshold >= 2 chains."
        )
    return SlashWatcher(
        config=config,
        substrate_client=substrate_client,
        chain_write_lock=chain_write_lock,
        observer=observer,
        poll_interval=poll_interval,
        max_concurrent=max_concurrent,
        aggregator=aggregator,
    )


__all__ = [
    "TAG_FRAU",
    "FRAUD_DISCRIMINANT_WRONG_AMOUNT",
    "FRAUD_DISCRIMINANT_TX_NOT_FOUND",
    "FRAUD_DISCRIMINANT_WRONG_BENEFICIARY",
    "KUPO_SYNC_SAFETY_MARGIN_SLOTS",
    "WrongAmount",
    "TxNotFound",
    "WrongBeneficiary",
    "FraudProof",
    "encode_fraud_proof",
    "build_frau_preimage",
    "compute_frau_digest",
    "PendingBondedRequest",
    "ClassifierOutcome",
    "SlashVerdict",
    "SlashObservation",
    "CardanoSlashObserver",
    "classify_fraud",
    "SlashWatcher",
    "maybe_create_slash_watcher",
    "CARDANO_MAINNET_GENESIS_HASH",
    "CARDANO_PREPROD_GENESIS_HASH",
    "CARDANO_PREVIEW_GENESIS_HASH",
    "NETWORK_MAGIC_TO_GENESIS_HASH",
]
