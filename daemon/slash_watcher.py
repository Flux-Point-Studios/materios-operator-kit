"""Prosecute fraudulent bonded settlement evidence using Cardano observations.

The live runtime metadata selects FRAU or request-bound FRV2 signatures.
Unknown contracts and expired requests are refused before signing; runtime
and expiry are checked again under the shared write lock before submission.
Peer signatures are aggregated only for the locally computed digest.

FraudProof SCALE discriminants are WrongAmount=0 (u128 LE), TxNotFound=1,
and WrongBeneficiary=2 ([u8; 28]). FRV2 adds a request identity over the
requester, normalized u64 submission block, evidence, and settlement mode.
Rust-generated parity fixtures pin these encodings.

Cardano query failures remain deferrals. TxNotFound requires a synced Kupo
follower; a missing response alone never proves fraud.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
from dataclasses import astuple, dataclass
from typing import Any, Optional, Union

import aiohttp
from scalecodec.utils.ss58 import ss58_decode

from daemon import health_server

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
    checked_runtime_context,
    request_expired,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Legacy FRAU domain tag + FraudProof SCALE encoder.
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
TAG_FRV2: bytes = b"FRV2"

# FraudProof variant discriminants — pinned to the Rust enum order in
# `pallets/intent-settlement/src/types.rs` lines 448-464. SCALE assigns
# discriminants by SOURCE DECLARATION ORDER (NOT alphabetical), so the
# order below is load-bearing — do NOT reorder without checking the
# pallet source AND bumping the parity-vector test.
FRAUD_DISCRIMINANT_WRONG_AMOUNT: int = 0
FRAUD_DISCRIMINANT_TX_NOT_FOUND: int = 1
FRAUD_DISCRIMINANT_WRONG_BENEFICIARY: int = 2

# Most slots Kupo's checkpoint may trail the Cardano tip read just before it
# for an empty lookup to prove a tx absent. An honest payout sits at least
# MinFinalityDepth (15) blocks, about 300 slots, behind the tip.
KUPO_TIP_LAG_MAX_SLOTS: int = 60

# Cardano blocks between the two absence proofs TxNotFound needs.
TX_ABSENCE_CONFIRM_BLOCKS: int = 10

# Per Cardano network (Shelley genesis hash), an output reference that was
# spent long ago. A Kupo that returns it with ``spent_at`` keeps spent outputs
# and has indexed that far back. Preprod: created and spent at slot 10608917
# (db-sync, read 2026-10-18). A network without an entry cannot prove fraud.
SPENT_CANARIES: dict[bytes, tuple[str, int]] = {
    CARDANO_PREPROD_GENESIS_HASH: (
        "842905900c2c33c4d1d0efd9ad73f7a2e8326c547bab73bf6b950e12fab275d5", 0,
    ),
}


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
    """Build the legacy FRAU preimage that the pallet's
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
    """Hash a validated FRAU or FRV2 preimage with blake2_256."""
    return hashlib.blake2b(preimage, digest_size=32).digest()


# ---------------------------------------------------------------------------
# Pending settlement-request envelope as seen by the watcher.
#
# Mirrors :class:`daemon.settle_claim_attestor.PendingSettlementRequest`
# minus the chain-state ``voucher_digest`` (which the slash path
# doesn't need). The dispatcher converts the dicts returned by
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
    digest to the request identity under FRV2; the voucher is
    irrelevant to both slash protocols. We accept the field for shape
    uniformity with the settle path's dict shape but ignore it.

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


def encode_request_identity(request: PendingBondedRequest) -> bytes:
    """SCALE(requester[32], submitted_block:u64, evidence, settled_direct)."""
    requester = request.requester
    requester_bytes = bytes.fromhex(
        requester[2:] if requester.startswith("0x") else ss58_decode(requester)
    )
    if len(requester_bytes) != 32:
        raise ValueError("requester must encode to 32 bytes")
    fields = [requester_bytes]
    for value, width in (
        (request.submitted_block, 8), (request.cardano_tx_hash, 32),
        (request.observed_at_depth, 4), (request.observed_slot, 8),
        (request.beneficiary_addr_hash, 28), (request.amount_lovelace, 8),
        (request.mainchain_genesis_hash, 32),
    ):
        if width in (28, 32):
            if not isinstance(value, bytes) or len(value) != width:
                raise ValueError(f"request field must be {width} bytes")
            fields.append(value)
        else:
            if type(value) is not int or not 0 <= value < 1 << (8 * width):
                raise ValueError(f"request field must be u{8 * width}")
            fields.append(value.to_bytes(width, "little"))
    if type(request.settled_direct) is not bool:
        raise ValueError("settled_direct must be bool")
    return b"".join(fields) + bytes([request.settled_direct])


def build_frv2_preimage(chain_id: bytes, request: PendingBondedRequest, proof: FraudProof) -> bytes:
    if len(chain_id) != 32 or len(request.claim_id) != 32:
        raise ValueError("chain_id and claim_id must be 32 bytes")
    request_id = hashlib.blake2b(encode_request_identity(request), digest_size=32).digest()
    return TAG_FRV2 + chain_id + request.claim_id + request_id + encode_fraud_proof(proof)


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

    The SLASH_* outcomes correspond to the pallet's FraudProof variants;
    every other outcome moves on without dispatching.
    """
    NOT_FRAUD = "not_fraud"
    DEFERRED = "deferred"
    TRANSIENT_L1_ERROR = "transient_l1_error"
    OBSERVER_UNAVAILABLE = "observer_unavailable"
    # Kupo returned no output of the tx, but its absence is not proven yet
    # (see CardanoSlashObserver.prove_absence and TX_ABSENCE_CONFIRM_BLOCKS).
    TX_ABSENCE_UNPROVEN = "tx_absence_unproven"
    # The Kupo this watcher reads cannot prove fraud: it does not index every
    # address, or does not keep spent outputs.
    KUPO_COVERAGE_GAP = "kupo_coverage_gap"
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
    absence_proven: bool = False,
) -> Optional[FraudProof]:
    """Decide whether a bonded settlement request's evidence is
    fraudulent given the cert-daemon's independent Cardano
    observation.

    Returns a :class:`FraudProof` when the observation contradicts the
    evidence, and None for honest evidence or an incomplete observation.
    The caller must already have checked that the observer's Kupo indexes
    every address and keeps spent outputs.

    Priority order:
        1. ``TxNotFound``: Kupo returned no output of the tx
           (``kupo_no_matches_for_tx``) AND ``absence_proven`` — the
           caller's two proofs from a caught-up Kupo, see
           :meth:`CardanoSlashObserver.prove_absence`. Without them,
           None.
        2. ``WrongBeneficiary``: the tx exists but no output pays the
           claimed beneficiary; the observer surfaces another payee.
        3. ``WrongAmount``: the tx pays the claimed beneficiary a
           different lovelace total.
        4. None: honest evidence.

    Any other mismatch tag (Ogmios down, a failed Kupo lookup, a shape
    error) is an incomplete observation and never a proof.
    """
    if "kupo_no_matches_for_tx" in observation.mismatches:
        return TxNotFound() if absence_proven else None

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
    defensively (sec-review round-1 Vuln 3). ``most_recent_node_tip`` is
    the node's tip, not what Kupo has indexed, and never stands in for it.

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

        Live-prod note (2026-05-15): Kupo 2.9 content-negotiates the
        ``/health`` response body — without an explicit
        ``Accept: application/json`` request header it falls back to
        Prometheus text format (either as 200 with text body or 406
        depending on version). The slash watcher then can't parse
        the structured shape, returns None, and the dispatcher defers
        TxNotFound prosecution per the Vuln 3 fail-safe contract. The
        ``Accept`` header below is load-bearing — do NOT remove without
        confirming current Kupo continues to serve JSON by default.
        """
        url = f"{self.kupo_url}/health"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url,
                    timeout=self._timeout,
                    headers={"Accept": "application/json"},
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

    async def _coverage_gap(self, session: aiohttp.ClientSession, genesis: bytes) -> Optional[str]:
        """None when this Kupo indexes every address (``/patterns`` holds
        ``*``) and keeps spent outputs (it returns the network's spent
        canary with ``spent_at``); otherwise why it cannot prove fraud."""
        patterns = await self._kupo_get(session, "/patterns", {})
        if patterns is None:
            return "patterns_unavailable"
        if "*" not in patterns:
            return f"patterns_without_*:{len(patterns)}"
        canary = SPENT_CANARIES.get(genesis)
        if canary is None:
            return "no_spent_canary_for_network"
        tx, index = canary
        rows = await self._kupo_get(session, f"/matches/{index}@{tx}", {})
        if rows is None:
            return "spent_canary_unavailable"
        if not any(isinstance(row, dict) and row.get("spent_at") for row in rows):
            return "spent_outputs_not_kept"
        return None

    async def kupo_coverage_gap(self) -> Optional[str]:
        """:meth:`_coverage_gap` for the network Ogmios follows."""
        async with aiohttp.ClientSession() as session:
            genesis = await self.get_genesis_hash(session)
            if genesis is None:
                return "ogmios_genesis_hash_unavailable"
            return await self._coverage_gap(session, genesis)

    async def prove_absence(self, cardano_tx_hash_hex: str) -> tuple[Optional[int], str]:
        """The Cardano tip height at which a caught-up, fully indexing Kupo
        returned no output of the tx, or None with the reason.

        Order matters: the tip is read first, then Kupo's checkpoint, which
        must be within KUPO_TIP_LAG_MAX_SLOTS of it, then the lookup, so the
        lookup covers at least everything up to that checkpoint.
        """
        async with aiohttp.ClientSession() as session:
            tip = await self._ogmios_rpc(session, "queryNetwork/tip")
            height = await self._ogmios_rpc(session, "queryNetwork/blockHeight")
            if not (isinstance(tip, dict) and type(tip.get("slot")) is int and type(height) is int):
                return None, "ogmios_tip_unavailable"
            genesis = await self.get_genesis_hash(session)
            if genesis is None:
                return None, "ogmios_genesis_hash_unavailable"
            checkpoint = await self.kupo_checkpoint_slot()
            if checkpoint is None:
                return None, "kupo_checkpoint_unavailable"
            if checkpoint < tip["slot"] - KUPO_TIP_LAG_MAX_SLOTS:
                return None, f"kupo_behind_tip:{checkpoint}<{tip['slot']}-{KUPO_TIP_LAG_MAX_SLOTS}"
            gap = await self._coverage_gap(session, genesis)
            if gap is not None:
                return None, f"kupo_coverage:{gap}"
            matches = await self._kupo_get(session, f"/matches/*@{cardano_tx_hash_hex}", {})
            if matches is None:
                return None, "kupo_matches_unavailable"
            if matches:
                return None, "tx_found"
            return height, "absent"

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
        slash_obs.outputs = base.outputs
        if base.outputs is None:
            return slash_obs

        # Sec-review round-1 Vuln 2 and round 2: the parent's amount sum
        # counts a non-int ``value.coins`` as 0, which would understate the
        # payment and mis-trigger WrongAmount or WrongBeneficiary. Every
        # check reads the same rows the sum did.
        if kupo_shape_error(base.outputs, expected_beneficiary_blake2_224):
            slash_obs.mismatches.append("kupo_response_shape_error")
            return slash_obs
        if slash_obs.matched_address_lovelace == 0:
            slash_obs.actual_beneficiary_hash = first_payment_hash(
                base.outputs, expected_beneficiary_blake2_224,
            )
        return slash_obs


def kupo_shape_error(matches: list, expected_beneficiary_blake2_224: bytes) -> bool:
    """Whether an output paying the expected beneficiary carries a
    ``value`` the parent's amount sum cannot read as an int lovelace
    total (sec-review round-1 Vuln 2): a stringly-typed or boolean
    ``coins``, no coin field, or a ``value`` that is not a dict."""
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
            addr_hash = extract_payment_hash_from_cardano_address(address)
        except ValueError:
            continue
        if addr_hash != expected_beneficiary_blake2_224:
            continue
        value = m.get("value") or {}
        if not isinstance(value, dict):
            return True
        coins = value.get("coins")
        if coins is None:
            # Some Kupo builds use `lovelace`; mirror the parent's fallback.
            coins = value.get("lovelace")
        if coins is None or not isinstance(coins, int) or isinstance(coins, bool):
            return True
    return False


def first_payment_hash(matches: list, expected_beneficiary_blake2_224: bytes) -> Optional[bytes]:
    """The payment-key hash of the first output that does NOT pay the
    expected beneficiary, or None when no such output decodes.

    The beneficiary is filtered out first (sec-review round-1 Vuln 2):
    otherwise a shape error could surface the keeper's change output as
    the "actual" payee and slash an honest requester for
    ``WrongBeneficiary(change_hash)``. Outputs are taken in
    ``output_index`` order, then by address, so every watcher derives the
    same hash and the M-of-N envelope converges.
    """
    from daemon.cardano_address import (
        extract_payment_hash_from_cardano_address,
    )

    def _sort_key(entry: Any) -> tuple:
        if not isinstance(entry, dict):
            return (2**31, "")
        oi = entry.get("output_index")
        if not isinstance(oi, int):
            oi = 2**31 - 1
        addr = entry.get("address") if isinstance(entry.get("address"), str) else ""
        return (oi, addr)

    for m in sorted(matches, key=_sort_key):
        if not isinstance(m, dict):
            continue
        address = m.get("address")
        if not isinstance(address, str):
            continue
        try:
            candidate_hash = extract_payment_hash_from_cardano_address(address)
        except ValueError:
            # Script, enterprise and other non type-0/6 addresses.
            continue
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
         runtime-selected preimage, signs blake2_256(preimage) with the
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
        # Pending request (as a tuple) -> Cardano tip height of its tx's
        # first proven absence.
        self._absences: dict[tuple, int] = {}
        # Last logged runtime-context error and Kupo coverage gap, so each
        # change is logged once.
        self._context_error: Optional[str] = None
        self._coverage_reported: Optional[str] = "unreported"

    async def _runtime_context(self) -> Optional[dict]:
        try:
            context = checked_runtime_context(await asyncio.to_thread(self.client.get_settlement_runtime_context))
        except Exception as error:
            if self._context_error != type(error).__name__:
                self._context_error = type(error).__name__
                logger.error("slash_watcher: settlement runtime unavailable (%s); refusing to sign or submit",
                             type(error).__name__)
            return None
        self._context_error = None
        return context

    async def _report_status(self) -> None:
        """Reads the runtime context, which the client announces once per
        runtime, and whether Kupo can prove fraud, logged when it changes and
        exported as ``slash_kupo_coverage_ok``. Runs every poll, pending
        requests or not."""
        await self._runtime_context()
        try:
            gap = await self.observer.kupo_coverage_gap()
        except Exception as error:  # noqa: BLE001
            gap = f"probe_failed:{type(error).__name__}"
        health_server.update_metrics(slash_kupo_coverage_ok=int(gap is None))
        if gap == self._coverage_reported:
            return
        self._coverage_reported = gap
        if gap is None:
            logger.info("slash_watcher: Kupo at %s indexes every address and keeps spent outputs; it can prove fraud",
                        self.observer.kupo_url)
        else:
            logger.warning("slash_watcher: Kupo at %s cannot prove fraud (%s); no slash will be signed",
                           self.observer.kupo_url, gap)

    async def _request_context(self, request: PendingBondedRequest, verdict: SlashVerdict) -> Optional[dict]:
        context = await self._runtime_context()
        if context is None:
            verdict.outcome = ClassifierOutcome.DEFERRED
            verdict.detail = "runtime_unavailable"
            return None
        if request_expired(request, context):
            verdict.outcome = ClassifierOutcome.DEFERRED
            verdict.detail = "request_expired"
            logger.info("slash_watcher: request_expired claim_id=%s", request.claim_id.hex())
            return None
        return context

    async def _confirm_absence(self, request: PendingBondedRequest) -> tuple[bool, str]:
        """Whether the tx of ``request`` was proven absent twice, at Cardano
        tip heights at least TX_ABSENCE_CONFIRM_BLOCKS apart. A tx seen in
        between restarts the count; a replacement request starts its own."""
        key = astuple(request)
        height, reason = await self.observer.prove_absence(request.cardano_tx_hash.hex())
        if height is None:
            if reason == "tx_found":
                self._absences.pop(key, None)
            return False, reason
        first = self._absences.setdefault(key, height)
        if height - first < TX_ABSENCE_CONFIRM_BLOCKS:
            return False, f"absence_unconfirmed:first_height={first},height={height}"
        return True, f"absent_from_height={first}_to_{height}"

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

        if await self._request_context(request, verdict) is None:
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

        # Only `kupo_no_matches_for_tx` (a successful, empty lookup) can lead
        # to a proof; any other tag is an incomplete observation.
        if (
            observation.mismatches
            and "kupo_no_matches_for_tx" not in observation.mismatches
        ):
            logger.info(
                f"slash_watcher: observation incomplete for claim "
                f"{request.claim_id.hex()[:16]}... — "
                f"mismatches={observation.mismatches}. Will retry next tick."
            )
            verdict.outcome = ClassifierOutcome.OBSERVER_UNAVAILABLE
            verdict.detail = ", ".join(observation.mismatches)
            return verdict

        # Everything below compares the request with the network Ogmios
        # follows; an observer on another network proves nothing.
        if (
            observation.mainchain_genesis_hash is not None
            and observation.mainchain_genesis_hash != request.mainchain_genesis_hash
        ):
            verdict.outcome = ClassifierOutcome.DEFERRED
            verdict.detail = "cardano_network_mismatch"
            logger.warning(
                "slash_watcher: observer follows another Cardano network than claim %s... requests; not slashing",
                request.claim_id.hex()[:16],
            )
            return verdict

        absence_proven = False
        if "kupo_no_matches_for_tx" in observation.mismatches:
            absence_proven, reason = await self._confirm_absence(request)
            if not absence_proven:
                verdict.outcome = ClassifierOutcome.TX_ABSENCE_UNPROVEN
                verdict.detail = reason
                logger.info(
                    "slash_watcher: tx of claim %s... not proven absent (%s); not slashing yet",
                    request.claim_id.hex()[:16], reason,
                )
                return verdict

        proof = classify_fraud(request, observation, absence_proven=absence_proven)
        if proof is None:
            # State (b) with honest evidence — no slash. The settle
            # attestor's STCA path handles the close.
            verdict.outcome = ClassifierOutcome.NOT_FRAUD
            verdict.detail = (
                f"observed_to_beneficiary={observation.matched_address_lovelace} "
                f"vs evidence={request.amount_lovelace}"
            )
            return verdict

        # A negative claim about the tx's outputs needs a Kupo that sees
        # every address and every spent output (prove_absence checked this
        # for TxNotFound).
        if not absence_proven:
            gap = await self.observer.kupo_coverage_gap()
            if gap is not None:
                verdict.outcome = ClassifierOutcome.KUPO_COVERAGE_GAP
                verdict.detail = f"kupo_coverage:{gap}"
                logger.warning(
                    "slash_watcher: %s for claim %s... is unprovable on this Kupo (%s); not slashing",
                    type(proof).__name__, request.claim_id.hex()[:16], gap,
                )
                return verdict

        # Tag the outcome by variant for journalctl grep.
        if isinstance(proof, TxNotFound):
            verdict.outcome = ClassifierOutcome.SLASH_TX_NOT_FOUND
            verdict.detail = reason
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

        context = await self._request_context(request, verdict)
        if context is None:
            return verdict
        preimage = (
            build_frau_preimage(live_chain_id, request.claim_id, proof)
            if context["protocol"] == "FRAU"
            else build_frv2_preimage(live_chain_id, request, proof)
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
        else:
            envelope = [(pubkey_bytes, sig_bytes)]

        async with self._chain_write_lock:
            current = await self._request_context(request, verdict)
            if current is None:
                return verdict
            if current["protocol"] != context["protocol"]:
                verdict.outcome = ClassifierOutcome.DEFERRED
                verdict.detail = "runtime_changed"
                logger.warning("slash_watcher: runtime_changed; discarding envelope and retrying next tick")
                return verdict
            try:
                ext_hash = await asyncio.to_thread(
                    self.client.submit_slash_bad_settlement_evidence,
                    request.claim_id, proof, envelope,
                )
            except Exception as error:
                logger.warning("slash_watcher: submit_slash raised: %s", type(error).__name__)
                verdict.detail = f"submit_raised: {type(error).__name__}"
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
            self._absences.clear()
            return

        context = await self._runtime_context()
        if context is None:
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
            if request_expired(req, context):
                continue
            bonded.append(req)

        live = {astuple(req) for req in bonded}
        self._absences = {key: height for key, height in self._absences.items() if key in live}
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
            await self._report_status()
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
        aggregator = MultisigAggregator(
            gateway_url=gateway_url, committee=substrate_client.get_committee_pubkeys,
        )
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
    "KUPO_TIP_LAG_MAX_SLOTS",
    "TX_ABSENCE_CONFIRM_BLOCKS",
    "SPENT_CANARIES",
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
