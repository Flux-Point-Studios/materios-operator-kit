"""Cert-daemon module that attests Cardano-tx confirmation for the
`pallet-intent-settlement::attest_settle` extrinsic (task #266).

Why this exists
---------------
Before task #266 the settle_claim path on pallet-intent-settlement signed a
purely opaque payload — committee members vouched for a 32-byte
`cardano_tx_hash` without any cryptographic commitment to a Cardano-chain
fact. A colluding M-of-N could close a Materios claim against a fake
hash and drain pool capital with no on-chain trace (audit P0,
``settle-claim-l1-verification-design.md`` §1.2 attack scenarios A1-A5).

This module closes that gap. It polls Materios for pending
``ClaimSettlementRequests`` (the new Phase-1 storage introduced by the
sister PR landing in materios-intent-settlement), independently observes
each requested Cardano tx via Ogmios+Kupo, verifies that the eight facts
in the request match what the cert-daemon's own Cardano follower sees,
and signs the canonical ``STCA`` digest with the existing cert-daemon
sr25519 committee key. Each attestor's sig is now a falsifiable claim
about the Cardano chain — see design memo §2.4 / Appendix A.

The eight facts every attestor verifies before signing
------------------------------------------------------
1. ``cardano_tx_hash`` exists on Cardano (Kupo finds at least one match)
2. ``observed_at_depth >= MinFinalityDepth`` Cardano blocks
3. ``observed_slot`` matches the slot Kupo reports for the tx
4. The tx pays ``amount_lovelace`` lovelace to an address whose
   **28-byte payment-key hash** (extracted from CIP-0019 type-0 bytes
   ``[1..29]`` — i.e., raw key hash, NOT a blake2_224 of anything)
   matches ``beneficiary_addr_hash``. The pre-#272 implementation
   computed ``blake2_224(bech32_string_bytes)`` and was wrong — see
   ``daemon/cardano_address.py``.
5. The Cardano network's genesis hash matches ``mainchain_genesis_hash``
   (preprod vs mainnet domain separation)
6. The requester-supplied evidence agrees with on-chain
   ``Vouchers::<T>::get(claim_id)`` for ``amount_lovelace`` and
   ``beneficiary_addr_hash`` (which are derived from the voucher)
7. The Materios ``chain_id`` is the LIVE genesis hash (not env)
8. The chain-state-derived ``voucher_digest`` (from ``Vouchers[claim_id]``)
   binds the attestation to this specific voucher (memo §3.2 — closes
   attack A5: voucher recycling)

Mapping to design memo
----------------------
- Mechanism: ``Hybrid B + D`` (memo §2.3).
- Sig payload: ``STCA`` (memo §3.2). Exact 213-byte preimage
  (4B tag + 209B content) built by ``build_stca_preimage`` below.
  Hashed with blake2_256 and signed sr25519 (matches
  ``CommitteePubkey``/``CommitteeSig`` types reused from
  ``ensure_threshold_signatures`` per memo §3.7).
- Extrinsic: ``IntentSettlement::attest_settle(claim_id, signatures)``.
- Rate-limit: 8 concurrent submissions (memo §6 OQ#10, matches
  spec-207 batching ceiling).
- Refusal logic: if any of the eight facts disagree, REFUSE to sign and
  log the specific mismatch (memo §2.4 — "bound to a verifiable fact").
- Reuse: same sr25519 committee key as availability_cert +
  TEE-evidence paths (memo §0 compounding-leverage statement).
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
from dataclasses import dataclass, field
from typing import Any, Optional

import aiohttp

from daemon.cardano_address import (
    extract_payment_hash_from_cardano_address,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# STCA domain tag + preimage builder.
#
# PINNED to pallet-intent-settlement::settle_claim_attested_payload (memo §3.2).
# Byte order:
#     b"STCA" (4B) || chain_id (32B) || claim_id (32B)
#         || voucher_digest (32B) || cardano_tx_hash (32B)
#         || settled_direct (1B) || beneficiary_addr_blake2_224 (28B)
#         || amount_ada_lovelace_le (8B) || observed_at_depth_le (4B)
#         || observed_slot_le (8B) || mainchain_genesis_hash (32B)
# = 4 + 32 + 32 + 32 + 32 + 1 + 28 + 8 + 4 + 8 + 32 = 213 bytes
#
# The memo quotes "209 bytes preimage" — that's the content portion
# without the 4-byte tag. Total bytes passed to blake2 = 213.
# ---------------------------------------------------------------------------
TAG_STCA: bytes = b"STCA"
STCA_PREIMAGE_LEN: int = 213
STCA_CONTENT_LEN: int = 209  # for documentation/test alignment with memo


def build_stca_preimage(
    chain_id: bytes,
    claim_id: bytes,
    voucher_digest: bytes,
    cardano_tx_hash: bytes,
    settled_direct: bool,
    beneficiary_addr_blake2_224: bytes,
    amount_lovelace: int,
    observed_at_depth: int,
    observed_slot: int,
    mainchain_genesis_hash: bytes,
) -> bytes:
    """Build the exact byte-stream STCA preimage per memo §3.2.

    Field validation is exhaustive — every byte-length and integer-width
    assumption is enforced. A malformed pre-image silently producing a
    different hash than the pallet expects would be a CertHashMismatch-class
    bug (see memory ``feedback_sdk_runtime_arg_drift_recurrence.md``).

    Args:
        chain_id: 32-byte Materios genesis hash (live RPC value).
        claim_id: 32-byte ClaimId.
        voucher_digest: 32-byte digest of ``Vouchers::<T>::get(claim_id)``.
        cardano_tx_hash: 32-byte Cardano transaction hash.
        settled_direct: True if the claim was settled direct (no batch).
        beneficiary_addr_blake2_224: 28-byte blake2_224 of the voucher's
            beneficiary Cardano address.
        amount_lovelace: u64 lovelace amount paid on Cardano.
        observed_at_depth: u32 Cardano block depth of the tx (must be
            ``>= MinFinalityDepth``).
        observed_slot: u64 Cardano slot of the tx.
        mainchain_genesis_hash: 32-byte Cardano network genesis hash
            (pins preprod vs mainnet).

    Returns:
        The 213-byte preimage ready to be passed to blake2_256.

    Raises:
        ValueError: any byte-length or integer-range invariant violated.
    """
    if len(chain_id) != 32:
        raise ValueError(f"chain_id must be 32 bytes, got {len(chain_id)}")
    if len(claim_id) != 32:
        raise ValueError(f"claim_id must be 32 bytes, got {len(claim_id)}")
    if len(voucher_digest) != 32:
        raise ValueError(
            f"voucher_digest must be 32 bytes, got {len(voucher_digest)}"
        )
    if len(cardano_tx_hash) != 32:
        raise ValueError(
            f"cardano_tx_hash must be 32 bytes, got {len(cardano_tx_hash)}"
        )
    if len(beneficiary_addr_blake2_224) != 28:
        raise ValueError(
            f"beneficiary_addr_blake2_224 must be 28 bytes, got "
            f"{len(beneficiary_addr_blake2_224)}"
        )
    if len(mainchain_genesis_hash) != 32:
        raise ValueError(
            f"mainchain_genesis_hash must be 32 bytes, got "
            f"{len(mainchain_genesis_hash)}"
        )
    if not 0 <= amount_lovelace < 2**64:
        raise ValueError(f"amount_lovelace out of u64 range: {amount_lovelace}")
    if not 0 <= observed_at_depth < 2**32:
        raise ValueError(
            f"observed_at_depth out of u32 range: {observed_at_depth}"
        )
    if not 0 <= observed_slot < 2**64:
        raise ValueError(f"observed_slot out of u64 range: {observed_slot}")

    out = bytearray(STCA_PREIMAGE_LEN)
    pos = 0
    out[pos:pos + 4] = TAG_STCA
    pos += 4
    out[pos:pos + 32] = chain_id
    pos += 32
    out[pos:pos + 32] = claim_id
    pos += 32
    out[pos:pos + 32] = voucher_digest
    pos += 32
    out[pos:pos + 32] = cardano_tx_hash
    pos += 32
    out[pos] = 1 if settled_direct else 0
    pos += 1
    out[pos:pos + 28] = beneficiary_addr_blake2_224
    pos += 28
    out[pos:pos + 8] = amount_lovelace.to_bytes(8, "little")
    pos += 8
    out[pos:pos + 4] = observed_at_depth.to_bytes(4, "little")
    pos += 4
    out[pos:pos + 8] = observed_slot.to_bytes(8, "little")
    pos += 8
    out[pos:pos + 32] = mainchain_genesis_hash
    pos += 32
    if pos != STCA_PREIMAGE_LEN:
        # Static guard — every branch above adds a fixed width, so this
        # should be unreachable. Kept so silent drift triggers a loud
        # error instead of a wrong hash.
        raise AssertionError(
            f"STCA preimage builder wrote {pos} bytes, expected "
            f"{STCA_PREIMAGE_LEN}"
        )
    return bytes(out)


def compute_stca_digest(preimage: bytes) -> bytes:
    """Hash the STCA preimage with blake2_256 (same hasher as the pallet's
    ``domain_hash``). Returns 32 bytes.
    """
    if len(preimage) != STCA_PREIMAGE_LEN:
        raise ValueError(
            f"preimage must be {STCA_PREIMAGE_LEN} bytes, got {len(preimage)}"
        )
    return hashlib.blake2b(preimage, digest_size=32).digest()


# ---------------------------------------------------------------------------
# Cardano observation result + observer.
# ---------------------------------------------------------------------------


@dataclass
class CardanoTxObservation:
    """What the cert-daemon's Ogmios+Kupo follower observed for a Cardano tx.

    All eight fields below MUST be populated for a "verified" outcome.
    A None value in any field indicates the observation failed and the
    attestor MUST refuse to sign.

    `mismatches` is populated only when a successful observation
    nonetheless disagrees with the SettlementEvidence in the pending
    request — e.g. "Kupo says amount=4_000_000, evidence says
    5_000_000". Each mismatch string is human-readable and pinned to
    the specific fact that failed (the design memo's auditability
    property: each refusal is a falsifiable counterclaim).
    """
    tx_hash_hex: str
    cardano_tip_block_no: Optional[int] = None
    tx_block_no: Optional[int] = None
    observed_slot: Optional[int] = None
    matched_address_lovelace: Optional[int] = None
    beneficiary_addr_blake2_224: Optional[bytes] = None
    mainchain_genesis_hash: Optional[bytes] = None
    mismatches: list[str] = field(default_factory=list)

    @property
    def depth(self) -> Optional[int]:
        if self.cardano_tip_block_no is None or self.tx_block_no is None:
            return None
        return max(0, self.cardano_tip_block_no - self.tx_block_no)

    @property
    def ok(self) -> bool:
        return (
            not self.mismatches
            and self.cardano_tip_block_no is not None
            and self.tx_block_no is not None
            and self.observed_slot is not None
            and self.matched_address_lovelace is not None
            and self.beneficiary_addr_blake2_224 is not None
            and self.mainchain_genesis_hash is not None
        )


class CardanoTxObserver:
    """Queries Ogmios + Kupo to observe a Cardano tx by hash.

    Why two endpoints
    -----------------
    - **Kupo** is the tx-lookup-by-hash index. We use
      ``GET /matches/*?transaction_id=<hash>`` to enumerate outputs of
      the tx and discover its ``created_at.{slot_no, block_no}``.
    - **Ogmios** carries the network-level facts: current tip
      (for depth = tip_block_no - tx_block_no) and the genesis-hash
      query (for ``mainchain_genesis_hash`` verification).

    Both must be configured for the settle-attestation loop to run
    (soft-disable otherwise — same pattern as ``EvidenceSubmitter``).
    """

    def __init__(
        self,
        ogmios_url: str,
        kupo_url: str,
        timeout_seconds: float = 10.0,
    ):
        self.ogmios_url = ogmios_url.rstrip("/")
        self.kupo_url = kupo_url.rstrip("/")
        self._timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        # Cache genesis hash for the run — it's network-invariant and
        # we use it for every observation. Refreshed lazily if it ever
        # comes back None.
        self._cached_genesis: Optional[bytes] = None

    async def _ogmios_rpc(
        self, session: aiohttp.ClientSession, method: str, params: Optional[dict] = None
    ) -> Optional[dict]:
        """Send one JSON-RPC 2.0 request to Ogmios. Returns the ``result``
        dict on success, None on any failure (logged WARN)."""
        body = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params or {},
            "id": method,
        }
        try:
            async with session.post(
                self.ogmios_url,
                json=body,
                timeout=self._timeout,
            ) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    logger.warning(
                        f"settle_attestor: Ogmios {method} HTTP {resp.status}: "
                        f"{text[:200]}"
                    )
                    return None
                payload = await resp.json()
                if "error" in payload:
                    logger.warning(
                        f"settle_attestor: Ogmios {method} error: "
                        f"{payload.get('error')}"
                    )
                    return None
                return payload.get("result")
        except Exception as e:  # noqa: BLE001
            logger.warning(
                f"settle_attestor: Ogmios {method} raised "
                f"{type(e).__name__}: {e}"
            )
            return None

    async def get_tip_block_no(
        self, session: aiohttp.ClientSession
    ) -> Optional[int]:
        """Query Ogmios for the current ledger tip. Returns block number
        (height) or None on failure. We accept either the ``height``
        field that ledger-state queries return or the ``blockNo`` field
        some Ogmios versions emit."""
        result = await self._ogmios_rpc(session, "queryLedgerState/tip")
        if not isinstance(result, dict):
            return None
        # Ogmios 6.x: queryLedgerState/tip returns
        #   {"slot": N, "id": "<hash>"} for the chain tip.
        # The HEIGHT comes from queryNetwork/blockHeight (returns int
        # or "origin"). We query both and prefer blockHeight.
        height = await self._ogmios_rpc(session, "queryNetwork/blockHeight")
        if isinstance(height, int):
            return height
        if isinstance(height, dict):
            # Some versions wrap as {"height": N}.
            h = height.get("height")
            if isinstance(h, int):
                return h
        # Fall back to whatever the tip query returned (block number key
        # varies by version).
        for key in ("height", "block_no", "blockNo"):
            v = result.get(key)
            if isinstance(v, int):
                return v
        logger.warning(
            f"settle_attestor: Ogmios tip query returned no usable "
            f"height field: tip={result!r} blockHeight={height!r}"
        )
        return None

    async def get_genesis_hash(
        self, session: aiohttp.ClientSession
    ) -> Optional[bytes]:
        """Query Ogmios for the Cardano network genesis hash. Cached for
        the lifetime of the observer (genesis hash is invariant per
        network). Returns 32 raw bytes or None on failure."""
        if self._cached_genesis is not None:
            return self._cached_genesis
        # Ogmios 6.x exposes:
        #   queryNetwork/genesisConfiguration {"era": "shelley"} →
        #     {"hash": "<hex>", ...}
        result = await self._ogmios_rpc(
            session,
            "queryNetwork/genesisConfiguration",
            {"era": "shelley"},
        )
        if isinstance(result, dict):
            h = result.get("hash") or result.get("genesisHash")
            if isinstance(h, str):
                s = h[2:] if h.startswith("0x") else h
                try:
                    raw = bytes.fromhex(s)
                except ValueError:
                    raw = b""
                if len(raw) == 32:
                    self._cached_genesis = raw
                    return raw
        logger.warning(
            f"settle_attestor: Ogmios genesis-hash query returned "
            f"unusable result: {result!r}"
        )
        return None

    async def _kupo_get(
        self, session: aiohttp.ClientSession, path: str, params: dict
    ) -> Optional[list]:
        """GET against Kupo. Returns the JSON list on success, None on
        failure. Kupo returns plain JSON arrays for ``/matches``."""
        url = f"{self.kupo_url}{path}"
        try:
            async with session.get(
                url, params=params, timeout=self._timeout,
            ) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    logger.warning(
                        f"settle_attestor: Kupo {path} HTTP {resp.status}: "
                        f"{text[:200]}"
                    )
                    return None
                payload = await resp.json()
                if isinstance(payload, list):
                    return payload
                logger.warning(
                    f"settle_attestor: Kupo {path} returned non-list "
                    f"payload of type {type(payload).__name__}"
                )
                return None
        except Exception as e:  # noqa: BLE001
            logger.warning(
                f"settle_attestor: Kupo {path} raised "
                f"{type(e).__name__}: {e}"
            )
            return None

    async def observe(
        self,
        cardano_tx_hash_hex: str,
        expected_beneficiary_blake2_224: bytes,
    ) -> CardanoTxObservation:
        """Resolve every Cardano-side fact about ``cardano_tx_hash_hex``
        independently of what the request says.

        The caller compares each populated field against the
        SettlementEvidence in the pending request. Any disagreement
        appends a mismatch string to the observation and the attestor
        refuses to sign.

        We accept the beneficiary's expected blake2_224 only so we can
        sum lovelace across outputs that match it (the tx may have
        change outputs to the keeper's own address; we MUST count
        ONLY outputs that go to the voucher's beneficiary, otherwise
        attackers can game the amount-check by adding decoy outputs).
        """
        obs = CardanoTxObservation(tx_hash_hex=cardano_tx_hash_hex)
        if len(expected_beneficiary_blake2_224) != 28:
            obs.mismatches.append(
                "expected_beneficiary_blake2_224 not 28 bytes"
            )
            return obs

        async with aiohttp.ClientSession() as session:
            tip_block_no = await self.get_tip_block_no(session)
            if tip_block_no is None:
                obs.mismatches.append("ogmios_tip_unavailable")
                return obs
            obs.cardano_tip_block_no = tip_block_no

            genesis = await self.get_genesis_hash(session)
            if genesis is None:
                obs.mismatches.append("ogmios_genesis_hash_unavailable")
                return obs
            obs.mainchain_genesis_hash = genesis

            # Kupo: enumerate all matches for this tx hash. We use the
            # wildcard pattern ``*`` (all addresses) filtered by
            # ``transaction_id`` query — equivalent to "all outputs of
            # this tx, regardless of recipient." We need ALL of them
            # because some are the beneficiary's payout and some are
            # the keeper's change.
            matches = await self._kupo_get(
                session,
                "/matches/*",
                {"transaction_id": cardano_tx_hash_hex},
            )
            if not matches:
                obs.mismatches.append("kupo_no_matches_for_tx")
                return obs

            # Sum lovelace across outputs whose address blake2_224
            # matches ``expected_beneficiary_blake2_224``. Also pin
            # the slot/block_no from the first matched entry — all
            # outputs of one tx share the same created_at.
            total_to_beneficiary: int = 0
            slot_seen: Optional[int] = None
            block_seen: Optional[int] = None
            for m in matches:
                created_at = m.get("created_at") or {}
                if isinstance(created_at, dict):
                    s = created_at.get("slot_no")
                    if isinstance(s, int):
                        slot_seen = s
                    b = (
                        created_at.get("block_no")
                        or created_at.get("blockNo")
                        or created_at.get("height")
                    )
                    if isinstance(b, int):
                        block_seen = b
                address = m.get("address")
                if not isinstance(address, str):
                    continue
                # Extract the 28-byte payment-key hash from the Cardano
                # address. The pallet's `beneficiary_addr_hash` is the
                # 28-byte payment-key hash at `[1..29]` of the
                # CIP-0019 type-0 address bytes (see
                # `voucher_canonicalize::split_type0_address_bytes` in
                # materios-intent-settlement), NOT a blake2_224 of any
                # string. Pre-PR #272 this was computed as
                # `blake2_224(bech32_string_bytes)` and produced wrong
                # bytes; every attestation sig would have been rejected
                # silently in production.
                try:
                    addr_hash = extract_payment_hash_from_cardano_address(
                        address
                    )
                except ValueError:
                    # Address not a CIP-0019 type-0/type-6 base/enterprise
                    # address — treat as "did not pay our beneficiary"
                    # (an external script address or a type we don't
                    # support). Skip rather than refuse-the-tx; the
                    # outer loop's amount-summing logic handles
                    # zero-payment-to-beneficiary cases.
                    continue
                if addr_hash != expected_beneficiary_blake2_224:
                    continue
                value = m.get("value") or {}
                if isinstance(value, dict):
                    coins = value.get("coins") or value.get("lovelace") or 0
                    if isinstance(coins, int):
                        total_to_beneficiary += coins

            if slot_seen is None:
                obs.mismatches.append("kupo_match_missing_slot")
            else:
                obs.observed_slot = slot_seen

            if block_seen is None:
                obs.mismatches.append("kupo_match_missing_block_no")
            else:
                obs.tx_block_no = block_seen

            obs.beneficiary_addr_blake2_224 = expected_beneficiary_blake2_224
            obs.matched_address_lovelace = total_to_beneficiary

        return obs


def blake2_224_of_cardano_address(address: str) -> bytes:
    """DEPRECATED — kept only as a backwards-compatibility shim.

    This function previously computed
    ``hashlib.blake2b(address.encode("utf-8"), digest_size=28)``,
    which DID NOT MATCH the pallet's `voucher_canonicalize::
    split_type0_address_bytes` output (the pallet extracts the 28-byte
    payment-key hash from the raw CIP-0019 address bytes — no hashing).
    Pre-PR #272 every attestation sig would have been rejected silently
    in production.

    New callers MUST use
    `extract_payment_hash_from_cardano_address(address)` from
    `daemon.cardano_address`. This shim is preserved temporarily so the
    test suite's address-decoding import keeps resolving — the test
    body asserts the new semantic (payment-hash extraction). To be
    removed in a follow-up cleanup after one deploy soak.
    """
    return extract_payment_hash_from_cardano_address(address)


# ---------------------------------------------------------------------------
# Pending request envelope from the pallet.
# ---------------------------------------------------------------------------


@dataclass
class PendingSettlementRequest:
    """A row from ``ClaimSettlementRequests``. SCALE-decoded by
    ``SubstrateClient`` and handed to the dispatcher.

    Fields mirror ``SettlementRequestRecord<T>`` (memo §3.5) plus the
    chain-state ``voucher_digest`` pulled from ``Vouchers[claim_id]``
    by the SDK adapter. Cert-daemon does NOT trust the
    ``voucher_digest`` field from the request — it re-fetches it from
    chain state via ``SubstrateClient.get_voucher_digest`` so a
    colluding requester cannot lie about it.
    """
    claim_id: bytes              # 32B
    requester: str               # ss58 address
    submitted_block: int
    settled_direct: bool
    # SettlementEvidence
    cardano_tx_hash: bytes       # 32B
    observed_at_depth: int       # u32
    observed_slot: int           # u64
    beneficiary_addr_hash: bytes # 28B
    amount_lovelace: int         # u64
    mainchain_genesis_hash: bytes  # 32B
    # Chain-state-derived (fetched by SubstrateClient, NOT from the request)
    voucher_digest: bytes        # 32B


# ---------------------------------------------------------------------------
# Refusal-reason taxonomy.
#
# Each refusal is logged with one of these tags so an operator (and an
# external auditor) can rapidly grep journalctl for the specific safety
# property that fired. The strings are STABLE — keep them in sync with
# the memo's eight-fact list.
# ---------------------------------------------------------------------------
class RefusalReason:
    TX_NOT_FOUND = "tx_not_found"                 # fact 1
    FINALITY_BELOW_MIN = "finality_below_min"     # fact 2
    SLOT_MISMATCH = "slot_mismatch"               # fact 3
    AMOUNT_MISMATCH = "amount_mismatch"           # fact 4
    GENESIS_MISMATCH = "genesis_mismatch"         # fact 5
    VOUCHER_AMOUNT_MISMATCH = "voucher_amount_mismatch"  # fact 6a
    VOUCHER_ADDR_MISMATCH = "voucher_addr_mismatch"      # fact 6b
    VOUCHER_DIGEST_MISMATCH = "voucher_digest_mismatch"  # fact 8
    CHAIN_ID_UNAVAILABLE = "chain_id_unavailable"        # fact 7
    OBSERVER_UNAVAILABLE = "observer_unavailable"        # tooling, not safety
    DEPTH_OBSERVATION_UNAVAILABLE = "depth_observation_unavailable"


@dataclass
class AttestationVerdict:
    """Outcome of one attestation attempt against one pending request.

    Either ``signed=True`` (signature published) or ``signed=False``
    with ``refusal_reason`` set. A refusal is NOT an error — it's the
    central safety property of the design memo §2.4.
    """
    claim_id: bytes
    signed: bool
    refusal_reason: Optional[str] = None
    refusal_detail: Optional[str] = None
    extrinsic_hash: Optional[str] = None


# ---------------------------------------------------------------------------
# SettleClaimAttestor: glues the observer to a chain-write path.
# ---------------------------------------------------------------------------


class SettleClaimAttestor:
    """Top-level dispatcher for the cardano_tx_confirmed attestation type.

    Operational shape: a single background asyncio task started by
    ``CertDaemon.run()`` after substrate is connected. Each tick:
      1. Reads pending ``ClaimSettlementRequests`` from chain.
      2. For each row, fetches ``Vouchers[claim_id]`` digest from
         chain state (the chain-state-derived field per memo §3.2).
      3. Observes the requested Cardano tx via Ogmios+Kupo.
      4. Cross-checks the 8 facts; if any disagree, REFUSE.
      5. Otherwise builds the STCA preimage, signs blake2_256(preimage)
         with the existing cert-daemon sr25519 key, and submits
         ``IntentSettlement::attest_settle(claim_id, [(my_pubkey, sig)])``.

    Concurrency: bounded by ``max_concurrent`` (default 8, per memo §6
    OQ#10) via a Semaphore so a settle-storm can't fan out unbounded.
    Chain submission is serialized under the SHARED ``chain_write_lock``
    so nonce stays monotonic against the receipt-cert and
    TEE-evidence paths (memory ``feedback_polkadot_nonce_race_on_burst.md``).
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
    ):
        self.config = config
        self.client = substrate_client
        self._chain_write_lock = chain_write_lock
        self.observer = observer
        # ``MinFinalityDepth`` value comes from the pallet's runtime
        # config (memo §3.4). We read it via SubstrateClient at startup
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

    async def process_one(
        self, request: PendingSettlementRequest, live_chain_id: bytes
    ) -> AttestationVerdict:
        """Verify + sign one pending request.

        Returns an ``AttestationVerdict``. Refusals log the specific
        fact mismatch at WARNING (audit-trail). Successful sigs log
        at INFO.
        """
        async with self._sem:
            return await self._process_one_locked(request, live_chain_id)

    async def _process_one_locked(
        self, request: PendingSettlementRequest, live_chain_id: bytes
    ) -> AttestationVerdict:
        verdict = AttestationVerdict(claim_id=request.claim_id, signed=False)
        cardano_tx_hex = request.cardano_tx_hash.hex()

        # Fact 5: genesis pin. We can short-circuit before talking to
        # Cardano at all if the request was clearly built for a
        # different network.
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

        # Observe the Cardano tx. The observer hashes its own copy of
        # the beneficiary address (from the matched outputs) and
        # returns the sum of lovelace across outputs whose address
        # blake2_224 matches the expected beneficiary.
        obs = await self.observer.observe(
            cardano_tx_hex, request.beneficiary_addr_hash
        )

        if obs.mismatches and "kupo_no_matches_for_tx" in obs.mismatches:
            verdict.refusal_reason = RefusalReason.TX_NOT_FOUND
            verdict.refusal_detail = ", ".join(obs.mismatches)
            self._log_refusal(verdict)
            return verdict

        if obs.matched_address_lovelace is None or obs.observed_slot is None:
            verdict.refusal_reason = RefusalReason.OBSERVER_UNAVAILABLE
            verdict.refusal_detail = ", ".join(obs.mismatches) or "incomplete_observation"
            self._log_refusal(verdict)
            return verdict

        # Fact 4: amount paid to beneficiary matches.
        if obs.matched_address_lovelace != request.amount_lovelace:
            verdict.refusal_reason = RefusalReason.AMOUNT_MISMATCH
            verdict.refusal_detail = (
                f"observed_to_beneficiary={obs.matched_address_lovelace} "
                f"vs evidence={request.amount_lovelace}"
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

        # Fact 2: finality depth. We use the cert-daemon's OWN observed
        # depth, not the requester's claim — the requester's value is
        # public info pinned in storage but the attestor's sig commits
        # to its independently observed depth. Memo §3.2 makes
        # ``observed_at_depth`` the attestor's value.
        if obs.depth is None:
            verdict.refusal_reason = RefusalReason.DEPTH_OBSERVATION_UNAVAILABLE
            verdict.refusal_detail = (
                f"tip_block_no={obs.cardano_tip_block_no} "
                f"tx_block_no={obs.tx_block_no}"
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

        # Fact 8: voucher_digest pulled FROM CHAIN STATE — never from
        # the request. This is the recycling-attack guard (memo §3.2
        # and §2.4 row A5).
        try:
            on_chain_voucher_digest = await asyncio.to_thread(
                self.client.get_voucher_digest, request.claim_id
            )
        except Exception as e:  # noqa: BLE001
            verdict.refusal_reason = RefusalReason.VOUCHER_DIGEST_MISMATCH
            verdict.refusal_detail = (
                f"failed_to_fetch_voucher_digest_for_claim: "
                f"{type(e).__name__}: {e}"
            )
            self._log_refusal(verdict)
            return verdict
        if on_chain_voucher_digest is None:
            verdict.refusal_reason = RefusalReason.VOUCHER_DIGEST_MISMATCH
            verdict.refusal_detail = "no_voucher_for_claim_id"
            self._log_refusal(verdict)
            return verdict
        if on_chain_voucher_digest != request.voucher_digest:
            # Should be impossible if the dispatcher fed us the chain
            # value, but defense-in-depth: the value WE sign comes
            # from chain state, not from request.voucher_digest. If
            # they disagree refuse loudly because someone (a buggy
            # SDK?) tried to lie.
            verdict.refusal_reason = RefusalReason.VOUCHER_DIGEST_MISMATCH
            verdict.refusal_detail = (
                f"chain_state_voucher_digest="
                f"{on_chain_voucher_digest.hex()[:16]}... "
                f"vs request.voucher_digest={request.voucher_digest.hex()[:16]}..."
            )
            self._log_refusal(verdict)
            return verdict

        # Fact 6 (voucher consistency) — pallet enforces these on
        # request_settle today (memo §3.6 errors). Cert-daemon does
        # the parallel cross-check so it can produce a structured
        # refusal log when the pallet's check would also fire. We
        # query the voucher fields directly.
        try:
            voucher = await asyncio.to_thread(
                self.client.get_voucher, request.claim_id
            )
        except Exception as e:  # noqa: BLE001
            voucher = None
            logger.info(
                f"settle_attestor: voucher fetch raised for "
                f"{request.claim_id.hex()[:16]}...: {type(e).__name__}: "
                f"{e} — proceeding with chain-state voucher_digest as "
                f"the canonical binding"
            )
        if voucher is not None:
            v_amount = voucher.get("amount_lovelace")
            v_addr_hash = voucher.get("beneficiary_addr_blake2_224")
            if (
                isinstance(v_amount, int)
                and v_amount != request.amount_lovelace
            ):
                verdict.refusal_reason = RefusalReason.VOUCHER_AMOUNT_MISMATCH
                verdict.refusal_detail = (
                    f"voucher_amount_lovelace={v_amount} "
                    f"vs evidence_amount={request.amount_lovelace}"
                )
                self._log_refusal(verdict)
                return verdict
            if (
                isinstance(v_addr_hash, (bytes, bytearray))
                and bytes(v_addr_hash) != request.beneficiary_addr_hash
            ):
                verdict.refusal_reason = RefusalReason.VOUCHER_ADDR_MISMATCH
                verdict.refusal_detail = (
                    f"voucher_addr_hash={bytes(v_addr_hash).hex()[:16]}... "
                    f"vs evidence_addr_hash="
                    f"{request.beneficiary_addr_hash.hex()[:16]}..."
                )
                self._log_refusal(verdict)
                return verdict

        # All eight facts agree. Build the STCA preimage, sign, submit.
        # The preimage commits to OUR observed depth (obs.depth) — that
        # is the falsifiable claim per memo §3.2.
        preimage = build_stca_preimage(
            chain_id=live_chain_id,
            claim_id=request.claim_id,
            voucher_digest=on_chain_voucher_digest,
            cardano_tx_hash=request.cardano_tx_hash,
            settled_direct=request.settled_direct,
            beneficiary_addr_blake2_224=request.beneficiary_addr_hash,
            amount_lovelace=request.amount_lovelace,
            observed_at_depth=int(obs.depth),
            observed_slot=int(obs.observed_slot),
            mainchain_genesis_hash=request.mainchain_genesis_hash,
        )
        digest = compute_stca_digest(preimage)
        sig_bytes = self.client.keypair.sign(digest)
        pubkey_bytes = self.client.keypair.public_key

        # Submit on-chain. Hold the SHARED chain-write lock for the full
        # nonce + sign + submit triplet so we don't race with the
        # receipt-cert path or the evidence_submitter on the signer
        # nonce.
        async with self._chain_write_lock:
            try:
                ext_hash = await asyncio.to_thread(
                    self.client.submit_attest_settle,
                    request.claim_id,
                    pubkey_bytes,
                    sig_bytes,
                )
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    f"settle_attestor: submit_attest_settle raised for "
                    f"{request.claim_id.hex()[:16]}...: "
                    f"{type(e).__name__}: {e}"
                )
                verdict.refusal_reason = RefusalReason.OBSERVER_UNAVAILABLE
                verdict.refusal_detail = f"submit_raised: {type(e).__name__}"
                return verdict

        if not ext_hash:
            logger.warning(
                f"settle_attestor: submit_attest_settle returned empty hash "
                f"for {request.claim_id.hex()[:16]}... — retryable"
            )
            verdict.refusal_reason = RefusalReason.OBSERVER_UNAVAILABLE
            verdict.refusal_detail = "submit_no_hash"
            return verdict

        verdict.signed = True
        verdict.extrinsic_hash = ext_hash
        logger.info(
            f"settle_attestor: attest_settle OK for "
            f"claim_id={request.claim_id.hex()[:16]}... "
            f"cardano_tx={cardano_tx_hex[:16]}... "
            f"depth={obs.depth} ext_hash={ext_hash[:18]}..."
        )
        return verdict

    def _log_refusal(self, verdict: AttestationVerdict) -> None:
        """Log a refusal at WARNING with the fact tag + structured
        detail. This is the audit-trail surface — operators grep
        journalctl for ``settle_attestor: REFUSE`` to find any
        attestor that disagreed with a request."""
        logger.warning(
            f"settle_attestor: REFUSE attest_settle for "
            f"claim_id={verdict.claim_id.hex()[:16]}... "
            f"reason={verdict.refusal_reason} "
            f"detail={verdict.refusal_detail}"
        )

    async def _tick(self, live_chain_id: bytes) -> None:
        """Single poll tick — fetch pending requests, process each
        under the concurrency semaphore."""
        try:
            requests = await asyncio.to_thread(
                self.client.list_pending_settlement_requests
            )
        except Exception as e:  # noqa: BLE001
            logger.warning(
                f"settle_attestor: list_pending_settlement_requests raised "
                f"{type(e).__name__}: {e}"
            )
            return
        if not requests:
            return
        logger.info(
            f"settle_attestor: {len(requests)} pending settlement "
            f"request(s) — processing under sem cap "
            f"{self._sem._value}"  # type: ignore[attr-defined]
        )
        # SubstrateClient.list_pending_settlement_requests returns a list[dict]
        # (deliberately loose — see its docstring). Convert to the
        # PendingSettlementRequest dataclass here at the dispatcher boundary
        # before process_one consumes it via attribute access. A malformed row
        # (missing field) is logged and skipped rather than killing the batch.
        coros = []
        for r in requests:
            try:
                req = PendingSettlementRequest(**r)
            except TypeError as e:
                logger.warning(
                    f"settle_attestor: skipping malformed pending request row "
                    f"(claim_id={r.get('claim_id', b'').hex()[:16] if isinstance(r, dict) else '?'}...): "
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
                    "settle_attestor: substrate genesis not yet available "
                    "— skipping tick"
                )
            else:
                try:
                    chain_id = bytes.fromhex(
                        live_chain_id_hex.removeprefix("0x")
                    )
                except ValueError:
                    logger.warning(
                        f"settle_attestor: malformed live genesis "
                        f"{live_chain_id_hex!r} — skipping tick"
                    )
                    chain_id = b""
                if len(chain_id) == 32:
                    try:
                        await self._tick(chain_id)
                    except Exception as e:  # noqa: BLE001
                        logger.error(
                            f"settle_attestor: tick raised "
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
            f"settle_attestor: started "
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


def maybe_create_settle_claim_attestor(
    config: Any,
    substrate_client: Any,
    chain_write_lock: asyncio.Lock,
) -> Optional[SettleClaimAttestor]:
    """Construct a ``SettleClaimAttestor`` if its required deps are wired.

    Soft-disable contract: if ``OGMIOS_URL`` (already in DaemonConfig)
    OR ``KUPO_URL`` (new) is unset, return None. Older deploys without
    a Kupo follower stay on the receipt-cert + TEE-evidence paths
    only.

    Optional env:
        SETTLE_ATTESTOR_POLL_INTERVAL — seconds between polls (default 12).
        SETTLE_ATTESTOR_MAX_CONCURRENT — concurrency cap (default 8).
        SETTLE_ATTESTOR_MIN_FINALITY_DEPTH — fallback when the pallet
            constant cannot be read (default 15, matching memo §3.4).
    """
    ogmios_url = (getattr(config, "ogmios_url", "") or "").strip()
    kupo_url = (
        getattr(config, "kupo_url", "") or os.environ.get("KUPO_URL", "") or ""
    ).strip()
    if not ogmios_url or not kupo_url:
        logger.info(
            "settle_attestor: not configured "
            f"(ogmios_url={'set' if ogmios_url else 'missing'}, "
            f"kupo_url={'set' if kupo_url else 'missing'}) — "
            f"settle_claim attestation will not run on this node."
        )
        return None
    try:
        poll_interval = int(
            os.environ.get("SETTLE_ATTESTOR_POLL_INTERVAL", "12")
        )
    except ValueError:
        poll_interval = 12
    try:
        max_concurrent = int(
            os.environ.get(
                "SETTLE_ATTESTOR_MAX_CONCURRENT",
                str(SettleClaimAttestor.DEFAULT_MAX_CONCURRENT),
            )
        )
    except ValueError:
        max_concurrent = SettleClaimAttestor.DEFAULT_MAX_CONCURRENT
    try:
        fallback_min_finality = int(
            os.environ.get("SETTLE_ATTESTOR_MIN_FINALITY_DEPTH", "15")
        )
    except ValueError:
        fallback_min_finality = 15
    # Pull MinFinalityDepth from the pallet's runtime metadata if
    # available; fall back to env. The pallet exposes it as a
    # ``Get<u32>`` constant at ``IntentSettlement.MinFinalityDepth``.
    min_finality_depth = fallback_min_finality
    try:
        mfd = substrate_client.get_min_finality_depth()
        if isinstance(mfd, int) and mfd > 0:
            min_finality_depth = mfd
    except Exception as e:  # noqa: BLE001
        logger.info(
            f"settle_attestor: could not read MinFinalityDepth from "
            f"runtime ({type(e).__name__}); using fallback "
            f"{fallback_min_finality}"
        )
    observer = CardanoTxObserver(
        ogmios_url=ogmios_url,
        kupo_url=kupo_url,
    )
    return SettleClaimAttestor(
        config=config,
        substrate_client=substrate_client,
        chain_write_lock=chain_write_lock,
        observer=observer,
        min_finality_depth=min_finality_depth,
        poll_interval=poll_interval,
        max_concurrent=max_concurrent,
    )
