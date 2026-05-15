"""Python port of `pallet-intent-settlement::voucher_canonicalize` (task #278).

Mirrors the Rust source of truth at
``pallets/intent-settlement/src/voucher_canonicalize.rs`` BYTE-FOR-BYTE so
the cert-daemon can autonomously compute the same voucher digest the
pallet derives at ``compute_canonical_voucher_digest`` call time.

Why this module exists
----------------------
``daemon.substrate_client.get_voucher_digest`` originally tried two
chain-state paths:

  1. ``IntentSettlement::VoucherDigests[claim_id]`` — DOES NOT EXIST in
     the current runtime (the pallet computes the digest at
     ``attest_settle`` time, it never stores it).
  2. ``Vouchers[claim_id].voucher_digest`` — DOES NOT EXIST either (the
     ``Voucher`` struct in ``pallets/intent-settlement/src/types.rs``
     has ``batch_fairness_proof_digest``, which is a DIFFERENT,
     semantically-incompatible value).

Result: every pending ``ClaimSettlementRequests`` was dropped with
``"no matching voucher — dropping (chain invariant violation)"`` and
no attest_settle was ever submitted. Demo-time observation 2026-05-15
on block #194881.

This module is the fix: by porting ``compute_voucher_digest_with_address``
(and its CBOR helper ``build_type0_address_cbor``) to Python, the
cert-daemon can derive the canonical voucher digest directly from the
``Vouchers[claim_id]`` row + the four chain-identity constants.

Cross-team parity anchor
------------------------
For the 2026-05-15 demo claim at block #194881, the inputs are:

  - materios_chain_id = 0e46e33f639a56cc8780fd871d9a15e16d99af248526f907cb560cb40849f7bf
  - network_magic = 1 (preprod)
  - aegis_policy_script_hash = 1a2408175658ed625046dacc7496144cd0c65eb114d13a21a44fb532
  - settlement_version = 1
  - claim_id = f9ef6b1f0ae37c381cd8f66278ee8dbca1c7daf211ecb07dca39c8430826c671
  - policy_id = 0x00...00 (32B zero)
  - beneficiary_cardano_addr (57B raw) =
      01680a93dd4deb4873fc0aa31678eb02c57258717953ffc9a654b0af78
        997dc76f7d2f246567dfb99385704fb0fc94b4e72a5b40aaa76e24bd
  - amount_ada = 2_000_000
  - batch_fairness_proof_digest = 61949c5b8b1fc1022e46eff85afee751d213e490d16919855414ead0113b23b0
  - issued_block = 194766
  - expiry_slot_cardano = 200000000

  → voucher_digest = 0x5581f34496c1699cc96098474b0f1c73e2ffe4b79fabb9858d5bea05d810e69b

This vector is pinned by ``tests/test_voucher_canonicalize.py``. If
that test ever drifts, this module + the pallet have diverged and
attest_settle WILL silently reject all signatures.

Body byte layout (264 bytes for the typical type-0 case)
--------------------------------------------------------
  materios_chain_id          (32B)
  network_magic              (LE u32, 4B)
  aegis_policy_script_hash   (28B)
  settlement_version         (LE u32, 4B)
  claim_id                   (32B)
  policy_id                  (32B)
  beneficiary_address_cbor   (80B for type-0; variable for other shapes)
  amount_ada                 (LE u64, 8B)
  bfpr_digest                (32B)
  issued_block               (LE u32, 4B)
  expiry_slot_cardano        (LE u64, 8B)

Digest = blake2_256(b"VCHR" || body).

Plutus V3 Data CBOR for type-0 address
--------------------------------------
``builtin.serialise_data`` on the Aiken side emits indefinite-length
constr-0 markers (``0xd8 0x79 0x9f ... 0xff``), NOT the definite-length
shortcut. We hand-roll the 80-byte buffer to match Aiken's output
byte-for-byte (no Python CBOR crate; we are stdlib-only by design —
``no_std`` Python policy mirrors the pallet's ``no_std`` Rust).

See the pallet source's module-level doc-comment for the visual breakdown.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass


# Domain-separation tag matching `pallet-intent-settlement::types::TAG_VCHR`.
TAG_VCHR: bytes = b"VCHR"


# Fixed widths for type-0 (CIP-0019 base) addresses.
_PAYMENT_HASH_LEN: int = 28
_STAKE_HASH_LEN: int = 28
_RAW_TYPE0_ADDR_LEN: int = 57
_TYPE0_CBOR_LEN: int = 80


@dataclass(frozen=True)
class ChainIdentity:
    """The four chain-identity constants pinned into every voucher digest.

    Pulled from the pallet's runtime metadata at startup (see
    ``SubstrateClient.get_chain_identity``). Do NOT hardcode preprod values
    — the same daemon image must work on mainnet too.

    Fields mirror ``voucher_canonicalize::ChainIdentity`` in the pallet:

    Attributes:
        materios_chain_id: 32-byte Materios genesis hash (live RPC value,
            never an env-supplied stale value).
        network_magic: Cardano network magic — 1 for preprod, 764824073
            for mainnet, 2 for preview. Encoded LE u32 in the digest.
        aegis_policy_script_hash: 28-byte blake2b224 of the deployed
            ``aegis_policy_v1`` Aiken validator. MUST match the on-chain
            ``AegisPolicyParams.aegis_policy_v1_script_hash`` value.
        settlement_version: Settlement-protocol semver (u32). Bumped on
            any breaking pre-image change so pre/post-bump bundles
            domain-separate.
    """

    materios_chain_id: bytes
    network_magic: int
    aegis_policy_script_hash: bytes
    settlement_version: int

    def __post_init__(self) -> None:
        if len(self.materios_chain_id) != 32:
            raise ValueError(
                f"materios_chain_id must be 32 bytes, got "
                f"{len(self.materios_chain_id)}"
            )
        if not 0 <= self.network_magic < 2**32:
            raise ValueError(
                f"network_magic out of u32 range: {self.network_magic}"
            )
        if len(self.aegis_policy_script_hash) != 28:
            raise ValueError(
                f"aegis_policy_script_hash must be 28 bytes, got "
                f"{len(self.aegis_policy_script_hash)}"
            )
        if not 0 <= self.settlement_version < 2**32:
            raise ValueError(
                f"settlement_version out of u32 range: "
                f"{self.settlement_version}"
            )


def _blake2_256(data: bytes) -> bytes:
    """blake2b with 32-byte output — matches sp_core::hashing::blake2_256."""
    return hashlib.blake2b(data, digest_size=32).digest()


def build_type0_address_cbor(payment_hash: bytes, stake_hash: bytes) -> bytes:
    """Plutus V3 Data CBOR for a CIP-0019 type-0 base address.

    Exactly mirrors ``voucher_canonicalize::build_type0_address_cbor`` in
    the pallet, which itself mirrors what ``builtin.serialise_data`` emits
    on the Aiken side for an
    ``Address(VK(payment_hash), Some(Inline(VK(stake_hash))))``.

    Output is ALWAYS 80 bytes (5 nested constr-0 indef wrappers × 4B
    overhead each = 20B + 2× bstr(28) prefixes × 2B = 4B + 2× 28B
    hash data = 56B → total 80B exact).

    Layout::

        d8 79 9f                       -- constr-0 indef (Address)
          d8 79 9f                     --   VK(payment) constr-0 indef
            58 1c <28B payment hash>   --     bstr(28)
          ff
          d8 79 9f                     --   Some constr-0 indef
            d8 79 9f                   --     Inline constr-0 indef
              d8 79 9f                 --       VK(stake) constr-0 indef
                58 1c <28B stake hash> --         bstr(28)
              ff
            ff
          ff
        ff

    Args:
        payment_hash: 28-byte payment-key hash (CIP-0019 ``raw[1..29]``).
        stake_hash: 28-byte stake-key hash (CIP-0019 ``raw[29..57]``).

    Returns:
        Exactly 80 bytes of Plutus V3 Data CBOR.

    Raises:
        ValueError: either input is not exactly 28 bytes.
    """
    if len(payment_hash) != _PAYMENT_HASH_LEN:
        raise ValueError(
            f"payment_hash must be 28 bytes, got {len(payment_hash)}"
        )
    if len(stake_hash) != _STAKE_HASH_LEN:
        raise ValueError(
            f"stake_hash must be 28 bytes, got {len(stake_hash)}"
        )
    out = bytearray(_TYPE0_CBOR_LEN)
    # outer Address constr-0 indef
    out[0] = 0xd8
    out[1] = 0x79
    out[2] = 0x9f
    # payment credential: VK(payment_hash) constr-0 indef
    out[3] = 0xd8
    out[4] = 0x79
    out[5] = 0x9f
    out[6] = 0x58  # bytes, 1-byte length follows
    out[7] = 0x1c  # 28
    out[8:36] = payment_hash
    out[36] = 0xff  # close VK(payment)
    # stake credential: Some constr-0 indef
    out[37] = 0xd8
    out[38] = 0x79
    out[39] = 0x9f
    # Inline constr-0 indef
    out[40] = 0xd8
    out[41] = 0x79
    out[42] = 0x9f
    # VK(stake_hash) constr-0 indef
    out[43] = 0xd8
    out[44] = 0x79
    out[45] = 0x9f
    out[46] = 0x58
    out[47] = 0x1c
    out[48:76] = stake_hash
    out[76] = 0xff  # close VK(stake)
    out[77] = 0xff  # close Inline
    out[78] = 0xff  # close Some
    out[79] = 0xff  # close Address
    return bytes(out)


class AddressDecodeError(ValueError):
    """Raised when a raw Cardano address can't be split into a type-0 pair."""


def split_type0_address_bytes(raw: bytes) -> tuple[bytes, bytes]:
    """Split a 57-byte CIP-0019 type-0 address into ``(payment_hash, stake_hash)``.

    Matches ``voucher_canonicalize::split_type0_address_bytes`` in the
    pallet. The 57-byte bech32-decoded shape is
    ``0x01 || payment_hash(28) || stake_hash(28)`` — header byte 0x01
    means "payment VK + stake VK inline" (the only shape v1 supports).

    Args:
        raw: 57-byte address payload (bech32-decoded, no checksum,
            no HRP).

    Returns:
        ``(payment_hash, stake_hash)``, each exactly 28 bytes.

    Raises:
        AddressDecodeError: ``raw`` is not 57 bytes, OR header byte is
            not ``0x01`` (i.e., not a type-0 address — type-1 script,
            type-6 enterprise, etc. are NOT supported by v1 vouchers).
    """
    if len(raw) != _RAW_TYPE0_ADDR_LEN:
        raise AddressDecodeError(
            f"raw address must be 57 bytes, got {len(raw)}"
        )
    if raw[0] != 0x01:
        raise AddressDecodeError(
            f"unsupported address header 0x{raw[0]:02x} — only type-0 "
            f"(header 0x01) is valid in voucher v1"
        )
    return raw[1:29], raw[29:57]


def canonical_voucher_body_with_address(
    chain_identity: ChainIdentity,
    claim_id: bytes,
    policy_id: bytes,
    beneficiary_address_cbor: bytes,
    amount_ada: int,
    bfpr_digest: bytes,
    issued_block: int,
    expiry_slot_cardano: int,
) -> bytes:
    """Build the canonical voucher body (pre-digest).

    Exactly mirrors ``voucher_canonicalize::canonical_voucher_body_with_address``.
    Layout::

        chain_identity (32 + 4 + 28 + 4 = 68B)
          materios_chain_id           32B
          network_magic               LE u32 (4B)
          aegis_policy_script_hash    28B
          settlement_version          LE u32 (4B)
        claim_id                      32B
        policy_id                     32B
        beneficiary_address_cbor      80B for type-0 (variable for other shapes)
        amount_ada                    LE u64 (8B)
        bfpr_digest                   32B
        issued_block                  LE u32 (4B)
        expiry_slot_cardano           LE u64 (8B)

    Total = 264 bytes when ``beneficiary_address_cbor`` is the 80-byte
    type-0 form.

    Args:
        chain_identity: Four chain-identity constants (see
            :class:`ChainIdentity`).
        claim_id: 32-byte ClaimId.
        policy_id: 32-byte PolicyId.
        beneficiary_address_cbor: Pre-built Plutus V3 Data CBOR for the
            beneficiary address (80B for type-0; build via
            :func:`build_type0_address_cbor`).
        amount_ada: u64 lovelace amount.
        bfpr_digest: 32-byte Batch-Fairness-Proof Root digest.
        issued_block: u32 Materios block number at voucher mint.
        expiry_slot_cardano: u64 Cardano slot number beyond which the
            voucher cannot be settled.

    Returns:
        The concatenated body bytes ready to feed into
        :func:`compute_voucher_digest_with_address` (or hash directly with
        the ``VCHR`` tag).

    Raises:
        ValueError: any byte-length or integer-range invariant violated.
    """
    if len(claim_id) != 32:
        raise ValueError(f"claim_id must be 32 bytes, got {len(claim_id)}")
    if len(policy_id) != 32:
        raise ValueError(f"policy_id must be 32 bytes, got {len(policy_id)}")
    if len(bfpr_digest) != 32:
        raise ValueError(
            f"bfpr_digest must be 32 bytes, got {len(bfpr_digest)}"
        )
    if not 0 <= amount_ada < 2**64:
        raise ValueError(f"amount_ada out of u64 range: {amount_ada}")
    if not 0 <= issued_block < 2**32:
        raise ValueError(f"issued_block out of u32 range: {issued_block}")
    if not 0 <= expiry_slot_cardano < 2**64:
        raise ValueError(
            f"expiry_slot_cardano out of u64 range: {expiry_slot_cardano}"
        )
    # Note: beneficiary_address_cbor is NOT length-checked here. The
    # pallet's helper is shape-agnostic at this layer; the caller (i.e.,
    # `compute_voucher_digest_with_address`) is responsible for using the
    # correct CBOR shape for the address type. v1 supports type-0 only.
    body = bytearray()
    # Chain identity (68B).
    body += chain_identity.materios_chain_id
    body += chain_identity.network_magic.to_bytes(4, "little")
    body += chain_identity.aegis_policy_script_hash
    body += chain_identity.settlement_version.to_bytes(4, "little")
    # Voucher body proper.
    body += claim_id
    body += policy_id
    body += beneficiary_address_cbor
    body += amount_ada.to_bytes(8, "little")
    body += bfpr_digest
    body += issued_block.to_bytes(4, "little")
    body += expiry_slot_cardano.to_bytes(8, "little")
    return bytes(body)


def compute_voucher_digest_with_address(
    chain_identity: ChainIdentity,
    claim_id: bytes,
    policy_id: bytes,
    beneficiary_cardano_addr_raw: bytes,
    amount_ada: int,
    bfpr_digest: bytes,
    issued_block: int,
    expiry_slot_cardano: int,
) -> bytes:
    """Compute the canonical voucher digest.

    Mirrors ``voucher_canonicalize::compute_voucher_digest_with_address``
    BYTE-FOR-BYTE. The signed-bundle commits to this digest in the STCA
    pre-image (memo §3.2), so an off-by-one in any byte here silently
    breaks attest_settle for every pending claim.

    The function takes the RAW 57-byte beneficiary address (i.e.,
    ``voucher.beneficiary_cardano_addr`` straight off the chain state)
    rather than pre-split key hashes — this matches how the caller
    (``SubstrateClient.get_voucher_digest`` fallback) reads it off chain.
    Internally we split via :func:`split_type0_address_bytes` and CBOR-
    encode via :func:`build_type0_address_cbor`.

    Args:
        chain_identity: Four chain-identity constants.
        claim_id: 32-byte ClaimId.
        policy_id: 32-byte PolicyId.
        beneficiary_cardano_addr_raw: 57-byte CIP-0019 type-0 address
            (header 0x01 || payment_hash(28) || stake_hash(28)).
        amount_ada: u64 lovelace.
        bfpr_digest: 32-byte BFPR digest.
        issued_block: u32 issued-at block number.
        expiry_slot_cardano: u64 Cardano expiry slot.

    Returns:
        32-byte voucher digest = blake2_256(b"VCHR" || body).

    Raises:
        AddressDecodeError: address is not a valid type-0 form.
        ValueError: any other length/range invariant violated.
    """
    payment_hash, stake_hash = split_type0_address_bytes(
        beneficiary_cardano_addr_raw
    )
    cbor = build_type0_address_cbor(payment_hash, stake_hash)
    body = canonical_voucher_body_with_address(
        chain_identity=chain_identity,
        claim_id=claim_id,
        policy_id=policy_id,
        beneficiary_address_cbor=cbor,
        amount_ada=amount_ada,
        bfpr_digest=bfpr_digest,
        issued_block=issued_block,
        expiry_slot_cardano=expiry_slot_cardano,
    )
    return _blake2_256(TAG_VCHR + body)
