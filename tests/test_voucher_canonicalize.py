"""Tests for `daemon.voucher_canonicalize` (task #278).

The cert-daemon mirrors the pallet's voucher-digest computation
byte-for-byte. The two layers MUST stay in lockstep or every
``attest_settle`` signature is rejected silently. These tests pin:

  - The pinned cross-team parity vector (the 2026-05-15 demo claim).
  - The CBOR builder's exact 80-byte type-0 output.
  - The body-length invariant (264B for type-0).
  - The type-0 address splitter (length + header validation).
  - Rejection of non-type-0 (e.g. type-6 enterprise) addresses.
  - The substrate-client fallback path: when Vouchers[claim_id] exists
    but neither VoucherDigests nor a voucher_digest field do,
    ``get_voucher_digest`` recomputes from chain state.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from daemon.voucher_canonicalize import (
    AddressDecodeError,
    ChainIdentity,
    TAG_VCHR,
    build_type0_address_cbor,
    canonical_voucher_body_with_address,
    compute_voucher_digest_with_address,
    split_type0_address_bytes,
)


# ---------------------------------------------------------------------------
# Pinned parity vector from the 2026-05-15 demo at Materios block #194881.
# ---------------------------------------------------------------------------

PARITY_MATERIOS_CHAIN_ID = bytes.fromhex(
    "0e46e33f639a56cc8780fd871d9a15e16d99af248526f907cb560cb40849f7bf"
)
PARITY_NETWORK_MAGIC = 1  # preprod
PARITY_AEGIS_POLICY_SCRIPT_HASH = bytes.fromhex(
    "1a2408175658ed625046dacc7496144cd0c65eb114d13a21a44fb532"
)
PARITY_SETTLEMENT_VERSION = 1
PARITY_CLAIM_ID = bytes.fromhex(
    "f9ef6b1f0ae37c381cd8f66278ee8dbca1c7daf211ecb07dca39c8430826c671"
)
PARITY_POLICY_ID = bytes.fromhex(
    "0000000000000000000000000000000000000000000000000000000000000000"
)
PARITY_BENEFICIARY_RAW = bytes.fromhex(
    "01680a93dd4deb4873fc0aa31678eb02c57258717953ffc9a654b0af78"
    "997dc76f7d2f246567dfb99385704fb0fc94b4e72a5b40aaa76e24bd"
)
PARITY_AMOUNT_ADA = 2_000_000
PARITY_BFPR_DIGEST = bytes.fromhex(
    "61949c5b8b1fc1022e46eff85afee751d213e490d16919855414ead0113b23b0"
)
PARITY_ISSUED_BLOCK = 194766
PARITY_EXPIRY_SLOT = 200_000_000

PARITY_EXPECTED_DIGEST = bytes.fromhex(
    "5581f34496c1699cc96098474b0f1c73e2ffe4b79fabb9858d5bea05d810e69b"
)


@pytest.fixture
def parity_chain_identity() -> ChainIdentity:
    return ChainIdentity(
        materios_chain_id=PARITY_MATERIOS_CHAIN_ID,
        network_magic=PARITY_NETWORK_MAGIC,
        aegis_policy_script_hash=PARITY_AEGIS_POLICY_SCRIPT_HASH,
        settlement_version=PARITY_SETTLEMENT_VERSION,
    )


# ---------------------------------------------------------------------------
# Core parity check — the single most important assertion in this file.
# ---------------------------------------------------------------------------


def test_parity_vector_matches_pallet(
    parity_chain_identity: ChainIdentity,
) -> None:
    """The 2026-05-15 demo voucher digest MUST match what the pallet
    computed on chain at block #194881. If this fails, the Rust
    `voucher_canonicalize.rs` and this Python port have diverged."""
    digest = compute_voucher_digest_with_address(
        chain_identity=parity_chain_identity,
        claim_id=PARITY_CLAIM_ID,
        policy_id=PARITY_POLICY_ID,
        beneficiary_cardano_addr_raw=PARITY_BENEFICIARY_RAW,
        amount_ada=PARITY_AMOUNT_ADA,
        bfpr_digest=PARITY_BFPR_DIGEST,
        issued_block=PARITY_ISSUED_BLOCK,
        expiry_slot_cardano=PARITY_EXPIRY_SLOT,
    )
    assert digest == PARITY_EXPECTED_DIGEST, (
        f"voucher_digest drift: got 0x{digest.hex()} "
        f"expected 0x{PARITY_EXPECTED_DIGEST.hex()}"
    )


# ---------------------------------------------------------------------------
# CBOR builder pinned vectors + structure assertions.
# ---------------------------------------------------------------------------


def test_cbor_output_is_exactly_80_bytes() -> None:
    payment = bytes(range(28))
    stake = bytes(reversed(range(28)))
    cbor = build_type0_address_cbor(payment, stake)
    assert len(cbor) == 80


def test_cbor_outer_markers() -> None:
    payment = bytes(28)
    stake = bytes(28)
    cbor = build_type0_address_cbor(payment, stake)
    # outer Address constr-0 indef
    assert cbor[0:3] == b"\xd8\x79\x9f"
    # outer close
    assert cbor[79] == 0xff
    # bstr(28) prefix for payment
    assert cbor[6:8] == b"\x58\x1c"
    # bstr(28) prefix for stake
    assert cbor[46:48] == b"\x58\x1c"


def test_cbor_embeds_payment_and_stake_hashes() -> None:
    payment = bytes(range(0x10, 0x10 + 28))
    stake = bytes(range(0xA0, 0xA0 + 28))
    cbor = build_type0_address_cbor(payment, stake)
    assert cbor[8:36] == payment
    assert cbor[48:76] == stake


def test_cbor_distinct_inputs_yield_distinct_output() -> None:
    a = build_type0_address_cbor(bytes(28), bytes(28))
    b = build_type0_address_cbor(b"\xff" * 28, bytes(28))
    c = build_type0_address_cbor(bytes(28), b"\xff" * 28)
    assert a != b
    assert b != c
    assert a != c


def test_cbor_matches_pallet_pinned_vector() -> None:
    """The Aiken-side pinned vector for the property-test address — same
    bytes as the pallet's `address1_matches_pinned_hex` test."""
    payment = bytes.fromhex(
        "957887100ebe5f9b0f9f24968f021ef705b25c7aaa633258e288e0ae"
    )
    stake = bytes.fromhex(
        "1fe36222d4d45a1c70bfb94b65b3b8ce1adf2a94913d67c32212694c"
    )
    expected = bytes.fromhex(
        "d8799fd8799f581c957887100ebe5f9b0f9f24968f021ef705b25c7aaa6332"
        "58e288e0aeffd8799fd8799fd8799f581c1fe36222d4d45a1c70bfb94b65b3"
        "b8ce1adf2a94913d67c32212694cffffffff"
    )
    assert build_type0_address_cbor(payment, stake) == expected


def test_cbor_rejects_wrong_length_payment() -> None:
    with pytest.raises(ValueError, match="payment_hash"):
        build_type0_address_cbor(bytes(27), bytes(28))


def test_cbor_rejects_wrong_length_stake() -> None:
    with pytest.raises(ValueError, match="stake_hash"):
        build_type0_address_cbor(bytes(28), bytes(29))


# ---------------------------------------------------------------------------
# split_type0_address_bytes
# ---------------------------------------------------------------------------


def test_split_type0_roundtrip() -> None:
    payment, stake = split_type0_address_bytes(PARITY_BENEFICIARY_RAW)
    assert len(payment) == 28
    assert len(stake) == 28
    # 28 + 28 + header byte = 57
    assert PARITY_BENEFICIARY_RAW == bytes([0x01]) + payment + stake


def test_split_type0_rejects_wrong_length() -> None:
    with pytest.raises(AddressDecodeError, match="57 bytes"):
        split_type0_address_bytes(b"\x01" + bytes(50))


def test_split_type0_rejects_non_type0_header() -> None:
    # 0x60 is type-6 (enterprise: no stake credential) — common shape
    # that v1 vouchers MUST reject.
    raw = b"\x60" + bytes(56)
    with pytest.raises(AddressDecodeError, match="unsupported"):
        split_type0_address_bytes(raw)


def test_compute_voucher_digest_rejects_enterprise_address(
    parity_chain_identity: ChainIdentity,
) -> None:
    """Type-6 (enterprise, header 0x60) addresses must raise — they're
    unsupported in voucher v1 (matches pallet behavior)."""
    enterprise_raw = b"\x60" + bytes(56)
    with pytest.raises(AddressDecodeError):
        compute_voucher_digest_with_address(
            chain_identity=parity_chain_identity,
            claim_id=bytes(32),
            policy_id=bytes(32),
            beneficiary_cardano_addr_raw=enterprise_raw,
            amount_ada=0,
            bfpr_digest=bytes(32),
            issued_block=0,
            expiry_slot_cardano=0,
        )


# ---------------------------------------------------------------------------
# canonical body length invariant.
# ---------------------------------------------------------------------------


def test_voucher_body_has_expected_length(
    parity_chain_identity: ChainIdentity,
) -> None:
    cbor = build_type0_address_cbor(bytes(28), bytes(28))
    body = canonical_voucher_body_with_address(
        chain_identity=parity_chain_identity,
        claim_id=bytes(32),
        policy_id=bytes(32),
        beneficiary_address_cbor=cbor,
        amount_ada=0,
        bfpr_digest=bytes(32),
        issued_block=0,
        expiry_slot_cardano=0,
    )
    # 32 (chain_id) + 4 (network_magic) + 28 (script_hash) + 4 (sv)
    #   + 32 (claim_id) + 32 (policy_id) + 80 (beneficiary cbor)
    #   + 8 (amount) + 32 (bfpr) + 4 (issued_block) + 8 (expiry) = 264
    assert len(body) == 264


def test_voucher_body_field_layout(
    parity_chain_identity: ChainIdentity,
) -> None:
    """Spot-check that the body has the expected fields in the expected
    positions — guards against accidental reorder in
    `canonical_voucher_body_with_address`."""
    claim_id = bytes(range(0x20, 0x40))
    policy_id = bytes(range(0x40, 0x60))
    bfpr = bytes(range(0x80, 0xA0))
    cbor = build_type0_address_cbor(bytes(28), bytes(28))
    body = canonical_voucher_body_with_address(
        chain_identity=parity_chain_identity,
        claim_id=claim_id,
        policy_id=policy_id,
        beneficiary_address_cbor=cbor,
        amount_ada=0x0102030405060708,
        bfpr_digest=bfpr,
        issued_block=0x10203040,
        expiry_slot_cardano=0x1112131415161718,
    )
    # chain identity at [0:68]
    assert body[0:32] == PARITY_MATERIOS_CHAIN_ID
    assert body[32:36] == (1).to_bytes(4, "little")
    assert body[36:64] == PARITY_AEGIS_POLICY_SCRIPT_HASH
    assert body[64:68] == (1).to_bytes(4, "little")
    # claim_id, policy_id
    assert body[68:100] == claim_id
    assert body[100:132] == policy_id
    # cbor
    assert body[132:212] == cbor
    # amount, bfpr, issued, expiry — little-endian for the integer fields
    assert body[212:220] == (0x0102030405060708).to_bytes(8, "little")
    assert body[220:252] == bfpr
    assert body[252:256] == (0x10203040).to_bytes(4, "little")
    assert body[256:264] == (0x1112131415161718).to_bytes(8, "little")


# ---------------------------------------------------------------------------
# ChainIdentity validation.
# ---------------------------------------------------------------------------


def test_chain_identity_rejects_wrong_chain_id_length() -> None:
    with pytest.raises(ValueError, match="materios_chain_id"):
        ChainIdentity(
            materios_chain_id=bytes(31),
            network_magic=1,
            aegis_policy_script_hash=bytes(28),
            settlement_version=1,
        )


def test_chain_identity_rejects_wrong_script_hash_length() -> None:
    with pytest.raises(ValueError, match="aegis_policy_script_hash"):
        ChainIdentity(
            materios_chain_id=bytes(32),
            network_magic=1,
            aegis_policy_script_hash=bytes(27),
            settlement_version=1,
        )


def test_chain_identity_rejects_out_of_range_network_magic() -> None:
    with pytest.raises(ValueError, match="network_magic"):
        ChainIdentity(
            materios_chain_id=bytes(32),
            network_magic=2**32,
            aegis_policy_script_hash=bytes(28),
            settlement_version=1,
        )


def test_chain_identity_rejects_out_of_range_settlement_version() -> None:
    with pytest.raises(ValueError, match="settlement_version"):
        ChainIdentity(
            materios_chain_id=bytes(32),
            network_magic=1,
            aegis_policy_script_hash=bytes(28),
            settlement_version=-1,
        )


# ---------------------------------------------------------------------------
# compute_voucher_digest_with_address: argument validation.
# ---------------------------------------------------------------------------


def test_compute_rejects_wrong_claim_id_length(
    parity_chain_identity: ChainIdentity,
) -> None:
    with pytest.raises(ValueError, match="claim_id"):
        compute_voucher_digest_with_address(
            chain_identity=parity_chain_identity,
            claim_id=bytes(31),
            policy_id=bytes(32),
            beneficiary_cardano_addr_raw=PARITY_BENEFICIARY_RAW,
            amount_ada=0,
            bfpr_digest=bytes(32),
            issued_block=0,
            expiry_slot_cardano=0,
        )


def test_compute_rejects_negative_amount(
    parity_chain_identity: ChainIdentity,
) -> None:
    with pytest.raises(ValueError, match="amount_ada"):
        compute_voucher_digest_with_address(
            chain_identity=parity_chain_identity,
            claim_id=bytes(32),
            policy_id=bytes(32),
            beneficiary_cardano_addr_raw=PARITY_BENEFICIARY_RAW,
            amount_ada=-1,
            bfpr_digest=bytes(32),
            issued_block=0,
            expiry_slot_cardano=0,
        )


def test_tag_constant_value() -> None:
    """Guard against silent drift of the domain-separation tag."""
    assert TAG_VCHR == b"VCHR"
    assert len(TAG_VCHR) == 4


# ---------------------------------------------------------------------------
# SubstrateClient.get_voucher_digest — daemon path.
#
# Mocks the substrate-interface query layer so we don't need a live RPC.
# Verifies that when neither `VoucherDigests` nor a `voucher_digest`
# field exist (the current chain reality), the fallback derives the
# correct digest from chain state.
# ---------------------------------------------------------------------------


def _build_voucher_row() -> dict:
    """A chain-state Voucher row matching the parity vector. Mimics what
    substrate-interface decodes from storage. Field names match the
    pallet's `Voucher` struct (see pallets/.../types.rs)."""
    return {
        # The pallet exposes addresses as a "0x..." hex string for raw
        # bytes; substrate-interface may also return a list of ints. We
        # use the hex-string variant — the more common decoder output —
        # and verify the bytes-list variant in a separate test below.
        "claim_id": "0x" + PARITY_CLAIM_ID.hex(),
        "policy_id": "0x" + PARITY_POLICY_ID.hex(),
        "beneficiary_cardano_addr": "0x" + PARITY_BENEFICIARY_RAW.hex(),
        "amount_ada": PARITY_AMOUNT_ADA,
        "batch_fairness_proof_digest": "0x" + PARITY_BFPR_DIGEST.hex(),
        "issued_block": PARITY_ISSUED_BLOCK,
        "expiry_slot_cardano": PARITY_EXPIRY_SLOT,
        "committee_sigs": [],
    }


def _make_substrate_mock(
    voucher_row: dict | None,
    voucher_digests_present: bool = False,
    voucher_digest_field: bytes | None = None,
) -> MagicMock:
    """Construct a `SubstrateInterface` mock whose `query` and
    `get_constant` calls return chain-state for the parity vector.

    Args:
        voucher_row: dict the `Vouchers[claim_id]` query returns
            (None means no voucher).
        voucher_digests_present: True iff the `VoucherDigests` storage
            map is present (typically False — it doesn't exist today).
        voucher_digest_field: override the `voucher_digest` field on
            the voucher row (typically None — the field doesn't exist).
    """
    substrate = MagicMock()

    def query(module: str, storage_function: str, params: list | None = None):
        result = MagicMock()
        if storage_function == "VoucherDigests":
            result.value = (
                "0x" + bytes(32).hex() if voucher_digests_present else None
            )
            return result
        if storage_function == "Vouchers":
            if voucher_row is None:
                result.value = None
                return result
            row = dict(voucher_row)
            if voucher_digest_field is not None:
                row["voucher_digest"] = "0x" + voucher_digest_field.hex()
            result.value = row
            return result
        result.value = None
        return result

    def get_constant(module: str, name: str):
        c = MagicMock()
        if name == "MateriosChainId":
            # In real metadata this comes back as a 0x-hex string for H256.
            c.value = "0x" + PARITY_MATERIOS_CHAIN_ID.hex()
        elif name == "NetworkMagic":
            c.value = PARITY_NETWORK_MAGIC
        elif name == "AegisPolicyV1ScriptHash":
            c.value = "0x" + PARITY_AEGIS_POLICY_SCRIPT_HASH.hex()
        elif name == "SettlementVersion":
            c.value = PARITY_SETTLEMENT_VERSION
        else:
            return None
        return c

    substrate.query = MagicMock(side_effect=query)
    substrate.get_constant = MagicMock(side_effect=get_constant)
    return substrate


def _make_client_with_substrate(substrate_mock: MagicMock):
    """Build a SubstrateClient with a stubbed `.substrate` attribute.

    We avoid actually connecting — the client only needs `.substrate`
    populated for `get_voucher_digest` to work.
    """
    # Avoid touching network — patch Keypair so the constructor doesn't
    # error on signer_uri.
    with patch(
        "daemon.substrate_client.Keypair.create_from_uri",
        return_value=MagicMock(),
    ):
        from daemon.config import DaemonConfig  # local import: heavy
        from daemon.substrate_client import SubstrateClient

        config = MagicMock(spec=DaemonConfig)
        config.rpc_url = "ws://test"
        config.signer_uri = "//Alice"
        config.tx_max_retries = 1
        client = SubstrateClient(config)
    client.substrate = substrate_mock
    return client


def test_get_voucher_digest_derives_from_chain_state_when_nothing_stored() -> None:
    """The new fallback path: no VoucherDigests map, no voucher_digest
    field, but Vouchers[claim_id] exists. The client must recompute the
    digest via voucher_canonicalize and return the parity value."""
    substrate = _make_substrate_mock(
        voucher_row=_build_voucher_row(),
        voucher_digests_present=False,
        voucher_digest_field=None,
    )
    client = _make_client_with_substrate(substrate)
    digest = client.get_voucher_digest(PARITY_CLAIM_ID)
    assert digest == PARITY_EXPECTED_DIGEST


def test_get_voucher_digest_prefers_stored_voucher_digests_map() -> None:
    """If a future runtime adds VoucherDigests, that path wins over the
    derive-from-state fallback. Verifies forward-compat."""
    pinned = bytes(32)
    substrate = _make_substrate_mock(
        voucher_row=_build_voucher_row(),
        voucher_digests_present=True,
    )

    # Override the VoucherDigests path to return our pinned value.
    def query(module: str, storage_function: str, params: list | None = None):
        result = MagicMock()
        if storage_function == "VoucherDigests":
            result.value = "0x" + pinned.hex()
        elif storage_function == "Vouchers":
            result.value = _build_voucher_row()
        else:
            result.value = None
        return result

    substrate.query = MagicMock(side_effect=query)
    client = _make_client_with_substrate(substrate)
    digest = client.get_voucher_digest(PARITY_CLAIM_ID)
    assert digest == pinned


def test_get_voucher_digest_prefers_voucher_digest_field_over_derived() -> None:
    """If a future runtime adds a `voucher_digest` field on the Voucher
    row, that takes precedence over the derive-from-state fallback."""
    pinned = b"\x42" * 32

    def query(module: str, storage_function: str, params: list | None = None):
        result = MagicMock()
        if storage_function == "VoucherDigests":
            result.value = None
        elif storage_function == "Vouchers":
            row = dict(_build_voucher_row())
            row["voucher_digest"] = "0x" + pinned.hex()
            result.value = row
        else:
            result.value = None
        return result

    substrate = _make_substrate_mock(voucher_row=_build_voucher_row())
    substrate.query = MagicMock(side_effect=query)
    client = _make_client_with_substrate(substrate)
    digest = client.get_voucher_digest(PARITY_CLAIM_ID)
    assert digest == pinned


def test_get_voucher_digest_returns_none_when_no_voucher() -> None:
    """Missing voucher row → None (caller treats this as "drop request")."""
    substrate = _make_substrate_mock(voucher_row=None)
    client = _make_client_with_substrate(substrate)
    assert client.get_voucher_digest(PARITY_CLAIM_ID) is None


def test_get_voucher_digest_accepts_bytes_list_decoded_addr() -> None:
    """substrate-interface sometimes decodes BoundedVec<u8, ..> as a list
    of ints rather than a 0x-hex string. The daemon must accept both."""
    row = _build_voucher_row()
    row["beneficiary_cardano_addr"] = list(PARITY_BENEFICIARY_RAW)
    substrate = _make_substrate_mock(voucher_row=row)
    # We can't reuse the helper because we need to override query —
    # build directly.
    def query(module: str, storage_function: str, params: list | None = None):
        result = MagicMock()
        if storage_function == "VoucherDigests":
            result.value = None
        elif storage_function == "Vouchers":
            result.value = row
        else:
            result.value = None
        return result

    substrate.query = MagicMock(side_effect=query)
    client = _make_client_with_substrate(substrate)
    digest = client.get_voucher_digest(PARITY_CLAIM_ID)
    assert digest == PARITY_EXPECTED_DIGEST


def test_get_voucher_digest_returns_none_on_missing_chain_identity() -> None:
    """If the runtime doesn't expose chain-identity constants, the
    derive-from-state fallback can't run — return None and let the
    caller drop the request (don't fabricate a wrong digest)."""
    substrate = _make_substrate_mock(voucher_row=_build_voucher_row())
    substrate.get_constant = MagicMock(return_value=None)
    client = _make_client_with_substrate(substrate)
    assert client.get_voucher_digest(PARITY_CLAIM_ID) is None
