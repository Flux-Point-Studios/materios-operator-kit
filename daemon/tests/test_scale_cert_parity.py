"""Byte-parity fixture tests for spec-219 SCALE-canonical Cert encoding.

These vectors are embedded VERBATIM from the spec-219 design doc §6 and
are also embedded byte-for-byte in `pallets/orinq-receipts/src/tests.rs`
on the materios runtime side. The Python `scale_cert_encode` and the
Rust `<Cert as Encode>::encode()` MUST produce identical bytes for every
vector — drift = CI red on both repos before deploy, drift in production
= `CertHashMismatch` + `BadAttestStrike` + auto-slash.

Vector design:
  - V1: all-zeros smoke test — minimum input, catches off-by-one and
        endian errors at the layout level.
  - V2: preprod v6 live chain_id + realistic receipt fields. This is
        the happy path under correctly-configured daemons.
  - V3: STALE v5 chain_id, otherwise identical to V2. This is the exact
        pattern of MacBook's 15-day silent failure and faucet-attestor's
        confirmed-stale config. The invariant `V3.hash != V2.hash`
        asserts the bug class is now detectable at the byte level.

If a hash here disagrees with the design doc, STOP — diff first, don't
silently overwrite. The Plan agent computed these manually and arithmetic
error is possible.
"""
import hashlib

from daemon.cert_builder import scale_cert_encode, scale_cert_hash


# ─── Vector 1 — all-zeros (smoke / minimum) ──────────────────────────────
V1_INPUTS = dict(
    chain_genesis="00" * 32,
    receipt_id="00" * 32,
    content_hash="00" * 32,
    base_root_sha256="00" * 32,
    storage_locator_hash="00" * 32,
)
V1_EXPECTED_BYTES_HEX = (
    "6d61746572696f732d617661696c6162696c6974792d636572742d7631000000"
    "0000000000000000000000000000000000000000000000000000000000000000"
    "0000000000000000000000000000000000000000000000000000000000000000"
    "0000000000000000000000000000000000000000000000000000000000000000"
    "0000000000000000000000000000000000000000000000000000000000000000"
    "0000000000000000000000000000000000000000000000000000000000000000"
    "000000006d0100000201"
)
V1_EXPECTED_HASH_HEX = (
    "667f01e11cb9a7502765ce51e92568322b292270cbcb4fa9be6fcb5363bc8d69"
)


# ─── Vector 2 — preprod v6 live chain_id + realistic receipt ─────────────
V2_INPUTS = dict(
    chain_genesis="0e46e33f639a56cc8780fd871d9a15e16d99af248526f907cb560cb40849f7bf",
    receipt_id="aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899",
    content_hash="0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    base_root_sha256="fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210",
    storage_locator_hash="1111111111111111222222222222222233333333333333334444444444444444",
)
V2_EXPECTED_BYTES_HEX = (
    "6d61746572696f732d617661696c6162696c6974792d636572742d7631000000"
    "0e46e33f639a56cc8780fd871d9a15e16d99af248526f907cb560cb40849f7bf"
    "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
    "1111111111111111222222222222222233333333333333334444444444444444"
    "000000006d0100000201"
)
V2_EXPECTED_HASH_HEX = (
    "ba93d1287e96983e68edcf10c9b07ada515168c3e980609980c8d5a4ac48d667"
)


# ─── Vector 3 — STALE v5 chain_id (the bug class made testable) ──────────
# Identical to V2 except chain_id. This is the bug class spec-219 closes:
# a stale daemon proposes V3.hash, the runtime computes V2.hash from
# on-chain state, rejection is automatic and attributable.
V3_INPUTS = dict(
    chain_genesis="bc0531cb311281565036fb397a376f0e0fa37005589655f97a7924b2729a164c",
    receipt_id="aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899",
    content_hash="0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    base_root_sha256="fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210",
    storage_locator_hash="1111111111111111222222222222222233333333333333334444444444444444",
)
V3_EXPECTED_BYTES_HEX = (
    "6d61746572696f732d617661696c6162696c6974792d636572742d7631000000"
    "bc0531cb311281565036fb397a376f0e0fa37005589655f97a7924b2729a164c"
    "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
    "1111111111111111222222222222222233333333333333334444444444444444"
    "000000006d0100000201"
)
V3_EXPECTED_HASH_HEX = (
    "9fa7d1c2cbcb77079668a1bc828d16ab84c41c3939a907d6e93fb21090dddd47"
)


def test_v1_all_zeros_byte_parity():
    """V1: every input is 32 zero-bytes. Verifies the layout is exactly
    32+32+32+32+32+32 + 4+4+1+1 = 202 bytes and the pinned trailing
    constants encode as `00 00 00 00 6d 01 00 00 02 01`."""
    encoded = scale_cert_encode(**V1_INPUTS)
    assert len(encoded) == 202, (
        f"V1: expected 202 bytes, got {len(encoded)}"
    )
    assert encoded.hex() == V1_EXPECTED_BYTES_HEX, (
        f"V1 encoded bytes drift:\n"
        f"  got: {encoded.hex()}\n"
        f"  exp: {V1_EXPECTED_BYTES_HEX}"
    )
    h = scale_cert_hash(**V1_INPUTS).hex()
    assert h == V1_EXPECTED_HASH_HEX, (
        f"V1 hash drift — runtime parity broken:\n"
        f"  got: 0x{h}\n"
        f"  exp: 0x{V1_EXPECTED_HASH_HEX}"
    )
    # Double-check: hash chains through hashlib.sha256
    assert hashlib.sha256(encoded).hexdigest() == V1_EXPECTED_HASH_HEX


def test_v2_preprod_v6_byte_parity():
    """V2: realistic receipt on the live preprod v6 chain. This is the
    happy-path canonical hash for any current internal-committee
    daemon. The Rust pallet computes the same bytes from on-chain state
    when this receipt's record is in storage with these field values."""
    encoded = scale_cert_encode(**V2_INPUTS)
    assert len(encoded) == 202
    assert encoded.hex() == V2_EXPECTED_BYTES_HEX, (
        f"V2 encoded bytes drift:\n"
        f"  got: {encoded.hex()}\n"
        f"  exp: {V2_EXPECTED_BYTES_HEX}"
    )
    h = scale_cert_hash(**V2_INPUTS).hex()
    assert h == V2_EXPECTED_HASH_HEX, (
        f"V2 hash drift — runtime parity broken:\n"
        f"  got: 0x{h}\n"
        f"  exp: 0x{V2_EXPECTED_HASH_HEX}"
    )


def test_v3_stale_v5_chain_id_byte_parity():
    """V3: identical receipt fields as V2 but a STALE v5 chain_id. This
    is the exact failure pattern of MacBook + faucet-attestor. The
    runtime, computing from on-chain state which has the v6 chain_id,
    will produce V2.hash. The stale daemon proposing V3.hash will be
    rejected with `CertHashMismatch` and earn a `BadAttestStrike`."""
    encoded = scale_cert_encode(**V3_INPUTS)
    assert len(encoded) == 202
    assert encoded.hex() == V3_EXPECTED_BYTES_HEX, (
        f"V3 encoded bytes drift:\n"
        f"  got: {encoded.hex()}\n"
        f"  exp: {V3_EXPECTED_BYTES_HEX}"
    )
    h = scale_cert_hash(**V3_INPUTS).hex()
    assert h == V3_EXPECTED_HASH_HEX, (
        f"V3 hash drift — runtime parity broken:\n"
        f"  got: 0x{h}\n"
        f"  exp: 0x{V3_EXPECTED_HASH_HEX}"
    )


def test_v3_hash_differs_from_v2_hash():
    """INVARIANT: V3.hash != V2.hash.

    This asserts the bug class is now detectable at the byte level.
    Before spec-219, the Python builder also encoded chain_id and would
    likewise produce a different hash — but no runtime could verify it,
    so a stale daemon racing first won the cert_hash slot. Now the
    runtime computes V2.hash from on-chain state, V3.hash is rejected
    on the spot, and the stale daemon takes a `BadAttestStrike`.

    If this ever asserts that V3.hash == V2.hash, the SCALE encoding
    has lost chain_id sensitivity and the bug class is back."""
    h2 = scale_cert_hash(**V2_INPUTS)
    h3 = scale_cert_hash(**V3_INPUTS)
    assert h2 != h3, (
        "V3.hash == V2.hash — SCALE encoding lost chain_id sensitivity. "
        "Stale-chain_id bug class is no longer detectable. STOP."
    )
