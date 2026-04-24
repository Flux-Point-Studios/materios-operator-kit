"""Determinism tests for `daemon.cert_builder.build_cert`.

Context: `attest_availability_cert` on the Materios pallet rejects any
attestation whose cert_hash differs from a previously-recorded one for
the same receipt (Error::CertHashMismatch). Historically cert_builder
pulled `attested_at_epoch` from wall-clock Ogmios and `attestation_level`
from the local blob-verification depth, which drift across attesters
and wedged the pipeline. These tests lock in the determinism contract:
for a given set of chain-derived inputs, every attester produces the
same bytes.
"""

from daemon.cert_builder import build_cert
from daemon.models import AttestationLevel


# A deterministic set of inputs used across tests. Values are illustrative;
# nothing here depends on a live chain.
CHAIN_ID = "0xbc0531cb311281565036fb397a376f0e0fa37005589655f97a7924b2729a164c"
RECEIPT_ID = "0x682a60d1fa4f8f7812607933c81512674b644107b1af1d9615e3c16d45a648c7"
CONTENT_HASH = bytes.fromhex("9bb9fbd8c151eec8fe017a318a2d35c23f2e39d96c45b59d0fed88bfd3f3a3f1")
BASE_ROOT = bytes.fromhex("9bb9fbd8c151eec8fe017a318a2d35c23f2e39d96c45b59d0fed88bfd3f3a3f1")
STORAGE_LOCATOR = bytes.fromhex("11" * 32)
RETENTION_DAYS = 365
CERT_SCHEMA_VERSION = "1.0"


def _build(epoch, level):
    return build_cert(
        chain_id=CHAIN_ID,
        receipt_id=RECEIPT_ID,
        content_hash=CONTENT_HASH,
        base_root_sha256=BASE_ROOT,
        storage_locator_hash=STORAGE_LOCATOR,
        attested_at_epoch=epoch,
        retention_days=RETENTION_DAYS,
        attestation_level=level,
        cert_schema_version=CERT_SCHEMA_VERSION,
    )


def test_cert_is_byte_identical_on_same_inputs():
    """Calling build_cert twice with the same inputs must produce
    byte-identical dcbor_bytes and cert_hash."""
    a_bytes, a_hash = _build(epoch=285, level=AttestationLevel.HASH_VERIFIED)
    b_bytes, b_hash = _build(epoch=285, level=AttestationLevel.HASH_VERIFIED)
    assert a_bytes == b_bytes
    assert a_hash == b_hash


def test_cert_hash_ignores_attested_at_epoch():
    """Different Cardano epochs must NOT change cert_hash — different
    attesters query Ogmios at different moments (or hit sync-delayed
    instances) and previously produced divergent hashes, wedging the
    M-of-N gate. Epoch is pinned inside the cert body."""
    _, hash_early = _build(epoch=284, level=AttestationLevel.HASH_VERIFIED)
    _, hash_late = _build(epoch=285, level=AttestationLevel.HASH_VERIFIED)
    _, hash_zero = _build(epoch=0, level=AttestationLevel.HASH_VERIFIED)
    assert hash_early == hash_late == hash_zero


def test_cert_hash_ignores_attestation_level():
    """Different attestation_level values must NOT change cert_hash —
    one attester may achieve MERKLE_VERIFIED while another only achieves
    HASH_VERIFIED depending on local gateway chunk availability. Level is
    pinned inside the cert body."""
    _, hash_hash = _build(epoch=0, level=AttestationLevel.HASH_VERIFIED)
    _, hash_merkle = _build(epoch=0, level=AttestationLevel.ROOT_VERIFIED)
    assert hash_hash == hash_merkle


def test_cert_body_carries_pinned_fields_not_call_args():
    """The CBOR body should embed the pinned placeholder epoch (0) and
    HASH_VERIFIED level (2), regardless of what the caller passed."""
    import cbor2
    dcbor_bytes, _ = _build(epoch=999, level=AttestationLevel.ROOT_VERIFIED)
    arr = cbor2.loads(dcbor_bytes)
    assert arr[6] == 0, f"expected epoch=0 placeholder, got {arr[6]}"
    assert arr[8] == AttestationLevel.HASH_VERIFIED.value, (
        f"expected level={AttestationLevel.HASH_VERIFIED.value}, got {arr[8]}"
    )


def test_cert_hash_golden_fixture():
    """Regression guard: any change to the cert CBOR layout or hash
    algorithm will flip this digest. Bump the fixture only when the
    change is intentional AND all operator-kit deployments are being
    upgraded atomically (otherwise attesters will disagree on cert_hash)."""
    _, cert_hash = _build(epoch=0, level=AttestationLevel.HASH_VERIFIED)
    assert cert_hash.hex() == (
        "d667384e7c04ac25043af58dda69ffa8ac48888abce377ddc3f698e17a1e1f3e"
    ), f"cert_hash drifted: {cert_hash.hex()}"
