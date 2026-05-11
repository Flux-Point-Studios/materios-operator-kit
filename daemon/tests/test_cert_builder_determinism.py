"""Determinism contract tests for daemon.cert_builder.build_cert.

These tests pin the byte-level behavior of the cert builder so that any
future change that perturbs cert_hash across attesters is caught at PR
time. The pallet's `attest_availability_cert` extrinsic rejects with
`CertHashMismatch` if two attesters disagree on cert_hash; that rejection
is silent at the daemon level and silently strands receipts on chain
(7 of 18 compute_metering_v2 receipts on preprod, 2026-05-11 forensic).

Every test here asserts that two ways of expressing the same logical
input produce IDENTICAL cert_hash.
"""
import hashlib

import pytest

from daemon.cert_builder import (
    build_cert,
    CERT_EPOCH_PLACEHOLDER,
    CERT_ATTESTATION_LEVEL_PINNED,
    CERT_RETENTION_DAYS_PINNED,
    CERT_SCHEMA_VERSION_PINNED,
)
from daemon.models import AttestationLevel


CHAIN_ID = "bc0531cb311281565036fb397a376f0e0fa37005589655f97a7924b2729a164c"
RID = "0x7556a31f46c3429e79864820c270e246fdaccaec53404c9d66f342bf9ec32c1c"
CH_HEX = "35aa9f89f103767826b9d6476d5259e214351df60ecbae61043cc754cb7b29ee"
BR_HEX = "35aa9f89f103767826b9d6476d5259e214351df60ecbae61043cc754cb7b29ee"
SL_HEX = "00" * 32

CH_BYTES = bytes.fromhex(CH_HEX)
BR_BYTES = bytes.fromhex(BR_HEX)
SL_BYTES = bytes.fromhex(SL_HEX)


def _hash(**overrides):
    args = dict(
        chain_id=CHAIN_ID,
        receipt_id=RID,
        content_hash=CH_BYTES,
        base_root_sha256=BR_BYTES,
        storage_locator_hash=SL_BYTES,
    )
    args.update(overrides)
    return build_cert(**args)[1].hex()


def test_baseline_is_stable_across_repeats():
    """Repeated calls with identical inputs produce identical hashes."""
    h1 = _hash()
    h2 = _hash()
    h3 = _hash()
    assert h1 == h2 == h3


def test_chain_id_0x_prefix_normalizes():
    """`chain_id` with 0x prefix produces the same hash as bare hex."""
    assert _hash(chain_id=CHAIN_ID) == _hash(chain_id="0x" + CHAIN_ID)
    assert _hash(chain_id=CHAIN_ID) == _hash(chain_id="0X" + CHAIN_ID.upper())


def test_chain_id_case_normalizes():
    """`chain_id` uppercase = lowercase (case-insensitive hex)."""
    assert _hash(chain_id=CHAIN_ID) == _hash(chain_id=CHAIN_ID.upper())


def test_chain_id_empty_rejects():
    """Empty chain_id is fatal — silent default is the failure mode that
    actually stranded 12 receipts on preprod, so the function must refuse."""
    with pytest.raises(ValueError, match="empty"):
        _hash(chain_id="")


def test_chain_id_bytes_rejects():
    """Bytes for chain_id are rejected — CBOR-encodes differently than str."""
    with pytest.raises(TypeError):
        _hash(chain_id=bytes.fromhex(CHAIN_ID))


def test_receipt_id_0x_prefix_normalizes():
    """receipt_id with / without 0x produces the same hash."""
    assert _hash(receipt_id=RID) == _hash(receipt_id=RID[2:])


def test_receipt_id_case_normalizes():
    """receipt_id case-insensitive."""
    assert _hash(receipt_id=RID) == _hash(receipt_id=RID.upper())
    assert _hash(receipt_id=RID) == _hash(receipt_id="0x" + RID[2:].upper())


def test_receipt_id_bytes_accepted():
    """receipt_id passed as raw bytes(32) normalizes to the same hash as 0x-hex."""
    rid_bytes = bytes.fromhex(RID[2:])
    assert _hash(receipt_id=RID) == _hash(receipt_id=rid_bytes)


def test_content_hash_bytes_str_equivalent():
    """content_hash as bytes vs hex str produces same hash."""
    assert _hash(content_hash=CH_BYTES) == _hash(content_hash=CH_HEX)
    assert _hash(content_hash=CH_BYTES) == _hash(content_hash="0x" + CH_HEX)


def test_content_hash_uppercase_hex():
    """Case-insensitive hex."""
    assert _hash(content_hash=CH_BYTES) == _hash(content_hash=CH_HEX.upper())


def test_content_hash_list_of_ints():
    """substrate-interface sometimes returns Vec<u8> as list[int]."""
    assert _hash(content_hash=CH_BYTES) == _hash(content_hash=list(CH_BYTES))


def test_content_hash_wrong_length_rejects():
    """Wrong length is fatal — silent padding would change the hash."""
    with pytest.raises(ValueError, match="32"):
        _hash(content_hash=b"\x00" * 31)
    with pytest.raises(ValueError, match="32"):
        _hash(content_hash="ab" * 31)


def test_storage_locator_none_is_zeros():
    """A `None` storage_locator (legacy receipts) coerces to 32 zero bytes."""
    assert _hash(storage_locator_hash=SL_BYTES) == _hash(storage_locator_hash=None)


def test_ignored_kwargs_dont_affect_hash():
    """retention_days / cert_schema_version / attested_at_epoch / attestation_level
    are kwargs for API stability but the body uses pinned constants.
    Changing them must NOT change the hash."""
    base = _hash()
    assert base == _hash(retention_days=30)
    assert base == _hash(retention_days=730)
    assert base == _hash(cert_schema_version="2.0")
    assert base == _hash(cert_schema_version="v1")
    assert base == _hash(attested_at_epoch=2025)
    assert base == _hash(attestation_level=AttestationLevel.ROOT_VERIFIED)


def test_real_input_change_DOES_change_hash():
    """Sanity: actual receipt fields are still hashed. Changing one MUST
    change cert_hash."""
    base = _hash()
    different_ch = bytes.fromhex("ff" * 32)
    assert _hash(content_hash=different_ch) != base
    different_rid = "0x" + "aa" * 32
    assert _hash(receipt_id=different_rid) != base
    different_chain = "ff" * 32
    assert _hash(chain_id=different_chain) != base


def test_pinned_constants_match_documented_values():
    """The pinned constants are part of the cross-operator contract. They
    can only change via a coordinated runtime upgrade; pin them here so
    any accidental drift fails CI."""
    assert CERT_EPOCH_PLACEHOLDER == 0
    assert CERT_ATTESTATION_LEVEL_PINNED == AttestationLevel.HASH_VERIFIED
    assert CERT_RETENTION_DAYS_PINNED == 365
    assert CERT_SCHEMA_VERSION_PINNED == "1.0"


def test_known_vector_2026_05_11():
    """Pin the cert_hash for receipt 0x7556a3 against this exact input set.

    Today's daemons compute `eea9520527a2508b15e116aa0fbe564ea20fe177c9f7bc3b1a2d93e689513b46`
    for this input. Lock it in so any future build_cert change that
    changes the on-the-wire cert_hash format fails CI loudly — that's a
    breaking-change for all deployed operators and must be a deliberate
    upgrade with coordinated rollout, never an accident."""
    cert_hash = _hash()
    assert cert_hash == "eea9520527a2508b15e116aa0fbe564ea20fe177c9f7bc3b1a2d93e689513b46"


def test_dcbor_canonical_form_stable():
    """Final byte-for-byte invariant: the CBOR encoding is itself stable."""
    dcbor1, _ = build_cert(
        chain_id=CHAIN_ID, receipt_id=RID,
        content_hash=CH_BYTES, base_root_sha256=BR_BYTES,
        storage_locator_hash=SL_BYTES,
    )
    dcbor2, _ = build_cert(
        chain_id="0x" + CHAIN_ID.upper(), receipt_id=RID[2:].upper(),
        content_hash=CH_HEX, base_root_sha256=BR_HEX,
        storage_locator_hash=None,
    )
    assert dcbor1 == dcbor2
    # Sanity: the hash chains
    assert hashlib.sha256(dcbor1).digest() == hashlib.sha256(dcbor2).digest()
