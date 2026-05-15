"""Tests for `daemon.cardano_address` — CIP-0019 payment-hash extraction.

This module's correctness is load-bearing: every cert-daemon attestation
signature depends on the 28-byte `beneficiary_addr_hash` matching the
pallet's `voucher_canonicalize::split_type0_address_bytes` output exactly.
A bug here = silent sig rejection at `attest_settle` = chain liveness loss.

Pre-PR #272 the cert-daemon computed `blake2_224(bech32_string)` which
was wrong on every dimension; this test suite catches the regression
class.
"""

from __future__ import annotations

import pytest

from daemon.cardano_address import (
    _CHARSET,
    _convertbits,
    _hrp_expand,
    _polymod,
    decode_cardano_address,
    extract_payment_hash_from_cardano_address,
)


# ---------------------------------------------------------------------------
# Test utility: bech32 encoder (inverse of the production decoder).
#
# Kept TEST-ONLY so production surface stays decode-only. We need it here
# to build deterministic test vectors: pick a known payment hash, encode
# to a Cardano-shaped bech32 address, decode it back via production, assert
# we get the same bytes. End-to-end round-trip = full coverage.
# ---------------------------------------------------------------------------


def _bech32_create_checksum(hrp: str, data: list[int]) -> list[int]:
    polymod = _polymod(_hrp_expand(hrp) + data + [0, 0, 0, 0, 0, 0]) ^ 1
    return [(polymod >> 5 * (5 - i)) & 31 for i in range(6)]


def _bech32_encode(hrp: str, raw_bytes: bytes) -> str:
    """Bech32-encode raw bytes with the given HRP (Cardano-permissive — no
    90-char length cap). Used only by tests to construct known vectors."""
    data5 = _convertbits(list(raw_bytes), 8, 5, pad=True)
    combined = data5 + _bech32_create_checksum(hrp, data5)
    return hrp + "1" + "".join(_CHARSET[d] for d in combined)


def _bech32_encode_addr_test(raw: bytes) -> str:
    return _bech32_encode("addr_test", raw)


def _bech32_encode_addr_mainnet(raw: bytes) -> str:
    return _bech32_encode("addr", raw)


# ---------------------------------------------------------------------------
# Round-trip tests — the core correctness invariant.
# ---------------------------------------------------------------------------


class TestPaymentHashExtractionRoundTrip:
    """The pallet's `split_type0_address_bytes` takes raw 57-byte address
    bytes and returns `raw[1..29]` as the payment hash. We must match
    byte-for-byte, regardless of bech32 encoding."""

    def test_type0_testnet_payment_hash_round_trip(self):
        header = bytes([0x00])
        payment_hash = b"\x11" * 28
        stake_hash = b"\x22" * 28
        raw = header + payment_hash + stake_hash
        addr = _bech32_encode_addr_test(raw)
        got = extract_payment_hash_from_cardano_address(addr)
        assert got == payment_hash

    def test_type0_mainnet_payment_hash_round_trip(self):
        header = bytes([0x01])
        payment_hash = bytes(range(28))  # 0x00, 0x01, ..., 0x1b
        stake_hash = bytes(range(28, 56))
        raw = header + payment_hash + stake_hash
        addr = _bech32_encode_addr_mainnet(raw)
        got = extract_payment_hash_from_cardano_address(addr)
        assert got == payment_hash

    def test_type6_enterprise_no_stake_hash_still_works(self):
        """Type-6 enterprise addresses have NO stake hash — payload is
        only 29 bytes total (header + payment hash). Our extraction at
        [1..29] still correctly returns the payment hash."""
        header = bytes([0x60])  # type 6 mainnet enterprise
        payment_hash = b"\xaa" * 28
        raw = header + payment_hash
        addr = _bech32_encode_addr_mainnet(raw)
        got = extract_payment_hash_from_cardano_address(addr)
        assert got == payment_hash

    def test_all_zero_payment_hash(self):
        raw = bytes([0x00]) + b"\x00" * 28 + b"\x00" * 28
        addr = _bech32_encode_addr_test(raw)
        got = extract_payment_hash_from_cardano_address(addr)
        assert got == b"\x00" * 28

    def test_all_high_byte_payment_hash(self):
        raw = bytes([0x00]) + b"\xff" * 28 + b"\xff" * 28
        addr = _bech32_encode_addr_test(raw)
        got = extract_payment_hash_from_cardano_address(addr)
        assert got == b"\xff" * 28


class TestDecodeReturnsHrpAndPayload:
    def test_testnet_hrp(self):
        raw = bytes([0x00]) + b"\x11" * 28 + b"\x22" * 28
        addr = _bech32_encode_addr_test(raw)
        hrp, payload = decode_cardano_address(addr)
        assert hrp == "addr_test"
        assert payload == raw
        assert len(payload) == 57

    def test_mainnet_hrp(self):
        raw = bytes([0x01]) + b"\xaa" * 28 + b"\xbb" * 28
        addr = _bech32_encode_addr_mainnet(raw)
        hrp, payload = decode_cardano_address(addr)
        assert hrp == "addr"
        assert payload == raw


class TestDecoderRejectsInvalidInput:
    def test_empty_string(self):
        with pytest.raises(ValueError, match="empty address"):
            decode_cardano_address("")

    def test_missing_separator(self):
        with pytest.raises(ValueError, match="separator"):
            decode_cardano_address("addrtestqqqqqqqq")

    def test_invalid_char(self):
        raw = bytes([0x00]) + b"\x11" * 28 + b"\x22" * 28
        addr = _bech32_encode_addr_test(raw)
        # Replace last char (part of checksum) with 'b' — invalid in bech32 alphabet.
        bad = addr[:-1] + "b"
        with pytest.raises(ValueError, match="checksum|invalid"):
            decode_cardano_address(bad)

    def test_checksum_corruption(self):
        raw = bytes([0x00]) + b"\x11" * 28 + b"\x22" * 28
        addr = _bech32_encode_addr_test(raw)
        # Flip one char inside the data portion — fails checksum.
        i = len(addr) // 2
        bad = addr[:i] + ("q" if addr[i] != "q" else "p") + addr[i + 1:]
        with pytest.raises(ValueError, match="checksum"):
            decode_cardano_address(bad)

    def test_mixed_case_rejected(self):
        raw = bytes([0x00]) + b"\x11" * 28 + b"\x22" * 28
        addr = _bech32_encode_addr_test(raw)
        with pytest.raises(ValueError, match="mixed-case"):
            decode_cardano_address(addr[:3] + "X" + addr[4:])


class TestPaymentHashExtractionRejectsTooShortPayload:
    def test_29_byte_payload_works(self):
        # Type-6 enterprise: header(1) + payment(28) = 29 bytes — minimum.
        raw = bytes([0x60]) + b"\x77" * 28
        addr = _bech32_encode_addr_mainnet(raw)
        got = extract_payment_hash_from_cardano_address(addr)
        assert got == b"\x77" * 28

    def test_28_byte_payload_rejected(self):
        # 28 bytes — too short for payment hash at [1..29].
        # Build by hand bypassing the production encoder's pad assumption.
        raw = b"\x60" + b"\x11" * 27  # only 28 bytes total
        addr = _bech32_encode_addr_mainnet(raw)
        with pytest.raises(ValueError, match="too short"):
            extract_payment_hash_from_cardano_address(addr)
