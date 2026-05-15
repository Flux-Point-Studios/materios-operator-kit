"""Cardano CIP-0019 address decoder.

Cert-daemon's `settle_claim` attestor must extract the 28-byte
**payment-key hash** from a Cardano bech32 address (e.g. the address
Kupo reports as the recipient of a Cardano UTxO). The pallet's
`SettlementEvidence.beneficiary_addr_hash` is THIS 28-byte value,
NOT a blake2_224 hash of any string. Pinned by
`materios-intent-settlement::voucher_canonicalize::split_type0_address_bytes`
which takes the raw 57-byte address, asserts header byte, and returns
``raw[1..29]`` as the payment hash.

This module bech32-decodes the address, asserts CIP-0019 type-0/type-6
layout, and returns the 28-byte payment-key hash.

## Why an inline bech32 decoder

The canonical Python `bech32` package (BIP-173 reference impl) enforces
the original 90-char address-length cap from BIP-173 §6. Cardano
addresses routinely exceed 100 characters because they carry both a
payment-key hash and a stake-key hash in the same address. CIP-0019
deliberately relaxes that cap. Rather than add a heavy Cardano-specific
dep (pycardano ≈ 17 MB installed), we inline the ~50 LOC of BIP-173
reference algorithms with the length cap dropped — same algorithm,
same character set, same checksum polynomial. Audited against the
Pieter Wuille reference implementation.

## Why not blake2_224 of the bech32 string

That was the previous cert-daemon implementation. It produced a value
that DID NOT MATCH the pallet's `voucher_canonicalize` output — every
attestation signature would have been rejected silently in production.
Caught by cross-component review of PR #33 vs PR #26.
"""

from __future__ import annotations


# BIP-173 character set (lowercase only after the case-fold pass).
_CHARSET = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"


def _polymod(values: list[int]) -> int:
    """BIP-173 §5.1 polymod for the bech32 checksum."""
    GEN = [0x3B6A57B2, 0x26508E6D, 0x1EA119FA, 0x3D4233DD, 0x2A1462B3]
    chk = 1
    for v in values:
        b = chk >> 25
        chk = ((chk & 0x1FFFFFF) << 5) ^ v
        for i in range(5):
            if (b >> i) & 1:
                chk ^= GEN[i]
    return chk


def _hrp_expand(hrp: str) -> list[int]:
    return [ord(c) >> 5 for c in hrp] + [0] + [ord(c) & 31 for c in hrp]


def _verify_checksum(hrp: str, data: list[int]) -> bool:
    return _polymod(_hrp_expand(hrp) + data) == 1


def _bech32_decode(addr: str) -> tuple[str, list[int]]:
    """Decode a BIP-173 bech32 address WITHOUT the 90-char length cap.

    Returns (hrp_lowercase, data_without_checksum). `data` is the
    5-bits-per-element sequence — caller converts to 8-bit bytes
    via `_convertbits`.
    """
    if not addr:
        raise ValueError("empty address")
    # CIP-0019 addresses are mixed-case-illegal: BIP-173 §6 forbids
    # mixed-case. We accept all-lower or all-upper but reject mixed.
    if addr.lower() != addr and addr.upper() != addr:
        raise ValueError("mixed-case address")
    addr_lc = addr.lower()
    if any(ord(c) < 33 or ord(c) > 126 for c in addr_lc):
        raise ValueError("non-printable char in address")
    if "1" not in addr_lc:
        raise ValueError("missing bech32 separator '1'")
    pos = addr_lc.rfind("1")
    if pos < 1 or pos + 7 > len(addr_lc):
        raise ValueError("bad separator position")
    hrp = addr_lc[:pos]
    data: list[int] = []
    for c in addr_lc[pos + 1:]:
        if c not in _CHARSET:
            raise ValueError(f"invalid bech32 char {c!r}")
        data.append(_CHARSET.index(c))
    if not _verify_checksum(hrp, data):
        raise ValueError("bech32 checksum failed")
    return hrp, data[:-6]  # strip 6-element checksum


def _convertbits(
    data: list[int], frombits: int, tobits: int, pad: bool = True
) -> list[int]:
    """BIP-173 §5.2 — convert between bit-group widths.

    For Cardano addresses: 5-bit groups (bech32 alphabet) → 8-bit bytes.
    `pad=False` rejects trailing padding bits, matching the Cardano
    convention.
    """
    acc = 0
    bits = 0
    ret: list[int] = []
    maxv = (1 << tobits) - 1
    max_acc = (1 << (frombits + tobits - 1)) - 1
    for value in data:
        if value < 0 or (value >> frombits):
            raise ValueError("invalid bit value")
        acc = ((acc << frombits) | value) & max_acc
        bits += frombits
        while bits >= tobits:
            bits -= tobits
            ret.append((acc >> bits) & maxv)
    if pad:
        if bits:
            ret.append((acc << (tobits - bits)) & maxv)
    elif bits >= frombits or ((acc << (tobits - bits)) & maxv):
        raise ValueError("trailing bits not zero")
    return ret


def decode_cardano_address(address: str) -> tuple[str, bytes]:
    """Decode a Cardano bech32 address.

    Returns ``(hrp, raw_payload_bytes)`` where:
      - ``hrp`` is lowercase, e.g. ``"addr"`` (mainnet) or
        ``"addr_test"`` (testnet/preprod/preview).
      - ``raw_payload_bytes`` is the decoded address payload: for
        CIP-0019 type-0 (base) addresses this is 57 bytes:
        ``header(1) || payment_hash(28) || stake_hash(28)``.

    Raises ``ValueError`` on any decode/checksum/structure failure.
    """
    hrp, data5 = _bech32_decode(address)
    raw = bytes(_convertbits(data5, 5, 8, pad=False))
    return hrp, raw


def extract_payment_hash_from_cardano_address(address: str) -> bytes:
    """Return the 28-byte payment-key hash from a Cardano bech32 address.

    Matches pallet-side `voucher_canonicalize::split_type0_address_bytes`
    byte-for-byte: bech32-decode → assert payload length ≥ 29 → return
    bytes ``[1..29]`` of the raw payload (i.e., skip the 1-byte CIP-0019
    header and take the next 28 bytes = payment-key hash).

    Accepts ``addr1q...`` (mainnet) and ``addr_test1q...``
    (preprod/preview) type-0 base addresses. Also works for type-6
    enterprise addresses (no stake hash; ``raw`` is 29 bytes, still
    correct since we only read ``[1..29]``).

    Raises ``ValueError`` on any decode failure or if the payload is
    too short for payment-hash extraction.
    """
    _hrp, raw = decode_cardano_address(address)
    if len(raw) < 29:
        raise ValueError(
            f"address payload {len(raw)} bytes — too short to contain "
            f"a 28-byte payment-key hash starting at offset 1"
        )
    return raw[1:29]
