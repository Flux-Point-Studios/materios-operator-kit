"""SCALE-canonical availability certificate builder (spec-219).

The cert was previously a canonical-CBOR Python construction whose hash
the substrate runtime could not independently verify — any daemon with
a stale `CHAIN_ID` could race the correctly-configured peers and lock
new receipts at a wrong `availability_cert_hash`, stranding them
forever (`CertHashMismatch` on every subsequent attest). Root cause +
forensic at `feedback_cert_daemon_chain_id_must_be_set.md`.

Spec-219 moves cert canonicalisation **into the runtime**: the cert is
now a fixed-width 202-byte SCALE-encoded struct that the Rust pallet
computes itself from on-chain state + pinned constants, and
`attest_availability_cert` rejects any claim that disagrees with its
own computation. This module is the daemon-side counterpart: it must
produce byte-identical output to Rust's `<Cert as Encode>::encode()`,
hashed with SHA-256.

There is no CBOR bridge. `build_cert` has been deleted. Any consumer
that still imports it will fail at module load — intentional per the
design doc's hard-cut migration plan.
"""
import hashlib


# ─── Pinned cert body fields (must match pallet-orinq-receipts) ───────────
# These five constants are the byte-pinned cross-chain contract. Their
# Rust counterparts live in `pallets/orinq-receipts/src/types.rs`:
#
#   pub const CERT_DOMAIN_BYTES: &[u8; 32] =
#       b"materios-availability-cert-v1\x00\x00\x00";
#   pub const CERT_EPOCH_PLACEHOLDER: u32 = 0;
#   pub const CERT_RETENTION_DAYS: u32 = 365;
#   pub const CERT_ATTESTATION_LEVEL: u8 = 2;     // HASH_VERIFIED
#   pub const CERT_SCHEMA_VERSION: u8 = 1;
#
# Changing any of these requires a coordinated runtime upgrade (the
# `Cert` SCALE layout changes → every fixture hash changes → CI red on
# both repos before deploy).
CERT_DOMAIN_BYTES = b"materios-availability-cert-v1" + b"\x00" * 3  # 32 bytes
assert len(CERT_DOMAIN_BYTES) == 32, "domain separator must be exactly 32 bytes"
CERT_EPOCH_PLACEHOLDER = 0
CERT_RETENTION_DAYS = 365
CERT_ATTESTATION_LEVEL = 2     # HASH_VERIFIED
CERT_SCHEMA_VERSION = 1


def _to_bytes32(value, field_name: str) -> bytes:
    """Coerce a 32-byte field to canonical raw-bytes form.

    Accepted inputs:
      - `bytes` / `bytearray` of length 32 → returned as `bytes`
      - hex `str` of length 64 (no prefix) → decoded
      - hex `str` of length 66 with `0x` / `0X` prefix → prefix stripped + decoded
      - `None` → returned as 32 zero bytes (legacy storage_locator may be unset)
      - `list[int]` / `tuple[int, ...]` of 32 ints in 0..255 → packed
        (substrate-interface sometimes returns Vec<u8> fields this way)

    Anything else raises a clear `TypeError` / `ValueError` carrying
    `field_name` so the caller's stack trace points at the broken input.
    """
    if value is None:
        return b"\x00" * 32
    if isinstance(value, (bytes, bytearray)):
        if len(value) != 32:
            raise ValueError(
                f"{field_name}: expected 32 bytes, got {len(value)}"
            )
        return bytes(value)
    if isinstance(value, str):
        s = value.removeprefix("0x").removeprefix("0X")
        if len(s) != 64:
            raise ValueError(
                f"{field_name}: expected 64 hex chars (32 bytes), got {len(s)}"
            )
        try:
            return bytes.fromhex(s)
        except ValueError as e:
            raise ValueError(f"{field_name}: invalid hex: {e}") from e
    if isinstance(value, (list, tuple)):
        if len(value) != 32 or not all(
            isinstance(x, int) and 0 <= x <= 255 for x in value
        ):
            raise ValueError(
                f"{field_name}: list/tuple must be 32 ints in 0..255"
            )
        return bytes(value)
    raise TypeError(
        f"{field_name}: unsupported type {type(value).__name__}; "
        f"expected bytes, str (hex), None, or list[int]"
    )


def scale_cert_encode(
    chain_genesis,        # bytes(32) | hex str — live RPC chain_getBlockHash[0]
    receipt_id,           # bytes(32) | hex str
    content_hash,         # bytes(32) | hex str | list[int]
    base_root_sha256,     # bytes(32) | hex str | list[int]
    storage_locator_hash, # bytes(32) | hex str | list[int] | None -> zeros
) -> bytes:
    """Produce byte-identical output to Rust's `<Cert as Encode>::encode()`.

    Returns exactly 202 bytes laid out as (per design doc §6):

        offset  len  field
        ------  ---  ---------------
             0   32  domain (ASCII "materios-availability-cert-v1" + 3× 0x00)
            32   32  chain_id          (H256, raw 32 bytes)
            64   32  receipt_id        (H256, raw 32 bytes)
            96   32  content_hash      (raw 32 bytes)
           128   32  base_root         (raw 32 bytes)
           160   32  storage_locator   (raw 32 bytes)
           192    4  epoch             (u32 little-endian, pinned = 0)
           196    4  retention_days    (u32 little-endian, pinned = 365)
           200    1  attestation_level (u8, pinned = 2)
           201    1  schema_version    (u8, pinned = 1)

    None of these fields carry a SCALE length prefix; everything is
    fixed-width and there is no representational ambiguity. That's the
    point — CBOR's optional canonicalisation rules were the bug surface
    we are eliminating.
    """
    chain_id_b = _to_bytes32(chain_genesis, "chain_genesis")
    receipt_id_b = _to_bytes32(receipt_id, "receipt_id")
    content_b = _to_bytes32(content_hash, "content_hash")
    base_root_b = _to_bytes32(base_root_sha256, "base_root_sha256")
    locator_b = _to_bytes32(storage_locator_hash, "storage_locator_hash")
    return b"".join([
        CERT_DOMAIN_BYTES,
        chain_id_b,
        receipt_id_b,
        content_b,
        base_root_b,
        locator_b,
        CERT_EPOCH_PLACEHOLDER.to_bytes(4, "little"),
        CERT_RETENTION_DAYS.to_bytes(4, "little"),
        CERT_ATTESTATION_LEVEL.to_bytes(1, "little"),
        CERT_SCHEMA_VERSION.to_bytes(1, "little"),
    ])


def scale_cert_hash(
    chain_genesis,
    receipt_id,
    content_hash,
    base_root_sha256,
    storage_locator_hash,
) -> bytes:
    """SHA-256 of `scale_cert_encode(...)`.

    Returns the 32-byte canonical `cert_hash` that the runtime's
    `canonical_cert_hash(receipt_id)` will compute from the same inputs.
    Mismatch on `attest_availability_cert` = `CertHashMismatch` +
    `BadAttestStrike` + (at threshold) auto-slash.

    Signature mirrors `scale_cert_encode` so typos surface at the call
    site, not buried inside the encoder.
    """
    return hashlib.sha256(
        scale_cert_encode(
            chain_genesis=chain_genesis,
            receipt_id=receipt_id,
            content_hash=content_hash,
            base_root_sha256=base_root_sha256,
            storage_locator_hash=storage_locator_hash,
        )
    ).digest()
