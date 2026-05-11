"""dCBOR availability certificate builder.

`build_cert` must be byte-deterministic across operators. The pallet's
`attest_availability_cert` extrinsic rejects with `CertHashMismatch` if
two attesters submit different cert_hashes for the same receipt — so any
input variability (bytes vs str, with/without 0x prefix, retention_days
chosen per-operator, etc.) silently wedges receipts at the chain.

Strategy: every value that enters the CBOR cert body is normalized at
this layer, AND all operator-tunable knobs are pinned to module
constants. The function's signature accepts both bytes and hex strings
for the hash fields, but coerces internally so all attesters produce
byte-identical output regardless of how their upstream code (e.g. the
substrate-interface RPC client across versions) happens to represent
values.

History: prior versions of this file pinned `attested_at_epoch` and
`attestation_level` but still let `retention_days`, `cert_schema_version`,
chain_id 0x-prefix, and bytes-vs-str hash representation float. The
result: 7 of 18 compute_metering_v2 receipts on preprod stuck Pending
forever because external attestors picked different retention_days /
cert_schema_version values. See `feedback_cert_daemon_chain_id_must_be_set.md`
and the 2026-05-11 forensic investigation.
"""
import hashlib
import cbor2
from daemon.models import AttestationLevel


# ─── Pinned cert body fields ─────────────────────────────────────────────
# All cert body fields that DON'T come from chain state are pinned here.
# Changing any of these changes every future cert_hash → must be a
# coordinated runtime upgrade across all operators.
#
# Epoch + attestation_level used to be locally-derived (Cardano epoch
# from Ogmios; verification depth from blob check). retention_days +
# cert_schema_version were operator-configurable, which silently
# divided the committee into incompatible subsets. All four are now
# constants in this module.
CERT_EPOCH_PLACEHOLDER = 0
CERT_ATTESTATION_LEVEL_PINNED = AttestationLevel.HASH_VERIFIED
CERT_RETENTION_DAYS_PINNED = 365
CERT_SCHEMA_VERSION_PINNED = "1.0"


def _to_bytes32(value, field_name: str) -> bytes:
    """Coerce a hash field to canonical 32-byte form.

    Accepted inputs:
      - `bytes` / `bytearray` of length 32 → returned as `bytes`
      - hex `str` of length 64 (no prefix) → decoded
      - hex `str` of length 66 with `0x` prefix → prefix stripped + decoded
      - `None` → returned as 32 zero bytes (legacy storage_locator may be unset)
      - `list[int]` (32 ints in 0-255) → packed as bytes (substrate-interface
        sometimes returns Vec<u8> fields this way)

    Rejects everything else with a TypeError carrying `field_name` so the
    caller's stack trace points at the broken input.
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
        if len(value) != 32 or not all(isinstance(x, int) and 0 <= x <= 255 for x in value):
            raise ValueError(
                f"{field_name}: list/tuple must be 32 ints in 0..255"
            )
        return bytes(value)
    raise TypeError(
        f"{field_name}: unsupported type {type(value).__name__}; "
        f"expected bytes, str (hex), None, or list[int]"
    )


def _to_bare_hex_chain_id(value: str) -> str:
    """Coerce chain_id to bare lowercase hex (no 0x prefix).

    Accepts with or without 0x prefix, accepts mixed case. Rejects bytes
    and anything that doesn't decode cleanly — chain_id is a known string
    field in our system and accidentally passing bytes here used to
    silently produce a different cert (because cbor2 encodes bytes and
    str differently).
    """
    if not isinstance(value, str):
        raise TypeError(f"chain_id must be str, got {type(value).__name__}")
    s = value.removeprefix("0x").removeprefix("0X").lower()
    if len(s) == 0:
        raise ValueError("chain_id is empty — set CHAIN_ID env to the live chain genesis hash")
    # Validate it's hex-shaped (lets us catch typos early; doesn't enforce length
    # because genesis hashes can vary in repr across substrate versions).
    if not all(c in "0123456789abcdef" for c in s):
        raise ValueError(f"chain_id is not bare hex: {value!r}")
    return s


def _to_canonical_receipt_id(value) -> str:
    """Coerce receipt_id to canonical 0x-prefixed lowercase hex.

    Accepts bytes, with/without 0x prefix, mixed case. The cert body
    embeds the receipt_id as a CBOR text string — same byte representation
    on every attester is mandatory.
    """
    if isinstance(value, (bytes, bytearray)):
        if len(value) != 32:
            raise ValueError(f"receipt_id bytes must be 32 long, got {len(value)}")
        return "0x" + value.hex()
    if isinstance(value, str):
        s = value.removeprefix("0x").removeprefix("0X").lower()
        if len(s) != 64:
            raise ValueError(f"receipt_id hex must be 64 chars, got {len(s)}")
        if not all(c in "0123456789abcdef" for c in s):
            raise ValueError(f"receipt_id is not hex: {value!r}")
        return "0x" + s
    raise TypeError(
        f"receipt_id: unsupported type {type(value).__name__}; expected bytes or hex str"
    )


def build_cert(
    chain_id: str,
    receipt_id,                         # str (with or without 0x) or bytes(32)
    content_hash,                        # bytes(32) | hex str | list[int]
    base_root_sha256,                    # bytes(32) | hex str | list[int]
    storage_locator_hash,                # bytes(32) | hex str | list[int] | None
    attested_at_epoch: int = 0,          # IGNORED — pinned in body
    retention_days: int = 0,             # IGNORED — pinned in body
    attestation_level: AttestationLevel = AttestationLevel.HASH_VERIFIED,  # IGNORED
    cert_schema_version: str = "",       # IGNORED — pinned in body
) -> tuple[bytes, bytes]:
    """Build a dCBOR availability certificate.

    Returns `(dcbor_bytes, cert_hash)` where `cert_hash = sha256(dcbor_bytes)`.
    The cert body is a canonical CBOR 10-element array per
    AVAILABILITY_CERT_SPEC.md.

    Determinism: cert_hash depends ONLY on (chain_id normalized to bare hex,
    receipt_id normalized to 0x-prefixed lowercase hex, the three hash fields
    coerced to bytes(32)). Everything else in the body is a pinned constant.

    `attested_at_epoch`, `retention_days`, `attestation_level`, and
    `cert_schema_version` are kept as keyword args for API stability with
    older callers but their values are NOT read; the CBOR body uses the
    module-level constants instead.
    """
    chain_id_norm = _to_bare_hex_chain_id(chain_id)
    receipt_id_norm = _to_canonical_receipt_id(receipt_id)
    content_hash_b = _to_bytes32(content_hash, "content_hash")
    base_root_b = _to_bytes32(base_root_sha256, "base_root_sha256")
    storage_locator_b = _to_bytes32(storage_locator_hash, "storage_locator_hash")

    cert_array = [
        "materios-availability-cert-v1",      # 0: domain separator
        chain_id_norm,                         # 1: bare-hex genesis
        receipt_id_norm,                       # 2: 0x-prefixed lowercase receipt id
        content_hash_b,                        # 3: bytes32
        base_root_b,                           # 4: bytes32
        storage_locator_b,                     # 5: bytes32
        CERT_EPOCH_PLACEHOLDER,                # 6: pinned
        CERT_RETENTION_DAYS_PINNED,            # 7: pinned (was operator-tunable)
        CERT_ATTESTATION_LEVEL_PINNED.value,   # 8: pinned
        CERT_SCHEMA_VERSION_PINNED,            # 9: pinned (was operator-tunable)
    ]
    dcbor_bytes = cbor2.dumps(cert_array, canonical=True)
    cert_hash = hashlib.sha256(dcbor_bytes).digest()
    return dcbor_bytes, cert_hash
