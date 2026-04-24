import hashlib
import cbor2
from daemon.models import AttestationLevel

# Fixed field values used to guarantee cert_hash determinism across attesters.
# The pallet requires all M-of-N attesters to submit the IDENTICAL cert_hash
# for the same receipt — any divergence rejects with Error::CertHashMismatch
# and the receipt never certifies. Previously these fields were taken from
# operator-local state (wall-clock Cardano epoch from Ogmios, locally-computed
# attestation_level from blob verification depth), which drift across
# attesters and caused the pipeline to wedge. They are now pinned so that
# cert_hash is a pure function of (chain_id, receipt_id, content_hash,
# base_root_sha256, storage_locator_hash, retention_days, cert_schema_version).
# Epoch + attestation_level are still carried as CBOR fields for downstream
# audit-tool compatibility, but set to placeholder/floor values.
CERT_EPOCH_PLACEHOLDER = 0
CERT_ATTESTATION_LEVEL_PINNED = AttestationLevel.HASH_VERIFIED


def build_cert(
    chain_id: str,
    receipt_id: str,
    content_hash: bytes,
    base_root_sha256: bytes,
    storage_locator_hash: bytes,
    attested_at_epoch: int,   # accepted for API-compat, NOT used in cert body
    retention_days: int,
    attestation_level: AttestationLevel,   # accepted for API-compat, NOT used
    cert_schema_version: str = "1.0",
) -> tuple[bytes, bytes]:
    """Build a dCBOR availability certificate.

    Returns (dcbor_bytes, cert_hash) where cert_hash = SHA-256(dcbor_bytes).
    The cert is a 10-element CBOR array per AVAILABILITY_CERT_SPEC.md.

    **Determinism contract:** For a given (chain_id, receipt_id, content_hash,
    base_root_sha256, storage_locator_hash, retention_days,
    cert_schema_version), every attester MUST produce byte-identical
    dcbor_bytes. The `attested_at_epoch` and `attestation_level` args are
    retained in the function signature so callers don't need to change, but
    are ignored; the cert body uses `CERT_EPOCH_PLACEHOLDER` and
    `CERT_ATTESTATION_LEVEL_PINNED` instead.
    """
    cert_array = [
        "materios-availability-cert-v1",    # 0: domain separator
        chain_id,                            # 1: Materios chain genesis hash
        receipt_id,                          # 2: receipt ID hex string
        content_hash,                        # 3: bytes32
        base_root_sha256,                    # 4: bytes32
        storage_locator_hash,                # 5: bytes32
        CERT_EPOCH_PLACEHOLDER,              # 6: pinned (was wall-clock Cardano epoch)
        retention_days,                      # 7: retention commitment
        CERT_ATTESTATION_LEVEL_PINNED.value, # 8: pinned to HASH_VERIFIED (was 1/2/3 local)
        cert_schema_version,                 # 9: schema version
    ]
    dcbor_bytes = cbor2.dumps(cert_array, canonical=True)
    cert_hash = hashlib.sha256(dcbor_bytes).digest()
    return dcbor_bytes, cert_hash
