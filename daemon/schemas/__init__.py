"""Schema-hash discriminator constants and verifier dispatch helpers.

Cert-daemon's `blob_verifier.BlobVerifier.verify` dispatches on the on-chain
`receipt.schema_hash` field to determine which verification path to apply:

  - `LEGACY_SCHEMA_HASH` (32 zero bytes) → blob chunk-Merkle path. The
    on-chain `base_root_sha256` MUST equal merkle_root(chunk_hashes).

  - `SCHEMA_HASH_COMPUTE_METERING_V2` / `SCHEMA_HASH_COMPUTE_METERING_V2_1`
    → trust-the-discriminator path. Chunk integrity is independently
    verified (chunk-Merkle MUST equal content_hash, so the envelope bytes
    are cryptographically pinned). The on-chain `base_root_sha256` is then
    accepted as a trusted semantic root for the schema class. Downstream
    consumers (billing API, observers) revalidate the semantic root from
    the envelope bytes themselves; cert-daemon's job is chunk integrity
    plus class assertion, not full semantic recompute.

  - Anything else → REJECT with a clear log. Unknown schemas must not pass
    silently; new schemas register here.

See `feedback_v2_contract_drift_chain_break.md` and the
2026-05-11 task #198 diagnostic for the full motivation. The split between
chunk integrity (cert-daemon) and semantic validation (downstream) mirrors
Cardano L1's own M-of-N attestation model.
"""

from daemon.schemas.compute_metering import (
    LEGACY_SCHEMA_HASH,
    SCHEMA_HASH_COMPUTE_METERING_V2,
    SCHEMA_HASH_COMPUTE_METERING_V2_1,
    TRUSTED_DISCRIMINATOR_SCHEMAS,
    schema_name,
)

__all__ = [
    "LEGACY_SCHEMA_HASH",
    "SCHEMA_HASH_COMPUTE_METERING_V2",
    "SCHEMA_HASH_COMPUTE_METERING_V2_1",
    "TRUSTED_DISCRIMINATOR_SCHEMAS",
    "schema_name",
]
