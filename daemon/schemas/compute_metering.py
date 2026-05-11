"""compute_metering schema hashes — kept lockstep with the gateway TS.

Gateway source of truth:
  services/blob-gateway/src/schemas/compute_metering_v2.ts

    export const SCHEMA_VERSION       = "compute_metering_v2";
    export const SCHEMA_VERSION_V2_1  = "compute_metering_v2_1";
    export const SCHEMA_HASH_HEX      = createHash("sha256")
                                        .update(SCHEMA_VERSION, "utf-8")
                                        .digest("hex");
    export const SCHEMA_HASH_V2_1_HEX = createHash("sha256")
                                        .update(SCHEMA_VERSION_V2_1, "utf-8")
                                        .digest("hex");

A drift between the TS and Python values would silently disable verification
for the affected class. The constants below are the byte values; both
encoders MUST produce identical 32-byte outputs (sha256 over UTF-8 of the
exact version string).
"""

from __future__ import annotations

import hashlib
from typing import FrozenSet, Optional


SCHEMA_VERSION_COMPUTE_METERING_V2 = "compute_metering_v2"
SCHEMA_VERSION_COMPUTE_METERING_V2_1 = "compute_metering_v2_1"

SCHEMA_HASH_COMPUTE_METERING_V2: bytes = hashlib.sha256(
    SCHEMA_VERSION_COMPUTE_METERING_V2.encode("utf-8")
).digest()
SCHEMA_HASH_COMPUTE_METERING_V2_1: bytes = hashlib.sha256(
    SCHEMA_VERSION_COMPUTE_METERING_V2_1.encode("utf-8")
).digest()

LEGACY_SCHEMA_HASH: bytes = b"\x00" * 32

TRUSTED_DISCRIMINATOR_SCHEMAS: FrozenSet[bytes] = frozenset(
    {
        SCHEMA_HASH_COMPUTE_METERING_V2,
        SCHEMA_HASH_COMPUTE_METERING_V2_1,
    }
)


def schema_name(schema_hash: bytes) -> Optional[str]:
    """Return a human-readable name for a known schema hash, or None.

    Used only in logs — never in cert_hash inputs, so a future name change
    doesn't affect M-of-N determinism.
    """
    if schema_hash == LEGACY_SCHEMA_HASH:
        return "legacy_blob"
    if schema_hash == SCHEMA_HASH_COMPUTE_METERING_V2:
        return SCHEMA_VERSION_COMPUTE_METERING_V2
    if schema_hash == SCHEMA_HASH_COMPUTE_METERING_V2_1:
        return SCHEMA_VERSION_COMPUTE_METERING_V2_1
    return None
