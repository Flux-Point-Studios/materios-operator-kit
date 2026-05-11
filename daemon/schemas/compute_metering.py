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


SCHEMA_VERSION_COMPUTE_METERING_V2 = "compute_metering_v2"
SCHEMA_VERSION_COMPUTE_METERING_V2_1 = "compute_metering_v2_1"

SCHEMA_HASH_COMPUTE_METERING_V2: bytes = hashlib.sha256(
    SCHEMA_VERSION_COMPUTE_METERING_V2.encode("utf-8")
).digest()
SCHEMA_HASH_COMPUTE_METERING_V2_1: bytes = hashlib.sha256(
    SCHEMA_VERSION_COMPUTE_METERING_V2_1.encode("utf-8")
).digest()

LEGACY_SCHEMA_HASH: bytes = b"\x00" * 32

# Canonical TRUSTED_DISCRIMINATOR_SCHEMAS + schema_name() live in
# `daemon/schemas/__init__.py` so additional schema modules can register
# without circular imports. Importers should pull from `daemon.schemas`
# (the package), not this file directly.
