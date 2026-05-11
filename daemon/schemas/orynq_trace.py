"""orynq_trace_v1 schema — Claude Code task-closure trace anchored to Materios.

The orynq-drain daemon (materios-orynq-drain.service, source at
materios-orynq-hook/drain.mjs) fires one Materios receipt per Claude Code
task closure. Each receipt's payload is the JSON.stringify of the task's
publicView; the on-chain `base_root_sha256` is the trace's *semantic*
Merkle root computed over the trace event tree, NOT the chunk-Merkle of
the JSON encoding.

Cert-daemon's chunk-Merkle path can't accept these — the JSON encoding's
chunk hashes don't match the trace's semantic root. With the
schema-aware dispatcher (task #198), this class registers itself as a
trusted discriminator: chunk integrity is independently verified
(merkle_root(chunks) == content_hash, so the JSON bytes are pinned),
and `base_root_sha256` is accepted on faith as the trace's semantic
root. Downstream consumers (anyone reading the trace JSON) can
independently re-derive the trace's Merkle root and validate the
on-chain commitment.

Lockstep with @fluxpointstudios/orynq-sdk-anchors-materios: the SDK
must set this exact `schema_hash` value when submitting orynq receipts.
"""

from __future__ import annotations

import hashlib


SCHEMA_VERSION_ORYNQ_TRACE_V1 = "orynq_trace_v1"

SCHEMA_HASH_ORYNQ_TRACE_V1: bytes = hashlib.sha256(
    SCHEMA_VERSION_ORYNQ_TRACE_V1.encode("utf-8")
).digest()
