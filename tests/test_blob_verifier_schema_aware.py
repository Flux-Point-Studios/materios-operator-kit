"""Tests for cert-daemon schema-aware verification dispatch (task #198).

Context: live diagnostic on 2026-05-11 found cert-daemon was rejecting 93% of
receipts since 2026-05-08 with `Merkle root mismatch`. The receipts were
`compute_metering_v2` envelopes where on-chain `base_root_sha256` is a
*semantic* root over canonical-CBOR envelope fields, NOT the chunk-Merkle.
Cert-daemon's hardcoded chunk-Merkle path rejected them.

Fix: dispatch on `receipt.schema_hash`. Legacy (zero) → chunk-Merkle.
`compute_metering_v2*` → trust-the-discriminator with chunk-integrity
proof (chunk-Merkle == content_hash). Unknown schemas → reject.

Live evidence the new path needs to accept (audited end-to-end):
  receipt_id        = 0xcb43b6b84784e9d16c7ef01749de6cf872885d9e8858ec68a05ea109d45144ad
  content_hash      = 0xe93878cbf718dab6307c93724f43726c9bddaa44f099d72a6644baae68acfddf
  base_root_sha256  = 0x7d1f3a684834a7393cae2ab0930958aa7005e5628efaaea334fa75d79968933d  (semantic, NOT chunk-Merkle)
  manifest.chunks   = [{sha256: e93878cb..., size: 2161, path: "chunks/0.bin"}]
  schema_hash       = SCHEMA_HASH_COMPUTE_METERING_V2 (= sha256("compute_metering_v2"))
"""

from __future__ import annotations

import asyncio
from typing import Optional

from daemon.blob_verifier import BlobVerifier
from daemon.config import DaemonConfig
from daemon.merkle import sha256
from daemon.models import (
    AttestationLevel,
    BlobManifest,
    ChunkInfo,
    ReceiptRecord,
)
from daemon.schemas import (
    LEGACY_SCHEMA_HASH,
    SCHEMA_HASH_COMPUTE_METERING_V2,
    SCHEMA_HASH_COMPUTE_METERING_V2_1,
    SCHEMA_HASH_ORYNQ_TRACE_V1,
)


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


def _make_receipt(
    receipt_id: str,
    content_hash: bytes,
    base_root: bytes,
    schema_hash: bytes,
    monitor_config_hash: Optional[bytes] = None,
) -> ReceiptRecord:
    return ReceiptRecord(
        receipt_id=receipt_id,
        content_hash=content_hash,
        base_root_sha256=base_root,
        storage_locator_hash=b"\x22" * 32,
        schema_hash=schema_hash,
        base_manifest_hash=b"\x44" * 32,
        safety_manifest_hash=b"\x00" * 32,
        monitor_config_hash=monitor_config_hash if monitor_config_hash is not None else content_hash,
        attestation_evidence_hash=b"\x00" * 32,
    )


# ---------------------------------------------------------------------------
# v2 envelope (schema_hash set) — the canonical case from the 2026-05-11 audit
# ---------------------------------------------------------------------------


def test_v2_envelope_with_correct_chunk_integrity_verifies_at_root():
    """The full live-evidence case from task #198. Single-chunk envelope,
    chunk-Merkle == content_hash, base_root_sha256 is a different value
    (the semantic CBOR root). Must accept at ROOT_VERIFIED — that's what
    restores anchor throughput.
    """
    from aiohttp import web

    envelope_bytes = b"canonical-cbor-envelope-bytes-here-2161-actual-bytes-in-prod"
    envelope_hash = sha256(envelope_bytes)
    semantic_root = sha256(b"some-other-root-over-parsed-fields")
    assert envelope_hash != semantic_root  # sanity

    receipt = _make_receipt(
        "0xcb43b6b84784e9d16c7ef01749de6cf872885d9e8858ec68a05ea109d45144ad",
        content_hash=envelope_hash,
        base_root=semantic_root,
        schema_hash=SCHEMA_HASH_COMPUTE_METERING_V2,
    )

    async def _go():
        async def handler(request):
            return web.Response(body=envelope_bytes, content_type="application/octet-stream")

        app = web.Application()
        app.router.add_get("/chunk", handler)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        try:
            manifest = BlobManifest(
                receipt_id=receipt.receipt_id,
                chunks=[
                    ChunkInfo(
                        index=0,
                        url=f"http://127.0.0.1:{port}/chunk",
                        sha256_hash=envelope_hash,
                        size=len(envelope_bytes),
                    )
                ],
            )
            verifier = BlobVerifier(DaemonConfig())
            return await verifier.verify(receipt, manifest)
        finally:
            await runner.cleanup()

    result = _run(_go())
    assert result.attestation_level == AttestationLevel.ROOT_VERIFIED, (
        f"v2 envelope rejected at {result.attestation_level.name}, errors={result.errors}. "
        f"This was the production bug: cert-daemon kept rejecting because base_root "
        f"is the semantic CBOR root, not chunk-Merkle."
    )
    assert result.chunks_verified == 1
    assert result.computed_root == envelope_hash  # informational


def test_v2_envelope_rejected_when_chunk_merkle_does_not_match_content_hash():
    """Security guard: if the chunk-Merkle doesn't equal content_hash, the
    envelope bytes don't match the on-chain pin. Refuse — this is the gate
    that keeps the trust-the-discriminator path honest. A malicious gateway
    that serves tampered envelope bytes is caught here.
    """
    from aiohttp import web

    real_envelope = b"original-envelope"
    real_envelope_hash = sha256(real_envelope)
    tampered_envelope = b"tampered-envelope-different-bytes"

    # Receipt's content_hash pins the ORIGINAL envelope; gateway returns
    # tampered bytes whose hash is also present in the manifest's chunk hash
    # (consistency at the manifest layer). Cert-daemon recomputes from data
    # and catches the divergence.
    receipt = _make_receipt(
        "0xfe" * 32,
        content_hash=real_envelope_hash,  # on-chain pin
        base_root=sha256(b"semantic-root"),
        schema_hash=SCHEMA_HASH_COMPUTE_METERING_V2,
    )

    async def _go():
        async def handler(request):
            return web.Response(body=tampered_envelope, content_type="application/octet-stream")

        app = web.Application()
        app.router.add_get("/chunk", handler)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        try:
            # Manifest declares the TAMPERED chunk hash (so chunk-hash check
            # passes at the per-chunk level), but the chunk-Merkle won't
            # equal the on-chain content_hash.
            tampered_hash = sha256(tampered_envelope)
            manifest = BlobManifest(
                receipt_id=receipt.receipt_id,
                chunks=[
                    ChunkInfo(
                        index=0,
                        url=f"http://127.0.0.1:{port}/chunk",
                        sha256_hash=tampered_hash,
                        size=len(tampered_envelope),
                    )
                ],
            )
            verifier = BlobVerifier(DaemonConfig())
            return await verifier.verify(receipt, manifest)
        finally:
            await runner.cleanup()

    result = _run(_go())
    assert result.attestation_level < AttestationLevel.ROOT_VERIFIED, (
        "v2 envelope accepted with tampered chunk bytes — chunk integrity "
        "check failed to gate the discriminator path."
    )
    assert any("chunk integrity check failed" in e for e in result.errors)


def test_v2_inline_record_with_empty_chunks_verifies():
    """The original task #186 case but routed via the new discriminator
    path: schema_hash set, chunks=[], base_root == content_hash. Should
    still accept (this is the inline-record convention).
    """
    content_hash = sha256(b"compute-metering-record-2161-bytes")
    receipt = _make_receipt(
        "0x130d8766910ed23e7c414814a7e1cdbb558d8e946b0e0eaf43615cd55032a5ec",
        content_hash=content_hash,
        base_root=content_hash,
        schema_hash=SCHEMA_HASH_COMPUTE_METERING_V2,
    )
    manifest = BlobManifest(receipt_id=receipt.receipt_id, chunks=[], total_size=0)

    async def _go():
        verifier = BlobVerifier(DaemonConfig())
        return await verifier.verify(receipt, manifest)

    result = _run(_go())
    assert result.attestation_level == AttestationLevel.ROOT_VERIFIED, (
        f"v2 inline self-rooted rejected at {result.attestation_level.name}, errors={result.errors}"
    )


def test_orynq_trace_v1_envelope_verifies_with_semantic_root():
    """orynq_trace_v1 receipts use a semantic root (Merkle of the trace
    event tree) as base_root_sha256, while content_hash pins the
    JSON-serialised publicView. The two intentionally differ — the
    discriminator path accepts on faith because chunk integrity is
    verified independently.

    This is the live-evidence shape of receipts produced by
    materios-orynq-drain.service (drain.mjs → anchors-materios
    submitCertifiedReceipt) — the 93% rejection class from task #198/200.
    """
    from aiohttp import web

    json_bytes = b'{"taskId":"task-xyz","events":[{"kind":"prompt"}]}'
    json_hash = sha256(json_bytes)
    semantic_trace_root = sha256(b"merkle-of-trace-events-distinct-value")
    assert json_hash != semantic_trace_root

    receipt = _make_receipt(
        "0xdr" + "00" * 31,
        content_hash=json_hash,
        base_root=semantic_trace_root,
        schema_hash=SCHEMA_HASH_ORYNQ_TRACE_V1,
    )

    async def _go():
        async def handler(request):
            return web.Response(body=json_bytes, content_type="application/octet-stream")

        app = web.Application()
        app.router.add_get("/chunk", handler)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        try:
            manifest = BlobManifest(
                receipt_id=receipt.receipt_id,
                chunks=[ChunkInfo(0, f"http://127.0.0.1:{port}/chunk", json_hash, len(json_bytes))],
            )
            verifier = BlobVerifier(DaemonConfig())
            return await verifier.verify(receipt, manifest)
        finally:
            await runner.cleanup()

    result = _run(_go())
    assert result.attestation_level == AttestationLevel.ROOT_VERIFIED, (
        f"orynq_trace_v1 rejected at {result.attestation_level.name}, errors={result.errors}. "
        f"This is the live production rejection class — drain.mjs receipts must verify."
    )
    assert result.chunks_verified == 1


def test_v2_1_schema_hash_takes_same_path():
    """SCHEMA_HASH_COMPUTE_METERING_V2_1 (Wave 3 Phase 2 attestation-evidence
    variant) is also in TRUSTED_DISCRIMINATOR_SCHEMAS. Verify it dispatches
    the same way.
    """
    from aiohttp import web

    envelope_bytes = b"v2_1-envelope-with-attestation-evidence"
    envelope_hash = sha256(envelope_bytes)
    receipt = _make_receipt(
        "0xa1" * 32,
        content_hash=envelope_hash,
        base_root=sha256(b"semantic-root-v2-1"),
        schema_hash=SCHEMA_HASH_COMPUTE_METERING_V2_1,
    )

    async def _go():
        async def handler(request):
            return web.Response(body=envelope_bytes, content_type="application/octet-stream")

        app = web.Application()
        app.router.add_get("/chunk", handler)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        try:
            manifest = BlobManifest(
                receipt_id=receipt.receipt_id,
                chunks=[ChunkInfo(0, f"http://127.0.0.1:{port}/chunk", envelope_hash, len(envelope_bytes))],
            )
            verifier = BlobVerifier(DaemonConfig())
            return await verifier.verify(receipt, manifest)
        finally:
            await runner.cleanup()

    result = _run(_go())
    assert result.attestation_level == AttestationLevel.ROOT_VERIFIED


# ---------------------------------------------------------------------------
# Legacy path (schema_hash = 0x00...) — must remain unchanged
# ---------------------------------------------------------------------------


def test_legacy_zero_schema_still_uses_chunk_merkle_path():
    """Receipts that pre-date the schema_hash discriminator (and any blob
    upload that doesn't claim to be a metering envelope) must continue to
    require chunk-Merkle == base_root_sha256.
    """
    from aiohttp import web

    chunk_data = b"a-normal-blob-chunk"
    chunk_hash = sha256(chunk_data)
    # Legacy: base_root_sha256 IS the chunk-Merkle (single-leaf = leaf)
    receipt = _make_receipt(
        "0xcd" * 32,
        content_hash=b"\x99" * 32,
        base_root=chunk_hash,
        schema_hash=LEGACY_SCHEMA_HASH,
    )

    async def _go():
        async def handler(request):
            return web.Response(body=chunk_data, content_type="application/octet-stream")

        app = web.Application()
        app.router.add_get("/chunk", handler)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        try:
            manifest = BlobManifest(
                receipt_id=receipt.receipt_id,
                chunks=[ChunkInfo(0, f"http://127.0.0.1:{port}/chunk", chunk_hash, len(chunk_data))],
            )
            verifier = BlobVerifier(DaemonConfig())
            return await verifier.verify(receipt, manifest)
        finally:
            await runner.cleanup()

    result = _run(_go())
    assert result.attestation_level == AttestationLevel.ROOT_VERIFIED
    assert result.chunks_verified == 1


def test_legacy_blob_rejected_on_merkle_mismatch_unchanged():
    """Regression guard: real blob whose Merkle disagrees with on-chain
    base_root_sha256 must still be rejected on the legacy path.
    """
    from aiohttp import web

    chunk_data = b"a-normal-blob-chunk"
    chunk_hash = sha256(chunk_data)
    wrong_root = sha256(b"this-is-not-the-merkle-root")
    receipt = _make_receipt(
        "0xcd" * 32,
        content_hash=b"\x99" * 32,
        base_root=wrong_root,
        schema_hash=LEGACY_SCHEMA_HASH,
    )

    async def _go():
        async def handler(request):
            return web.Response(body=chunk_data, content_type="application/octet-stream")

        app = web.Application()
        app.router.add_get("/chunk", handler)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        try:
            manifest = BlobManifest(
                receipt_id=receipt.receipt_id,
                chunks=[ChunkInfo(0, f"http://127.0.0.1:{port}/chunk", chunk_hash, len(chunk_data))],
            )
            verifier = BlobVerifier(DaemonConfig())
            return await verifier.verify(receipt, manifest)
        finally:
            await runner.cleanup()

    result = _run(_go())
    assert result.attestation_level < AttestationLevel.ROOT_VERIFIED
    assert any("Merkle root mismatch" in e for e in result.errors)


# ---------------------------------------------------------------------------
# Unknown-schema rejection
# ---------------------------------------------------------------------------


def test_v2_inline_record_with_zero_content_hash_is_rejected_sentinel_guard():
    """Security guard (review #1, 2026-05-11): the chunks=[] + base_root
    == content_hash branch must REJECT when content_hash is the all-zero
    sentinel. Otherwise a submitter could grind a zero content_hash +
    base_root and free-ride ROOT_VERIFIED via the discriminator path
    without uploading any envelope at all.
    """
    zero_hash = b"\x00" * 32
    receipt = _make_receipt(
        "0xab" * 32,
        content_hash=zero_hash,
        base_root=zero_hash,
        schema_hash=SCHEMA_HASH_COMPUTE_METERING_V2,
    )
    manifest = BlobManifest(receipt_id=receipt.receipt_id, chunks=[], total_size=0)

    async def _go():
        verifier = BlobVerifier(DaemonConfig())
        return await verifier.verify(receipt, manifest)

    result = _run(_go())
    assert result.attestation_level < AttestationLevel.ROOT_VERIFIED, (
        f"sentinel zero-bytes content_hash was accepted at {result.attestation_level.name} "
        "— the all-zero sentinel guard is the only thing keeping a malicious "
        "submitter from grinding chunks=[] + base_root==content_hash==0x00 for free."
    )


def test_malformed_schema_hash_length_is_rejected_at_dispatch():
    """Defense against RPC decode drift / codec bugs: a schema_hash that
    isn't exactly 32 bytes must NOT fall through to "unknown schema" and
    look like a class-registration omission. Surface the malformation
    explicitly so operators see the real cause (substrate_client decode
    path) in the log.
    """
    chunk_data = b"data"
    chunk_hash = sha256(chunk_data)
    # Pass a 16-byte schema_hash — half-length, plausible under a partial
    # decode bug.
    short_schema = b"\x77" * 16
    receipt = _make_receipt(
        "0xbe" * 32,
        content_hash=chunk_hash,
        base_root=chunk_hash,
        schema_hash=short_schema,
    )

    async def _go():
        manifest = BlobManifest(
            receipt_id=receipt.receipt_id,
            chunks=[ChunkInfo(0, "http://nowhere/chunk", chunk_hash, len(chunk_data))],
        )
        verifier = BlobVerifier(DaemonConfig())
        return await verifier.verify(receipt, manifest)

    result = _run(_go())
    assert result.attestation_level < AttestationLevel.ROOT_VERIFIED
    # Error should call out the length specifically — not the generic
    # "Unknown schema_hash" message — so operators can find the decode path.
    assert any("not a 32-byte" in e or "expected 32-byte" in e for e in result.errors), (
        f"malformed schema_hash didn't surface a length-specific error: {result.errors}"
    )


def test_unknown_schema_hash_is_rejected_not_silently_passed():
    """A receipt with a schema_hash we don't recognize must NOT fall through
    to the legacy chunk-Merkle path. New schemas must register in
    `daemon/schemas/` before they verify.
    """
    bogus_schema = sha256(b"some-future-schema-we-do-not-know-about-yet")
    chunk_data = b"data"
    chunk_hash = sha256(chunk_data)
    receipt = _make_receipt(
        "0xbe" * 32,
        content_hash=chunk_hash,
        base_root=chunk_hash,
        schema_hash=bogus_schema,
    )

    async def _go():
        # No HTTP needed — verifier should reject before touching network.
        manifest = BlobManifest(
            receipt_id=receipt.receipt_id,
            chunks=[ChunkInfo(0, "http://nowhere/chunk", chunk_hash, len(chunk_data))],
        )
        verifier = BlobVerifier(DaemonConfig())
        return await verifier.verify(receipt, manifest)

    result = _run(_go())
    assert result.attestation_level < AttestationLevel.ROOT_VERIFIED
    assert any("Unknown schema_hash" in e for e in result.errors)
