"""Tests for the cert-daemon self-rooted manifest path (task #186).

Bug context (live evidence captured 2026-05-06 23:57 UTC, Node-2 cert-daemon):

    [CRITICAL] daemon.blob_verifier: Merkle root mismatch for 0x130d8766...:
        on-chain=2fdf7b9e..., computed=0000000000000000000000000000000000000000000000000000000000000000
    [INFO]     daemon.cert_daemon: Verification ... level=HASH_VERIFIED, chunks=0/0
    [ERROR]    daemon.cert_daemon: Verification REJECTED for 0x130d8766...:
                level=HASH_VERIFIED (need ROOT_VERIFIED)

Root cause:
  The blob-gateway's `compute_metering_v2` route stores manifests as
  self-rooted: `{ schema, record, chunks: [], rootHash: content_hash }`.
  The on-chain receipt has `base_root_sha256 == content_hash` because the
  receipt-submitter copies the `rootHash` from the upload-completion
  callback into `submit_receipt_v2(... baseRootSha256ContentHash ...)`.

  The cert-daemon's `BlobVerifier.verify` always applies the blob-style
  Merkle compute path:
    - len(chunks)=0  →  computed_root = merkle_root([]) = b'\\x00' * 32
    - on-chain      = content_hash (e.g. 0x2fdf7b9e...)
    - mismatch      → returns HASH_VERIFIED (Merkle-mismatch sentinel)
    - daemon gate (task #184)  → REJECTED at HASH_VERIFIED < ROOT_VERIFIED.

  Net effect: every `compute_metering_v2` record posted on chain stays
  uncertified forever. The Wave 1+2 billing API can never anchor metering
  records to Cardano L1 because the cert chain stalls.

Fix design (Option A, defended):
  Special-case self-rooted manifests in `BlobVerifier.verify`.

  Self-rooted is signalled by a TWO-PART, on-chain-derived predicate:
    1. `manifest.chunks == []`   (the manifest claims no blob chunks)
    2. `receipt.base_root_sha256 == receipt.content_hash`   (on-chain
       confirms the receipt was submitted with rootHash = content_hash)

  Both conditions must hold. Either alone is insufficient:
    - chunks=[] alone could be a malformed blob upload; the on-chain
      base_root would NOT match content_hash in that case.
    - base_root == content_hash alone could be an artefact of a single-
      chunk blob whose data hashes equal its content_hash; the manifest
      would have a non-empty chunks list with a real URL.

  When both hold, return ROOT_VERIFIED directly — there is no chunk data
  to fetch (records aren't blobs, they're inline in the gateway's
  manifest body), and the cert_hash determinism is preserved because
  every committee member sees the SAME on-chain (content_hash,
  base_root_sha256) pair and the SAME (manifest.chunks=[]) condition.

Why NOT Option B (re-derive content_hash from canonical CBOR of `record`):
  - cert-daemon would need to import the cross-language canonical CBOR
    encoder (currently TS-only in `services/blob-gateway/src/schemas/
    compute_metering_v2.ts`). That's >300 LOC of careful float64 / map-
    sort / byte-prefix code. Cannot ship in <2h with confidence.
  - Locator endpoint `/locators/:receiptId` strips `record` from the
    response body — would require either a new gateway route or auth-
    gating the existing `/blobs/:contentHash/manifest` route, which the
    parallel agent on PR #134 is still fixing.
  - Trust delta vs Option A is small: the worker's sr25519 signature has
    ALREADY been verified by the gateway against canonical CBOR before
    the manifest is stored. If the gateway is malicious, it could lie
    about chunks=[] just as easily as it could lie about a CBOR record.
    The real defense is the M-of-N committee + Cardano anchor, not a
    second canonical encoder in the cert-daemon.

M-of-N committee determinism (per `feedback_mofn_hash_determinism.md`):
  cert_hash is built in `cert_builder.py` from purely on-chain fields
  (chain_id, receipt_id, content_hash, base_root_sha256,
  storage_locator_hash, retention_days, cert_schema_version). Neither
  the new self-rooted check nor the verification level affects cert_hash
  — `attestation_level` arg is accepted for API compatibility but
  ignored (set to `CERT_ATTESTATION_LEVEL_PINNED`). So this fix is safe
  to roll out attester-by-attester: a v1 attester (without this patch)
  rejects a v2 receipt at HASH_VERIFIED while a v3 attester (with this
  patch) accepts at ROOT_VERIFIED, but if both reach ROOT_VERIFIED they
  produce byte-identical cert_hashes. M-of-N still requires `M`
  attesters on the new code; rollout plan is in the PR.
"""

from __future__ import annotations

import asyncio
from typing import Optional

import pytest

from daemon.blob_verifier import BlobVerifier
from daemon.config import DaemonConfig
from daemon.merkle import sha256
from daemon.models import (
    AttestationLevel,
    BlobManifest,
    ChunkInfo,
    ReceiptRecord,
)


# --- helpers ---------------------------------------------------------------


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


def _make_v2_receipt(
    receipt_id: str,
    content_hash: bytes,
    base_root: Optional[bytes] = None,
) -> ReceiptRecord:
    """Build a receipt that mimics what `submit_receipt_v2` produces for
    `compute_metering_v2`: `base_root_sha256 == content_hash` because the
    upload-completion callback's `rootHash` is set to `content_hash`.

    By default (`base_root=None`) returns the v2-canonical case
    (base_root == content_hash). Pass an explicit `base_root` to
    construct an out-of-band failure case (Merkle compute should still
    fire and reject).
    """
    return ReceiptRecord(
        receipt_id=receipt_id,
        content_hash=content_hash,
        base_root_sha256=base_root if base_root is not None else content_hash,
        storage_locator_hash=b"\x22" * 32,
        schema_hash=b"\x00" * 32,  # gateway/submitter currently zero this
        base_manifest_hash=b"\x44" * 32,
        safety_manifest_hash=b"\x00" * 32,
        monitor_config_hash=b"\x00" * 32,
        attestation_evidence_hash=b"\x00" * 32,
    )


# --- tests -----------------------------------------------------------------


def test_self_rooted_manifest_verifies_directly():
    """Canonical case: a v2 self-rooted manifest verifies at ROOT_VERIFIED.

    Setup mirrors live receipt 0x130d8766... captured 2026-05-06 23:57:
      - manifest.chunks == []
      - on-chain content_hash == on-chain base_root_sha256 (here
        0x2fdf7b9e8eb56096d10b37cf497e8a27f71dc4c22d7daaa8814195397e8b0e20)

    No HTTP server is needed because there are no chunks to fetch — the
    verifier should detect self-rooted-ness from `(manifest, receipt)`
    alone and short-circuit to ROOT_VERIFIED.
    """
    content_hash = bytes.fromhex(
        "2fdf7b9e8eb56096d10b37cf497e8a27f71dc4c22d7daaa8814195397e8b0e20"
    )
    receipt = _make_v2_receipt(
        "0x130d8766910ed23e7c414814a7e1cdbb558d8e946b0e0eaf43615cd55032a5ec",
        content_hash=content_hash,
    )
    manifest = BlobManifest(
        receipt_id=receipt.receipt_id,
        chunks=[],
        total_size=0,
    )

    async def _go():
        verifier = BlobVerifier(DaemonConfig())
        return await verifier.verify(receipt, manifest)

    result = _run(_go())

    # The load-bearing assertion: self-rooted reaches ROOT_VERIFIED so
    # the daemon's gate (MIN_ATTESTATION_LEVEL_TO_ATTEST = ROOT_VERIFIED)
    # accepts the cert.
    assert result.attestation_level == AttestationLevel.ROOT_VERIFIED, (
        f"v2 self-rooted manifest verified at {result.attestation_level.name}, "
        f"expected ROOT_VERIFIED. The daemon would silently leave every "
        f"compute_metering_v2 receipt uncertified forever."
    )
    assert result.errors == [], f"unexpected errors: {result.errors}"
    # No chunks were fetched, so chunks_verified stays at 0.
    assert result.chunks_verified == 0
    assert result.chunks_total == 0


def test_self_rooted_manifest_rejected_when_base_root_differs_from_content_hash():
    """Negative case: a manifest claims chunks=[] but the on-chain
    `base_root_sha256` does NOT equal `content_hash`. This is NOT a v2
    self-rooted receipt — must NOT short-circuit. Stay below
    ROOT_VERIFIED so the daemon rejects it (no false-positive ROOT
    attestations on malformed blob uploads).
    """
    content_hash = sha256(b"v2-record-canonical-cbor")
    different_root = sha256(b"some-merkle-root-from-actual-blob")
    assert content_hash != different_root  # sanity

    receipt = _make_v2_receipt(
        "0xab" * 32,
        content_hash=content_hash,
        base_root=different_root,  # CRITICAL: differs from content_hash
    )
    manifest = BlobManifest(
        receipt_id=receipt.receipt_id,
        chunks=[],
        total_size=0,
    )

    async def _go():
        verifier = BlobVerifier(DaemonConfig())
        return await verifier.verify(receipt, manifest)

    result = _run(_go())

    # Must NOT reach ROOT_VERIFIED — the on-chain commitment
    # (base_root_sha256) doesn't match content_hash, so we cannot trust
    # the self-rooted shortcut.
    assert result.attestation_level < AttestationLevel.ROOT_VERIFIED, (
        f"verifier short-circuited on chunks=[] alone (level="
        f"{result.attestation_level.name}); must require base_root_sha256 "
        f"== content_hash too."
    )


def test_blob_manifest_with_chunks_unaffected_by_self_rooted_path():
    """Regression guard: a normal blob receipt (non-empty chunks, with a
    Merkle root that legitimately matches its leaves) must STILL go
    through the Merkle compute path and reach ROOT_VERIFIED. This proves
    the self-rooted shortcut doesn't bypass the Merkle check for real
    blobs.
    """
    from aiohttp import web

    chunk_data = b"hello-blob"
    chunk_hash = sha256(chunk_data)
    receipt = _make_v2_receipt(
        "0xcd" * 32,
        content_hash=b"\x99" * 32,  # something else entirely
        base_root=chunk_hash,        # 1-leaf Merkle root == leaf hash
    )

    async def _go():
        async def handler(request):
            return web.Response(
                body=chunk_data, content_type="application/octet-stream"
            )

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
                        sha256_hash=chunk_hash,
                        size=len(chunk_data),
                    )
                ],
            )
            verifier = BlobVerifier(DaemonConfig())
            return await verifier.verify(receipt, manifest)
        finally:
            await runner.cleanup()

    result = _run(_go())
    # Merkle compute MUST run for non-empty chunks; the chunk's hash IS
    # the 1-leaf Merkle root, which matches base_root_sha256.
    assert result.attestation_level == AttestationLevel.ROOT_VERIFIED
    assert result.chunks_verified == 1
    assert result.computed_root == chunk_hash


def test_self_rooted_short_circuit_does_not_change_cert_hash_inputs():
    """M-of-N determinism guard: the self-rooted path must NOT introduce
    any operator-local state into the verification result that could
    leak into cert_hash.

    Spec-219 fixed this structurally — `scale_cert_encode` takes ONLY
    on-chain fields (chain_genesis, receipt_id, 3× hash fields). There
    is no `attestation_level` or `epoch` parameter at all. So even if
    the self-rooted short-circuit returns ROOT_VERIFIED for one attester
    and HASH_VERIFIED for another, the cert_hash is identical by
    construction. This test guards the contract: the signature must
    never gain a parameter sourced from local verifier state.
    """
    import inspect
    from daemon.cert_builder import (
        CERT_ATTESTATION_LEVEL,
        CERT_EPOCH_PLACEHOLDER,
        scale_cert_encode,
        scale_cert_hash,
    )

    # The set of parameters accepted by scale_cert_encode MUST be
    # exactly the on-chain field set — no attestation_level, no epoch,
    # no retention_days, no cert_schema_version. If anyone adds one,
    # this assertion fires and the M-of-N determinism rule is preserved.
    sig = inspect.signature(scale_cert_encode)
    assert set(sig.parameters.keys()) == {
        "chain_genesis",
        "receipt_id",
        "content_hash",
        "base_root_sha256",
        "storage_locator_hash",
    }, (
        f"scale_cert_encode signature changed: {list(sig.parameters)}. "
        "Adding any operator-local parameter (attestation_level, epoch, "
        "retention_days, ...) re-opens the M-of-N CertHashMismatch class "
        "that spec-219 closed. See feedback_mofn_hash_determinism.md."
    )

    # Sanity: produces a stable 32-byte hash regardless of which call
    # site invoked it (positional or keyword).
    common = dict(
        chain_genesis="ab" * 32,
        receipt_id="0x" + "cd" * 32,
        content_hash=b"\x11" * 32,
        base_root_sha256=b"\x11" * 32,  # self-rooted: == content_hash
        storage_locator_hash=b"\x22" * 32,
    )
    assert len(scale_cert_hash(**common)) == 32

    # Pinned-constants tripwire (cross-checked against pallet types.rs):
    assert CERT_ATTESTATION_LEVEL == 2  # HASH_VERIFIED
    assert CERT_EPOCH_PLACEHOLDER == 0


def test_self_rooted_when_base_root_equals_content_hash_but_chunks_present():
    """Edge case: on-chain base_root == content_hash AND manifest has
    chunks (e.g. a v1 blob upload that happens to land on a Merkle root
    equal to the content hash, which happens for any 1-chunk blob whose
    chunk == content_hash — a CSV file uploaded as one chunk for example).

    The self-rooted shortcut MUST still defer to the Merkle path and
    only declare ROOT_VERIFIED if the actual chunk-derived root matches.
    This protects the gateway-trust model: when chunks ARE provided, we
    must verify them. The shortcut applies only when chunks=[] — the
    gateway is explicitly telling us "no blob to verify, the record is
    inline".
    """
    from aiohttp import web

    chunk_data = b"contents-of-the-csv"
    chunk_hash = sha256(chunk_data)
    # Receipt where content_hash = base_root_sha256 = chunk_hash. This
    # could legitimately happen for a 1-chunk blob whose hash equals
    # the upload's content_hash.
    receipt = _make_v2_receipt(
        "0xef" * 32,
        content_hash=chunk_hash,
        base_root=chunk_hash,
    )

    async def _go():
        async def handler(request):
            return web.Response(
                body=chunk_data, content_type="application/octet-stream"
            )

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
                        sha256_hash=chunk_hash,
                        size=len(chunk_data),
                    )
                ],
            )
            verifier = BlobVerifier(DaemonConfig())
            return await verifier.verify(receipt, manifest)
        finally:
            await runner.cleanup()

    result = _run(_go())
    # Should reach ROOT_VERIFIED via the actual Merkle compute path,
    # NOT the self-rooted shortcut. We verify by checking that a chunk
    # was fetched.
    assert result.attestation_level == AttestationLevel.ROOT_VERIFIED
    assert result.chunks_verified == 1, (
        "verifier short-circuited via self-rooted path despite chunks "
        "being present — must defer to Merkle compute when manifest "
        "claims any chunks."
    )
