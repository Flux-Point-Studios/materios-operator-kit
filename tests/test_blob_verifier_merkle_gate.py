"""Tests for the cert-daemon verification gate (task #184).

Bug context (live evidence captured 2026-04-26 23:30 UTC, Node-2 cert-daemon
:task-180): the daemon logged a `CRITICAL: Merkle root mismatch` and then
proceeded to save the cert and submit it on-chain anyway.

    [CRITICAL] daemon.blob_verifier: Merkle root mismatch for 0x8912b3e0…:
        on-chain=652fe92756…, computed=cdb1a929c0…
    [INFO]     daemon.cert_daemon: Verification ... level=HASH_VERIFIED, chunks=1/1
    [INFO]     daemon.cert_store: Saved cert ...
    [INFO]     daemon.substrate_client: Cert attested for 0x8912b3e0…

Root cause:
  - `BlobVerifier.verify` sets `attestation_level = HASH_VERIFIED` on line 62
    BEFORE the Merkle-root comparison.
  - On mismatch (line 71-79) it appends an error and logs CRITICAL but does
    NOT downgrade `attestation_level` back to FETCHED.
  - `CertDaemon.process_receipt` gates on
        `if verification.attestation_level < AttestationLevel.HASH_VERIFIED:`
    which lets HASH_VERIFIED through. So a verifier that detected a mismatch
    still passes the gate.

Attack surface:
  A malicious blob-gateway returns chunks whose hashes are internally
  consistent with a manifest the gateway also serves, but those chunks do
  NOT correspond to the on-chain `base_root_sha256`. The cert-daemon
  attests anyway, an honest user later fetches the blob expecting the
  on-chain root, and finds the data was lying.

Cert format note (PR #5):
  The cert body itself is now `(chain_id, receipt_id, content_hash,
  base_root_sha256, storage_locator_hash, retention_days,
  cert_schema_version)` — pulled entirely from on-chain state + config.
  Nothing the attester actually verified is in the cert body. So the
  off-chain pre-attestation gate is the ONLY mechanism stopping a wrong-
  content blob from getting an availability cert. That gate must require
  the Merkle root match the on-chain commitment.

Fix: gate must require `attestation_level >= ROOT_VERIFIED`.

These tests use the real `BlobVerifier` against an in-process aiohttp
server so we exercise the actual fetch + hash + Merkle code, not mocks.
"""

from __future__ import annotations

import asyncio
import hashlib
from typing import Optional

import pytest
from aiohttp import web

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


def _make_receipt(receipt_id: str, base_root: bytes) -> ReceiptRecord:
    return ReceiptRecord(
        receipt_id=receipt_id,
        content_hash=b"\x11" * 32,
        base_root_sha256=base_root,
        storage_locator_hash=b"\x22" * 32,
        schema_hash=b"\x33" * 32,
        base_manifest_hash=b"\x44" * 32,
        safety_manifest_hash=b"\x55" * 32,
        monitor_config_hash=b"\x66" * 32,
        attestation_evidence_hash=b"\x77" * 32,
    )


class _ChunkServer:
    """Tiny aiohttp server that serves a single chunk at /chunk."""

    def __init__(self, chunk_bytes: bytes):
        self.chunk_bytes = chunk_bytes
        self.runner: Optional[web.AppRunner] = None
        self.site: Optional[web.TCPSite] = None
        self.port: int = 0

    async def start(self):
        async def handler(request):
            return web.Response(body=self.chunk_bytes, content_type="application/octet-stream")

        app = web.Application()
        app.router.add_get("/chunk", handler)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await self.site.start()
        # Get the actual bound port
        self.port = self.site._server.sockets[0].getsockname()[1]

    async def stop(self):
        if self.runner:
            await self.runner.cleanup()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}/chunk"


# --- tests -----------------------------------------------------------------


def test_single_chunk_correct_data_reaches_root_verified():
    """Honest path: 1-chunk blob whose sha256 == on-chain base_root.

    For 1-leaf trees, `merkle_root([h]) == h`, so when chunk_hash matches
    base_root_sha256, the verifier reaches ROOT_VERIFIED.
    """
    chunk_data = b"hello-materios"
    chunk_hash = sha256(chunk_data)
    receipt = _make_receipt("0xaa" * 32, base_root=chunk_hash)

    async def _go():
        server = _ChunkServer(chunk_data)
        await server.start()
        try:
            manifest = BlobManifest(
                receipt_id=receipt.receipt_id,
                chunks=[ChunkInfo(index=0, url=server.url, sha256_hash=chunk_hash, size=len(chunk_data))],
            )
            verifier = BlobVerifier(DaemonConfig())
            return await verifier.verify(receipt, manifest)
        finally:
            await server.stop()

    result = _run(_go())
    assert result.attestation_level == AttestationLevel.ROOT_VERIFIED
    assert result.errors == []
    assert result.computed_root == receipt.base_root_sha256


def test_single_chunk_wrong_onchain_root_does_not_reach_root_verified():
    """Attack path: blob-gateway returns chunk whose hash matches the
    manifest but does NOT match the on-chain base_root. Verifier MUST NOT
    return >= ROOT_VERIFIED.

    This is the exact scenario from receipt 0x8912b3e0… in the live log.
    """
    chunk_data = b"malicious-content"
    chunk_hash = sha256(chunk_data)
    wrong_onchain_root = sha256(b"the-real-content-the-user-uploaded")
    assert chunk_hash != wrong_onchain_root  # sanity

    receipt = _make_receipt("0xbb" * 32, base_root=wrong_onchain_root)

    async def _go():
        server = _ChunkServer(chunk_data)
        await server.start()
        try:
            manifest = BlobManifest(
                receipt_id=receipt.receipt_id,
                # Manifest is gateway-controlled, so a malicious gateway can
                # claim sha256_hash matches the chunk_data it serves. Both
                # internal-consistent (chunk_hash check passes) and
                # untrustworthy (doesn't match on-chain root).
                chunks=[ChunkInfo(index=0, url=server.url, sha256_hash=chunk_hash, size=len(chunk_data))],
            )
            verifier = BlobVerifier(DaemonConfig())
            return await verifier.verify(receipt, manifest)
        finally:
            await server.stop()

    result = _run(_go())
    # CRITICAL: must NOT be ROOT_VERIFIED (or higher) — that's the gate the
    # daemon now uses to decide whether to attest.
    assert result.attestation_level < AttestationLevel.ROOT_VERIFIED, (
        f"Verifier returned {result.attestation_level.name} for a Merkle-mismatched blob — "
        f"daemon would attest a wrong-content cert. This is the task #184 bug."
    )
    # The verifier still records what it computed for forensic logs.
    assert result.computed_root == chunk_hash
    assert any("Merkle root mismatch" in err for err in result.errors)


def test_chunk_hash_mismatch_stops_at_fetched():
    """Earlier-stage failure: chunk content doesn't even match the manifest's
    declared chunk hash. Must stay at FETCHED (or below).
    """
    chunk_data = b"actual-bytes-served"
    declared_hash = sha256(b"different-bytes-claimed-in-manifest")
    receipt = _make_receipt("0xcc" * 32, base_root=declared_hash)

    async def _go():
        server = _ChunkServer(chunk_data)
        await server.start()
        try:
            manifest = BlobManifest(
                receipt_id=receipt.receipt_id,
                chunks=[ChunkInfo(index=0, url=server.url, sha256_hash=declared_hash, size=len(chunk_data))],
            )
            verifier = BlobVerifier(DaemonConfig())
            return await verifier.verify(receipt, manifest)
        finally:
            await server.stop()

    result = _run(_go())
    assert result.attestation_level == AttestationLevel.FETCHED
    assert any("hash mismatch" in err for err in result.errors)


# --- daemon-level gate test ------------------------------------------------


def test_daemon_gate_rejects_hash_verified_only():
    """End-to-end-ish: simulate process_receipt's gate logic explicitly.

    The gate that protects on-chain attestation lives in
    `cert_daemon.py:process_receipt` as:

        if verification.attestation_level < AttestationLevel.<MIN>: return

    Pre-fix: MIN was HASH_VERIFIED → Merkle-mismatched receipts (which leave
    level=HASH_VERIFIED in the result) silently pass the gate. Post-fix:
    MIN must be ROOT_VERIFIED.
    """
    from daemon.cert_daemon import MIN_ATTESTATION_LEVEL_TO_ATTEST

    # Before this fix, MIN_ATTESTATION_LEVEL_TO_ATTEST didn't exist —
    # the import alone tells us the explicit constant has been added.
    # The constant must be at ROOT_VERIFIED so HASH_VERIFIED-only
    # verifications are rejected.
    assert MIN_ATTESTATION_LEVEL_TO_ATTEST == AttestationLevel.ROOT_VERIFIED
    # Simulate a verification result that's HASH_VERIFIED only (Merkle
    # mismatch case): it must be BELOW the threshold.
    assert AttestationLevel.HASH_VERIFIED < MIN_ATTESTATION_LEVEL_TO_ATTEST
    # ROOT_VERIFIED must be allowed through.
    assert AttestationLevel.ROOT_VERIFIED >= MIN_ATTESTATION_LEVEL_TO_ATTEST


def test_daemon_does_not_attest_on_merkle_mismatch():
    """Integration-shaped test: drive `CertDaemon.process_receipt` with a
    receipt whose blob has a wrong-content chunk (Merkle mismatch case),
    and assert that:
      - cert_store.save() is NEVER called
      - submit_availability_cert() is NEVER called

    Mocks at the substrate + locator + cert_store seams (same style as
    test_cert_daemon.py). The blob_verifier runs for real against an
    in-process aiohttp server, so we exercise the actual fetch+hash+merkle
    code path and the actual gate decision.
    """
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    from daemon.cert_daemon import CertDaemon
    from daemon.blob_verifier import BlobVerifier
    from daemon.locator_registry import LocatorRegistry  # noqa: F401  for type ref

    chunk_data = b"y" * 128
    chunk_hash = sha256(chunk_data)
    wrong_root = sha256(b"on-chain-truth")
    receipt = _make_receipt("0xee" * 32, base_root=wrong_root)
    receipt_id = receipt.receipt_id

    async def _go():
        server = _ChunkServer(chunk_data)
        await server.start()
        try:
            config = DaemonConfig()
            config.checkpoint_enabled = False
            config.content_validation_enabled = False

            daemon = CertDaemon.__new__(CertDaemon)
            daemon.config = config
            daemon.client = MagicMock()
            daemon.client.get_receipt.return_value = receipt
            daemon.client.keypair = SimpleNamespace(
                ss58_address="5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY",
                public_key=b"\x00" * 32,
            )

            # Real verifier (THIS is the code path we want to exercise).
            daemon.verifier = BlobVerifier(config)

            # Mocked locator: returns a manifest pointing at our local server.
            mock_manifest = BlobManifest(
                receipt_id=receipt_id,
                chunks=[ChunkInfo(index=0, url=server.url, sha256_hash=chunk_hash, size=len(chunk_data))],
            )

            class _AsyncLocator:
                async def resolve(self, rid, content_hash=None):
                    return mock_manifest

            daemon.locator = _AsyncLocator()
            daemon.content_validator = MagicMock()
            daemon.cert_store = MagicMock()
            daemon.cert_store.exists.return_value = False
            daemon.checkpointer = MagicMock()
            daemon.pending = {}
            daemon._notified_ids = {}
            daemon._pruned_warned_blocks = set()
            daemon._running = True

            await daemon.process_receipt(receipt_id)

            return daemon
        finally:
            await server.stop()

    daemon = _run(_go())
    # The load-bearing assertions — ANY of these failing is a security bug.
    assert not daemon.cert_store.save.called, (
        f"cert_store.save called {daemon.cert_store.save.call_count}× — "
        f"daemon saved a cert despite Merkle mismatch (task #184 bug)."
    )
    assert not daemon.client.submit_availability_cert.called, (
        f"substrate_client.submit_availability_cert called "
        f"{daemon.client.submit_availability_cert.call_count}× — "
        f"daemon attested a wrong-content blob on chain (task #184 bug)."
    )


def test_blob_verifier_downgrades_level_on_merkle_mismatch():
    """Belt-and-suspenders: even if a future caller forgets the explicit
    gate, the verifier itself should signal the failure by NOT advancing
    past HASH_VERIFIED on Merkle mismatch. Tests the result enum directly.
    """
    chunk_data = b"x" * 64
    chunk_hash = sha256(chunk_data)
    wrong_root = sha256(b"different")
    receipt = _make_receipt("0xdd" * 32, base_root=wrong_root)

    async def _go():
        server = _ChunkServer(chunk_data)
        await server.start()
        try:
            manifest = BlobManifest(
                receipt_id=receipt.receipt_id,
                chunks=[ChunkInfo(index=0, url=server.url, sha256_hash=chunk_hash, size=len(chunk_data))],
            )
            verifier = BlobVerifier(DaemonConfig())
            return await verifier.verify(receipt, manifest)
        finally:
            await server.stop()

    result = _run(_go())
    assert result.attestation_level <= AttestationLevel.HASH_VERIFIED
    # Specifically: never claims ROOT_VERIFIED.
    assert result.attestation_level != AttestationLevel.ROOT_VERIFIED
