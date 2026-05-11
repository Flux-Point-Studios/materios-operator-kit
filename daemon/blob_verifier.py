import logging
import os
from typing import Optional
import aiohttp

from daemon.config import DaemonConfig
from daemon.models import (
    AttestationLevel,
    BlobManifest,
    ReceiptRecord,
    VerificationResult,
)
from daemon.merkle import sha256, merkle_root
from daemon.schemas import (
    LEGACY_SCHEMA_HASH,
    TRUSTED_DISCRIMINATOR_SCHEMAS,
    schema_name,
)

logger = logging.getLogger(__name__)


class BlobVerifier:
    def __init__(self, config: DaemonConfig):
        self.config = config

    async def verify(self, receipt: ReceiptRecord, manifest: BlobManifest) -> VerificationResult:
        """Verify a receipt's blob integrity and root commitment.

        Dispatches on `receipt.schema_hash`:

          - `LEGACY_SCHEMA_HASH` (32 zero bytes — default for blob uploads
            before the schema_hash discriminator was wired through the
            gateway notify path): chunk-Merkle path. The on-chain
            `base_root_sha256` MUST equal `merkle_root(chunk_hashes)`.
            Includes the task #186 self-rooted shortcut for empty-chunks
            metering records that landed before this dispatcher existed.

          - A schema in `TRUSTED_DISCRIMINATOR_SCHEMAS` (currently
            `compute_metering_v2`, `compute_metering_v2_1`):
            trust-the-discriminator path. Chunk integrity is independently
            verified (`chunk-Merkle == content_hash` — the envelope bytes
            are cryptographically pinned by the on-chain content_hash).
            The on-chain `base_root_sha256` is then accepted as a trusted
            *semantic* root for the schema class. Cert-daemon attests
            chunk integrity + class assertion; downstream consumers
            (billing API, observers) re-derive the semantic root from the
            envelope bytes themselves. This split mirrors Cardano's M-of-N
            attestation model.

          - Anything else: REJECT with a clear error. New schemas must
            register in `daemon/schemas/` before they verify.

        Trust framing for the discriminator path: a malicious gateway
        could lie about `schema_hash` just as easily as it could lie
        about chunk URLs in the legacy path. The defense is the M-of-N
        committee threshold and the Cardano L1 anchor of the cert batch,
        not a second verifier inside cert-daemon. The chunk-Merkle ==
        content_hash check still pins the envelope bytes byte-for-byte
        across all attesters.

        See `daemon/schemas/__init__.py` and the 2026-05-11 task #198
        diagnostic for full context.
        """
        sh = receipt.schema_hash
        name = schema_name(sh)

        if sh in TRUSTED_DISCRIMINATOR_SCHEMAS:
            return await self._verify_trusted_discriminator(
                receipt, manifest, schema_label=name or "unknown_trusted",
            )

        if sh == LEGACY_SCHEMA_HASH:
            return await self._verify_legacy_chunk_merkle(receipt, manifest)

        # Unknown schema — reject with a clear log line. Don't silently
        # fall through to the legacy path: that's how class drift hides.
        logger.error(
            f"Unknown schema_hash {sh.hex()} on receipt {receipt.receipt_id}; "
            f"refusing to verify. Add the schema to daemon/schemas/ before "
            f"these receipts can attest."
        )
        result = VerificationResult(
            attestation_level=AttestationLevel.FETCHED,
            chunks_total=len(manifest.chunks),
        )
        result.errors.append(
            f"Unknown schema_hash {sh.hex()} — verifier has no dispatch rule "
            f"for this class. Register it in daemon/schemas/."
        )
        return result

    # ------------------------------------------------------------------
    # Legacy blob chunk-Merkle path (schema_hash = 32 zero bytes)
    # ------------------------------------------------------------------
    async def _verify_legacy_chunk_merkle(
        self, receipt: ReceiptRecord, manifest: BlobManifest,
    ) -> VerificationResult:
        """Original blob verification path: chunk hashes → Merkle root →
        compare to on-chain base_root_sha256.

        Retains the task #186 self-rooted shortcut for receipts that landed
        before the schema_hash discriminator existed: when manifest.chunks
        is empty AND base_root_sha256 == content_hash, accept as self-rooted.
        New v2 receipts should set schema_hash and hit the trusted-
        discriminator path instead; this shortcut is for backward compat.
        """
        if not manifest.chunks and receipt.base_root_sha256 == receipt.content_hash:
            logger.info(
                f"Self-rooted manifest for {receipt.receipt_id} "
                f"(chunks=[], base_root==content_hash="
                f"{receipt.content_hash.hex()[:16]}...) — "
                f"verification short-circuits to ROOT_VERIFIED."
            )
            return VerificationResult(
                attestation_level=AttestationLevel.ROOT_VERIFIED,
                computed_root=receipt.base_root_sha256,
                chunks_total=0,
                chunks_verified=0,
            )

        chunks_fetched, chunk_data_list, fetch_err = await self._fetch_and_hash_chunks(
            manifest,
        )
        if fetch_err is not None:
            return fetch_err

        result = VerificationResult(
            attestation_level=AttestationLevel.HASH_VERIFIED,
            chunks_total=len(manifest.chunks),
            chunks_verified=len(chunks_fetched),
            chunk_data_list=chunk_data_list,
        )

        computed_root = merkle_root(chunks_fetched)
        result.computed_root = computed_root

        if computed_root == receipt.base_root_sha256:
            result.attestation_level = AttestationLevel.ROOT_VERIFIED
        else:
            result.errors.append(
                f"Merkle root mismatch: on-chain {receipt.base_root_sha256.hex()}, "
                f"computed {computed_root.hex()}"
            )
            logger.critical(
                f"Merkle root mismatch for {receipt.receipt_id}: "
                f"on-chain={receipt.base_root_sha256.hex()}, computed={computed_root.hex()}"
            )

        return result

    # ------------------------------------------------------------------
    # Trust-the-discriminator path (schema_hash ∈ TRUSTED_DISCRIMINATOR_SCHEMAS)
    # ------------------------------------------------------------------
    async def _verify_trusted_discriminator(
        self, receipt: ReceiptRecord, manifest: BlobManifest, schema_label: str,
    ) -> VerificationResult:
        """Schema-discriminator verification: chunk integrity pins the
        envelope bytes; on-chain base_root_sha256 is accepted as the
        trusted semantic root for the schema class.

        Required invariant for acceptance:
            merkle_root(chunk_hashes) == content_hash

        What this invariant proves and DOESN'T prove:
          PROVES: the chunks the gateway served to THIS attester hash to
          the same content_hash the chain recorded. Envelope bytes are
          deterministic across all committee members who see the same
          content_hash → same chunks → same hashes.
          DOES NOT prove: that base_root_sha256 (the semantic root) was
          honestly derived from those envelope bytes. The semantic root
          remains caller-supplied; this verifier accepts it on faith for
          schemas in `TRUSTED_DISCRIMINATOR_SCHEMAS`.

        The defense against a dishonest semantic root is the M-of-N
        committee threshold + Cardano L1 anchor — NOT a second
        verification inside cert-daemon. Downstream consumers (billing
        API, observers) that read the envelope bytes + the on-chain
        base_root_sha256 are responsible for re-deriving the semantic
        root and rejecting mismatches at THEIR layer.

        For v2.0 this trust split is acceptable. A future schema joining
        `TRUSTED_DISCRIMINATOR_SCHEMAS` whose semantic root cannot be
        independently re-derived by downstream consumers SHOULD instead
        register a per-schema canonicalization hook here. See followup
        task on the security review of this commit.

        Sub-case (chunks=[]): a metering record that is fully inline in
        the manifest body (gateway stores `{record, chunks: []}`).
        Convention: content_hash == base_root_sha256 (the rootHash
        callback value) AND the empty-chunks Merkle equals zero. We treat
        the empty-chunks case as equivalent to "envelope integrity is the
        record itself", matching task #186's self-rooted shortcut but now
        gated by the explicit schema discriminator.
        """
        if not manifest.chunks:
            # Inline-record self-rooted case. base_root_sha256 == content_hash
            # is the on-chain assertion that the rootHash callback was wired
            # correctly. We ALSO require content_hash != zero-bytes — the
            # sentinel `b'\\x00' * 32` is what `merkle_root([])` returns and
            # what unset chain fields decode to; without this guard a
            # malicious or buggy submitter could grind an all-zero
            # content_hash + base_root and get ROOT_VERIFIED for free.
            # (Security-review followup, 2026-05-11.)
            if (
                receipt.base_root_sha256 == receipt.content_hash
                and receipt.content_hash != LEGACY_SCHEMA_HASH
            ):
                logger.info(
                    f"Schema-discriminator self-rooted for {receipt.receipt_id} "
                    f"(schema={schema_label}, chunks=[], "
                    f"base_root==content_hash) → ROOT_VERIFIED"
                )
                return VerificationResult(
                    attestation_level=AttestationLevel.ROOT_VERIFIED,
                    computed_root=receipt.base_root_sha256,
                    chunks_total=0,
                    chunks_verified=0,
                )
            # Discriminator set but no chunks AND base_root != content_hash:
            # malformed. Refuse — the v2 inline convention requires the
            # rootHash callback to be wired.
            result = VerificationResult(
                attestation_level=AttestationLevel.FETCHED,
                chunks_total=0,
            )
            result.errors.append(
                f"schema={schema_label} but chunks=[] and "
                f"base_root_sha256({receipt.base_root_sha256.hex()[:16]}...) != "
                f"content_hash({receipt.content_hash.hex()[:16]}...). "
                f"Inline records require rootHash callback to set them equal."
            )
            logger.error(result.errors[-1])
            return result

        # Non-empty chunks: verify chunk integrity by recomputing the
        # chunk-Merkle and asserting it equals content_hash. content_hash
        # IS the canonical envelope-bytes pin for schemas in
        # TRUSTED_DISCRIMINATOR_SCHEMAS (single envelope chunk for v2,
        # possibly multi-chunk for future bulk schemas).
        chunks_fetched, chunk_data_list, fetch_err = await self._fetch_and_hash_chunks(
            manifest,
        )
        if fetch_err is not None:
            return fetch_err

        result = VerificationResult(
            attestation_level=AttestationLevel.HASH_VERIFIED,
            chunks_total=len(manifest.chunks),
            chunks_verified=len(chunks_fetched),
            chunk_data_list=chunk_data_list,
        )

        chunk_root = merkle_root(chunks_fetched)
        result.computed_root = chunk_root

        if chunk_root != receipt.content_hash:
            # Envelope bytes don't hash to the on-chain content_hash. The
            # gateway/submitter pipeline is broken — refuse. (Tampering or
            # serialization drift would land here.)
            result.errors.append(
                f"schema={schema_label} chunk integrity check failed: "
                f"merkle_root(chunks)={chunk_root.hex()} != "
                f"content_hash={receipt.content_hash.hex()}. "
                f"Envelope bytes do not match the on-chain content_hash pin."
            )
            logger.critical(result.errors[-1])
            return result

        # Chunk integrity passes — on-chain base_root_sha256 is the
        # schema's trusted semantic root. Accept at ROOT_VERIFIED.
        result.attestation_level = AttestationLevel.ROOT_VERIFIED
        logger.info(
            f"Schema-discriminator accepted for {receipt.receipt_id} "
            f"(schema={schema_label}, chunks={len(manifest.chunks)}, "
            f"chunk_root==content_hash={receipt.content_hash.hex()[:16]}..., "
            f"base_root_sha256={receipt.base_root_sha256.hex()[:16]}... "
            f"accepted as semantic root)"
        )
        return result

    # ------------------------------------------------------------------
    # Shared chunk-fetch helper (both dispatch paths use this)
    # ------------------------------------------------------------------
    async def _fetch_and_hash_chunks(self, manifest: BlobManifest):
        """Fetch every chunk in the manifest, verify each hash matches the
        manifest-declared hash, and return the list of chunk hashes in
        manifest order.

        Returns `(chunk_hashes, chunk_data_list, error_result)`:
          - On success, error_result is None.
          - On failure (fetch error, size violation, or hash mismatch),
            returns a partially-populated VerificationResult with errors
            set and attestation_level capped at FETCHED.
        """
        chunk_hashes = []
        chunk_data_list = []
        timeout = aiohttp.ClientTimeout(total=self.config.blob_fetch_timeout)

        default_headers = {}
        api_key = os.environ.get("BLOB_GATEWAY_API_KEY", "") or getattr(
            self.config, "blob_gateway_api_key", "",
        )
        if api_key:
            default_headers["x-api-key"] = api_key

        async with aiohttp.ClientSession(timeout=timeout, headers=default_headers) as session:
            for chunk in manifest.chunks:
                chunk_data = await self._fetch_chunk(session, chunk.url)
                if chunk_data is None:
                    err = VerificationResult(
                        attestation_level=AttestationLevel.FETCHED,
                        chunks_total=len(manifest.chunks),
                        chunks_verified=len(chunk_hashes),
                    )
                    err.errors.append(
                        f"Failed to fetch chunk {chunk.index}: {chunk.url}"
                    )
                    return chunk_hashes, chunk_data_list, err

                if len(chunk_data) > self.config.max_chunk_size:
                    err = VerificationResult(
                        attestation_level=AttestationLevel.FETCHED,
                        chunks_total=len(manifest.chunks),
                        chunks_verified=len(chunk_hashes),
                    )
                    err.errors.append(
                        f"Chunk {chunk.index} exceeds max size: {len(chunk_data)}"
                    )
                    return chunk_hashes, chunk_data_list, err

                computed_hash = sha256(chunk_data)
                if computed_hash != chunk.sha256_hash:
                    err = VerificationResult(
                        attestation_level=AttestationLevel.FETCHED,
                        chunks_total=len(manifest.chunks),
                        chunks_verified=len(chunk_hashes),
                    )
                    err.errors.append(
                        f"Chunk {chunk.index} hash mismatch: "
                        f"expected {chunk.sha256_hash.hex()}, got {computed_hash.hex()}"
                    )
                    return chunk_hashes, chunk_data_list, err

                chunk_hashes.append(computed_hash)
                chunk_data_list.append(chunk_data)

        return chunk_hashes, chunk_data_list, None

    async def _fetch_chunk(self, session: aiohttp.ClientSession, url: str) -> Optional[bytes]:
        # Support file:// URLs for local chunk reading (used by chaos drills and local tests)
        if url.startswith("file://"):
            path = os.path.normpath(url[7:])
            # Block path traversal — file:// reads must stay within blob_local_dir
            allowed_dir = os.path.normpath(self.config.blob_local_dir)
            if not path.startswith(allowed_dir):
                logger.warning(f"Path traversal blocked in file:// URL: {path}")
                return None
            try:
                with open(path, "rb") as f:
                    return f.read()
            except Exception as e:
                logger.warning(f"Local chunk read failed {path}: {e}")
                return None

        for attempt in range(self.config.max_blob_fetch_retries):
            try:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        return await resp.read()
                    logger.warning(f"Chunk fetch {url} returned {resp.status} (attempt {attempt + 1})")
            except Exception as e:
                logger.warning(f"Chunk fetch {url} failed (attempt {attempt + 1}): {e}")
        return None
