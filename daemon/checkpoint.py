"""Cardano L1 checkpointing for Materios availability certificates.

Collects certified receipts into batches, computes a Merkle root with
context-bound leaves, and submits the root to Cardano via the anchor worker.

Leaf binding prevents replay/mixing:
    leaf_i = SHA256(b"materios-checkpoint-v1" || chain_id || receipt_id || cert_hash)

Crash-proof: pending leaves and last checkpoint block are persisted to a
JSON state file on PVC so nothing is lost across daemon restarts.
"""

import hashlib
import json
import logging
import math
import os
import time
from datetime import datetime
from typing import Optional

import requests
from substrateinterface import Keypair

from daemon.config import DaemonConfig

logger = logging.getLogger(__name__)


def _sha256(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def merkle_root(leaves: list[bytes]) -> bytes:
    """Compute SHA-256 Merkle root from a list of leaf hashes."""
    if not leaves:
        return b"\x00" * 32
    if len(leaves) == 1:
        return leaves[0]

    # Pad to even length by duplicating last leaf
    layer = list(leaves)
    while len(layer) > 1:
        if len(layer) % 2 != 0:
            layer.append(layer[-1])
        next_layer = []
        for i in range(0, len(layer), 2):
            next_layer.append(_sha256(layer[i] + layer[i + 1]))
        layer = next_layer
    return layer[0]


def compute_anchor_id(root_hash_hex: str, manifest_hash_hex: str) -> str:
    """Deterministically synthesize the anchorId from (rootHash, manifestHash).

    Pre-image: rootHash raw bytes ++ manifestHash raw bytes (each parsed from
    its hex representation, any leading 0x stripped). SHA-256 over those raw
    bytes, then 0x-prefix the digest hex.

    This matches the algorithm in the Materios anchor worker (see
    `services/anchor-worker-materios/src/anchor.ts::deriveAnchorId`, which does
    `sha256(Buffer.from(rootHex.slice(2) + manifestHex.slice(2), "hex"))`) so:
      * the daemon can index batch metadata under the same anchorId BEFORE
        the anchor-worker responds (no round-trip dependency for retries),
      * external auditors can re-derive anchorId from on-chain rootHash +
        manifestHash with no extra inputs,
      * the cert-daemon ↔ anchor-worker round-trip is idempotent when the
        request is replayed.
    """
    root_bytes = bytes.fromhex(root_hash_hex.removeprefix("0x").removeprefix("0X"))
    manifest_bytes = bytes.fromhex(manifest_hash_hex.removeprefix("0x").removeprefix("0X"))
    return "0x" + hashlib.sha256(root_bytes + manifest_bytes).hexdigest()


class CardanoCheckpointer:
    """Batches certified receipts and periodically checkpoints to Cardano."""

    def __init__(self, config: DaemonConfig, state_file: str):
        self.config = config
        self.batch_size = config.checkpoint_batch_size
        self.interval_minutes = config.checkpoint_interval
        self.state_file = state_file
        self.pending_leaves: list[dict] = []
        self.last_checkpointed_block: int = 0
        self.last_flush_time: float = 0.0
        # Derive Keypair ONCE at startup. Used for:
        #   1. submitter_address — published in batch_metadata (NEVER include the
        #      raw signer_uri, which may be a BIP39 mnemonic; anchor worker
        #      forwards batch_metadata into Cardano L1 tx metadata — a seed
        #      leak here is permanent).
        #   2. signing the gateway's `/batches/:anchorId` PUT (task #122) so
        #      the daemon authenticates with sr25519 sig instead of needing a
        #      static API key in compose env.
        self.keypair: Keypair = Keypair.create_from_uri(config.signer_uri)
        self.submitter_address: str = self.keypair.ss58_address
        self._load_state()

    def _load_state(self):
        """Restore pending list + last checkpoint block from PVC."""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file) as f:
                    state = json.load(f)
                self.pending_leaves = state.get("pending_leaves", [])
                self.last_checkpointed_block = state.get("last_checkpointed_block", 0)
                self.last_flush_time = state.get("last_flush_time", 0.0)
                # Task #116: an M-of-N committee emits one AvailabilityCertified
                # event per signer for the same (receipt_id, cert_hash). Earlier
                # versions of add_cert appended every event, inflating the leaf
                # count by ~M. Dedupe any such residue carried over in state on
                # startup so the next flush counts unique facts only.
                pre = len(self.pending_leaves)
                self.pending_leaves = self._dedupe_leaves(self.pending_leaves)
                removed = pre - len(self.pending_leaves)
                if removed > 0:
                    logger.warning(
                        f"Checkpoint state load: removed {removed} duplicate "
                        f"(receipt_id, cert_hash) leaves carried over from prior runs"
                    )
                    self._save_state()
                logger.info(
                    f"Checkpoint state loaded: {len(self.pending_leaves)} pending, "
                    f"last_block={self.last_checkpointed_block}"
                )
            except Exception as e:
                logger.warning(f"Failed to load checkpoint state: {e}")

    @staticmethod
    def _dedupe_leaves(leaves: list[dict]) -> list[dict]:
        """Collapse leaves sharing the same (receipt_id, cert_hash).

        A re-attestation with a DIFFERENT cert_hash for the same receipt_id is
        a distinct fact and is preserved. Only same-(receipt_id, cert_hash)
        duplicates — the M-of-N committee echoes — are merged.

        The first occurrence is kept (preserves earliest block_num + timestamp,
        which is what `should_flush` uses for the leaf-age trigger).
        """
        seen: set[tuple[str, str]] = set()
        out: list[dict] = []
        for leaf in leaves:
            key = (leaf.get("receipt_id", ""), leaf.get("cert_hash", ""))
            if key in seen:
                continue
            seen.add(key)
            out.append(leaf)
        return out

    def _save_state(self):
        """Persist to PVC — survives daemon restart."""
        try:
            tmp = self.state_file + ".tmp"
            with open(tmp, "w") as f:
                json.dump(
                    {
                        "pending_leaves": self.pending_leaves,
                        "last_checkpointed_block": self.last_checkpointed_block,
                        "last_flush_time": self.last_flush_time,
                    },
                    f,
                )
            os.replace(tmp, self.state_file)
        except Exception as e:
            logger.error(f"Failed to save checkpoint state: {e}")

    def add_cert(self, receipt_id: str, cert_hash: bytes, block_num: int):
        """Add a certified receipt to the pending batch.

        Task #116: collapse duplicate (receipt_id, cert_hash) facts. An M-of-N
        committee emits one AvailabilityCertified event per signer for the
        same cert; treating each as a separate Merkle leaf inflates the
        Cardano L1 anchor by a factor of ~M. A re-attestation with a DIFFERENT
        cert_hash IS a distinct fact and is appended normally.
        """
        cert_hash_hex = cert_hash.hex()
        for existing in self.pending_leaves:
            if (
                existing.get("receipt_id") == receipt_id
                and existing.get("cert_hash") == cert_hash_hex
            ):
                logger.debug(
                    f"Checkpoint: dedupe duplicate cert for {receipt_id[:16]}... "
                    f"at block {block_num} (already pending from block "
                    f"{existing.get('block_num')})"
                )
                return

        self.pending_leaves.append(
            {
                "receipt_id": receipt_id,
                "cert_hash": cert_hash_hex,
                "block_num": block_num,
                "timestamp": time.time(),
            }
        )
        self._save_state()
        logger.debug(
            f"Checkpoint: added cert for {receipt_id[:16]}... "
            f"({len(self.pending_leaves)}/{self.batch_size})"
        )

    def should_flush(self) -> bool:
        """Check if a flush is due (time, batch-size, or leaf-age trigger)."""
        if not self.pending_leaves:
            return False
        # Batch size trigger
        if len(self.pending_leaves) >= self.batch_size:
            return True
        # Oldest leaf age trigger
        oldest_ts = min(
            leaf.get("timestamp", time.time()) for leaf in self.pending_leaves
        )
        if time.time() - oldest_ts > self.config.max_leaf_wait_seconds:
            return True
        # Time-based trigger
        if self.interval_minutes <= 0:
            return False
        elapsed = (time.time() - self.last_flush_time) / 60.0
        return elapsed >= self.interval_minutes

    def flush(self, current_best_block: int = 0, live_chain_genesis: Optional[str] = None) -> bool:
        """Build Merkle root and submit checkpoint to Cardano anchor worker.

        `live_chain_genesis` MUST be the authoritative chain genesis hash from
        `chain_getBlockHash[0]` (the value cert_daemon stores on
        `self._live_chain_genesis`). It binds every leaf and the manifest so
        cross-chain replay / stale-env divergence is impossible by construction
        (task #207 successor to cert-daemon PR #22). Passing `None` is a hard
        error — flushes with an unknown chain context could anchor stale-chain
        leaves to Cardano L1, which is unrecoverable.

        Only includes leaves confirmed by at least `finality_confirmations` blocks.
        Returns True if checkpoint was submitted successfully.
        """
        if not self.pending_leaves:
            return True

        if not self.config.cardano_anchor_url:
            logger.warning("Checkpoint flush skipped: CARDANO_ANCHOR_URL not configured")
            return False

        if not live_chain_genesis:
            logger.error(
                "Checkpoint flush refused: live_chain_genesis is unset. "
                "The caller (cert_daemon.poll loop) MUST pass "
                "self._live_chain_genesis. Skipping flush — leaves stay queued "
                "and will retry next interval once live genesis is available."
            )
            return False

        chain_id_norm = live_chain_genesis.removeprefix("0x").removeprefix("0X").lower()

        # Filter by confirmation depth
        confirmed_cutoff = current_best_block - self.config.finality_confirmations
        eligible = [l for l in self.pending_leaves if l["block_num"] <= confirmed_cutoff]
        if not eligible:
            logger.debug(
                f"Checkpoint: no leaves confirmed yet "
                f"(cutoff={confirmed_cutoff}, best={current_best_block})"
            )
            return True  # nothing confirmed enough yet

        chain_id_bytes = bytes.fromhex(chain_id_norm)

        leaves = []
        for leaf in eligible:
            receipt_id_bytes = bytes.fromhex(leaf["receipt_id"].removeprefix("0x"))
            cert_hash_bytes = bytes.fromhex(leaf["cert_hash"])
            bound_leaf = _sha256(
                b"materios-checkpoint-v1" + chain_id_bytes + receipt_id_bytes + cert_hash_bytes
            )
            leaves.append(bound_leaf)

        root = merkle_root(leaves)
        from_block = min(l["block_num"] for l in eligible)
        to_block = max(l["block_num"] for l in eligible)
        count = len(leaves)

        logger.info(
            f"Checkpoint: flushing {count} leaves, blocks {from_block}-{to_block}, "
            f"root={root.hex()[:16]}..."
        )

        manifest = {
            "materios_chain_id": chain_id_norm,
            "cardano_network_id": self.config.cardano_network_id,
            "from_block": from_block,
            "to_block": to_block,
            "count": count,
            "root": root.hex(),
        }
        manifest_hash = hashlib.sha256(
            json.dumps(manifest, sort_keys=True).encode()
        ).hexdigest()

        # Task #117: synthesize the anchorId locally so the daemon, the anchor
        # worker, and the gateway-side reverse-lookup index all agree on the
        # same id before any network round-trip. The daemon can then save
        # batch history under this id and the gateway POST is independent of
        # the anchor-worker's response shape.
        anchor_id = compute_anchor_id(root.hex(), manifest_hash)

        # Build batch metadata for anchor worker to post as backup
        batch_metadata = {
            "anchorId": anchor_id,
            "rootHash": root.hex(),
            "leafCount": len(leaves),
            "leafHashes": [lh.hex() for lh in leaves],
            "blockRangeStart": from_block,
            "blockRangeEnd": to_block,
            "submitter": self.submitter_address,
            "timestamp": datetime.utcnow().isoformat(),
            "source": "daemon",
        }

        anchor_response = self._submit_to_cardano(
            root, manifest, manifest_hash, anchor_id, batch_metadata
        )

        if anchor_response is not None:
            leaf_hashes = [lh.hex() for lh in leaves]
            # Defensive: if the worker returned a different anchorId (would
            # indicate an algorithm drift between daemon and worker — a real
            # bug we'd need to know about), prefer the worker's so the
            # gateway-side index matches what's referenced on-chain. Log the
            # mismatch loudly so it doesn't go unnoticed.
            worker_anchor_id = anchor_response.get("anchorId", "") or ""
            if worker_anchor_id and worker_anchor_id.lower() != anchor_id.lower():
                logger.error(
                    f"Anchor ID mismatch: daemon computed {anchor_id}, "
                    f"worker returned {worker_anchor_id}. Using worker's value."
                )
                anchor_id = worker_anchor_id

            self._save_batch_history(
                eligible, leaves, root, manifest, manifest_hash, anchor_id
            )

            # Post batch metadata to blob gateway for SDK verification.
            # This is the canonical write path; anchor-worker also fires a
            # belt-and-suspenders backup PUT.
            self._post_batch_metadata(anchor_id, root.hex(), eligible, leaf_hashes)

            self.last_checkpointed_block = to_block
            self.last_flush_time = time.time()
            # Remove only the eligible leaves; keep unconfirmed ones
            self.pending_leaves = [l for l in self.pending_leaves if l["block_num"] > confirmed_cutoff]
            self._save_state()
            logger.info(
                f"Checkpoint submitted: root={root.hex()[:16]}... "
                f"anchorId={anchor_id[:18]}... ({count} certs)"
            )
        else:
            logger.error("Checkpoint submission failed — will retry on next flush")

        return anchor_response is not None

    def _save_batch_history(
        self,
        eligible: list[dict],
        leaf_hashes: list[bytes],
        root: bytes,
        manifest: dict,
        manifest_hash: str,
        anchor_id: str,
    ):
        """Append completed batch to checkpoint-history.json for verification.

        Includes anchor_id so the local history file is keyed the same way as
        the gateway's `/batches/:anchorId` reverse-lookup index. Matters for
        replay/repair: a future tool can scan history.json and POST any
        missing batches back to the gateway without recomputing the id.
        """
        history_file = self.state_file.replace("checkpoint-state.json", "checkpoint-history.json")
        try:
            if os.path.exists(history_file):
                with open(history_file) as f:
                    history = json.load(f)
            else:
                history = []

            batch = {
                "timestamp": time.time(),
                "anchor_id": anchor_id,
                "root_hash": root.hex(),
                "manifest_hash": manifest_hash,
                "manifest": manifest,
                "leaves": [
                    {
                        "receipt_id": e["receipt_id"],
                        "cert_hash": e["cert_hash"],
                        "block_num": e["block_num"],
                        "leaf_hash": lh.hex(),
                    }
                    for e, lh in zip(eligible, leaf_hashes)
                ],
            }
            history.append(batch)

            tmp = history_file + ".tmp"
            with open(tmp, "w") as f:
                json.dump(history, f, indent=2)
            os.replace(tmp, history_file)
            logger.debug(f"Batch history saved: {len(history)} batches total")
        except Exception as e:
            logger.warning(f"Failed to save batch history: {e}")

    def _post_batch_metadata(
        self,
        anchor_id: str,
        root_hash: str,
        eligible_leaves: list[dict],
        leaf_hashes: list[str],
    ) -> bool:
        """PUT batch metadata to blob gateway for SDK verification.

        Returns True on 2xx, False on any error or missing config. The
        endpoint is idempotent (PUT semantics on the gateway side), so this
        can be safely retried.
        """
        blob_gateway_url = self.config.blob_gateway_url or os.environ.get("BLOB_GATEWAY_URL", "")
        blob_gateway_api_key = self.config.blob_gateway_api_key or os.environ.get("BLOB_GATEWAY_API_KEY", "")

        if not blob_gateway_url:
            logger.debug("BLOB_GATEWAY_URL not set, skipping batch metadata post")
            return False

        try:
            batch_metadata = {
                "anchorId": anchor_id,
                "rootHash": root_hash,
                "leafCount": len(leaf_hashes),
                "leafHashes": leaf_hashes,
                "blockRangeStart": min(leaf.get("block_num", 0) for leaf in eligible_leaves),
                "blockRangeEnd": max(leaf.get("block_num", 0) for leaf in eligible_leaves),
                "submitter": self.submitter_address,
                "timestamp": datetime.utcnow().isoformat(),
                "source": "daemon",
            }

            # Strip 0x prefix from anchor_id for URL path. The gateway
            # canonicalizes via its own stripHexPrefix on save AND read, so
            # the resource is reachable either way — but the daemon emits
            # the prefix-less form to match the gateway's storage layout
            # exactly (`<hex>.json`).
            anchor_id_clean = anchor_id
            if anchor_id_clean.startswith("0x"):
                anchor_id_clean = anchor_id_clean[2:]

            # Auth: sr25519 sig over `materios-upload-v1|{anchorId}|{address}|{ts}`
            # (task #122). Pre-image format pinned by gateway's upload-auth.ts.
            # `contentHash` slot is `anchorId` per the route's resolveAuth call.
            # IMPORTANT: gateway uses the URL-path anchorId (no 0x), so the
            # signing string MUST use the same form — sign with anchor_id_clean,
            # not the 0x-prefixed form.
            ts = int(time.time())
            signing_string = (
                f"materios-upload-v1|{anchor_id_clean}|{self.submitter_address}|{ts}"
            )
            sig_bytes = self.keypair.sign(signing_string.encode("utf-8"))
            sig_hex = "0x" + sig_bytes.hex()

            headers = {
                "Content-Type": "application/json",
                "x-upload-sig": sig_hex,
                "x-uploader-address": self.submitter_address,
                "x-upload-ts": str(ts),
            }
            # Belt-and-suspenders: if an API key is also configured, attach it.
            # Gateway resolveAuth checks api-key BEFORE upload-sig, so when both
            # are present it'll authenticate via api-key. Either path produces a
            # 200; we prefer sig because it doesn't require provisioning a
            # static secret in compose env.
            if blob_gateway_api_key:
                headers["x-api-key"] = blob_gateway_api_key

            # PUT is idempotent and matches the gateway's documented upsert
            # contract (routes/batches.ts wires both POST and PUT to the same
            # handler; PUT is preferred for re-tries).
            resp = requests.put(
                f"{blob_gateway_url}/batches/{anchor_id_clean}",
                json=batch_metadata,
                headers=headers,
                timeout=10,
            )
            if resp.status_code in (200, 201):
                logger.info(
                    f"Posted batch metadata for anchor {anchor_id[:16]}... to gateway"
                )
                return True
            logger.warning(
                f"Failed to post batch metadata for anchor {anchor_id[:16]}...: "
                f"{resp.status_code} {resp.text[:200]}"
            )
            return False
        except Exception as e:
            logger.warning(f"Error posting batch metadata to gateway: {e}")
            return False

    def _submit_to_cardano(
        self,
        root: bytes,
        manifest: dict,
        manifest_hash: str,
        anchor_id: str,
        batch_metadata: dict | None = None,
    ) -> Optional[dict]:
        """POST checkpoint to the Cardano anchor worker.

        Passes a deterministically-computed `anchorId` so the worker preserves
        it across the chain (idempotency). Returns the anchor response dict on
        success (containing anchorId, blockHash, etc.), or None on failure.
        """
        payload = {
            "anchorId": anchor_id,
            "contentHash": root.hex(),
            "rootHash": root.hex(),
            "manifestHash": manifest_hash,
        }
        if batch_metadata:
            payload["batchMetadata"] = batch_metadata

        for attempt in range(3):
            try:
                resp = requests.post(
                    f"{self.config.cardano_anchor_url}/anchor",
                    json=payload,
                    headers={
                        "Content-Type": "application/json",
                        "x-internal-token": self.config.anchor_worker_token,
                    },
                    timeout=60,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    logger.info(f"Cardano checkpoint tx: {data}")
                    return data
                else:
                    logger.error(
                        f"Cardano anchor worker returned {resp.status_code}: {resp.text}"
                    )
            except Exception as e:
                logger.error(f"Checkpoint submit attempt {attempt + 1} failed: {e}")
                if attempt < 2:
                    time.sleep(2**attempt)

        return None
