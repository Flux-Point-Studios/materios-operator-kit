"""Tests for cert-daemon's anchorId wire-up to the gateway batches index.

Bug context (task #117): cert-daemon's `_submit_to_cardano` payload omitted
`anchorId`, the anchor-worker echoed it back as undefined, and the daemon
never POSTed the leaf-list to gateway `/batches/{anchorId}`. As a result,
`GET /batches/{anchorId}` returned 404 for every Cardano anchor on preprod,
even though the on-chain metadata under label 8746 was correct.

This test module locks in:

  * `compute_anchor_id` is deterministic and matches the algorithm in
    `services/anchor-worker-materios/src/anchor.ts::submitAnchor`
    (sha256(rootBytes ++ manifestBytes), 0x-prefixed).

  * `flush()` synthesizes the anchorId locally, passes it in the
    `/anchor` payload, and POSTs the same anchorId to the gateway after
    Cardano confirmation.

  * `_post_batch_metadata` PUTs the leaf-list under the deterministic id
    with the correct shape (rootHash, leafCount, leafHashes, blockRange,
    submitter, timestamp).

  * `_save_batch_history` records anchor_id alongside the leaves so a
    future replay tool can find batches missing from the gateway.

We hit no live services here; the substrate keypair derivation is the only
side-effect. All HTTP I/O is mocked at `requests.post` / `requests.put`.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from daemon.checkpoint import CardanoCheckpointer, compute_anchor_id, merkle_root
from daemon.config import DaemonConfig


# --- helpers ---------------------------------------------------------------


def _hex(b: bytes) -> str:
    return b.hex()


def _make_checkpointer(tmpdir: str) -> CardanoCheckpointer:
    """Build a CardanoCheckpointer wired against a temp state dir.

    Uses //Alice for signer_uri so SS58 derivation succeeds in offline
    tests. CARDANO_ANCHOR_URL points at a sentinel hostname that requests
    will never actually reach because we mock requests.post + requests.put.
    """
    config = DaemonConfig()
    # 32-byte chain id — must be valid hex (used as `bytes.fromhex`).
    config.chain_id = "00" * 32
    config.cardano_network_id = "11" * 32
    config.cardano_anchor_url = "http://anchor-worker.test:3333"
    config.anchor_worker_token = "test-token"
    config.blob_gateway_url = "http://gateway.test:8080"
    config.blob_gateway_api_key = "test-api-key"
    config.signer_uri = "//Alice"
    config.checkpoint_batch_size = 100
    config.checkpoint_interval = 60
    config.finality_confirmations = 0  # don't gate on confirmations in tests
    config.max_leaf_wait_seconds = 90
    state_file = os.path.join(tmpdir, "checkpoint-state.json")
    return CardanoCheckpointer(config, state_file)


# --- compute_anchor_id deterministic algorithm ----------------------------


class TestComputeAnchorId:
    def test_deterministic_for_known_inputs(self):
        """Same (root, manifest) → same anchorId every time."""
        root = "a" * 64
        manifest = "b" * 64
        a = compute_anchor_id(root, manifest)
        b = compute_anchor_id(root, manifest)
        assert a == b
        assert a.startswith("0x")
        assert len(a) == 66  # 0x + 64 hex chars

    def test_strips_0x_prefix_idempotent(self):
        """Leading 0x on either input must not change the output."""
        root_clean = "a" * 64
        manifest_clean = "b" * 64
        a = compute_anchor_id(root_clean, manifest_clean)
        b = compute_anchor_id("0x" + root_clean, manifest_clean)
        c = compute_anchor_id(root_clean, "0x" + manifest_clean)
        d = compute_anchor_id("0x" + root_clean, "0x" + manifest_clean)
        # All four must agree.
        assert a == b == c == d

    def test_differs_when_inputs_swap(self):
        """Order matters: rootHash != manifestHash (otherwise we have a bug)."""
        root = "a" * 64
        manifest = "b" * 64
        normal = compute_anchor_id(root, manifest)
        swapped = compute_anchor_id(manifest, root)
        assert normal != swapped

    def test_matches_anchor_worker_typescript_algorithm(self):
        """Pin the byte-exact algorithm so a future drift breaks this test.

        Equivalent JS:
            sha256(Buffer.from(root.slice(2) + manifest.slice(2), "hex"))
        Re-implement here against the spec and assert agreement.
        """
        root_hex = "deadbeef" + "00" * 28  # 32 bytes
        manifest_hex = "abcd1234" + "ff" * 28  # 32 bytes
        # Reference implementation: parse each hex separately, concat raw
        # bytes, sha256.
        root_bytes = bytes.fromhex(root_hex)
        manifest_bytes = bytes.fromhex(manifest_hex)
        expected = "0x" + hashlib.sha256(root_bytes + manifest_bytes).hexdigest()

        actual = compute_anchor_id(root_hex, manifest_hex)
        assert actual == expected

    def test_uppercase_hex_normalized(self):
        """0X / mixed-case hex must produce the same id as lowercase."""
        root_lc = "deadbeef" + "00" * 28
        manifest_lc = "abcd1234" + "ff" * 28
        a = compute_anchor_id(root_lc, manifest_lc)
        b = compute_anchor_id(root_lc.upper(), manifest_lc.upper())
        c = compute_anchor_id("0X" + root_lc.upper(), manifest_lc)
        assert a == b == c


# --- end-to-end flush() wiring --------------------------------------------


class TestFlushPostsAnchorIdEverywhere:
    def _seed_pending_leaves(self, checkpointer: CardanoCheckpointer):
        """Drop two synthetic leaves on the pending list."""
        checkpointer.pending_leaves = [
            {
                "receipt_id": "0x" + ("11" * 32),
                "cert_hash": "22" * 32,
                "block_num": 100,
                "timestamp": 1700000000.0,
            },
            {
                "receipt_id": "0x" + ("33" * 32),
                "cert_hash": "44" * 32,
                "block_num": 101,
                "timestamp": 1700000001.0,
            },
        ]

    def test_anchor_payload_includes_deterministic_anchor_id(self):
        """Cert-daemon must pass anchorId in the /anchor payload (the bug)."""
        with tempfile.TemporaryDirectory() as tmp:
            cp = _make_checkpointer(tmp)
            self._seed_pending_leaves(cp)

            captured: dict = {}

            def fake_post(url, json=None, headers=None, timeout=None):
                captured["url"] = url
                captured["payload"] = json
                resp = MagicMock()
                resp.status_code = 200
                resp.json.return_value = {
                    "success": True,
                    "anchorId": json["anchorId"],
                    "blockHash": "0x" + "ff" * 32,
                    "contentHash": json["rootHash"],
                    "rootHash": json["rootHash"],
                    "manifestHash": json["manifestHash"],
                }
                return resp

            def fake_put(url, json=None, headers=None, timeout=None):
                captured.setdefault("gateway_calls", []).append(
                    {"url": url, "payload": json, "headers": headers}
                )
                resp = MagicMock()
                resp.status_code = 200
                return resp

            with patch("daemon.checkpoint.requests.post", side_effect=fake_post):
                with patch("daemon.checkpoint.requests.put", side_effect=fake_put):
                    ok = cp.flush(current_best_block=200)

            assert ok is True
            # Anchor-worker payload must include anchorId
            assert "anchorId" in captured["payload"], (
                "Bug regression: /anchor payload missing anchorId"
            )
            assert captured["payload"]["anchorId"].startswith("0x")
            assert len(captured["payload"]["anchorId"]) == 66

            # And the anchorId must be the deterministic compute_anchor_id of
            # (rootHash, manifestHash) — which is what the worker will also
            # compute, so the two will agree.
            expected_id = compute_anchor_id(
                captured["payload"]["rootHash"], captured["payload"]["manifestHash"]
            )
            assert captured["payload"]["anchorId"] == expected_id

    def test_gateway_put_uses_same_anchor_id(self):
        """The /batches/<anchorId> URL must use the same id we sent to /anchor."""
        with tempfile.TemporaryDirectory() as tmp:
            cp = _make_checkpointer(tmp)
            self._seed_pending_leaves(cp)

            seen: dict = {}

            def fake_post(url, json=None, headers=None, timeout=None):
                seen["anchor_payload"] = json
                resp = MagicMock()
                resp.status_code = 200
                resp.json.return_value = {
                    "success": True,
                    "anchorId": json["anchorId"],
                    "blockHash": "0x" + "00" * 32,
                    "contentHash": json["rootHash"],
                    "rootHash": json["rootHash"],
                    "manifestHash": json["manifestHash"],
                }
                return resp

            def fake_put(url, json=None, headers=None, timeout=None):
                seen["gateway_url"] = url
                seen["gateway_payload"] = json
                seen["gateway_headers"] = headers
                resp = MagicMock()
                resp.status_code = 200
                return resp

            with patch("daemon.checkpoint.requests.post", side_effect=fake_post):
                with patch("daemon.checkpoint.requests.put", side_effect=fake_put):
                    cp.flush(current_best_block=200)

            sent_anchor_id = seen["anchor_payload"]["anchorId"]
            sent_id_clean = sent_anchor_id.removeprefix("0x")

            # Gateway URL must include the same anchorId (without 0x prefix).
            assert seen["gateway_url"].endswith(f"/batches/{sent_id_clean}"), (
                f"Gateway URL {seen['gateway_url']!r} does not end with "
                f"/batches/{sent_id_clean!r}"
            )

            # Gateway payload must carry leaf list and matching anchorId.
            assert seen["gateway_payload"]["anchorId"] == sent_anchor_id
            assert seen["gateway_payload"]["leafCount"] == 2
            assert len(seen["gateway_payload"]["leafHashes"]) == 2
            assert seen["gateway_payload"]["rootHash"] == seen["anchor_payload"]["rootHash"]
            # Block range covers both leaves.
            assert seen["gateway_payload"]["blockRangeStart"] == 100
            assert seen["gateway_payload"]["blockRangeEnd"] == 101
            # API key header propagated.
            assert seen["gateway_headers"]["x-api-key"] == "test-api-key"

    def test_worker_can_override_anchor_id(self):
        """If anchor-worker returns a *different* anchorId, the daemon must
        defer to the worker (chain is source of truth) and POST the gateway
        under the worker's id, with a loud error log."""
        with tempfile.TemporaryDirectory() as tmp:
            cp = _make_checkpointer(tmp)
            self._seed_pending_leaves(cp)

            override_id = "0x" + "ab" * 32

            def fake_post(url, json=None, headers=None, timeout=None):
                resp = MagicMock()
                resp.status_code = 200
                # Worker returns a DIFFERENT anchorId than the daemon sent.
                resp.json.return_value = {
                    "success": True,
                    "anchorId": override_id,
                    "blockHash": "0x" + "00" * 32,
                    "contentHash": json["rootHash"],
                    "rootHash": json["rootHash"],
                    "manifestHash": json["manifestHash"],
                }
                return resp

            seen_gateway_url = []

            def fake_put(url, json=None, headers=None, timeout=None):
                seen_gateway_url.append(url)
                resp = MagicMock()
                resp.status_code = 200
                return resp

            with patch("daemon.checkpoint.requests.post", side_effect=fake_post):
                with patch("daemon.checkpoint.requests.put", side_effect=fake_put):
                    cp.flush(current_best_block=200)

            # The gateway PUT must use the override id, not the locally-computed one.
            assert seen_gateway_url, "Gateway PUT was never invoked"
            assert seen_gateway_url[0].endswith(f"/batches/{override_id.removeprefix('0x')}")

    def test_history_records_anchor_id(self):
        """checkpoint-history.json must record anchor_id per batch so a
        replay tool can find batches missing from the gateway."""
        with tempfile.TemporaryDirectory() as tmp:
            cp = _make_checkpointer(tmp)
            self._seed_pending_leaves(cp)

            def fake_post(url, json=None, headers=None, timeout=None):
                resp = MagicMock()
                resp.status_code = 200
                resp.json.return_value = {
                    "success": True,
                    "anchorId": json["anchorId"],
                    "blockHash": "0x" + "00" * 32,
                    "contentHash": json["rootHash"],
                    "rootHash": json["rootHash"],
                    "manifestHash": json["manifestHash"],
                }
                return resp

            def fake_put(url, json=None, headers=None, timeout=None):
                resp = MagicMock()
                resp.status_code = 200
                return resp

            with patch("daemon.checkpoint.requests.post", side_effect=fake_post):
                with patch("daemon.checkpoint.requests.put", side_effect=fake_put):
                    cp.flush(current_best_block=200)

            # checkpoint-history.json should now exist alongside state file
            history_path = cp.state_file.replace(
                "checkpoint-state.json", "checkpoint-history.json"
            )
            assert os.path.exists(history_path)
            with open(history_path) as f:
                history = json.load(f)
            assert len(history) == 1
            batch = history[0]
            assert "anchor_id" in batch
            assert batch["anchor_id"].startswith("0x")
            # The recorded id must be reachable via /batches/<that-id>.
            expected_id = compute_anchor_id(batch["root_hash"], batch["manifest_hash"])
            assert batch["anchor_id"] == expected_id
            # And the leaves carry the per-receipt info needed for verification.
            assert len(batch["leaves"]) == 2
            for leaf in batch["leaves"]:
                assert "receipt_id" in leaf
                assert "cert_hash" in leaf
                assert "leaf_hash" in leaf

    def test_gateway_failure_does_not_lose_checkpoint(self):
        """Gateway 5xx is a soft failure — the cert-daemon must still mark
        the checkpoint as flushed so we don't double-anchor. Replay tooling
        will use checkpoint-history.json to fix the gap."""
        with tempfile.TemporaryDirectory() as tmp:
            cp = _make_checkpointer(tmp)
            self._seed_pending_leaves(cp)

            def fake_post(url, json=None, headers=None, timeout=None):
                resp = MagicMock()
                resp.status_code = 200
                resp.json.return_value = {
                    "success": True,
                    "anchorId": json["anchorId"],
                    "blockHash": "0x" + "00" * 32,
                    "contentHash": json["rootHash"],
                    "rootHash": json["rootHash"],
                    "manifestHash": json["manifestHash"],
                }
                return resp

            def fake_put(url, json=None, headers=None, timeout=None):
                resp = MagicMock()
                resp.status_code = 503
                resp.text = "gateway down"
                return resp

            with patch("daemon.checkpoint.requests.post", side_effect=fake_post):
                with patch("daemon.checkpoint.requests.put", side_effect=fake_put):
                    ok = cp.flush(current_best_block=200)

            # Cardano submission was OK → checkpoint succeeded as a whole.
            assert ok is True
            # Pending leaves cleared (so we don't re-anchor on next flush).
            assert cp.pending_leaves == []
            # History still records the batch (replay path is preserved).
            history_path = cp.state_file.replace(
                "checkpoint-state.json", "checkpoint-history.json"
            )
            assert os.path.exists(history_path)


# --- _post_batch_metadata isolated --------------------------------------


class TestPostBatchMetadata:
    def test_returns_true_on_2xx(self):
        with tempfile.TemporaryDirectory() as tmp:
            cp = _make_checkpointer(tmp)

            def fake_put(url, json=None, headers=None, timeout=None):
                resp = MagicMock()
                resp.status_code = 200
                return resp

            with patch("daemon.checkpoint.requests.put", side_effect=fake_put):
                ok = cp._post_batch_metadata(
                    "0x" + "00" * 32,
                    "11" * 32,
                    [{"block_num": 5}, {"block_num": 9}],
                    ["aa" * 32, "bb" * 32],
                )
            assert ok is True

    def test_returns_false_on_4xx(self):
        with tempfile.TemporaryDirectory() as tmp:
            cp = _make_checkpointer(tmp)

            def fake_put(url, json=None, headers=None, timeout=None):
                resp = MagicMock()
                resp.status_code = 401
                resp.text = "no auth"
                return resp

            with patch("daemon.checkpoint.requests.put", side_effect=fake_put):
                ok = cp._post_batch_metadata(
                    "0x" + "00" * 32,
                    "11" * 32,
                    [{"block_num": 5}],
                    ["aa" * 32],
                )
            assert ok is False

    def test_returns_false_when_url_unconfigured(self):
        with tempfile.TemporaryDirectory() as tmp:
            cp = _make_checkpointer(tmp)
            cp.config.blob_gateway_url = ""
            # Also clear the env in case a host machine has it set.
            with patch.dict(os.environ, {"BLOB_GATEWAY_URL": ""}, clear=False):
                ok = cp._post_batch_metadata(
                    "0x" + "00" * 32,
                    "11" * 32,
                    [{"block_num": 5}],
                    ["aa" * 32],
                )
            assert ok is False

    def test_url_strips_0x_prefix(self):
        """Gateway storage uses the prefix-less hex form. Daemon must match."""
        with tempfile.TemporaryDirectory() as tmp:
            cp = _make_checkpointer(tmp)

            seen = {}

            def fake_put(url, json=None, headers=None, timeout=None):
                seen["url"] = url
                resp = MagicMock()
                resp.status_code = 200
                return resp

            with patch("daemon.checkpoint.requests.put", side_effect=fake_put):
                cp._post_batch_metadata(
                    "0x" + "ab" * 32,
                    "cd" * 32,
                    [{"block_num": 5}],
                    ["ee" * 32],
                )
            # 0x stripped from the URL path component.
            assert seen["url"] == f"http://gateway.test:8080/batches/{'ab' * 32}"


# --- merkle_root sanity (no behaviour change here, but pin it) -----------


def test_merkle_root_single_leaf_passthrough():
    leaf = b"\x01" * 32
    assert merkle_root([leaf]) == leaf


def test_merkle_root_two_leaves():
    a = b"\x01" * 32
    b = b"\x02" * 32
    expected = hashlib.sha256(a + b).digest()
    assert merkle_root([a, b]) == expected
