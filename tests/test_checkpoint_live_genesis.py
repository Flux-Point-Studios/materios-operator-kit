"""Tests for checkpoint.flush() live-chain-genesis migration (task #207 successor).

Background: before 2026-05-12 `flush()` read `self.config.chain_id` for both
leaf binding and the on-Cardano manifest `materios_chain_id` field. That
env value drifted stale across chain resets (5 receipts stranded on preprod
v6 because daemons cached the v5 hash). cert_daemon PR #22 migrated
`build_cert` to `self._live_chain_genesis`; this PR completes the migration
by making `flush()` take `live_chain_genesis` as a required kwarg.

These tests pin the migration contract:
  1. Flushing without `live_chain_genesis` is a no-op (returns False), not a
     silent stale-binding.
  2. Leaf binding uses the kwarg, not `config.chain_id` — proven by setting
     them to different values and asserting the on-wire request matches the
     kwarg.
  3. The on-wire manifest also embeds the kwarg, not the env.
  4. 0x prefix + case normalization is applied (so callers can pass either
     `0x…` or bare hex).
"""
import hashlib
import json
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from daemon.checkpoint import CardanoCheckpointer, merkle_root, _sha256
from daemon.config import DaemonConfig


LIVE_CHAIN_ID = "0e46e33f639a56cc8780fd871d9a15e16d99af248526f907cb560cb40849f7bf"
STALE_CONFIG_CHAIN_ID = "bc0531cb311281565036fb397a376f0e0fa37005589655f97a7924b2729a164c"

RECEIPT_ID = "0x7556a31f46c3429e79864820c270e246fdaccaec53404c9d66f342bf9ec32c1c"
CERT_HASH_HEX = "eea9520527a2508b15e116aa0fbe564ea20fe177c9f7bc3b1a2d93e689513b46"


def _make_checkpointer(tmpdir: str) -> CardanoCheckpointer:
    config = DaemonConfig()
    # Intentionally set config.chain_id to a STALE value to prove the live
    # kwarg, not env, is what flush uses.
    config.chain_id = STALE_CONFIG_CHAIN_ID
    config.cardano_network_id = "11" * 32
    config.cardano_anchor_url = "http://anchor-worker.test:3333"
    config.anchor_worker_token = "test-token"
    config.blob_gateway_url = "http://gateway.test:8080"
    config.signer_uri = "//Alice"
    config.checkpoint_batch_size = 1
    config.checkpoint_interval = 0
    config.finality_confirmations = 0
    config.data_dir = tmpdir
    state_file = os.path.join(tmpdir, "checkpoint-state.json")
    return CardanoCheckpointer(config, state_file=state_file)


def _add_one_leaf(cp: CardanoCheckpointer) -> None:
    cp.add_cert(RECEIPT_ID, bytes.fromhex(CERT_HASH_HEX), block_num=100)


def test_flush_without_live_chain_genesis_returns_false():
    """Calling flush() with no live_chain_genesis must NOT silently bind the
    stale env value — it must refuse and leave leaves queued."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cp = _make_checkpointer(tmpdir)
        _add_one_leaf(cp)

        # Patch requests so a network attempt would loudly fail.
        with patch("daemon.checkpoint.requests") as mock_requests:
            ok = cp.flush(current_best_block=200)
            assert ok is False
            # Crucial: no HTTP attempt — flush refused before reaching network.
            mock_requests.post.assert_not_called()
            mock_requests.put.assert_not_called()
        # And the leaves remain queued for next interval.
        assert len(cp.pending_leaves) == 1


def _expected_manifest_hash(chain_id: str, cardano_net_id: str, from_block: int,
                            to_block: int, count: int, root_hex: str) -> str:
    manifest = {
        "materios_chain_id": chain_id,
        "cardano_network_id": cardano_net_id,
        "from_block": from_block,
        "to_block": to_block,
        "count": count,
        "root": root_hex,
    }
    return hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest()


def test_flush_manifest_hash_binds_live_chain_id_not_config():
    """Even with a stale config.chain_id, the on-wire manifestHash must
    reflect the live_chain_genesis kwarg (since manifest['materios_chain_id']
    feeds into the hash)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cp = _make_checkpointer(tmpdir)
        _add_one_leaf(cp)
        assert cp.config.chain_id == STALE_CONFIG_CHAIN_ID  # sanity

        with patch("daemon.checkpoint.requests") as mock_requests:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"anchorId": "0xdeadbeef"}
            mock_requests.post.return_value = mock_response
            mock_requests.put.return_value = mock_response

            ok = cp.flush(current_best_block=200, live_chain_genesis=LIVE_CHAIN_ID)
            assert ok is True

            payload = mock_requests.post.call_args_list[0].kwargs["json"]
            actual_root = payload["rootHash"]
            actual_manifest_hash = payload["manifestHash"]

        expected_under_live = _expected_manifest_hash(
            LIVE_CHAIN_ID, cp.config.cardano_network_id, 100, 100, 1, actual_root
        )
        expected_under_stale = _expected_manifest_hash(
            STALE_CONFIG_CHAIN_ID, cp.config.cardano_network_id, 100, 100, 1, actual_root
        )
        assert actual_manifest_hash == expected_under_live
        assert actual_manifest_hash != expected_under_stale


def test_leaf_binding_uses_live_chain_id():
    """The Merkle root computed inside flush() must use live_chain_genesis
    as the binding prefix, not config.chain_id."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cp = _make_checkpointer(tmpdir)
        _add_one_leaf(cp)

        with patch("daemon.checkpoint.requests") as mock_requests:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"anchorId": "0xdeadbeef"}
            mock_requests.post.return_value = mock_response
            mock_requests.put.return_value = mock_response

            cp.flush(current_best_block=200, live_chain_genesis=LIVE_CHAIN_ID)

            payload = mock_requests.post.call_args_list[0].kwargs["json"]
            root_hex = payload["rootHash"]

        # Recompute what flush *should* have produced, using LIVE_CHAIN_ID.
        expected_leaf = _sha256(
            b"materios-checkpoint-v1"
            + bytes.fromhex(LIVE_CHAIN_ID)
            + bytes.fromhex(RECEIPT_ID.removeprefix("0x"))
            + bytes.fromhex(CERT_HASH_HEX)
        )
        expected_root = merkle_root([expected_leaf]).hex()
        assert root_hex == expected_root

        # And confirm a binding with the STALE config value would NOT match.
        stale_leaf = _sha256(
            b"materios-checkpoint-v1"
            + bytes.fromhex(STALE_CONFIG_CHAIN_ID)
            + bytes.fromhex(RECEIPT_ID.removeprefix("0x"))
            + bytes.fromhex(CERT_HASH_HEX)
        )
        stale_root = merkle_root([stale_leaf]).hex()
        assert root_hex != stale_root


def test_flush_accepts_0x_prefix():
    """Callers can pass `0x…` or bare hex; both produce identical bindings."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cp = _make_checkpointer(tmpdir)
        _add_one_leaf(cp)
        cp2 = _make_checkpointer(tempfile.mkdtemp())
        _add_one_leaf(cp2)

        with patch("daemon.checkpoint.requests") as mock_requests:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"anchorId": "0x"}
            mock_requests.post.return_value = mock_response
            mock_requests.put.return_value = mock_response

            cp.flush(current_best_block=200, live_chain_genesis=LIVE_CHAIN_ID)
            cp2.flush(current_best_block=200, live_chain_genesis="0x" + LIVE_CHAIN_ID.upper())

            p1 = mock_requests.post.call_args_list[0].kwargs["json"]
            p2 = mock_requests.post.call_args_list[1].kwargs["json"]
            assert p1["rootHash"] == p2["rootHash"]
            assert p1["manifestHash"] == p2["manifestHash"]
