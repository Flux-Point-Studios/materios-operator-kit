"""Task #116 — CardanoCheckpointer pending-leaves dedup.

Audit on preprod (trace 5554a6d2-35a2-463e-9b5c-821f7eb41946) measured
5.80 leaves per anchor for only 1.85 unique receipts — the M=3 committee
emits one AvailabilityCertified event per signer for the same
(receipt_id, cert_hash), and the cert-daemon was treating each echo as a
distinct Merkle leaf. The fix collapses same-(receipt_id, cert_hash)
duplicates while keeping distinct cert_hash re-attestations as separate facts.

Tests:
  1. add_cert called 3× with the same (receipt_id, cert_hash) → 1 leaf
  2. add_cert called 2× with same receipt_id but DIFFERENT cert_hash → 2 leaves
  3. _load_state on a state file with pre-existing duplicates dedupes on startup
  4. Block-num and timestamp from the FIRST committee event are preserved
     (downstream `should_flush` uses oldest timestamp + min block_num)
  5. Mixed scenario: distinct receipt_ids, same receipt_id+different cert_hash,
     and committee echoes all interleaved
"""
import json
import os
import tempfile

import pytest

from daemon.checkpoint import CardanoCheckpointer
from daemon.config import DaemonConfig


@pytest.fixture
def tmp_state_file():
    """Yield a path under a tmpdir; CardanoCheckpointer writes JSON state there."""
    with tempfile.TemporaryDirectory() as d:
        yield os.path.join(d, "checkpoint-state.json")


def _make_checkpointer(state_file: str) -> CardanoCheckpointer:
    """Construct a CardanoCheckpointer with minimal config.

    signer_uri=//Alice is the canonical dev key — Keypair.create_from_uri
    accepts it directly without network calls. No RPC / Cardano traffic
    occurs in __init__; the only side effects are SS58 derivation and
    state-file load.
    """
    config = DaemonConfig(
        signer_uri="//Alice",
        chain_id="00" * 32,
        cardano_anchor_url="",
        anchor_worker_token="",
        checkpoint_batch_size=10,
        checkpoint_interval=60,
    )
    return CardanoCheckpointer(config, state_file)


# ---------------------------------------------------------------------------
# Test 1: 3 identical events from M=3 committee collapse to 1 leaf
# ---------------------------------------------------------------------------
def test_three_committee_echoes_collapse_to_one_leaf(tmp_state_file):
    cp = _make_checkpointer(tmp_state_file)
    receipt_id = "0x" + "ab" * 32
    cert_hash = b"\xcd" * 32

    cp.add_cert(receipt_id, cert_hash, block_num=100)
    cp.add_cert(receipt_id, cert_hash, block_num=100)
    cp.add_cert(receipt_id, cert_hash, block_num=100)

    assert len(cp.pending_leaves) == 1, (
        f"Expected 1 leaf for 3 committee echoes of the same "
        f"(receipt_id, cert_hash); got {len(cp.pending_leaves)}"
    )
    assert cp.pending_leaves[0]["receipt_id"] == receipt_id
    assert cp.pending_leaves[0]["cert_hash"] == cert_hash.hex()


def test_dedupe_persists_to_state_file(tmp_state_file):
    """After dedup, re-loading state must still show 1 leaf."""
    cp = _make_checkpointer(tmp_state_file)
    receipt_id = "0x" + "ab" * 32
    cert_hash = b"\xcd" * 32

    cp.add_cert(receipt_id, cert_hash, block_num=100)
    cp.add_cert(receipt_id, cert_hash, block_num=100)
    cp.add_cert(receipt_id, cert_hash, block_num=100)

    with open(tmp_state_file) as f:
        on_disk = json.load(f)
    assert len(on_disk["pending_leaves"]) == 1


# ---------------------------------------------------------------------------
# Test 2: same receipt_id + DIFFERENT cert_hash → 2 leaves (re-attestation)
# ---------------------------------------------------------------------------
def test_same_receipt_different_cert_hash_keeps_both_leaves(tmp_state_file):
    cp = _make_checkpointer(tmp_state_file)
    receipt_id = "0x" + "ef" * 32
    cert_hash_v1 = b"\x11" * 32
    cert_hash_v2 = b"\x22" * 32

    cp.add_cert(receipt_id, cert_hash_v1, block_num=200)
    cp.add_cert(receipt_id, cert_hash_v2, block_num=210)

    assert len(cp.pending_leaves) == 2, (
        f"Same receipt_id with two different cert_hash values is two distinct "
        f"facts; expected 2 leaves, got {len(cp.pending_leaves)}"
    )
    cert_hashes = {leaf["cert_hash"] for leaf in cp.pending_leaves}
    assert cert_hashes == {cert_hash_v1.hex(), cert_hash_v2.hex()}


# ---------------------------------------------------------------------------
# Test 3: state file with pre-existing duplicates dedupes on startup
# ---------------------------------------------------------------------------
def test_load_state_dedupes_preexisting_duplicates(tmp_state_file):
    """Simulate state from an old daemon binary that didn't dedup; new
    code must clean it up on _load_state."""
    receipt_id = "0x" + "aa" * 32
    cert_hash_hex = ("bb" * 32)
    other_id = "0x" + "11" * 32
    other_hash_hex = ("22" * 32)

    bloated_state = {
        "pending_leaves": [
            # 5 echoes of (receipt_id, cert_hash) — committee size 5
            {"receipt_id": receipt_id, "cert_hash": cert_hash_hex,
             "block_num": 1000, "timestamp": 1000.0},
            {"receipt_id": receipt_id, "cert_hash": cert_hash_hex,
             "block_num": 1000, "timestamp": 1000.5},
            {"receipt_id": receipt_id, "cert_hash": cert_hash_hex,
             "block_num": 1000, "timestamp": 1001.0},
            {"receipt_id": receipt_id, "cert_hash": cert_hash_hex,
             "block_num": 1000, "timestamp": 1001.5},
            {"receipt_id": receipt_id, "cert_hash": cert_hash_hex,
             "block_num": 1000, "timestamp": 1002.0},
            # 1 distinct receipt
            {"receipt_id": other_id, "cert_hash": other_hash_hex,
             "block_num": 1010, "timestamp": 1010.0},
        ],
        "last_checkpointed_block": 999,
        "last_flush_time": 0.0,
    }
    with open(tmp_state_file, "w") as f:
        json.dump(bloated_state, f)

    cp = _make_checkpointer(tmp_state_file)

    assert len(cp.pending_leaves) == 2, (
        f"Loaded state had 5 committee echoes + 1 distinct = 6 raw entries; "
        f"after dedup expected 2 unique facts, got {len(cp.pending_leaves)}"
    )
    keys = {(leaf["receipt_id"], leaf["cert_hash"]) for leaf in cp.pending_leaves}
    assert keys == {
        (receipt_id, cert_hash_hex),
        (other_id, other_hash_hex),
    }

    # The deduped state must have been written back to disk so a future load
    # is also clean (idempotent)
    with open(tmp_state_file) as f:
        on_disk = json.load(f)
    assert len(on_disk["pending_leaves"]) == 2


# ---------------------------------------------------------------------------
# Test 4: kept-leaf is the FIRST one — preserves earliest block_num and timestamp
# ---------------------------------------------------------------------------
def test_dedup_keeps_first_event_metadata(tmp_state_file):
    """should_flush() uses min(block_num) and min(timestamp); a dedup that
    kept a LATER duplicate would silently delay the leaf-age flush trigger."""
    cp = _make_checkpointer(tmp_state_file)
    receipt_id = "0x" + "33" * 32
    cert_hash = b"\x44" * 32

    cp.add_cert(receipt_id, cert_hash, block_num=500)
    first_ts = cp.pending_leaves[0]["timestamp"]

    cp.add_cert(receipt_id, cert_hash, block_num=505)
    cp.add_cert(receipt_id, cert_hash, block_num=510)

    assert len(cp.pending_leaves) == 1
    assert cp.pending_leaves[0]["block_num"] == 500
    assert cp.pending_leaves[0]["timestamp"] == first_ts


# ---------------------------------------------------------------------------
# Test 5: mixed traffic — committee echoes + reattest + distinct receipts
# ---------------------------------------------------------------------------
def test_mixed_traffic_dedup(tmp_state_file):
    cp = _make_checkpointer(tmp_state_file)
    rid_a = "0x" + "0a" * 32
    rid_b = "0x" + "0b" * 32
    rid_c = "0x" + "0c" * 32
    h1 = b"\x01" * 32
    h2 = b"\x02" * 32

    # rid_a: 3 committee echoes for cert_hash=h1
    cp.add_cert(rid_a, h1, 1)
    cp.add_cert(rid_a, h1, 1)
    cp.add_cert(rid_a, h1, 1)
    # rid_a: re-attestation with different cert_hash=h2 (distinct fact)
    cp.add_cert(rid_a, h2, 2)
    cp.add_cert(rid_a, h2, 2)  # echo of the re-attestation
    # rid_b: 3 committee echoes for cert_hash=h1
    cp.add_cert(rid_b, h1, 3)
    cp.add_cert(rid_b, h1, 3)
    cp.add_cert(rid_b, h1, 3)
    # rid_c: single event
    cp.add_cert(rid_c, h1, 4)

    # Unique facts: (rid_a, h1), (rid_a, h2), (rid_b, h1), (rid_c, h1) = 4
    assert len(cp.pending_leaves) == 4
    keys = {(leaf["receipt_id"], leaf["cert_hash"]) for leaf in cp.pending_leaves}
    assert keys == {
        (rid_a, h1.hex()),
        (rid_a, h2.hex()),
        (rid_b, h1.hex()),
        (rid_c, h1.hex()),
    }
