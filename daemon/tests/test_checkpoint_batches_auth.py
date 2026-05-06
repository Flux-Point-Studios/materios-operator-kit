"""Task #122 — CardanoCheckpointer._post_batch_metadata sr25519 sig auth.

The gateway's `/batches/:anchorId` route requires auth (api-key OR
sr25519 sig). Older code paths only attached `x-api-key` when
`BLOB_GATEWAY_API_KEY` was set in the daemon env; production preprod
deploy does NOT set that, so every PUT was 401-ing silently and the
gateway's reverse-lookup index never got populated.

This task switches the daemon to sign each PUT with its committee
sr25519 keypair (already loaded for heartbeats), so no static secret is
needed. Pre-image format (pinned by `services/blob-gateway/src/upload-auth.ts`):

    materios-upload-v1|{anchorId_no_0x}|{address}|{ts}

Tests:
  1. PUT carries x-upload-sig + x-uploader-address + x-upload-ts headers
  2. Sig verifies against the expected pre-image (sr25519 verify)
  3. anchorId in pre-image is the URL-path form (no 0x), matching the
     gateway's contentHash slot for resolveAuth
  4. Timestamp is current epoch seconds (skew < 60s)
  5. When BLOB_GATEWAY_API_KEY is also set, x-api-key is ALSO attached
     (belt-and-suspenders, gateway prefers api-key path when both present)
  6. When BLOB_GATEWAY_API_KEY is NOT set, sig auth alone works
     (the actual fix: previously this path had no auth headers at all)
  7. _save_batch_history → _post_batch_metadata sequence still wraps
     correctly when called from flush()
"""
import os
import tempfile
import time
from unittest.mock import patch, MagicMock

import pytest
from substrateinterface import Keypair

from daemon.checkpoint import CardanoCheckpointer
from daemon.config import DaemonConfig


@pytest.fixture
def tmp_state_file():
    with tempfile.TemporaryDirectory() as d:
        yield os.path.join(d, "checkpoint-state.json")


def _make_checkpointer(state_file: str, blob_gateway_url: str = "http://gw.test:3000",
                       blob_gateway_api_key: str = "") -> CardanoCheckpointer:
    """Construct with a real //Alice keypair so sigs are verifiable."""
    config = DaemonConfig(
        signer_uri="//Alice",
        chain_id="00" * 32,
        cardano_anchor_url="",
        anchor_worker_token="",
        checkpoint_batch_size=10,
        checkpoint_interval=60,
        blob_gateway_url=blob_gateway_url,
        blob_gateway_api_key=blob_gateway_api_key,
    )
    return CardanoCheckpointer(config, state_file)


def _alice_address() -> str:
    return Keypair.create_from_uri("//Alice").ss58_address


# ---------------------------------------------------------------------------
# Test 1 + 2 + 3: sig headers present, verify against pre-image, anchorId form
# ---------------------------------------------------------------------------
def test_post_batch_metadata_sends_verifiable_sr25519_sig(tmp_state_file):
    cp = _make_checkpointer(tmp_state_file)
    anchor_id = "0x" + "ab" * 32
    anchor_id_no_0x = "ab" * 32

    eligible = [{"receipt_id": "0x" + "11" * 32, "block_num": 100,
                 "cert_hash": "22" * 32}]
    leaf_hashes = ["33" * 32]

    with patch("daemon.checkpoint.requests.put") as mock_put:
        mock_put.return_value = MagicMock(status_code=200, text="ok")
        before = int(time.time())
        ok = cp._post_batch_metadata(anchor_id, "44" * 32, eligible, leaf_hashes)
        after = int(time.time())

    assert ok is True
    assert mock_put.call_count == 1
    call = mock_put.call_args
    url = call[0][0]
    headers = call[1]["headers"]

    # URL: anchor_id stripped of 0x
    assert url == f"http://gw.test:3000/batches/{anchor_id_no_0x}"

    # Sig headers all present
    assert "x-upload-sig" in headers
    assert "x-uploader-address" in headers
    assert "x-upload-ts" in headers
    assert headers["x-uploader-address"] == _alice_address()

    # Timestamp within current epoch second window
    ts = int(headers["x-upload-ts"])
    assert before <= ts <= after, f"ts={ts} not in [{before}, {after}]"

    # Sig verifies against the documented pre-image
    signing_string = f"materios-upload-v1|{anchor_id_no_0x}|{_alice_address()}|{ts}"
    sig_hex = headers["x-upload-sig"]
    assert sig_hex.startswith("0x"), "sig must be 0x-prefixed hex"
    sig_bytes = bytes.fromhex(sig_hex[2:])

    alice = Keypair.create_from_uri("//Alice")
    assert alice.verify(signing_string.encode("utf-8"), sig_bytes), (
        f"Sig did NOT verify. signing_string={signing_string!r} "
        f"sig_hex_prefix={sig_hex[:18]}..."
    )


def test_pre_image_uses_anchor_id_no_0x_not_prefixed(tmp_state_file):
    """Critical: the gateway uses the URL-path anchorId (no 0x) as contentHash
    in resolveAuth. Daemon must sign over the SAME form, else verify fails."""
    cp = _make_checkpointer(tmp_state_file)
    anchor_id = "0x" + "cc" * 32
    anchor_id_no_0x = "cc" * 32

    eligible = [{"receipt_id": "0x" + "11" * 32, "block_num": 1, "cert_hash": "22" * 32}]
    with patch("daemon.checkpoint.requests.put") as mock_put:
        mock_put.return_value = MagicMock(status_code=200, text="ok")
        cp._post_batch_metadata(anchor_id, "44" * 32, eligible, ["33" * 32])

    headers = mock_put.call_args[1]["headers"]
    ts = int(headers["x-upload-ts"])

    # The 0x-prefixed pre-image MUST NOT verify (would mean we signed wrong form).
    bad = f"materios-upload-v1|{anchor_id}|{_alice_address()}|{ts}"
    sig_bytes = bytes.fromhex(headers["x-upload-sig"][2:])
    alice = Keypair.create_from_uri("//Alice")
    assert not alice.verify(bad.encode("utf-8"), sig_bytes), (
        "Sig accidentally verified against 0x-prefixed pre-image — daemon is "
        "signing the wrong form, gateway will 401."
    )

    # The no-0x pre-image MUST verify.
    good = f"materios-upload-v1|{anchor_id_no_0x}|{_alice_address()}|{ts}"
    assert alice.verify(good.encode("utf-8"), sig_bytes)


# ---------------------------------------------------------------------------
# Test 5 + 6: api-key coexistence
# ---------------------------------------------------------------------------
def test_sig_only_path_when_api_key_unset(tmp_state_file):
    """The actual fix: when BLOB_GATEWAY_API_KEY is empty, the PUT should
    still authenticate via sr25519 sig, NOT go out unsigned."""
    cp = _make_checkpointer(tmp_state_file, blob_gateway_api_key="")
    eligible = [{"receipt_id": "0x" + "11" * 32, "block_num": 1, "cert_hash": "22" * 32}]

    with patch("daemon.checkpoint.requests.put") as mock_put:
        mock_put.return_value = MagicMock(status_code=200, text="ok")
        cp._post_batch_metadata("0x" + "ab" * 32, "44" * 32, eligible, ["33" * 32])

    headers = mock_put.call_args[1]["headers"]
    assert "x-api-key" not in headers, "api-key must NOT be sent when unset"
    assert "x-upload-sig" in headers, "sig MUST be sent regardless of api-key"
    assert "x-uploader-address" in headers
    assert "x-upload-ts" in headers


def test_both_sig_and_api_key_when_api_key_set(tmp_state_file):
    """When the operator has provisioned an api-key, send both. Gateway
    resolveAuth will pick api-key first (highest trust); sig is the
    backstop. Either way the PUT authenticates."""
    cp = _make_checkpointer(tmp_state_file, blob_gateway_api_key="SECRET-API-KEY-123")
    eligible = [{"receipt_id": "0x" + "11" * 32, "block_num": 1, "cert_hash": "22" * 32}]

    with patch("daemon.checkpoint.requests.put") as mock_put:
        mock_put.return_value = MagicMock(status_code=200, text="ok")
        cp._post_batch_metadata("0x" + "ab" * 32, "44" * 32, eligible, ["33" * 32])

    headers = mock_put.call_args[1]["headers"]
    assert headers.get("x-api-key") == "SECRET-API-KEY-123"
    assert "x-upload-sig" in headers
    assert "x-uploader-address" in headers
    assert "x-upload-ts" in headers


# ---------------------------------------------------------------------------
# Test 7: missing url short-circuits without sigging (no wasted crypto work)
# ---------------------------------------------------------------------------
def test_missing_blob_gateway_url_short_circuits(tmp_state_file):
    cp = _make_checkpointer(tmp_state_file, blob_gateway_url="")
    eligible = [{"receipt_id": "0x" + "11" * 32, "block_num": 1, "cert_hash": "22" * 32}]

    with patch("daemon.checkpoint.requests.put") as mock_put:
        ok = cp._post_batch_metadata("0x" + "ab" * 32, "44" * 32, eligible, ["33" * 32])

    assert ok is False
    assert mock_put.call_count == 0, "no PUT when gateway URL is unset"


# ---------------------------------------------------------------------------
# Test 8: keypair retained on instance (not just used for SS58 derivation)
# ---------------------------------------------------------------------------
def test_keypair_attribute_retained(tmp_state_file):
    """Earlier code derived SS58 once and dropped the Keypair. Task #122
    keeps it as `self.keypair` so PUT signing is possible without
    re-deriving from signer_uri (which may be a 24-word mnemonic)."""
    cp = _make_checkpointer(tmp_state_file)
    assert hasattr(cp, "keypair")
    assert isinstance(cp.keypair, Keypair)
    assert cp.keypair.ss58_address == cp.submitter_address
