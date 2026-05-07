"""Tests for `daemon.evidence_submitter` (task #143).

These tests exercise the chain-submission daemon path with mocks at the
substrate-interface + aiohttp seams. No live chain or HTTP server is
required.

Coverage matrix:

    Submission path:
      - happy path: GET pending → submit_evidence ext → POST mark_submitted
      - skip when receipt_id not yet on chain (content_hash query empty)
      - skip when payload assembly fails (bad base64)
      - skip on PalletDisabled error (kill-switch ON)
      - skip on generic submit failure (no ack)

    Payload assembly:
      - arm_trustzone with valid base64 chain → SCALE-encoded Vec<Vec<u8>>
      - arm_trustzone with empty cert_chain_b64 → ValueError
      - arm_trustzone with bad-base64 cert → ValueError
      - unknown evidence_type variant raises NotImplementedError

    Cursor handling:
      - successful tick advances cursor to next_since
      - failed mark_submitted does NOT advance past the failure point

    Factory (maybe_create_evidence_submitter):
      - missing token → returns None (soft-disable)
      - missing gateway URL → returns None (soft-disable)
      - both present → returns EvidenceSubmitter

    Class-body integrity:
      - guards against indentation drift on EvidenceSubmitter methods
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import tempfile
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from daemon.evidence_submitter import (
    EVIDENCE_TYPE_TO_PALLET_VARIANT,
    MAX_EVIDENCE_PAYLOAD_BYTES,
    TERMINAL_ERROR_REASONS,
    EvidenceSubmitter,
    _build_arm_trustzone_payload_bytes,
    _build_evidence_payload_bytes,
    _classify_chain_error,
    _content_hash_from_receipt_id_via_chain,
    _is_pallet_disabled_error,
    maybe_create_evidence_submitter,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

ATTESTOR_SS58 = "5Dd7WuLMyb71NT1Bea6oEZH8Je3MkQzamHVeU4tmQbtPWq2v"
RECEIPT_ID_HEX = "a1" * 32  # 64 hex chars
CONTENT_HASH_BYTES = bytes(range(32))


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def _run(coro):
    """Mirror the harness used by the rest of the suite (test_auto_bond.py
    etc.). The repo doesn't ship pytest-asyncio so each async test drives
    its own event loop."""
    return asyncio.new_event_loop().run_until_complete(coro)


def _make_substrate_mock(
    *,
    receipts_query_value: dict | None = None,
    encode_scale_return: bytes = b"\x04\x00",  # one-element vec, single 0-byte cert
    submit_success: bool = True,
    error_message: str | None = None,
    extrinsic_hash: str = "0x" + ("ee" * 32),
):
    """Build a minimal substrate-interface stub with the seams we hit."""
    si = MagicMock()

    # Storage query (Receipts → content_hash)
    receipts_result = SimpleNamespace(
        value=receipts_query_value if receipts_query_value is not None else {
            "content_hash": list(CONTENT_HASH_BYTES),
        }
    )
    si.query = MagicMock(return_value=receipts_result)

    # encode_scale (Vec<Bytes> → SCALE bytes)
    si.encode_scale = MagicMock(return_value=encode_scale_return)

    # compose_call → opaque object
    si.compose_call = MagicMock(return_value=MagicMock(name="composed_call"))

    # create_signed_extrinsic → opaque object
    si.create_signed_extrinsic = MagicMock(name="signed_extrinsic")

    # submit_extrinsic → success/failure receipt
    receipt = MagicMock()
    receipt.is_success = submit_success
    receipt.error_message = error_message
    receipt.extrinsic_hash = extrinsic_hash if submit_success else None
    receipt.block_hash = "0x" + ("bb" * 32)
    si.submit_extrinsic = MagicMock(return_value=receipt)

    return si


def _make_client_mock(substrate=None) -> MagicMock:
    client = MagicMock()
    client.substrate = substrate or _make_substrate_mock()
    client.keypair = SimpleNamespace(
        ss58_address=ATTESTOR_SS58,
        public_key=b"\xaa" * 32,
    )
    return client


def _make_submitter(
    *,
    client=None,
    poll_interval: int = 30,
    page_size: int = 50,
    submitter_token: str = "tok",
    admin_token: str | None = None,
    gateway_url: str = "http://gateway.test",
    failed_state_path: str | None = None,
) -> EvidenceSubmitter:
    config = SimpleNamespace(blob_base_url=gateway_url, blob_gateway_url=gateway_url)
    # Default to a per-test tmp file so the failed-row state never collides
    # with /data and never leaks across tests.
    if failed_state_path is None:
        fd, failed_state_path = tempfile.mkstemp(
            prefix="evidence-failed-", suffix=".json"
        )
        os.close(fd)
        # Start fresh — mkstemp creates a 0-byte file, but our loader
        # tolerates that.
    sub = EvidenceSubmitter(
        config=config,
        substrate_client=client or _make_client_mock(),
        chain_write_lock=asyncio.Lock(),
        gateway_url=gateway_url,
        submitter_token=submitter_token,
        admin_token=admin_token or submitter_token,
        poll_interval=poll_interval,
        page_size=page_size,
        failed_state_path=failed_state_path,
    )
    # Tests drive _tick() directly without going through _run_forever, so
    # the running flag is never set by start(). Flip it on so the
    # `if not self._running: break` guard inside the per-row loop doesn't
    # short-circuit the test.
    sub._running = True
    return sub


# ---------------------------------------------------------------------------
# Class-body integrity
# ---------------------------------------------------------------------------

def test_evidence_submitter_methods_intact():
    """Guard against indentation drift in the class body."""
    for name in (
        "ensure_registered",
        "fetch_pending",
        "mark_submitted",
        "submit_one",
        "_compose_submit_evidence_call",
        "_tick",
        "_run_forever",
        "start",
        "stop",
    ):
        assert callable(getattr(EvidenceSubmitter, name, None)), (
            f"EvidenceSubmitter.{name} missing — likely an indentation bug "
            f"nested it inside a module-level function."
        )


# ---------------------------------------------------------------------------
# Pallet variant mapping pinned
# ---------------------------------------------------------------------------

def test_evidence_type_variant_map_pinned():
    """The map must match `pallets/tee-attestation/src/types.rs::EvidenceType`.
    Append-only — never re-order or remove entries."""
    assert EVIDENCE_TYPE_TO_PALLET_VARIANT == {
        "amd_sev_snp": "AmdSevSnp",
        "intel_tdx": "IntelTdx",
        "arm_trustzone": "ArmTrustZone",
        "reproducible_build": "ReproducibleBuild",
        "zk_vm_execution": "ZkVmExecution",
    }


# ---------------------------------------------------------------------------
# Payload assembly
# ---------------------------------------------------------------------------

class TestBuildArmTrustZonePayload:
    def test_valid_chain_calls_encode_scale(self):
        substrate = _make_substrate_mock()
        payload = {
            "cert_chain_b64": [_b64(b"\x01\x02"), _b64(b"\x03\x04\x05")],
            "device_model": "Pixel-X",
            "security_level": "StrongBox",
        }
        out = _build_arm_trustzone_payload_bytes(substrate, payload)
        # encode_scale was called with the decoded bytes list
        substrate.encode_scale.assert_called_once()
        kwargs = substrate.encode_scale.call_args.kwargs
        assert kwargs["type_string"] == "Vec<Bytes>"
        assert kwargs["value"] == [b"\x01\x02", b"\x03\x04\x05"]
        # And we got bytes back (the mock returns bytes).
        assert isinstance(out, bytes)

    def test_empty_chain_raises(self):
        with pytest.raises(ValueError, match="missing or empty"):
            _build_arm_trustzone_payload_bytes(
                _make_substrate_mock(), {"cert_chain_b64": []}
            )

    def test_missing_chain_raises(self):
        with pytest.raises(ValueError, match="missing or empty"):
            _build_arm_trustzone_payload_bytes(
                _make_substrate_mock(), {"device_model": "X"}
            )

    def test_non_string_entry_raises(self):
        with pytest.raises(ValueError, match="must be a base64 string"):
            _build_arm_trustzone_payload_bytes(
                _make_substrate_mock(),
                {"cert_chain_b64": [123]},
            )

    def test_bad_base64_raises(self):
        with pytest.raises(ValueError, match="not valid base64"):
            _build_arm_trustzone_payload_bytes(
                _make_substrate_mock(),
                {"cert_chain_b64": ["@@not-valid-base64@@"]},
            )


class TestBuildEvidencePayloadDispatch:
    def test_arm_trustzone_dispatches(self):
        substrate = _make_substrate_mock()
        out = _build_evidence_payload_bytes(
            substrate,
            "arm_trustzone",
            {"cert_chain_b64": [_b64(b"\x01")]},
        )
        assert isinstance(out, bytes)
        substrate.encode_scale.assert_called_once()

    def test_other_types_raise_not_implemented(self):
        for kind in ("amd_sev_snp", "intel_tdx", "reproducible_build", "zk_vm_execution"):
            with pytest.raises(NotImplementedError):
                _build_evidence_payload_bytes(_make_substrate_mock(), kind, {})


# ---------------------------------------------------------------------------
# Helpers — pallet disabled detector
# ---------------------------------------------------------------------------

class TestIsPalletDisabledError:
    @pytest.mark.parametrize("err", [
        "PalletDisabled",
        "TeeAttestation.PalletDisabled",
        '{"name":"PalletDisabled","docs":[]}',
        "Module: 'TeeAttestation', Disabled",
    ])
    def test_disabled_strings_match(self, err):
        assert _is_pallet_disabled_error(err) is True

    @pytest.mark.parametrize("err", [
        None,
        "",
        "InsufficientBond",
        "TooManyEntries",
        "VerificationFailed",
    ])
    def test_other_strings_dont_match(self, err):
        assert _is_pallet_disabled_error(err) is False


# ---------------------------------------------------------------------------
# content_hash resolver
# ---------------------------------------------------------------------------

class TestContentHashResolver:
    def test_returns_bytes_from_list(self):
        substrate = _make_substrate_mock(
            receipts_query_value={"content_hash": list(CONTENT_HASH_BYTES)}
        )
        out = _content_hash_from_receipt_id_via_chain(substrate, RECEIPT_ID_HEX)
        assert out == CONTENT_HASH_BYTES

    def test_returns_bytes_from_hex_string(self):
        substrate = _make_substrate_mock(
            receipts_query_value={"content_hash": "0x" + CONTENT_HASH_BYTES.hex()}
        )
        out = _content_hash_from_receipt_id_via_chain(substrate, RECEIPT_ID_HEX)
        assert out == CONTENT_HASH_BYTES

    def test_returns_bytes_from_raw_bytes(self):
        substrate = _make_substrate_mock(
            receipts_query_value={"content_hash": CONTENT_HASH_BYTES}
        )
        out = _content_hash_from_receipt_id_via_chain(substrate, RECEIPT_ID_HEX)
        assert out == CONTENT_HASH_BYTES

    def test_returns_none_on_missing_receipt(self):
        substrate = _make_substrate_mock()
        substrate.query = MagicMock(return_value=SimpleNamespace(value=None))
        out = _content_hash_from_receipt_id_via_chain(substrate, RECEIPT_ID_HEX)
        assert out is None

    def test_returns_none_on_query_exception(self):
        substrate = _make_substrate_mock()
        substrate.query = MagicMock(side_effect=RuntimeError("rpc down"))
        out = _content_hash_from_receipt_id_via_chain(substrate, RECEIPT_ID_HEX)
        assert out is None


# ---------------------------------------------------------------------------
# submit_one — happy path + skip cases
# ---------------------------------------------------------------------------

def test_submit_one_happy_path():
    substrate = _make_substrate_mock()
    client = _make_client_mock(substrate)
    sub = _make_submitter(client=client)

    row = {
        "id": 7,
        "receipt_id": RECEIPT_ID_HEX,
        "evidence_type": "arm_trustzone",
        "payload": {"cert_chain_b64": [_b64(b"\x01\x02")]},
    }
    ext = _run(sub.submit_one(row))
    assert ext == "0x" + ("ee" * 32)
    # compose_call was called once with the right arguments shape
    substrate.compose_call.assert_called_once()
    kwargs = substrate.compose_call.call_args.kwargs
    assert kwargs["call_module"] == "TeeAttestation"
    assert kwargs["call_function"] == "submit_evidence"
    params = kwargs["call_params"]
    assert params["receipt_id"] == "0x" + RECEIPT_ID_HEX
    assert params["content_hash"] == list(CONTENT_HASH_BYTES)
    assert params["entry"]["evidence_type"] == "ArmTrustZone"
    assert params["entry"]["payload"].startswith("0x")


def test_submit_one_skips_when_receipt_not_on_chain():
    substrate = _make_substrate_mock()
    substrate.query = MagicMock(return_value=SimpleNamespace(value=None))
    client = _make_client_mock(substrate)
    sub = _make_submitter(client=client)

    row = {
        "id": 1,
        "receipt_id": RECEIPT_ID_HEX,
        "evidence_type": "arm_trustzone",
        "payload": {"cert_chain_b64": [_b64(b"\x01")]},
    }
    ext = _run(sub.submit_one(row))
    assert ext is None
    substrate.compose_call.assert_not_called()


def test_submit_one_skips_on_payload_assembly_failure():
    substrate = _make_substrate_mock()
    client = _make_client_mock(substrate)
    sub = _make_submitter(client=client)

    row = {
        "id": 2,
        "receipt_id": RECEIPT_ID_HEX,
        "evidence_type": "arm_trustzone",
        "payload": {"cert_chain_b64": []},  # empty → ValueError → skip
    }
    ext = _run(sub.submit_one(row))
    assert ext is None
    substrate.compose_call.assert_not_called()


def test_submit_one_skips_on_pallet_disabled():
    substrate = _make_substrate_mock(
        submit_success=False,
        error_message="TeeAttestation.PalletDisabled",
    )
    client = _make_client_mock(substrate)
    sub = _make_submitter(client=client)

    row = {
        "id": 3,
        "receipt_id": RECEIPT_ID_HEX,
        "evidence_type": "arm_trustzone",
        "payload": {"cert_chain_b64": [_b64(b"\x01")]},
    }
    ext = _run(sub.submit_one(row))
    assert ext is None


def test_submit_one_skips_on_generic_submit_failure():
    substrate = _make_substrate_mock(
        submit_success=False,
        error_message="VerificationFailed",
    )
    client = _make_client_mock(substrate)
    sub = _make_submitter(client=client)

    row = {
        "id": 4,
        "receipt_id": RECEIPT_ID_HEX,
        "evidence_type": "arm_trustzone",
        "payload": {"cert_chain_b64": [_b64(b"\x01")]},
    }
    ext = _run(sub.submit_one(row))
    assert ext is None


def test_submit_one_skips_unsupported_evidence_type():
    substrate = _make_substrate_mock()
    client = _make_client_mock(substrate)
    sub = _make_submitter(client=client)

    row = {
        "id": 5,
        "receipt_id": RECEIPT_ID_HEX,
        "evidence_type": "amd_sev_snp",
        "payload": {},
    }
    ext = _run(sub.submit_one(row))
    assert ext is None
    substrate.compose_call.assert_not_called()


# ---------------------------------------------------------------------------
# _tick — cursor advancement
# ---------------------------------------------------------------------------

def test_tick_advances_cursor_on_success():
    substrate = _make_substrate_mock()
    client = _make_client_mock(substrate)
    sub = _make_submitter(client=client)

    # Mock fetch_pending + mark_submitted as AsyncMocks so we don't open
    # real HTTP connections.
    sub.fetch_pending = AsyncMock(
        return_value=(
            [
                {
                    "id": 10,
                    "receipt_id": RECEIPT_ID_HEX,
                    "evidence_type": "arm_trustzone",
                    "payload": {"cert_chain_b64": [_b64(b"\x01")]},
                },
                {
                    "id": 11,
                    "receipt_id": RECEIPT_ID_HEX,
                    "evidence_type": "arm_trustzone",
                    "payload": {"cert_chain_b64": [_b64(b"\x02")]},
                },
            ],
            11,  # next_since
        )
    )
    sub.mark_submitted = AsyncMock(return_value=True)

    _run(sub._tick())
    assert sub._cursor == 11
    assert sub.mark_submitted.await_count == 2


def test_tick_does_not_advance_past_failed_ack():
    substrate = _make_substrate_mock()
    client = _make_client_mock(substrate)
    sub = _make_submitter(client=client)

    sub.fetch_pending = AsyncMock(
        return_value=(
            [
                {
                    "id": 20,
                    "receipt_id": RECEIPT_ID_HEX,
                    "evidence_type": "arm_trustzone",
                    "payload": {"cert_chain_b64": [_b64(b"\x01")]},
                },
                {
                    "id": 21,
                    "receipt_id": RECEIPT_ID_HEX,
                    "evidence_type": "arm_trustzone",
                    "payload": {"cert_chain_b64": [_b64(b"\x02")]},
                },
            ],
            21,
        )
    )
    # First ack succeeds, second one fails (gateway 500).
    sub.mark_submitted = AsyncMock(side_effect=[True, False])

    _run(sub._tick())
    # Cursor MUST NOT have advanced past the failure.
    # last_acked = 20, next_since = 21 → cursor stays at 20.
    assert sub._cursor == 20


def test_tick_with_empty_pending_is_noop():
    sub = _make_submitter()
    sub.fetch_pending = AsyncMock(return_value=([], sub._cursor))
    sub.mark_submitted = AsyncMock(return_value=True)
    before = sub._cursor
    _run(sub._tick())
    assert sub._cursor == before
    sub.mark_submitted.assert_not_called()


# ---------------------------------------------------------------------------
# Factory: maybe_create_evidence_submitter
# ---------------------------------------------------------------------------

class TestMaybeCreate:
    def test_missing_token_returns_none(self, monkeypatch):
        monkeypatch.delenv("EVIDENCE_SUBMITTER_TOKEN", raising=False)
        monkeypatch.delenv("SPONSORED_RECEIPT_SUBMITTER_TOKEN", raising=False)
        monkeypatch.setenv("EVIDENCE_SUBMITTER_GATEWAY_URL", "http://gateway")
        out = maybe_create_evidence_submitter(
            SimpleNamespace(blob_base_url=""),
            _make_client_mock(),
            asyncio.Lock(),
        )
        assert out is None

    def test_missing_gateway_returns_none(self, monkeypatch):
        monkeypatch.setenv("EVIDENCE_SUBMITTER_TOKEN", "tok")
        monkeypatch.delenv("EVIDENCE_SUBMITTER_GATEWAY_URL", raising=False)
        monkeypatch.delenv("BLOB_GATEWAY_URL", raising=False)
        out = maybe_create_evidence_submitter(
            SimpleNamespace(blob_base_url="", blob_gateway_url=""),
            _make_client_mock(),
            asyncio.Lock(),
        )
        assert out is None

    def test_both_present_returns_submitter(self, monkeypatch):
        monkeypatch.setenv("EVIDENCE_SUBMITTER_TOKEN", "tok")
        monkeypatch.setenv("EVIDENCE_SUBMITTER_GATEWAY_URL", "http://gateway")
        monkeypatch.setenv("EVIDENCE_SUBMITTER_POLL_INTERVAL", "45")
        monkeypatch.setenv("EVIDENCE_SUBMITTER_PAGE_SIZE", "25")
        out = maybe_create_evidence_submitter(
            SimpleNamespace(blob_base_url=""),
            _make_client_mock(),
            asyncio.Lock(),
        )
        assert out is not None
        assert out.gateway_url == "http://gateway"
        assert out._submitter_token == "tok"
        assert out._poll_interval == 45
        assert out._page_size == 25

    def test_falls_back_to_config_blob_base_url(self, monkeypatch):
        monkeypatch.setenv("EVIDENCE_SUBMITTER_TOKEN", "tok")
        monkeypatch.delenv("EVIDENCE_SUBMITTER_GATEWAY_URL", raising=False)
        monkeypatch.delenv("BLOB_GATEWAY_URL", raising=False)
        out = maybe_create_evidence_submitter(
            SimpleNamespace(
                blob_base_url="http://from-config", blob_gateway_url=""
            ),
            _make_client_mock(),
            asyncio.Lock(),
        )
        assert out is not None
        assert out.gateway_url == "http://from-config"


# ---------------------------------------------------------------------------
# P1 #1 fix: cursor must clamp to first unacked row when an EARLIER row in
# the batch fails while LATER rows succeed. Old behavior advanced cursor
# past the failure, permanently filtering it out of the daemon's view.
# ---------------------------------------------------------------------------

def test_tick_clamps_cursor_to_first_unacked_id():
    """When fetch returns rows [10, 11, 12] and row 10 fails (retryable —
    e.g. content_hash not on chain yet) while rows 11 + 12 ack, the cursor
    must NOT advance past 9 — otherwise the gateway query
    (`WHERE id > ? AND submitted_to_chain_at IS NULL`) drops row 10 from
    the daemon's view forever.
    """
    substrate = _make_substrate_mock()
    client = _make_client_mock(substrate)
    sub = _make_submitter(client=client)
    sub._cursor = 0

    rows_in = [
        {
            "id": 10,
            "receipt_id": RECEIPT_ID_HEX,
            "evidence_type": "arm_trustzone",
            "payload": {"cert_chain_b64": [_b64(b"\x01")]},
        },
        {
            "id": 11,
            "receipt_id": RECEIPT_ID_HEX,
            "evidence_type": "arm_trustzone",
            "payload": {"cert_chain_b64": [_b64(b"\x02")]},
        },
        {
            "id": 12,
            "receipt_id": RECEIPT_ID_HEX,
            "evidence_type": "arm_trustzone",
            "payload": {"cert_chain_b64": [_b64(b"\x03")]},
        },
    ]
    sub.fetch_pending = AsyncMock(return_value=(rows_in, 12))

    # Stub submit_one so row 10 returns None (transient miss), 11 + 12 succeed.
    real_submit_one = sub.submit_one

    async def fake_submit_one(row):
        rid = int(row["id"])
        if rid == 10:
            return None  # retryable — content_hash not on chain yet
        return "0x" + ("ee" * 32)

    sub.submit_one = fake_submit_one
    sub.mark_submitted = AsyncMock(return_value=True)

    _run(sub._tick())

    # Cursor must be clamped to 9 (just before the earliest unacked id).
    # Old buggy logic would have advanced to 12 (next_since), permanently
    # losing row 10.
    assert sub._cursor == 9
    # Row 10 was NOT recorded as terminal — it's a retryable miss.
    assert 10 not in sub._failed_rows


def test_tick_re_fetches_unacked_row_after_clamp():
    """End-to-end: tick #1 clamps cursor to 9 because row 10 misses, tick #2
    re-fetches starting at since=9 and successfully processes row 10."""
    substrate = _make_substrate_mock()
    client = _make_client_mock(substrate)
    sub = _make_submitter(client=client)
    sub._cursor = 0

    base_rows = [
        {
            "id": 10,
            "receipt_id": RECEIPT_ID_HEX,
            "evidence_type": "arm_trustzone",
            "payload": {"cert_chain_b64": [_b64(b"\x01")]},
        },
        {
            "id": 11,
            "receipt_id": RECEIPT_ID_HEX,
            "evidence_type": "arm_trustzone",
            "payload": {"cert_chain_b64": [_b64(b"\x02")]},
        },
    ]

    fetch_calls: list[int] = []

    async def fake_fetch():
        fetch_calls.append(sub._cursor)
        # Mirror the gateway query: returns rows where id > since AND
        # submitted_to_chain_at IS NULL.
        out = [r for r in base_rows if int(r["id"]) > sub._cursor]
        next_since = max((int(r["id"]) for r in out), default=sub._cursor)
        return out, next_since

    sub.fetch_pending = fake_fetch

    miss_count = {"n": 0}

    async def fake_submit_one(row):
        rid = int(row["id"])
        if rid == 10 and miss_count["n"] == 0:
            miss_count["n"] += 1
            return None  # tick 1: row 10 transient miss
        return "0x" + ("ee" * 32)  # tick 1: row 11 OK; tick 2: row 10 OK

    sub.submit_one = fake_submit_one
    sub.mark_submitted = AsyncMock(return_value=True)

    _run(sub._tick())  # tick 1: row 10 fails, row 11 acks
    assert sub._cursor == 9, f"after tick 1, expected cursor=9, got {sub._cursor}"

    _run(sub._tick())  # tick 2: row 10 acks
    # Cursor advances past 11 now that everything is acked.
    assert sub._cursor >= 11
    # Two distinct fetch calls observed, second since=9.
    assert fetch_calls == [0, 9]


# ---------------------------------------------------------------------------
# P1 #2 fix: terminal pallet errors must mark row failed locally so the
# daemon stops re-submitting (each rejection burns the declared weight as
# fees).
# ---------------------------------------------------------------------------

def test_terminal_error_marks_row_failed_locally(tmp_path):
    """A pallet `VerificationFailed` is structurally terminal — the verifier
    rejected the bytes, resubmitting won't help. The daemon must:
      1. record the row id + reason in the local skip-bit store
      2. persist that to disk (survives restart)
      3. NOT call compose_call on the next tick for the same row
    """
    failed_state = str(tmp_path / "evidence-failed.json")

    substrate = _make_substrate_mock(
        submit_success=False,
        error_message="TeeAttestation.VerificationFailed",
    )
    client = _make_client_mock(substrate)
    sub = _make_submitter(client=client, failed_state_path=failed_state)

    row = {
        "id": 42,
        "receipt_id": RECEIPT_ID_HEX,
        "evidence_type": "arm_trustzone",
        "payload": {"cert_chain_b64": [_b64(b"\x01\x02\x03")]},
    }

    # First submit attempt: pallet rejects, row gets recorded locally.
    ext = _run(sub.submit_one(row))
    assert ext is None
    assert 42 in sub._failed_rows
    assert sub._failed_rows[42]["reason"] == "VerificationFailed"
    assert sub._failed_rows[42]["receipt_id"]
    # State persisted to disk.
    assert os.path.exists(failed_state)
    with open(failed_state) as f:
        persisted = json.load(f)
    persisted_ids = {entry["row_id"] for entry in persisted["rows"]}
    assert 42 in persisted_ids

    # Second tick: row 42 surfaces again (gateway still has it as pending),
    # but the daemon must filter it BEFORE composing any chain call.
    substrate.compose_call.reset_mock()
    sub.fetch_pending = AsyncMock(return_value=([row], 42))
    sub.mark_submitted = AsyncMock(return_value=True)

    _run(sub._tick())

    # No second compose_call (the row was filtered upstream of submit_one).
    substrate.compose_call.assert_not_called()
    # mark_submitted not called either — row is locally terminal, not acked.
    sub.mark_submitted.assert_not_called()
    # Cursor advanced past 42 because nothing was waiting on it.
    assert sub._cursor >= 42


def test_terminal_error_includes_too_many_entries():
    """`TooManyEntries` is also terminal — once the receipt's evidence cap
    is hit, no future submit for the same row can succeed."""
    sub = _make_submitter(
        client=_make_client_mock(_make_substrate_mock(
            submit_success=False,
            error_message='{"name":"TooManyEntries","docs":[]}',
        ))
    )
    row = {
        "id": 50,
        "receipt_id": RECEIPT_ID_HEX,
        "evidence_type": "arm_trustzone",
        "payload": {"cert_chain_b64": [_b64(b"\x01")]},
    }
    ext = _run(sub.submit_one(row))
    assert ext is None
    assert 50 in sub._failed_rows
    assert sub._failed_rows[50]["reason"] == "TooManyEntries"


def test_pallet_disabled_is_NOT_terminal():
    """`PalletDisabled` (kill-switch) is retryable — operator may flip it
    back off without a chain reset. Must NOT be added to the local
    failed-row store, otherwise we'd ignore the row forever."""
    sub = _make_submitter(
        client=_make_client_mock(_make_substrate_mock(
            submit_success=False,
            error_message="TeeAttestation.PalletDisabled",
        ))
    )
    row = {
        "id": 60,
        "receipt_id": RECEIPT_ID_HEX,
        "evidence_type": "arm_trustzone",
        "payload": {"cert_chain_b64": [_b64(b"\x01")]},
    }
    ext = _run(sub.submit_one(row))
    assert ext is None
    # Critical: NOT in the failed-row map.
    assert 60 not in sub._failed_rows


def test_classify_chain_error():
    """Direct unit coverage for the classifier."""
    assert _classify_chain_error("VerificationFailed") == "terminal"
    assert _classify_chain_error("TeeAttestation.VerificationFailed") == "terminal"
    assert _classify_chain_error("TooManyEntries") == "terminal"
    assert _classify_chain_error("PayloadTooLarge") == "terminal"
    # PalletDisabled overrides the substring search.
    assert _classify_chain_error("PalletDisabled") == "retryable"
    assert _classify_chain_error("TeeAttestation.PalletDisabled") == "retryable"
    # Anything unknown defaults to retryable so we don't lose receipts on
    # transient errors.
    assert _classify_chain_error(None) == "retryable"
    assert _classify_chain_error("") == "retryable"
    assert _classify_chain_error("RuntimeError: rpc down") == "retryable"
    assert _classify_chain_error("Module: 'TeeAttestation', Disabled") == "retryable"


def test_terminal_error_reasons_pinned():
    """Sentinel: the terminal-reason map MUST include these three pallet
    errors + the three pre-flight sentinels. Removing one would silently
    re-introduce the retry-forever bug for that error."""
    assert "VerificationFailed" in TERMINAL_ERROR_REASONS
    assert "TooManyEntries" in TERMINAL_ERROR_REASONS
    assert "PayloadTooLarge" in TERMINAL_ERROR_REASONS
    assert "PayloadAssemblyError" in TERMINAL_ERROR_REASONS
    assert "UnsupportedEvidenceType" in TERMINAL_ERROR_REASONS


# ---------------------------------------------------------------------------
# P2 #3: payload-size precheck. Saves the round-trip + tx fee for rows
# whose payload exceeds the pallet's BoundedVec cap.
# ---------------------------------------------------------------------------

def test_payload_size_precheck_marks_row_terminal(monkeypatch):
    """A payload > MAX_EVIDENCE_PAYLOAD_BYTES (16 KiB) is rejected client-
    side BEFORE the chain submission. The pallet would reject it at SCALE
    decode anyway; doing it locally saves the fee."""
    # 16 KiB + 1 — the pallet's BoundedVec cap is 16384 bytes.
    big_bytes = b"\xab" * (MAX_EVIDENCE_PAYLOAD_BYTES + 1)

    substrate = _make_substrate_mock()
    # Stub _build_evidence_payload_bytes to return the over-cap blob without
    # actually invoking the SCALE encoder.
    monkeypatch.setattr(
        "daemon.evidence_submitter._build_evidence_payload_bytes",
        lambda _s, _t, _p: big_bytes,
    )

    client = _make_client_mock(substrate)
    sub = _make_submitter(client=client)
    row = {
        "id": 77,
        "receipt_id": RECEIPT_ID_HEX,
        "evidence_type": "arm_trustzone",
        "payload": {"cert_chain_b64": [_b64(b"\x01")]},
    }
    ext = _run(sub.submit_one(row))
    assert ext is None
    # No chain submit happened — the precheck short-circuited.
    substrate.compose_call.assert_not_called()
    # And the row is locally terminal so it stays skipped.
    assert 77 in sub._failed_rows
    assert sub._failed_rows[77]["reason"] == "PayloadTooLarge"


def test_payload_at_exact_cap_is_allowed(monkeypatch):
    """A payload of exactly MAX_EVIDENCE_PAYLOAD_BYTES bytes is the maximum
    pallet-acceptable size — must NOT be rejected locally."""
    cap_bytes = b"\xab" * MAX_EVIDENCE_PAYLOAD_BYTES

    substrate = _make_substrate_mock()
    monkeypatch.setattr(
        "daemon.evidence_submitter._build_evidence_payload_bytes",
        lambda _s, _t, _p: cap_bytes,
    )

    client = _make_client_mock(substrate)
    sub = _make_submitter(client=client)
    row = {
        "id": 78,
        "receipt_id": RECEIPT_ID_HEX,
        "evidence_type": "arm_trustzone",
        "payload": {"cert_chain_b64": [_b64(b"\x01")]},
    }
    ext = _run(sub.submit_one(row))
    assert ext is not None  # happy-path: chain returns 0xee*32
    substrate.compose_call.assert_called_once()
    assert 78 not in sub._failed_rows


def test_payload_assembly_failure_is_terminal():
    """Bad gateway shape (empty cert chain, malformed base64, …) is terminal
    — those bytes don't morph into something acceptable on retry."""
    sub = _make_submitter()
    row = {
        "id": 88,
        "receipt_id": RECEIPT_ID_HEX,
        "evidence_type": "arm_trustzone",
        "payload": {"cert_chain_b64": []},  # empty → ValueError
    }
    ext = _run(sub.submit_one(row))
    assert ext is None
    assert 88 in sub._failed_rows
    assert sub._failed_rows[88]["reason"] == "PayloadAssemblyError"


def test_unsupported_evidence_type_is_terminal():
    """Phase-3+ evidence types raise NotImplementedError → mark terminal so
    the cursor advances and the row stops re-fetching every poll."""
    sub = _make_submitter()
    row = {
        "id": 99,
        "receipt_id": RECEIPT_ID_HEX,
        "evidence_type": "amd_sev_snp",
        "payload": {},
    }
    ext = _run(sub.submit_one(row))
    assert ext is None
    assert 99 in sub._failed_rows
    assert sub._failed_rows[99]["reason"] == "UnsupportedEvidenceType"


# ---------------------------------------------------------------------------
# P2 #6: never silently fall back to an all-zero ext_hash.
# ---------------------------------------------------------------------------

def test_no_zero_placeholder_when_receipt_hash_unset():
    """If the chain reports is_success=True but extrinsic_hash AND
    block_hash are both empty/None, the daemon must NOT ack with a zero
    placeholder — must return None so the next tick retries.

    Silent zero placeholders previously burned us in the v2 contract drift
    incident (`feedback_v2_contract_drift_chain_break.md`).
    """
    substrate = _make_substrate_mock()
    # Override receipt to pretend we got success but no usable hash.
    receipt = MagicMock()
    receipt.is_success = True
    receipt.error_message = None
    receipt.extrinsic_hash = None
    receipt.block_hash = None
    substrate.submit_extrinsic = MagicMock(return_value=receipt)

    client = _make_client_mock(substrate)
    sub = _make_submitter(client=client)
    row = {
        "id": 130,
        "receipt_id": RECEIPT_ID_HEX,
        "evidence_type": "arm_trustzone",
        "payload": {"cert_chain_b64": [_b64(b"\x01")]},
    }
    ext = _run(sub.submit_one(row))
    assert ext is None  # No silent 0x000... fallback.
    # Row is NOT terminal — the chain call may have succeeded; we just
    # couldn't extract the hash. Next tick retries (and the pallet's
    # idempotency at (receipt_id, attest_key_hash) handles a possible
    # double-landing.)
    assert 130 not in sub._failed_rows


def test_block_hash_used_when_extrinsic_hash_missing():
    """If extrinsic_hash is missing but block_hash is set, fall back to
    block_hash (pre-existing behavior — not a regression target)."""
    substrate = _make_substrate_mock()
    receipt = MagicMock()
    receipt.is_success = True
    receipt.error_message = None
    receipt.extrinsic_hash = None
    receipt.block_hash = "0x" + ("bb" * 32)
    substrate.submit_extrinsic = MagicMock(return_value=receipt)

    client = _make_client_mock(substrate)
    sub = _make_submitter(client=client)
    row = {
        "id": 131,
        "receipt_id": RECEIPT_ID_HEX,
        "evidence_type": "arm_trustzone",
        "payload": {"cert_chain_b64": [_b64(b"\x01")]},
    }
    ext = _run(sub.submit_one(row))
    assert ext == "0x" + ("bb" * 32)


# ---------------------------------------------------------------------------
# Failed-row state persistence: must survive restart.
# ---------------------------------------------------------------------------

def test_failed_rows_persist_across_restart(tmp_path):
    """Build a submitter, mark a row terminal, build a NEW submitter pointed
    at the same state path, verify the failed set is preserved."""
    state_path = str(tmp_path / "evidence-failed.json")
    sub_a = _make_submitter(failed_state_path=state_path)
    sub_a._record_failed_row(
        7, "VerificationFailed",
        receipt_id="ab" * 32, evidence_type="arm_trustzone",
    )
    sub_a._record_failed_row(
        8, "TooManyEntries",
        receipt_id="cd" * 32, evidence_type="arm_trustzone",
    )

    # New process / new submitter, same path.
    sub_b = _make_submitter(failed_state_path=state_path)
    assert 7 in sub_b._failed_rows
    assert 8 in sub_b._failed_rows
    assert sub_b._failed_rows[7]["reason"] == "VerificationFailed"
    assert sub_b._failed_rows[8]["reason"] == "TooManyEntries"


def test_failed_rows_load_tolerates_missing_file(tmp_path):
    """Bootstrapping with a non-existent state path is fine — empty map."""
    sub = _make_submitter(failed_state_path=str(tmp_path / "nope.json"))
    assert sub._failed_rows == {}


def test_failed_rows_load_tolerates_malformed_json(tmp_path):
    """Bootstrapping with a corrupt state path logs WARN, returns empty —
    must not crash the daemon at startup."""
    state_path = str(tmp_path / "evidence-failed.json")
    with open(state_path, "w") as f:
        f.write("{not-json")
    sub = _make_submitter(failed_state_path=state_path)
    assert sub._failed_rows == {}


# ---------------------------------------------------------------------------
# Cursor + terminal-row interaction: a tick where every row is locally
# terminal still advances the cursor past them (otherwise we'd re-fetch
# them every poll forever).
# ---------------------------------------------------------------------------

def test_tick_advances_cursor_when_all_rows_are_terminal(tmp_path):
    """All rows in the page are already in the failed-row store → no chain
    work happens, but the cursor must still step forward past them so the
    daemon doesn't re-fetch the same poisoned rows every poll."""
    state_path = str(tmp_path / "evidence-failed.json")
    substrate = _make_substrate_mock()
    client = _make_client_mock(substrate)
    sub = _make_submitter(client=client, failed_state_path=state_path)
    sub._cursor = 0

    # Pre-poison rows 100 and 101.
    sub._record_failed_row(100, "VerificationFailed", receipt_id=RECEIPT_ID_HEX)
    sub._record_failed_row(101, "TooManyEntries", receipt_id=RECEIPT_ID_HEX)

    sub.fetch_pending = AsyncMock(
        return_value=(
            [
                {
                    "id": 100,
                    "receipt_id": RECEIPT_ID_HEX,
                    "evidence_type": "arm_trustzone",
                    "payload": {"cert_chain_b64": [_b64(b"\x01")]},
                },
                {
                    "id": 101,
                    "receipt_id": RECEIPT_ID_HEX,
                    "evidence_type": "arm_trustzone",
                    "payload": {"cert_chain_b64": [_b64(b"\x02")]},
                },
            ],
            101,
        )
    )
    sub.mark_submitted = AsyncMock(return_value=True)

    _run(sub._tick())

    # No chain calls.
    substrate.compose_call.assert_not_called()
    # No acks.
    sub.mark_submitted.assert_not_called()
    # But cursor steps past the terminal rows (otherwise we re-loop on them).
    assert sub._cursor >= 101
