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
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from daemon.evidence_submitter import (
    EVIDENCE_TYPE_TO_PALLET_VARIANT,
    EvidenceSubmitter,
    _build_arm_trustzone_payload_bytes,
    _build_evidence_payload_bytes,
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
) -> EvidenceSubmitter:
    config = SimpleNamespace(blob_base_url=gateway_url, blob_gateway_url=gateway_url)
    sub = EvidenceSubmitter(
        config=config,
        substrate_client=client or _make_client_mock(),
        chain_write_lock=asyncio.Lock(),
        gateway_url=gateway_url,
        submitter_token=submitter_token,
        admin_token=admin_token or submitter_token,
        poll_interval=poll_interval,
        page_size=page_size,
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
