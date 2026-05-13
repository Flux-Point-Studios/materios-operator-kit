"""Post-spec-219 the runtime's `attest_availability_cert` returns `Ok(())`
on cert-hash mismatch so the `BadAttestStrike` + `AutoSlashedForBadAttest`
state writes persist (per `pallet-balances`-style slashing — see PR #23 /
bug_005 architectural note in the agent report). That means
`ExtrinsicReceipt.is_success` is no longer sufficient: the daemon MUST scan
`triggered_events` for its own signer's strike/slash markers. Otherwise it
silently racks up strikes, gets auto-ejected from committee, and the first
warning is "nothing is attesting anymore."

These tests pin the verdict logic with mocked substrate receipts. They do
NOT spin a substrate node — the scanner is pure event-list inspection over
the dict shape that substrate-interface emits.
"""
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import List, Optional
from unittest.mock import MagicMock, patch

import pytest

from daemon.substrate_client import SubstrateClient, SubmitCertOutcome


# --------------------------------------------------------------------------
# Mock helpers — mirror the shape substrate-interface returns from
# `submit_extrinsic(wait_for_inclusion=True).triggered_events`.
# --------------------------------------------------------------------------

OUR_SS58 = "5OurSignerXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXxxxx"
FOREIGN_SS58 = "5SomeoneElseYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYyyyy"
RECEIPT_ID = "0x" + "11" * 32


def _ev(module: str, event_id: str, attrs: dict) -> SimpleNamespace:
    return SimpleNamespace(
        value={
            "event": {
                "module_id": module,
                "event_id": event_id,
                "attributes": attrs,
            }
        }
    )


def _receipt(is_success: bool, events: List, block_hash: str = "0xdeadbeef"):
    return SimpleNamespace(
        is_success=is_success,
        error_message=None,
        block_hash=block_hash,
        triggered_events=events,
    )


@pytest.fixture
def client():
    """SubstrateClient wired up enough to call `_scan_for_bad_attest` /
    `submit_availability_cert` without a real WS connection."""
    c = SubstrateClient.__new__(SubstrateClient)
    c.config = SimpleNamespace(tx_max_retries=3, signer_uri="//Alice", rpc_url="ws://x")
    c.substrate = MagicMock()
    c.keypair = SimpleNamespace(ss58_address=OUR_SS58)
    return c


# --------------------------------------------------------------------------
# _scan_for_bad_attest — pure verdict logic
# --------------------------------------------------------------------------

class TestScanForBadAttest:
    def test_no_orinq_events_returns_none(self, client):
        receipt = _receipt(True, [
            _ev("System", "ExtrinsicSuccess", {}),
            _ev("Balances", "Transfer", {"from": OUR_SS58, "to": FOREIGN_SS58, "amount": 100}),
        ])
        assert client._scan_for_bad_attest(receipt, RECEIPT_ID) is None

    def test_attestation_recorded_event_is_not_a_strike(self, client):
        """The success case — runtime accepted our claim. AttestationRecorded
        / AvailabilityCertified must not be misread as failure."""
        receipt = _receipt(True, [
            _ev("OrinqReceipts", "AttestationRecorded", {
                "attester": OUR_SS58, "receipt_id": RECEIPT_ID, "cert_hash": "0x" + "aa" * 32,
            }),
            _ev("System", "ExtrinsicSuccess", {}),
        ])
        assert client._scan_for_bad_attest(receipt, RECEIPT_ID) is None

    def test_foreign_signer_strike_is_ignored(self, client):
        """A `BadAttestStrike` for someone else in the same inclusion block
        must NOT fail OUR submit. The runtime field is `attester` — we only
        own our own SS58."""
        receipt = _receipt(True, [
            _ev("OrinqReceipts", "BadAttestStrike", {
                "attester": FOREIGN_SS58,
                "receipt_id": RECEIPT_ID,
                "claimed": "0x" + "bb" * 32,
                "canonical": "0x" + "cc" * 32,
                "strikes": 1,
            }),
        ])
        assert client._scan_for_bad_attest(receipt, RECEIPT_ID) is None

    def test_our_signer_strike_returns_failure_outcome(self, client):
        receipt = _receipt(True, [
            _ev("OrinqReceipts", "BadAttestStrike", {
                "attester": OUR_SS58,
                "receipt_id": RECEIPT_ID,
                "claimed": "0x" + "bb" * 32,
                "canonical": "0x" + "cc" * 32,
                "strikes": 1,
            }),
        ])
        outcome = client._scan_for_bad_attest(receipt, RECEIPT_ID)
        assert outcome is not None
        assert outcome.success is False
        assert outcome.bad_attest_strike is True
        assert outcome.auto_slashed is False
        assert outcome.strikes == 1
        assert outcome.claimed == bytes.fromhex("bb" * 32)
        assert outcome.canonical == bytes.fromhex("cc" * 32)
        assert bool(outcome) is False  # __bool__ contract

    def test_strike_at_threshold_co_emits_auto_slash(self, client):
        """When strikes cross `BadAttestSlashThreshold` the runtime emits
        BOTH `BadAttestStrike` (the final one) AND `AutoSlashedForBadAttest`
        in the same dispatch. Outcome must reflect both."""
        receipt = _receipt(True, [
            _ev("OrinqReceipts", "BadAttestStrike", {
                "attester": OUR_SS58,
                "receipt_id": RECEIPT_ID,
                "claimed": "0x" + "bb" * 32,
                "canonical": "0x" + "cc" * 32,
                "strikes": 1,  # reset to 0 after slash; pre-reset value here
            }),
            _ev("OrinqReceipts", "AutoSlashedForBadAttest", {
                "attester": OUR_SS58,
                "amount": 1_000_000_000_000,
                "remaining_bond": 0,
            }),
        ])
        outcome = client._scan_for_bad_attest(receipt, RECEIPT_ID)
        assert outcome is not None
        assert outcome.bad_attest_strike is True
        assert outcome.auto_slashed is True
        assert outcome.slashed_amount == 1_000_000_000_000

    def test_malformed_event_does_not_crash_scanner(self, client):
        """Defensive: if substrate-interface decodes an event into a shape
        we don't expect, the scanner must skip it rather than crash. A
        crash here would make us re-submit and double-strike."""
        broken = SimpleNamespace(value=None)
        receipt = _receipt(True, [
            broken,
            _ev("OrinqReceipts", "BadAttestStrike", {
                "attester": OUR_SS58,
                "receipt_id": RECEIPT_ID,
                "claimed": None,  # missing field
                "canonical": "0x" + "cc" * 32,
                "strikes": 2,
            }),
        ])
        outcome = client._scan_for_bad_attest(receipt, RECEIPT_ID)
        assert outcome is not None
        assert outcome.bad_attest_strike is True
        assert outcome.claimed is None  # _maybe_bytes32 swallows the None
        assert outcome.canonical == bytes.fromhex("cc" * 32)

    def test_triggered_events_access_failure_returns_none(self, client):
        """If `receipt.triggered_events` itself raises (some older
        substrate-interface versions did this on contested decodings),
        we fail OPEN — treat Ok dispatch as accepted attest. Failing
        closed would force a re-submit and risk a real double-strike if
        the runtime did actually accept us."""
        receipt = MagicMock()
        receipt.is_success = True
        type(receipt).triggered_events = property(
            lambda self: (_ for _ in ()).throw(RuntimeError("decode failed"))
        )
        assert client._scan_for_bad_attest(receipt, RECEIPT_ID) is None


# --------------------------------------------------------------------------
# submit_availability_cert — integrates with substrate, returns outcome
# --------------------------------------------------------------------------

class TestSubmitAvailabilityCert:
    def test_accepted_attest_returns_success_outcome(self, client):
        """Happy path: dispatch Ok, no strike events → success."""
        client.substrate.compose_call = MagicMock(return_value=MagicMock())
        client.substrate.create_signed_extrinsic = MagicMock(return_value=MagicMock())
        client.substrate.submit_extrinsic = MagicMock(return_value=_receipt(
            True,
            [_ev("OrinqReceipts", "AttestationRecorded", {
                "attester": OUR_SS58, "receipt_id": RECEIPT_ID, "cert_hash": "0x" + "aa" * 32,
            })],
        ))
        outcome = client.submit_availability_cert(RECEIPT_ID, b"\xaa" * 32)
        assert outcome.success is True
        assert bool(outcome) is True
        assert outcome.bad_attest_strike is False
        # Single attempt — no retry loop on success.
        assert client.substrate.submit_extrinsic.call_count == 1

    def test_struck_attest_returns_strike_outcome_no_retry(self, client):
        """If the runtime strikes us, retrying would re-strike against the
        same canonical hash. The submit path MUST NOT loop on strike."""
        client.substrate.compose_call = MagicMock(return_value=MagicMock())
        client.substrate.create_signed_extrinsic = MagicMock(return_value=MagicMock())
        client.substrate.submit_extrinsic = MagicMock(return_value=_receipt(
            True,
            [_ev("OrinqReceipts", "BadAttestStrike", {
                "attester": OUR_SS58,
                "receipt_id": RECEIPT_ID,
                "claimed": "0x" + "bb" * 32,
                "canonical": "0x" + "cc" * 32,
                "strikes": 1,
            })],
        ))
        outcome = client.submit_availability_cert(RECEIPT_ID, b"\xbb" * 32)
        assert outcome.success is False
        assert outcome.bad_attest_strike is True
        assert outcome.strikes == 1
        assert client.substrate.submit_extrinsic.call_count == 1, (
            "must NOT retry on bad-attest strike — that's a permanent verdict"
        )

    def test_tx_inclusion_failure_retries_then_fails(self, client):
        """Mempool/include-time failure (e.g. nonce race) still retries —
        only attestation-rejection skips retry. Time.sleep is patched out
        so the test isn't slow."""
        client.substrate.compose_call = MagicMock(return_value=MagicMock())
        client.substrate.create_signed_extrinsic = MagicMock(return_value=MagicMock())
        client.substrate.submit_extrinsic = MagicMock(return_value=_receipt(
            False, [], block_hash="0x0"
        ))
        with patch("daemon.substrate_client.time.sleep"):
            outcome = client.submit_availability_cert(RECEIPT_ID, b"\x00" * 32)
        assert outcome.success is False
        assert outcome.bad_attest_strike is False
        assert client.substrate.submit_extrinsic.call_count == client.config.tx_max_retries
