"""Tests for the auto-bond flow in cert-daemon.

The cert-daemon must auto-post `OrinqReceipts.bond(amount)` on startup so that
`join_committee` does not fail with `InsufficientBond`. Without this, every
freshly-onboarded operator hits an infinite retry loop on join_committee,
burning MOTRA on each failed attempt. See README auto-bond section for the
operator-facing contract.

These tests mock the SubstrateClient at the seam so no live chain is required.
"""

from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from daemon.cert_daemon import CertDaemon
from daemon.config import DaemonConfig


# --- regression: class-body integrity --------------------------------------
# A prior revision of this PR accidentally de-classed `run` and
# `_request_faucet_drip` by placing `_is_insufficient_bond_error` mid-class
# at column 0 — Python kept parsing subsequent `    async def` blocks as
# nested defs inside the helper, silently turning two class methods into
# unreachable closures. Mocked unit tests didn't catch it because they
# never exercise `run` or `_request_faucet_drip`. This assertion does.


def test_class_methods_intact():
    """Guard against indentation drift that would nest class methods inside
    an unrelated module-level helper."""
    for name in ("run", "_request_faucet_drip", "_ensure_bond",
                 "_ensure_committee_membership", "process_receipt"):
        assert callable(getattr(CertDaemon, name, None)), (
            f"CertDaemon.{name} missing — likely an indentation bug "
            f"nested it inside a module-level function. Check that "
            f"`_is_insufficient_bond_error` lives AFTER the class body."
        )


# --- helpers ---------------------------------------------------------------


# 1_000 MATRA at 6 decimals = 1_000_000_000 base units.
REQUIRED_BOND_BASE = 1_000_000_000
ATTESTOR_SS58 = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"


def _make_daemon(
    *,
    bond_requirement: int,
    current_bond: int,
    free_balance: int,
    bond_submit_success: bool = True,
    bond_submit_raises: Exception | None = None,
) -> CertDaemon:
    """Build a CertDaemon with a fully-mocked SubstrateClient.

    The client exposes the four seams the auto-bond flow uses:
      - `get_bond_requirement()`
      - `get_attestor_bond(addr)`
      - `get_free_balance(addr)`
      - `submit_bond(amount)`
    and the `keypair.ss58_address` used for address lookups.
    """
    config = DaemonConfig()
    daemon = CertDaemon.__new__(CertDaemon)
    daemon.config = config
    daemon.client = MagicMock()
    daemon.client.keypair = SimpleNamespace(
        ss58_address=ATTESTOR_SS58,
        public_key=b"\x00" * 32,
    )
    daemon.client.get_bond_requirement = MagicMock(return_value=bond_requirement)
    daemon.client.get_attestor_bond = MagicMock(return_value=current_bond)
    daemon.client.get_free_balance = MagicMock(return_value=free_balance)

    if bond_submit_raises is not None:
        daemon.client.submit_bond = MagicMock(side_effect=bond_submit_raises)
    else:
        daemon.client.submit_bond = MagicMock(
            return_value=(bond_submit_success, "0xdeadbeef")
        )

    async def _no_discord(*args, **kwargs):
        return None

    daemon.send_discord = _no_discord
    return daemon


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


# --- tests -----------------------------------------------------------------


def test_skip_when_already_bonded(caplog):
    """If current bond >= requirement, no bond tx is submitted."""
    daemon = _make_daemon(
        bond_requirement=REQUIRED_BOND_BASE,
        current_bond=REQUIRED_BOND_BASE,
        free_balance=0,
    )
    with caplog.at_level(logging.INFO, logger="daemon.cert_daemon"):
        _run(daemon._ensure_bond())

    daemon.client.submit_bond.assert_not_called()
    assert any(
        "already bonded" in rec.message.lower() for rec in caplog.records
    ), f"expected skip-log, got {[r.message for r in caplog.records]}"


def test_skip_when_over_bonded():
    """Bonding more than the requirement is also a no-op (idempotence)."""
    daemon = _make_daemon(
        bond_requirement=REQUIRED_BOND_BASE,
        current_bond=REQUIRED_BOND_BASE * 2,
        free_balance=0,
    )
    _run(daemon._ensure_bond())
    daemon.client.submit_bond.assert_not_called()


def test_bonds_missing_delta():
    """If current < required and free >= delta, bond exactly delta."""
    current = REQUIRED_BOND_BASE // 4  # 25% bonded
    daemon = _make_daemon(
        bond_requirement=REQUIRED_BOND_BASE,
        current_bond=current,
        free_balance=REQUIRED_BOND_BASE * 10,  # plenty
    )
    _run(daemon._ensure_bond())

    daemon.client.submit_bond.assert_called_once()
    args, kwargs = daemon.client.submit_bond.call_args
    submitted_amount = args[0] if args else kwargs["amount"]
    assert submitted_amount == REQUIRED_BOND_BASE - current


def test_bonds_full_amount_when_nothing_bonded():
    """With zero prior bond, the daemon bonds the full requirement."""
    daemon = _make_daemon(
        bond_requirement=REQUIRED_BOND_BASE,
        current_bond=0,
        free_balance=REQUIRED_BOND_BASE * 2,
    )
    _run(daemon._ensure_bond())

    daemon.client.submit_bond.assert_called_once()
    args, kwargs = daemon.client.submit_bond.call_args
    submitted_amount = args[0] if args else kwargs["amount"]
    assert submitted_amount == REQUIRED_BOND_BASE


def test_insufficient_free_matra_logs_warning_no_submit(caplog):
    """If free balance < delta-needed, log a clear warning and do NOT submit."""
    daemon = _make_daemon(
        bond_requirement=REQUIRED_BOND_BASE,
        current_bond=0,
        free_balance=REQUIRED_BOND_BASE // 2,  # half of what's needed
    )
    with caplog.at_level(logging.WARNING, logger="daemon.cert_daemon"):
        _run(daemon._ensure_bond())

    daemon.client.submit_bond.assert_not_called()
    warned = "\n".join(r.message for r in caplog.records if r.levelno >= logging.WARNING)
    assert "insufficient" in warned.lower() or "not enough" in warned.lower() or "faucet" in warned.lower(), (
        f"expected a clear warning about insufficient free MATRA, got: {warned!r}"
    )


def test_insufficient_free_matra_does_not_crash():
    """A short operator balance must NEVER crash the daemon startup."""
    daemon = _make_daemon(
        bond_requirement=REQUIRED_BOND_BASE,
        current_bond=0,
        free_balance=0,
    )
    # Should return cleanly, not raise.
    _run(daemon._ensure_bond())


def test_requirement_zero_is_skip():
    """Preprod bootstrap / upgrade grace: BondRequirement=0 means skip."""
    daemon = _make_daemon(
        bond_requirement=0,
        current_bond=0,
        free_balance=0,
    )
    _run(daemon._ensure_bond())
    daemon.client.submit_bond.assert_not_called()


def test_bond_submit_failure_does_not_crash(caplog):
    """If the bond submit fails at the substrate layer, log and continue."""
    daemon = _make_daemon(
        bond_requirement=REQUIRED_BOND_BASE,
        current_bond=0,
        free_balance=REQUIRED_BOND_BASE * 2,
        bond_submit_success=False,
    )
    with caplog.at_level(logging.WARNING, logger="daemon.cert_daemon"):
        _run(daemon._ensure_bond())
    # One call submitted; daemon did not raise.
    daemon.client.submit_bond.assert_called_once()


def test_bond_submit_exception_is_swallowed():
    """An unexpected exception from the submit path is caught so startup
    still proceeds to the join_committee attempt."""
    daemon = _make_daemon(
        bond_requirement=REQUIRED_BOND_BASE,
        current_bond=0,
        free_balance=REQUIRED_BOND_BASE * 2,
        bond_submit_raises=RuntimeError("rpc glitch"),
    )
    _run(daemon._ensure_bond())  # must not raise


def test_bond_is_one_shot_on_insufficient_bond():
    """The fallback path: if join_committee returns InsufficientBond AFTER
    auto-bond ran once, the daemon re-runs the bond flow exactly once for
    that attempt — it does NOT loop forever retrying the bond when the
    requirement has already been met on-chain.
    """
    # Simulate: first ensure_bond call bonds successfully. Second call
    # (triggered by the InsufficientBond fallback) sees current==required
    # and is a no-op, guarding against infinite loops.
    daemon = _make_daemon(
        bond_requirement=REQUIRED_BOND_BASE,
        current_bond=0,
        free_balance=REQUIRED_BOND_BASE * 2,
    )
    _run(daemon._ensure_bond())
    assert daemon.client.submit_bond.call_count == 1

    # After first bond, chain state shows we're at requirement.
    daemon.client.get_attestor_bond.return_value = REQUIRED_BOND_BASE
    _run(daemon._ensure_bond())
    # No additional submit_bond calls.
    assert daemon.client.submit_bond.call_count == 1


def test_ensure_committee_membership_retries_bond_on_insufficient_bond_error():
    """Integration at the dispatcher seam: if the join_committee submit reports
    an `InsufficientBond` error, `_ensure_committee_membership` must call the
    bond-ensuring path exactly once, then attempt join again — and must NOT
    infinite-loop on persistent InsufficientBond (bounded retries)."""
    daemon = _make_daemon(
        bond_requirement=REQUIRED_BOND_BASE,
        current_bond=REQUIRED_BOND_BASE,  # already bonded per state
        free_balance=0,
    )
    # Mock the substrate interface surface used by _ensure_committee_membership
    substrate = MagicMock()
    substrate.query.return_value = []  # CommitteeMembers empty -> not a member
    call_stub = MagicMock()
    substrate.compose_call.return_value = call_stub
    substrate.create_signed_extrinsic.return_value = MagicMock()

    # First submit_extrinsic returns InsufficientBond, second returns success.
    fail_receipt = MagicMock(is_success=False, error_message={"name": "InsufficientBond"})
    ok_receipt = MagicMock(is_success=True, error_message=None)
    substrate.submit_extrinsic.side_effect = [fail_receipt, ok_receipt]
    daemon.client.substrate = substrate

    _run(daemon._ensure_committee_membership())

    # Two join attempts (one fail, one success after bond re-check), AND
    # _ensure_bond was invoked — but since bond was already adequate, no
    # submit_bond tx was needed.
    assert substrate.submit_extrinsic.call_count == 2
    daemon.client.submit_bond.assert_not_called()


def test_ensure_committee_membership_bounded_on_persistent_insufficient_bond():
    """If the chain keeps returning InsufficientBond (e.g. governance raised
    the requirement and the operator is broke), the handler must give up
    after a bounded number of attempts rather than spinning forever."""
    daemon = _make_daemon(
        bond_requirement=REQUIRED_BOND_BASE * 100,  # huge new requirement
        current_bond=REQUIRED_BOND_BASE,
        free_balance=0,  # can't pay the delta
    )
    substrate = MagicMock()
    substrate.query.return_value = []
    substrate.compose_call.return_value = MagicMock()
    substrate.create_signed_extrinsic.return_value = MagicMock()
    fail_receipt = MagicMock(is_success=False, error_message={"name": "InsufficientBond"})
    # Always returns InsufficientBond.
    substrate.submit_extrinsic.return_value = fail_receipt
    daemon.client.substrate = substrate

    _run(daemon._ensure_committee_membership())

    # Must be bounded: at most 2 join attempts (original + one fallback).
    assert substrate.submit_extrinsic.call_count <= 2
    # And no bond tx submitted because free balance is zero.
    daemon.client.submit_bond.assert_not_called()
