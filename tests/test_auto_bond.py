"""Tests for the auto-bond flow in cert-daemon.

Root's `OrinqReceipts.join_committee(member)` refuses a member bonded below
`BondRequirement`, so on a network whose profile bonds, the cert-daemon posts
`OrinqReceipts.bond(amount)` while its signer waits outside the committee. See
README auto-bond section for the operator-facing contract.

These tests mock the SubstrateClient at the seam so no live chain is required.
"""

from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

from daemon.cert_daemon import CertDaemon
from daemon.config import DaemonConfig


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
    daemon._chain_write_lock = None
    daemon._pending_lock = None
    daemon._concurrency_sem = None
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


def test_bond_is_one_shot_across_committee_checks():
    """Every committee check outside the committee runs the bond flow; once
    the bond meets the requirement on chain, later checks submit nothing."""
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
