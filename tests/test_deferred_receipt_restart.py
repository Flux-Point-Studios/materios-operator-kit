"""Deferred receipts survive a restart (#64 F6).

A receipt whose processing the RPC failed, or whose refused attest the chain
did not settle yet, waits in ``pending`` while the cursor moves past its block.
The state file holds the deferred receipts with the cursor, so a restarted
daemon retries each one with the retries it has left, checks the chain before
signing, and never signs again an attest that landed. Their number is bounded.
"""
import asyncio
import json
import time

from daemon import cert_daemon
from daemon.models import PendingReceipt
from tests.fake_chain import (
    GENESIS, RECEIPT_ID, attest_daemon, certify_by_others, criticals, fail_receipt_reads, fake_endpoint,
    included, metric, receipt_scenario, signatures,
)

OTHER_GENESIS = "0x" + "99" * 32


def restart(ledger, tmp_path):
    """A new daemon process on the same data dir: a fresh client and the state on disk."""
    endpoint = fake_endpoint(ledger)
    daemon = attest_daemon(ledger, endpoint, tmp_path, last_processed_block=0)
    daemon.load_state()
    return daemon, endpoint


def persisted(daemon):
    with open(daemon.config.state_file) as f:
        return json.load(f)


def test_a_deferred_receipt_survives_a_restart(tmp_path):
    ledger, _, daemon = receipt_scenario(tmp_path)
    fail_receipt_reads(daemon, [BrokenPipeError(32, "Broken pipe")])
    asyncio.run(daemon._process_block_range(100))
    assert RECEIPT_ID in daemon.pending and persisted(daemon)["last_processed_block"] == 100

    restarted, _ = restart(ledger, tmp_path)
    assert restarted.last_processed_block == 100 and list(restarted.pending) == [RECEIPT_ID]
    asyncio.run(restarted.retry_pending())

    assert included(ledger) == [RECEIPT_ID] and restarted.pending == {}
    assert persisted(restarted)["deferred"] == {}


def test_a_restart_never_signs_again_a_deferred_attest_that_landed(tmp_path):
    """The submit landed, then its reply and the retry died with the socket."""
    ledger, endpoint, daemon = receipt_scenario(tmp_path)
    endpoint.opened[0].lose_submit_response = True
    endpoint.failure = BrokenPipeError(32, "Broken pipe")
    asyncio.run(daemon._process_block_range(100))
    assert RECEIPT_ID in daemon.pending
    done = metric("certs_already_on_chain_total")

    restarted, fresh = restart(ledger, tmp_path)
    asyncio.run(restarted.retry_pending())

    assert included(ledger) == [RECEIPT_ID] and restarted.pending == {}
    assert signatures(endpoint) + signatures(fresh) == 1
    assert metric("certs_already_on_chain_total") == done + 1


def test_a_restart_keeps_the_retries_a_deferred_receipt_has_left(tmp_path, caplog, monkeypatch):
    monkeypatch.setattr(cert_daemon, "MAX_RECEIPT_RPC_RETRIES", 2)
    ledger, _, daemon = receipt_scenario(tmp_path)
    attempts = fail_receipt_reads(daemon, [BrokenPipeError(32, "Broken pipe")] * 10)

    async def before_the_restart():
        await daemon._process_block_range(100)
        await daemon.retry_pending()

    asyncio.run(before_the_restart())
    restarted, _ = restart(ledger, tmp_path)
    attempts_after = fail_receipt_reads(restarted, [BrokenPipeError(32, "Broken pipe")] * 10)
    asyncio.run(restarted.retry_pending())

    assert (len(attempts), len(attempts_after)) == (2, 1)
    assert restarted.pending == {} and included(ledger) == []
    assert "RPC_RETRIES_EXHAUSTED" in caplog.text
    assert persisted(restarted)["deferred"] == {}


def test_deferred_receipts_are_bounded(tmp_path, caplog, monkeypatch):
    monkeypatch.setattr(cert_daemon, "MAX_DEFERRED_RECEIPTS", 2)
    ledger, _, daemon = receipt_scenario(tmp_path)
    receipts = [RECEIPT_ID] + ["0x" + f"{n:02x}" * 32 for n in (1, 2)]
    for receipt in receipts[1:]:
        ledger.add_receipt(receipt, block=100)
    fail_receipt_reads(daemon, [BrokenPipeError(32, "Broken pipe")] * 3)

    asyncio.run(daemon._process_block_range(100))

    assert list(daemon.pending) == receipts[:2]
    assert sorted(persisted(daemon)["deferred"]) == sorted(receipts[:2])
    assert f"dropping receipt {receipts[2][:16]}...: DEFERRED_LIMIT" in caplog.text


def test_a_restart_rechecks_a_refused_attest_against_the_hash_it_claimed(tmp_path):
    """The daemon's claim is not canonical and no member had attested yet, so
    its refusal is deferred unsettled. The others certify the canonical hash
    before the restart; the restarted daemon fails the claim instead of
    counting the receipt done."""
    ledger, endpoint, daemon = receipt_scenario(tmp_path)
    daemon._live_chain_genesis = OTHER_GENESIS
    asyncio.run(daemon._process_block_range(100))
    assert RECEIPT_ID in daemon.pending and daemon.discord == []
    certify_by_others(ledger)
    done = metric("certs_already_on_chain_total")

    restarted, fresh = restart(ledger, tmp_path)
    restarted._live_chain_genesis = OTHER_GENESIS
    asyncio.run(restarted.retry_pending())

    assert restarted.pending == {} and len(criticals(restarted)) == 1
    assert metric("certs_already_on_chain_total") == done
    assert signatures(endpoint) + signatures(fresh) == 1


def test_a_state_file_written_before_deferred_receipts_were_kept_loads_its_cursor(tmp_path):
    ledger, _, daemon = receipt_scenario(tmp_path)
    with open(daemon.config.state_file, "w") as f:
        json.dump({"last_processed_block": 100, "chain_genesis": GENESIS}, f)

    restarted, _ = restart(ledger, tmp_path)

    assert restarted.last_processed_block == 100 and restarted.pending == {}


def test_a_receipt_waiting_on_its_locator_is_not_kept_as_deferred(tmp_path):
    """Its retry needs the receipt record, which the state file does not hold."""
    ledger, _, daemon = receipt_scenario(tmp_path)
    receipt = daemon.client.get_receipt(RECEIPT_ID)
    daemon.pending[RECEIPT_ID] = PendingReceipt(receipt_id=RECEIPT_ID, receipt=receipt, first_seen=time.time())

    daemon.save_state()

    assert persisted(daemon)["deferred"] == {}
