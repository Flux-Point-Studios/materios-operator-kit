"""A receipt whose processing the RPC failed is carried to a later tick instead
of being dropped as the cursor moves past its block, and its retry is
idempotent.

The cursor still advances block by block; the receipt waits in ``pending``,
which already drives fast polling and ``retry_pending``. Retries are bounded
and the give-up is logged by name. A retried attest can never count twice:
the runtime keeps one entry per signer, and spec 242's pool refuses an attest
it would not count as 1010 "Transaction call is not expected". How that
refusal is settled is pinned in test_attest_refusal_verdict.py.
"""
import asyncio
import errno

import pytest

from daemon import cert_daemon, health_server
from daemon import substrate_client as sc
from daemon.substrate_client import RPCTimeoutError
from tests.fake_chain import (
    RECEIPT_ID, FakeChain, canonical_cert_hash, connected_client, fail_receipt_reads, included, kernel_timeout,
    receipt_scenario,
)

CERT_HASH = canonical_cert_hash(RECEIPT_ID)


def wrapped(cause):
    try:
        raise cause
    except Exception as error:
        try:
            raise RuntimeError("decode of the node's reply failed") from error
        except RuntimeError as outer:
            return outer


RPC_FAILURES = [
    BrokenPipeError(32, "Broken pipe"),
    wrapped(ConnectionResetError(104, "Connection reset by peer")),
    RPCTimeoutError("RPC 'query' exceeded 30.0s budget on the write conn"),
    OSError(errno.EBADF, "Bad file descriptor"),
    kernel_timeout(),
]


@pytest.fixture(autouse=True)
def instant_submit_backoff(monkeypatch):
    monkeypatch.setattr(sc.time, "sleep", lambda _seconds: None)


@pytest.mark.parametrize("failure", RPC_FAILURES,
                         ids=["BrokenPipeError", "wrapped-reset", "RPCTimeoutError", "EBADF", "kernel-timeout"])
def test_a_receipt_the_rpc_failed_is_retried_on_a_later_tick(failure, tmp_path):
    ledger, _, daemon = receipt_scenario(tmp_path)
    fail_receipt_reads(daemon, [failure])

    async def ticks():
        await daemon._process_block_range(100)
        assert daemon.last_processed_block == 100, "the cursor does not wait on the receipt"
        assert included(ledger) == [] and RECEIPT_ID in daemon.pending
        await daemon.retry_pending()

    asyncio.run(ticks())
    assert included(ledger) == [RECEIPT_ID]
    assert daemon.pending == {}


def test_a_receipt_survives_an_endpoint_outage_longer_than_one_reconnect(tmp_path):
    ledger, endpoint, daemon = receipt_scenario(tmp_path)
    endpoint.opened[0].failure = BrokenPipeError(32, "Broken pipe")
    endpoint.up = False

    async def ticks():
        await daemon._process_block_range(100)
        assert RECEIPT_ID in daemon.pending and daemon.client._write.si is None
        endpoint.up = True
        await daemon.retry_pending()

    asyncio.run(ticks())
    assert included(ledger) == [RECEIPT_ID] and daemon.pending == {}


def test_a_non_rpc_failure_is_not_retried(tmp_path, caplog):
    ledger, _, daemon = receipt_scenario(tmp_path)
    attempts = fail_receipt_reads(daemon, [KeyError("content_hash")])

    async def ticks():
        await daemon._process_block_range(100)
        await daemon.retry_pending()

    asyncio.run(ticks())
    assert attempts == [RECEIPT_ID] and daemon.pending == {} and included(ledger) == []
    assert "process_receipt failed" in caplog.text


def test_rpc_retries_are_bounded_and_the_give_up_is_named(tmp_path, caplog, monkeypatch):
    monkeypatch.setattr(cert_daemon, "MAX_RECEIPT_RPC_RETRIES", 3, raising=False)
    ledger, _, daemon = receipt_scenario(tmp_path)
    attempts = fail_receipt_reads(daemon, [BrokenPipeError(32, "Broken pipe")] * 10)

    async def ticks():
        await daemon._process_block_range(100)
        for _ in range(6):
            await daemon.retry_pending()

    asyncio.run(ticks())
    assert len(attempts) == 1 + 3
    assert daemon.pending == {} and included(ledger) == []
    assert "RPC_RETRIES_EXHAUSTED" in caplog.text


def test_a_submit_that_died_on_the_rpc_leaves_no_cert_and_attests_later(tmp_path):
    ledger, endpoint, daemon = receipt_scenario(tmp_path)

    def dies_with_the_endpoint(extrinsic, wait_for_inclusion=False):
        endpoint.up = False
        raise BrokenPipeError(32, "Broken pipe")

    endpoint.opened[0].submit_extrinsic = dies_with_the_endpoint

    async def ticks():
        await daemon._process_block_range(100)
        assert not daemon.cert_store.exists(RECEIPT_ID), "a stored cert would skip the retry"
        assert RECEIPT_ID in daemon.pending
        endpoint.up = True
        await daemon.retry_pending()

    asyncio.run(ticks())
    assert included(ledger) == [RECEIPT_ID] and daemon.cert_store.exists(RECEIPT_ID)
    assert not [m for level, m in daemon.discord if level == "critical"]


def test_a_deferred_receipt_whose_attest_landed_is_not_signed_again(tmp_path):
    """The submit landed, then the socket died before its reply and on the
    retry too, so the receipt was deferred. Its retry finds the attest on
    chain and signs nothing."""
    ledger, endpoint, daemon = receipt_scenario(tmp_path)
    write = endpoint.opened[0]
    write.lose_submit_response = True
    endpoint.failure = BrokenPipeError(32, "Broken pipe")

    async def ticks():
        await daemon._process_block_range(100)
        assert RECEIPT_ID in daemon.pending
        endpoint.failure = None
        await daemon.retry_pending()

    asyncio.run(ticks())
    assert included(ledger) == [RECEIPT_ID] and daemon.pending == {}
    assert sum(si.calls.count("create_signed_extrinsic") for si in endpoint.opened) == 1
    assert not [m for level, m in daemon.discord if level == "critical"]


def test_a_resubmitted_attest_that_already_landed_is_done_and_signed_once():
    """The first submit lands but its reply dies with the socket. The retry
    resends the same signed bytes, which the pool refuses as outdated; the
    chain shows this signer's attest, so the receipt is done."""
    ledger = FakeChain()
    ledger.add_receipt(RECEIPT_ID, block=100)
    client, endpoint = connected_client(ledger)
    ledger.members.add(client.keypair.public_key)
    endpoint.opened[0].lose_submit_response = True

    outcome = client.submit_availability_cert(RECEIPT_ID, CERT_HASH)

    assert outcome.success and outcome.already_on_chain
    assert ledger.included == [(client.keypair.public_key, 0, RECEIPT_ID)]
    assert sum(si.calls.count("create_signed_extrinsic") for si in endpoint.opened) == 1


def test_a_receipt_certified_by_others_meanwhile_is_done():
    ledger = FakeChain()
    ledger.add_receipt(RECEIPT_ID, block=100)
    client, _ = connected_client(ledger)
    ledger.members.add(client.keypair.public_key)
    ledger.certified.add(RECEIPT_ID)

    outcome = client.submit_availability_cert(RECEIPT_ID, CERT_HASH)

    assert outcome.success and outcome.already_on_chain
    assert ledger.included == []


def test_a_call_not_expected_refusal_without_our_attest_on_chain_is_a_failure():
    """A non-member's attest is refused with the same 1010. Another member's
    recorded attest does not make it done."""
    ledger = FakeChain()
    ledger.add_receipt(RECEIPT_ID, block=100)
    other_member = b"\x07" * 32
    ledger.members.add(other_member)
    ledger.apply_attest(other_member, 0, RECEIPT_ID)
    client, _ = connected_client(ledger)

    outcome = client.submit_availability_cert(RECEIPT_ID, CERT_HASH)

    assert not outcome.success
    assert "Transaction call is not expected" in outcome.error_message
    assert [signer for signer, _, _ in ledger.included] == [other_member]


def test_a_receipt_certified_while_it_was_verified_raises_no_alarm(tmp_path):
    ledger, _, daemon = receipt_scenario(tmp_path)
    verify = daemon.verifier.verify

    async def verify_while_others_certify(receipt, manifest):
        ledger.certified.add(RECEIPT_ID)
        return await verify(receipt, manifest)

    daemon.verifier.verify = verify_while_others_certify
    certs_before = health_server._metrics.get("certs_submitted_total", 0)

    asyncio.run(daemon._process_block_range(100))

    assert daemon.discord == [] and daemon.pending == {}
    assert health_server._metrics.get("certs_submitted_total", 0) == certs_before
    assert included(ledger) == []
