"""A refused attest is settled by what the chain the refusing pool judged holds.

Spec 242's pool refuses an attest as 1010 "Transaction call is not expected"
when the runtime would not count it: the signer is not a committee member, the
receipt is unknown, certified or already carries this signer's attest, or the
claimed hash is not canonical (materios 37dd387, ``validate_attestation``). It
judges at its node's best block, so the daemon reads that node's best block
once, receipt, committee and attestation together, and the attest is:

- done when the signer is a member and the receipt is certified under the
  claimed hash, or records this signer's attest under it;
- failed when the signer is not a member, or the receipt is certified or being
  attested under another hash;
- otherwise deferred: a later tick rechecks the chain before signing again.

sat230 on 4ca96fd, 2026-10-21: four 1010 refusals, each because another member
had certified first, read as done, but no metric counted them.
"""
import asyncio

import pytest

from daemon import cert_daemon
from daemon import substrate_client as sc
from daemon.substrate_client import AttestUnsettled
from tests.fake_chain import (
    CALL_NOT_EXPECTED, OTHER_MEMBERS, RECEIPT_ID, FakeChain, canonical_cert_hash, certify_by_others,
    connected_client, criticals, metric, receipt_scenario, refuse_submits, signatures,
)

CERT_HASH = canonical_cert_hash(RECEIPT_ID)
PRIORITY_TOO_LOW = {"code": 1014, "message": "Priority is too low: (1 vs 1)",
                    "data": "The transaction has too low priority to replace another transaction already in the pool."}


@pytest.fixture(autouse=True)
def instant_submit_backoff(monkeypatch):
    monkeypatch.setattr(sc.time, "sleep", lambda _seconds: None)


def others_certify_during_verification(daemon, ledger):
    """The receipt reads uncertified, then the other members certify it while
    its blobs are verified: the submit that follows is refused."""
    verify = daemon.verifier.verify

    async def verify_while_others_certify(receipt, manifest):
        certify_by_others(ledger)
        return await verify(receipt, manifest)

    daemon.verifier.verify = verify_while_others_certify


def test_a_refusal_for_a_receipt_certified_at_best_but_not_finalized_is_done_and_counted(tmp_path):
    """sat230's case: another member certified first. The pool judged at the
    best block, so the verdict reads the best block, not the finalized one,
    and the done receipt is counted."""
    ledger, endpoint, daemon = receipt_scenario(tmp_path)
    ledger.finalized = ledger.head()
    others_certify_during_verification(daemon, ledger)
    done, submitted, failures = (metric("certs_already_on_chain_total"), metric("certs_submitted_total"),
                                 metric("verification_failures_total"))

    asyncio.run(daemon._process_block_range(100))

    assert daemon.pending == {} and daemon.discord == []
    assert metric("certs_already_on_chain_total") == done + 1
    assert metric("certs_submitted_total") == submitted
    assert metric("verification_failures_total") == failures
    assert signatures(endpoint) == 1


def test_the_verdict_reads_the_refusing_node_not_a_lagging_read_conn():
    """The write conn's node refused the attest because it records this
    signer's attest; the read conn's node is a block behind and does not."""
    ledger = FakeChain()
    ledger.add_receipt(RECEIPT_ID, block=100)
    client, endpoint = connected_client(ledger)
    ledger.members.add(client.keypair.public_key)
    endpoint.opened[1].best = ledger.head()
    ledger.apply_attest(client.keypair.public_key, 0, RECEIPT_ID)

    outcome = client.submit_availability_cert(RECEIPT_ID, CERT_HASH)

    assert outcome.success and outcome.already_on_chain
    assert signatures(endpoint) == 1


def test_the_verdict_reads_one_block():
    """This signer's attest is recorded when the pool refuses its resend;
    another member's attest then certifies the receipt between the verdict's
    reads, which removes the Attestations entry. Read at one block, the
    refusal is done at once."""
    ledger = FakeChain()
    ledger.add_receipt(RECEIPT_ID, block=100)
    ledger.members.add(OTHER_MEMBERS[0])
    client, endpoint = connected_client(ledger)
    ledger.members.add(client.keypair.public_key)
    ledger.apply_attest(client.keypair.public_key, 0, RECEIPT_ID)
    write = endpoint.opened[0]
    query = write.query

    def certified_after_the_receipt_read(module, storage_function, params=None, block_hash=None):
        row = query(module, storage_function, params, block_hash)
        if storage_function == "Receipts" and RECEIPT_ID not in ledger.certified:
            ledger.apply_attest(OTHER_MEMBERS[0], 0, RECEIPT_ID)
        return row

    write.query = certified_after_the_receipt_read

    outcome = client.submit_availability_cert(RECEIPT_ID, CERT_HASH)

    assert outcome.success and outcome.already_on_chain
    assert signatures(endpoint) == 1


def test_a_non_members_refusal_on_a_receipt_others_certified_is_a_failure(tmp_path):
    ledger, endpoint, daemon = receipt_scenario(tmp_path)
    ledger.members.discard(daemon.client.keypair.public_key)
    others_certify_during_verification(daemon, ledger)
    done, failures = metric("certs_already_on_chain_total"), metric("verification_failures_total")

    asyncio.run(daemon._process_block_range(100))

    assert len(criticals(daemon)) == 1 and "Transaction call is not expected" in criticals(daemon)[0]
    assert metric("verification_failures_total") == failures + 1
    assert metric("certs_already_on_chain_total") == done
    assert signatures(endpoint) == 1, "a refusal for good is not resubmitted"


def test_a_bad_hash_refusal_on_a_receipt_others_certified_is_a_failure(tmp_path):
    """The daemon builds its cert against another genesis, so its claim is not
    the hash the receipt was certified under."""
    ledger, endpoint, daemon = receipt_scenario(tmp_path)
    daemon._live_chain_genesis = "0x" + "99" * 32
    others_certify_during_verification(daemon, ledger)
    done, failures = metric("certs_already_on_chain_total"), metric("verification_failures_total")

    asyncio.run(daemon._process_block_range(100))

    assert len(criticals(daemon)) == 1
    assert metric("verification_failures_total") == failures + 1
    assert metric("certs_already_on_chain_total") == done


def test_a_bad_hash_refusal_on_a_receipt_others_are_attesting_fails_at_once():
    """Another member's recorded attest carries the canonical hash, so a claim
    of any other hash can never count."""
    ledger = FakeChain()
    ledger.add_receipt(RECEIPT_ID, block=100)
    ledger.members.add(OTHER_MEMBERS[0])
    ledger.apply_attest(OTHER_MEMBERS[0], 0, RECEIPT_ID)
    client, endpoint = connected_client(ledger)
    ledger.members.add(client.keypair.public_key)

    outcome = client.submit_availability_cert(RECEIPT_ID, b"\xdd" * 32)

    assert not outcome.success and "Transaction call is not expected" in outcome.error_message
    assert endpoint.opened[0].calls.count("submit_extrinsic") == 1
    assert [signer for signer, _, _ in ledger.included] == [OTHER_MEMBERS[0]]


def test_an_unexplained_refusal_raises_the_node_verdict_after_one_submit():
    """Another member's attest of the same hash is recorded, but not this
    signer's: the refusal is not explained yet."""
    ledger = FakeChain(threshold=3)
    ledger.add_receipt(RECEIPT_ID, block=100)
    ledger.members.add(OTHER_MEMBERS[0])
    ledger.apply_attest(OTHER_MEMBERS[0], 0, RECEIPT_ID)
    client, endpoint = connected_client(ledger)
    ledger.members.add(client.keypair.public_key)
    write = refuse_submits(endpoint, CALL_NOT_EXPECTED)

    with pytest.raises(AttestUnsettled) as raised:
        client.submit_availability_cert(RECEIPT_ID, CERT_HASH)

    assert "Transaction call is not expected" in str(raised.value)
    assert raised.value.cert_hash == CERT_HASH
    assert write.calls.count("submit_extrinsic") == 1


def test_an_unexplained_refusal_is_deferred_and_done_once_the_chain_shows_it(tmp_path):
    ledger, endpoint, daemon = receipt_scenario(tmp_path)
    write = refuse_submits(endpoint, CALL_NOT_EXPECTED)
    done = metric("certs_already_on_chain_total")

    async def ticks():
        await daemon._process_block_range(100)
        assert RECEIPT_ID in daemon.pending and daemon.last_processed_block == 100
        assert not daemon.cert_store.exists(RECEIPT_ID) and daemon.discord == []
        certify_by_others(ledger)
        write.failure = None
        await daemon.retry_pending()

    asyncio.run(ticks())
    assert daemon.pending == {} and daemon.discord == []
    assert metric("certs_already_on_chain_total") == done + 1
    assert signatures(endpoint) == 1, "the recheck signs nothing"


def test_an_unexplained_refusal_later_certified_under_another_hash_is_a_failure(tmp_path):
    """The daemon's claim is not canonical and no member had attested yet, so
    the refusal reads unsettled. Once the others certify the canonical hash,
    the recheck fails the claim rather than count the receipt done."""
    ledger, endpoint, daemon = receipt_scenario(tmp_path)
    daemon._live_chain_genesis = "0x" + "99" * 32
    done, failures = metric("certs_already_on_chain_total"), metric("verification_failures_total")

    async def ticks():
        await daemon._process_block_range(100)
        assert RECEIPT_ID in daemon.pending and daemon.discord == []
        certify_by_others(ledger)
        await daemon.retry_pending()

    asyncio.run(ticks())
    assert daemon.pending == {} and len(criticals(daemon)) == 1
    assert metric("verification_failures_total") == failures + 1
    assert metric("certs_already_on_chain_total") == done
    assert signatures(endpoint) == 1


def test_a_resend_refused_while_the_attest_waits_in_the_pool_is_not_a_failure(tmp_path):
    """#64 F5: the pool holds this signer's attest, whose submit reply was
    lost, and refuses the re-signed one (same nonce, same provides tag) as
    1014. Inclusion takes longer than a submit; the receipt is deferred, and
    its recheck finds the attest included."""
    ledger, endpoint, daemon = receipt_scenario(tmp_path)
    write = refuse_submits(endpoint, PRIORITY_TOO_LOW)
    done, failures = metric("certs_already_on_chain_total"), metric("verification_failures_total")

    async def ticks():
        await daemon._process_block_range(100)
        assert RECEIPT_ID in daemon.pending and daemon.discord == []
        ledger.apply_attest(daemon.client.keypair.public_key, 0, RECEIPT_ID)
        write.failure = None
        await daemon.retry_pending()

    asyncio.run(ticks())
    assert daemon.pending == {} and daemon.discord == []
    assert metric("verification_failures_total") == failures
    assert metric("certs_already_on_chain_total") == done + 1
    assert signatures(endpoint) == 1


def test_a_refusal_that_never_settles_is_a_failure_after_its_rechecks(tmp_path, monkeypatch):
    monkeypatch.setattr(cert_daemon, "MAX_RECEIPT_RPC_RETRIES", 2)
    ledger, endpoint, daemon = receipt_scenario(tmp_path)
    refuse_submits(endpoint, CALL_NOT_EXPECTED)
    failures = metric("verification_failures_total")

    async def ticks():
        await daemon._process_block_range(100)
        await daemon.retry_pending()
        assert RECEIPT_ID in daemon.pending and daemon.discord == []
        await daemon.retry_pending()

    asyncio.run(ticks())
    assert daemon.pending == {}
    assert len(criticals(daemon)) == 1 and "Transaction call is not expected" in criticals(daemon)[0]
    assert metric("verification_failures_total") == failures + 1
    assert endpoint.opened[0].calls.count("submit_extrinsic") == 3


def test_a_receipt_whose_locator_arrives_late_defers_an_unexplained_refusal(tmp_path):
    """The receipt waited on its locator for two ticks. Once that resolves,
    the retry's attest is refused for a reason the chain does not show yet:
    the receipt is deferred like any other, with retries of its own, and
    nothing escapes the retry pass."""
    ledger, endpoint, daemon = receipt_scenario(tmp_path)
    resolve = daemon.locator.resolve
    published = False

    async def resolve_once_published(receipt_id, content_hash=None):
        return await resolve(receipt_id, content_hash) if published else None

    daemon.locator.resolve = resolve_once_published
    refuse_submits(endpoint, CALL_NOT_EXPECTED)

    async def ticks():
        nonlocal published
        await daemon._process_block_range(100)
        await daemon.retry_pending()
        assert daemon.pending[RECEIPT_ID].last_error == "", "a locator wait"
        published = True
        await daemon.retry_pending()

    asyncio.run(ticks())
    deferred = daemon.pending[RECEIPT_ID]
    assert deferred.last_error.startswith("AttestUnsettled") and deferred.retries == 1
    assert deferred.cert_hash == CERT_HASH and daemon.discord == []
