"""A signer the chain lists as a backup lets the other members attest first.

``OrinqReceipts.BackupSigners`` lists committee accounts that are unpaid
backups: the pallet takes a listed signer's attest of a receipt from block
``ReceiptSubmittedAt + BackupSignerDelay``, and the pool validates an attest
for the block after its best, so the first attest it takes is one submitted
at best block ``ReceiptSubmittedAt + BackupSignerDelay - 1``. The daemon reads
the listing and the delay from the chain with its committee check. While it
is listed with a non-zero delay it verifies each receipt as ever, then parks
it, with the block it was submitted in, in the deferred receipts the state
file keeps. A wait ends when the others certify the receipt, or at the first
block the delay the chain has now allows, where the deferred retry rechecks
the chain and attests a receipt still uncertified. No wait is ever dropped.

A signer the chain does not list, and every signer on a runtime without
backup signers, attests exactly as before.
"""
import asyncio
import gzip
import json
import logging
import queue
from pathlib import Path

import pytest
from substrateinterface import SubstrateInterface

from daemon import cert_daemon
from daemon import substrate_client as sc
from daemon.models import AttestationLevel, VerificationResult
from daemon.substrate_client import RPCTimeoutError
from tests.fake_chain import (
    CALL_NOT_EXPECTED, OTHER_MEMBERS, RECEIPT_ID, FakeChain, attest_daemon, canonical_cert_hash, certify_by_others,
    connected_client, criticals, dialling_client, fail_receipt_reads, fake_endpoint, included, metric,
    receipt_scenario, refuse_submits, signatures,
)

SPEC_242_METADATA = Path(__file__).parent / "fixtures/custody/materios/metadata_spec242.hex.gz"

DELAY = 10
SUBMITTED_AT = 100
FIRST_VALID = SUBMITTED_AT + DELAY - 1
ATTEST = ["compose_call", "create_signed_extrinsic", "submit_extrinsic"]
VERDICT = ["get_chain_head", "query", "query", "query"]
BLOCK_AND_RECEIPT = ["get_block_hash", "get_events", "query"]
OTHER_RECEIPT = "0x" + "cd" * 32
OTHER_GENESIS = "0x" + "99" * 32


@pytest.fixture(autouse=True)
def instant_submit_backoff(monkeypatch):
    monkeypatch.setattr(sc.time, "sleep", lambda _seconds: None)


@pytest.fixture(autouse=True)
def info_logs(caplog):
    caplog.set_level(logging.INFO)


def list_as_backup(ledger, signer, delay=DELAY):
    ledger.backup_signers = {signer}
    ledger.backup_delay = delay
    ledger.number = SUBMITTED_AT


def backup_scenario(tmp_path):
    """``receipt_scenario`` at the receipt's block, the chain listing the
    daemon as a backup with a DELAY-block delay, which the daemon has read."""
    ledger, endpoint, daemon = receipt_scenario(tmp_path)
    list_as_backup(ledger, daemon.client.keypair.public_key)
    daemon.client.refresh_backup_delay()
    return ledger, endpoint, daemon


def verifications(daemon):
    """The receipts the daemon verifies from here on."""
    verify, verified = daemon.verifier.verify, []

    async def counted(receipt, manifest):
        verified.append(receipt.receipt_id)
        return await verify(receipt, manifest)

    daemon.verifier.verify = counted
    return verified


def verifying(daemon, results):
    """Each verification from here on gives the next of ``results``, then the real one."""
    verify, scripted = daemon.verifier.verify, list(results)

    async def scripted_first(receipt, manifest):
        return scripted.pop(0) if scripted else await verify(receipt, manifest)

    daemon.verifier.verify = scripted_first


def unfetched():
    """What the verifier returns when a chunk's fetch fails: no root computed."""
    return VerificationResult(attestation_level=AttestationLevel.FETCHED, chunks_total=1,
                              errors=["Failed to fetch chunk 0: https://gateway.test/chunks/0"])


def root_mismatch():
    """What the verifier returns when every chunk arrived and matched the
    manifest, and their Merkle root is not the receipt's."""
    return VerificationResult(attestation_level=AttestationLevel.HASH_VERIFIED, computed_root=b"\x01" * 32,
                              chunks_total=1, chunks_verified=1, errors=["Merkle root mismatch"])


def ticks(daemon, count=1):
    async def retry():
        for _ in range(count):
            await daemon.retry_pending()

    asyncio.run(retry())


def certify(ledger, receipts):
    for member in OTHER_MEMBERS:
        for receipt_id in receipts:
            ledger.apply_attest(member, ledger.nonces.get(member, 0), receipt_id)


def attested_by(ledger, daemon):
    return [receipt for signer, _, receipt in ledger.included if signer == daemon.client.keypair.public_key]


def unreadable_listing(read, error):
    """``read``'s node fails every BackupSigners read with ``error``; returns what undoes it."""
    query = read.query

    def failing(module, storage_function, params=None, block_hash=None):
        if storage_function == "BackupSigners":
            raise error
        return query(module, storage_function, params, block_hash)

    read.query = failing
    return lambda: setattr(read, "query", query)


class NoLocator:
    async def resolve(self, receipt_id, content_hash=None):
        return None


def noise(caplog):
    return [record.getMessage() for record in caplog.records if record.levelno >= logging.WARNING]


def backup_notes(caplog):
    return [record.getMessage() for record in caplog.records if "BackupSigners" in record.getMessage()]


def persisted(daemon):
    with open(daemon.config.state_file) as f:
        return json.load(f)


def restart(ledger, tmp_path):
    """A new daemon process on the same data dir, past its startup committee check."""
    endpoint = fake_endpoint(ledger)
    daemon = attest_daemon(ledger, endpoint, tmp_path, last_processed_block=0)
    daemon.load_state()
    asyncio.run(daemon._ensure_committee_membership())
    return daemon, endpoint


class Spec242Node:
    """A websocket onto a node running preprod's spec 242 runtime, which has
    no backup signers: it serves that runtime's recorded metadata and an
    empty state, under the RPC names substrate-interface 1.x asks by."""

    connected = True

    def __init__(self):
        self.frames = queue.Queue()
        head = "0x" + "ab" * 32
        version = {"specName": "materios", "specVersion": 242, "transactionVersion": 1}
        self.answers = {
            "system_chain": "Materios Preprod",
            "rpc_methods": {"methods": ["state_getMetadata", "state_getStorageAt"]},
            "chain_getHead": head,
            "chain_getBlockHash": head,
            "chain_getHeader": {"parentHash": "0x" + "aa" * 32, "number": "0x64"},
            "chain_getRuntimeVersion": version,
            "state_getRuntimeVersion": version,
            "state_getMetadata": gzip.decompress(SPEC_242_METADATA.read_bytes()).decode(),
            "state_getStorageAt": None,
            "state_getStorage": None,
        }

    def send(self, text):
        request = json.loads(text)
        self.frames.put(json.dumps({"jsonrpc": "2.0", "id": request["id"], "result": self.answers[request["method"]]}))

    def recv(self):
        return self.frames.get(timeout=5)

    def close(self):
        self.connected = False


def test_the_spec_242_runtime_read_through_substrate_interface_gives_no_delay(caplog):
    client = dialling_client(lambda: SubstrateInterface(websocket=Spec242Node(), auto_discover=False))

    client.refresh_backup_delay()
    client.refresh_backup_delay()

    assert client._backup_delay == 0
    assert len(backup_notes(caplog)) == 1 and noise(caplog) == []
    assert client._call(
        "query", "OrinqReceipts", "ReceiptSubmittedAt", [RECEIPT_ID], conn=client._read
    ).value is None, "the storage a backup signer's wait is counted from is in this runtime"


def test_a_runtime_without_backup_signers_gives_no_delay_and_says_so_once(caplog):
    client, _ = connected_client(FakeChain())

    client.refresh_backup_delay()
    client.refresh_backup_delay()

    assert client._backup_delay == 0
    assert len(backup_notes(caplog)) == 1 and noise(caplog) == []


def test_a_signer_the_chain_does_not_list_has_no_delay_and_reads_only_the_listing(caplog):
    ledger = FakeChain()
    list_as_backup(ledger, OTHER_MEMBERS[0])
    client, endpoint = connected_client(ledger)

    client.refresh_backup_delay()

    assert client._backup_delay == 0
    assert [si.calls for si in endpoint.opened] == [[], ["query"]]
    assert backup_notes(caplog) == [] and noise(caplog) == []


def test_a_listed_signer_follows_the_chains_delay_and_each_change_is_noted_once(caplog):
    ledger = FakeChain()
    client, _ = connected_client(ledger)
    list_as_backup(ledger, client.keypair.public_key)

    client.refresh_backup_delay()
    client.refresh_backup_delay()
    assert client._backup_delay == DELAY and len(backup_notes(caplog)) == 1

    ledger.backup_delay = 2 * DELAY
    client.refresh_backup_delay()
    assert client._backup_delay == 2 * DELAY

    ledger.backup_signers = set()
    for _ in range(3):
        client.refresh_backup_delay()
    assert client._backup_delay == 0 and noise(caplog) == []
    assert len([record for record in caplog.records if "no backup delay any more" in record.getMessage()]) == 1


def test_a_failed_read_keeps_the_last_delay():
    ledger = FakeChain()
    client, endpoint = connected_client(ledger)
    list_as_backup(ledger, client.keypair.public_key)
    client.refresh_backup_delay()
    read = endpoint.opened[1]
    read.failure, read.failing = RPCTimeoutError("no answer"), {"query"}

    with pytest.raises(RPCTimeoutError):
        client.refresh_backup_delay()

    assert client._backup_delay == DELAY


def test_the_first_block_a_backups_attest_is_valid_at_is_one_before_the_pallets():
    ledger = FakeChain()
    client, _ = connected_client(ledger)
    list_as_backup(ledger, client.keypair.public_key)
    client.refresh_backup_delay()
    ledger.add_receipt(RECEIPT_ID, block=SUBMITTED_AT)
    ledger.members.add(client.keypair.public_key)

    assert client.backup_attest_from(SUBMITTED_AT) == FIRST_VALID
    ledger.number = FIRST_VALID - 1
    assert ledger.too_early(client.keypair.public_key, RECEIPT_ID)
    ledger.number = FIRST_VALID
    assert client.submit_availability_cert(RECEIPT_ID, canonical_cert_hash(RECEIPT_ID)).success


def test_the_committee_check_reads_the_backup_delay(tmp_path):
    ledger, _, daemon = receipt_scenario(tmp_path)
    list_as_backup(ledger, daemon.client.keypair.public_key)

    asyncio.run(daemon._ensure_committee_membership())
    assert daemon.client._backup_delay == DELAY

    ledger.backup_delay = 2 * DELAY
    asyncio.run(daemon._ensure_committee_membership())
    assert daemon.client._backup_delay == 2 * DELAY


def test_a_failed_listing_read_is_logged_under_its_own_name(tmp_path, caplog):
    ledger, endpoint, daemon = receipt_scenario(tmp_path)
    list_as_backup(ledger, daemon.client.keypair.public_key)
    unreadable_listing(endpoint.opened[1], ValueError("decode failed"))

    asyncio.run(daemon._ensure_committee_membership())

    assert daemon.client._backup_delay is None
    assert noise(caplog) == ["Backup signer listing read failed: ValueError: decode failed"]


def test_a_runtime_without_backup_signers_attests_as_before(tmp_path, caplog):
    ledger, endpoint, daemon = receipt_scenario(tmp_path)
    ledger.number = SUBMITTED_AT

    async def two_committee_checks_then_the_block():
        await daemon._ensure_committee_membership()
        await daemon._ensure_committee_membership()
        await daemon._process_block_range(SUBMITTED_AT)

    asyncio.run(two_committee_checks_then_the_block())

    assert included(ledger) == [RECEIPT_ID] and daemon.pending == {}
    assert endpoint.opened[0].calls == ATTEST
    assert len(backup_notes(caplog)) == 1 and noise(caplog) == []


@pytest.mark.parametrize("backup_signers", [None, set(OTHER_MEMBERS)], ids=["no-backup-signers", "unlisted"])
def test_a_signer_that_is_no_backup_attests_with_the_calls_it_made_before(tmp_path, backup_signers):
    ledger, endpoint, daemon = receipt_scenario(tmp_path)
    ledger.backup_signers, ledger.backup_delay, ledger.number = backup_signers, DELAY, SUBMITTED_AT
    daemon.client.refresh_backup_delay()
    write, read = endpoint.opened
    listing_read = list(read.calls)

    asyncio.run(daemon._process_block_range(SUBMITTED_AT))

    assert included(ledger) == [RECEIPT_ID]
    assert write.calls == ATTEST and read.calls == listing_read + BLOCK_AND_RECEIPT


def test_a_listed_signer_with_a_zero_delay_does_not_wait(tmp_path):
    ledger, endpoint, daemon = receipt_scenario(tmp_path)
    list_as_backup(ledger, daemon.client.keypair.public_key, delay=0)
    daemon.client.refresh_backup_delay()

    asyncio.run(daemon._process_block_range(SUBMITTED_AT))

    assert included(ledger) == [RECEIPT_ID] and endpoint.opened[0].calls == ATTEST


def test_a_backup_signer_verifies_the_receipt_then_waits(tmp_path, caplog):
    ledger, endpoint, daemon = backup_scenario(tmp_path)
    verified = verifications(daemon)
    failures = metric("verification_failures_total")

    asyncio.run(daemon._process_block_range(SUBMITTED_AT))

    assert verified == [RECEIPT_ID]
    assert signatures(endpoint) == 0 and included(ledger) == []
    waiting = daemon.pending[RECEIPT_ID]
    assert (waiting.submitted_at, waiting.retries) == (SUBMITTED_AT, 0)
    assert persisted(daemon)["deferred"][RECEIPT_ID]["submitted_at"] == SUBMITTED_AT
    assert not daemon.cert_store.exists(RECEIPT_ID)
    assert daemon.discord == [] and noise(caplog) == []
    assert metric("verification_failures_total") == failures


def test_a_waiting_receipt_does_not_hold_up_one_without_a_delay(tmp_path):
    """A receipt with no ReceiptSubmittedAt entry has no delay."""
    ledger, _, daemon = backup_scenario(tmp_path)
    ledger.add_receipt(OTHER_RECEIPT, block=SUBMITTED_AT)
    del ledger.submitted_at[OTHER_RECEIPT]

    asyncio.run(daemon._process_block_range(SUBMITTED_AT))

    assert included(ledger) == [OTHER_RECEIPT] and list(daemon.pending) == [RECEIPT_ID]


def test_a_waiting_receipt_is_left_alone_until_the_first_block_its_attest_is_valid_at(tmp_path):
    ledger, endpoint, daemon = backup_scenario(tmp_path)
    asyncio.run(daemon._process_block_range(SUBMITTED_AT))
    verified = verifications(daemon)
    write_calls = list(endpoint.opened[0].calls)
    ledger.number = FIRST_VALID - 1

    ticks(daemon)

    assert verified == [] and endpoint.opened[0].calls == write_calls
    assert daemon.pending[RECEIPT_ID].submitted_at == SUBMITTED_AT


def test_a_backup_signer_attests_a_receipt_still_uncertified_at_the_first_valid_block(tmp_path, caplog):
    ledger, _, daemon = backup_scenario(tmp_path)
    asyncio.run(daemon._process_block_range(SUBMITTED_AT))
    submitted = metric("certs_submitted_total")
    ledger.number = FIRST_VALID

    ticks(daemon)

    assert included(ledger) == [RECEIPT_ID] and daemon.pending == {}
    assert persisted(daemon)["deferred"] == {}
    assert metric("certs_submitted_total") == submitted + 1
    assert noise(caplog) == []


def test_a_wait_ends_with_the_block_the_others_certify_the_receipt_in(tmp_path, caplog):
    ledger, endpoint, daemon = backup_scenario(tmp_path)
    asyncio.run(daemon._process_block_range(SUBMITTED_AT))
    verified = verifications(daemon)
    write_calls = list(endpoint.opened[0].calls)
    done = metric("certs_already_on_chain_total")
    ledger.number = SUBMITTED_AT + 1
    certify_by_others(ledger)

    asyncio.run(daemon._process_block_range(SUBMITTED_AT + 1))

    assert daemon.pending == {} and persisted(daemon)["deferred"] == {}
    assert metric("certs_already_on_chain_total") == done + 1
    assert verified == [] and endpoint.opened[0].calls == write_calls
    assert daemon.discord == [] and noise(caplog) == []


def test_a_wait_ended_by_a_certification_under_another_hash_is_a_failure(tmp_path):
    """The waiting daemon built its cert against another genesis, so the hash
    it would have claimed is not the one the receipt was certified under."""
    ledger, endpoint, daemon = backup_scenario(tmp_path)
    daemon._live_chain_genesis = OTHER_GENESIS
    asyncio.run(daemon._process_block_range(SUBMITTED_AT))
    assert daemon.pending[RECEIPT_ID].cert_hash not in (None, canonical_cert_hash(RECEIPT_ID))
    done = metric("certs_already_on_chain_total")
    ledger.number = SUBMITTED_AT + 1
    certify_by_others(ledger)

    asyncio.run(daemon._process_block_range(SUBMITTED_AT + 1))

    assert len(criticals(daemon)) == 1 and daemon.pending == {}
    assert metric("certs_already_on_chain_total") == done and signatures(endpoint) == 0


def test_a_backup_signer_skips_at_its_block_a_receipt_the_others_certified_unseen(tmp_path, caplog):
    """The certification's block was never scanned, so the wait runs to its
    block, where the chain's verdict ends it."""
    ledger, endpoint, daemon = backup_scenario(tmp_path)
    asyncio.run(daemon._process_block_range(SUBMITTED_AT))
    verified = verifications(daemon)
    done, submitted = metric("certs_already_on_chain_total"), metric("certs_submitted_total")
    certify_by_others(ledger)
    ledger.number = FIRST_VALID

    ticks(daemon)

    assert signatures(endpoint) == 0 and verified == [] and daemon.pending == {}
    assert metric("certs_already_on_chain_total") == done + 1
    assert metric("certs_submitted_total") == submitted
    assert daemon.discord == [] and noise(caplog) == []


def test_a_receipt_certified_unseen_under_another_hash_is_a_failure_at_its_block(tmp_path):
    ledger, endpoint, daemon = backup_scenario(tmp_path)
    daemon._live_chain_genesis = OTHER_GENESIS
    asyncio.run(daemon._process_block_range(SUBMITTED_AT))
    done = metric("certs_already_on_chain_total")
    certify_by_others(ledger)
    ledger.number = FIRST_VALID

    ticks(daemon)

    assert len(criticals(daemon)) == 1 and daemon.pending == {}
    assert metric("certs_already_on_chain_total") == done
    assert signatures(endpoint) == 0


def test_a_lagging_submit_node_costs_no_retries_and_one_verification_a_block(tmp_path, caplog, monkeypatch):
    """Head skew: the read conn's node is at the first valid block while the
    node the attest is submitted to is still one block short of it."""
    monkeypatch.setattr(cert_daemon, "MAX_RECEIPT_RPC_RETRIES", 1)
    ledger, endpoint, daemon = backup_scenario(tmp_path)
    asyncio.run(daemon._process_block_range(SUBMITTED_AT))
    ledger.number = FIRST_VALID - 1
    write = endpoint.opened[0]
    write.best = ledger.head()
    ledger.number = FIRST_VALID
    verified = verifications(daemon)

    ticks(daemon, 3)

    waiting = daemon.pending[RECEIPT_ID]
    assert (waiting.submitted_at, waiting.retries) == (SUBMITTED_AT, 0)
    assert verified == [RECEIPT_ID], "one verification for the three ticks at this head"
    assert signatures(endpoint) == 0 and daemon.discord == [] and noise(caplog) == []

    ledger.number = FIRST_VALID + 1
    ticks(daemon, 3)
    assert verified == [RECEIPT_ID] * 2 and signatures(endpoint) == 0

    write.best = None
    ledger.number = FIRST_VALID + 2
    ticks(daemon)
    assert included(ledger) == [RECEIPT_ID] and daemon.pending == {}


def test_a_waiting_receipt_survives_a_restart(tmp_path):
    ledger, endpoint, daemon = backup_scenario(tmp_path)
    asyncio.run(daemon._process_block_range(SUBMITTED_AT))
    claimed = daemon.pending[RECEIPT_ID].cert_hash

    restarted, fresh = restart(ledger, tmp_path)
    waiting = restarted.pending[RECEIPT_ID]
    assert (waiting.submitted_at, waiting.cert_hash) == (SUBMITTED_AT, claimed)
    verified = verifications(restarted)
    ledger.number = FIRST_VALID - 1
    ticks(restarted)
    assert verified == [] and fresh.opened[0].calls == []

    ledger.number = FIRST_VALID
    ticks(restarted)
    assert included(ledger) == [RECEIPT_ID] and restarted.pending == {}
    assert signatures(endpoint) + signatures(fresh) == 1


def test_a_waiting_receipt_survives_both_sockets_dropping(tmp_path):
    ledger, endpoint, daemon = backup_scenario(tmp_path)
    asyncio.run(daemon._process_block_range(SUBMITTED_AT))
    for si in endpoint.opened:
        si.failure = BrokenPipeError(32, "Broken pipe")
    ledger.number = FIRST_VALID

    ticks(daemon)

    assert len(endpoint.opened) == 4, "each conn was replaced"
    assert included(ledger) == [RECEIPT_ID] and daemon.pending == {}


@pytest.mark.parametrize("entry", [
    {"retries": 2, "last_error": "RPCTimeoutError: no answer", "cert_hash": None},
    {"retries": 0, "last_error": "BackupSignerTooEarly: waits for block 110",
     "cert_hash": canonical_cert_hash(RECEIPT_ID).hex(), "not_before": 110},
], ids=["before-backup-signers", "wait-kept-as-a-block"])
def test_state_saved_by_an_earlier_daemon_still_loads(tmp_path, entry):
    ledger, endpoint, daemon = backup_scenario(tmp_path)
    with open(daemon.config.state_file, "w") as f:
        json.dump({"last_processed_block": 100, "chain_genesis": "", "deferred": {RECEIPT_ID: entry}}, f)

    daemon.load_state()
    loaded = daemon.pending[RECEIPT_ID]
    assert (loaded.retries, loaded.submitted_at, loaded.not_before) == (entry["retries"], None, 0)

    ticks(daemon)
    assert daemon.pending[RECEIPT_ID].submitted_at == SUBMITTED_AT and signatures(endpoint) == 0
    ledger.number = FIRST_VALID
    ticks(daemon)
    assert included(ledger) == [RECEIPT_ID]


def test_a_burst_of_300_receipts_in_one_block_loses_none_across_a_restart(tmp_path, caplog):
    ledger, endpoint, daemon = backup_scenario(tmp_path)
    burst = [RECEIPT_ID] + [f"0x{n:064x}" for n in range(1, 300)]
    for receipt_id in burst[1:]:
        ledger.add_receipt(receipt_id, block=SUBMITTED_AT)

    asyncio.run(daemon._process_block_range(SUBMITTED_AT))
    assert set(daemon.pending) == set(burst) and signatures(endpoint) == 0
    assert set(persisted(daemon)["deferred"]) == set(burst)

    restarted, fresh = restart(ledger, tmp_path)
    assert set(restarted.pending) == set(burst)
    done = metric("certs_already_on_chain_total")
    ledger.number = SUBMITTED_AT + 5
    certify(ledger, burst[:100])
    asyncio.run(restarted._process_block_range(SUBMITTED_AT + 5))
    assert set(restarted.pending) == set(burst[100:])
    assert metric("certs_already_on_chain_total") == done + 100

    ledger.number = FIRST_VALID
    ticks(restarted)
    assert sorted(attested_by(ledger, restarted)) == sorted(burst[100:]) and restarted.pending == {}
    assert signatures(fresh) == 200 and criticals(restarted) == []
    assert "dropping receipt" not in caplog.text and persisted(restarted)["deferred"] == {}


def test_waits_leave_the_failure_cap_to_failures(tmp_path, caplog):
    """256 receipts wait out a long delay, then the chain unlists the signer
    and one new receipt's read times out: it is deferred, not dropped for a
    cap the waits filled, and every one of the 257 is attested."""
    ledger, endpoint, daemon = receipt_scenario(tmp_path)
    list_as_backup(ledger, daemon.client.keypair.public_key, delay=50_000)
    daemon.client.refresh_backup_delay()
    parked = [RECEIPT_ID] + [f"0x{n:064x}" for n in range(1, cert_daemon.MAX_DEFERRED_RECEIPTS)]
    for receipt_id in parked[1:]:
        ledger.add_receipt(receipt_id, block=SUBMITTED_AT)
    asyncio.run(daemon._process_block_range(SUBMITTED_AT))
    assert len(daemon.pending) == cert_daemon.MAX_DEFERRED_RECEIPTS and signatures(endpoint) == 0

    ledger.backup_signers = set()
    asyncio.run(daemon._ensure_committee_membership())
    ledger.add_receipt(OTHER_RECEIPT, block=SUBMITTED_AT + 1)
    ledger.number = SUBMITTED_AT + 1
    fail_receipt_reads(daemon, [RPCTimeoutError("no answer")])
    asyncio.run(daemon._process_block_range(SUBMITTED_AT + 1))
    assert daemon.pending[OTHER_RECEIPT].retries == 1 and "dropping receipt" not in caplog.text

    ticks(daemon)
    assert sorted(included(ledger)) == sorted(parked + [OTHER_RECEIPT]) and daemon.pending == {}


def test_a_wait_is_never_dropped_for_the_failure_cap(tmp_path, caplog, monkeypatch):
    monkeypatch.setattr(cert_daemon, "MAX_DEFERRED_RECEIPTS", 1)
    ledger, _, daemon = backup_scenario(tmp_path)
    ledger.add_receipt(OTHER_RECEIPT, block=SUBMITTED_AT)
    fail_receipt_reads(daemon, [RPCTimeoutError("no answer")])

    asyncio.run(daemon._process_block_range(SUBMITTED_AT))

    assert daemon.pending[RECEIPT_ID].last_error.startswith("RPCTimeoutError")
    assert daemon.pending[OTHER_RECEIPT].submitted_at == SUBMITTED_AT
    assert "dropping receipt" not in caplog.text


@pytest.mark.parametrize("change, due_at", [
    (lambda ledger: setattr(ledger, "backup_delay", 0), SUBMITTED_AT + 1),
    (lambda ledger: setattr(ledger, "backup_signers", set()), SUBMITTED_AT + 1),
    (lambda ledger: setattr(ledger, "backup_delay", 5), SUBMITTED_AT + 4),
], ids=["zero-delay", "unlisted", "shorter-delay"])
def test_a_parked_receipt_follows_the_delay_the_chain_has_now(tmp_path, change, due_at):
    ledger, endpoint, daemon = receipt_scenario(tmp_path)
    list_as_backup(ledger, daemon.client.keypair.public_key, delay=100_000)
    daemon.client.refresh_backup_delay()
    asyncio.run(daemon._process_block_range(SUBMITTED_AT))
    assert daemon.pending[RECEIPT_ID].submitted_at == SUBMITTED_AT

    change(ledger)
    asyncio.run(daemon._ensure_committee_membership())
    ledger.number = due_at - 1
    if due_at > SUBMITTED_AT + 1:
        ticks(daemon)
        assert signatures(endpoint) == 0
    ledger.number = due_at
    ticks(daemon)

    assert included(ledger) == [RECEIPT_ID] and daemon.pending == {}


@pytest.mark.parametrize("reread", [True, False], ids=["delay-read-again", "delay-stale"])
def test_a_parked_receipt_waits_out_a_delay_the_chain_raised(tmp_path, reread, caplog):
    ledger, endpoint, daemon = backup_scenario(tmp_path)
    asyncio.run(daemon._process_block_range(SUBMITTED_AT))
    ledger.backup_delay = 3 * DELAY
    if reread:
        daemon.client.refresh_backup_delay()
    ledger.number = FIRST_VALID

    ticks(daemon, 2)

    waiting = daemon.pending[RECEIPT_ID]
    assert (waiting.submitted_at, waiting.retries) == (SUBMITTED_AT, 0)
    assert included(ledger) == [] and signatures(endpoint) == (0 if reread else 1)
    assert daemon.discord == [] and noise(caplog) == []

    ledger.number = SUBMITTED_AT + 3 * DELAY - 1
    ticks(daemon)
    assert included(ledger) == [RECEIPT_ID]


def test_a_reorg_that_moves_the_submission_later_moves_the_wait(tmp_path):
    ledger, endpoint, daemon = backup_scenario(tmp_path)
    asyncio.run(daemon._process_block_range(SUBMITTED_AT))
    ledger.submitted_at[RECEIPT_ID] = SUBMITTED_AT + 7
    ledger.number = FIRST_VALID

    ticks(daemon)
    assert signatures(endpoint) == 0 and daemon.pending[RECEIPT_ID].submitted_at == SUBMITTED_AT + 7

    ledger.number = FIRST_VALID + 7
    ticks(daemon)
    assert included(ledger) == [RECEIPT_ID]


def test_chunks_that_do_not_arrive_at_the_receipts_block_are_fetched_again_later(tmp_path, caplog):
    """The receipt verified when it was parked. At its block a chunk's fetch
    fails, which proves nothing about its content."""
    ledger, endpoint, daemon = backup_scenario(tmp_path)
    asyncio.run(daemon._process_block_range(SUBMITTED_AT))
    failures = metric("verification_failures_total")
    verifying(daemon, [unfetched()])
    ledger.number = FIRST_VALID

    ticks(daemon)
    deferred = daemon.pending[RECEIPT_ID]
    assert deferred.retries == 1 and deferred.last_error.startswith("ChunksUnfetched: Failed to fetch chunk 0")
    assert daemon.discord == [] and metric("verification_failures_total") == failures

    ticks(daemon)
    assert included(ledger) == [RECEIPT_ID] and daemon.pending == {}


def test_content_that_does_not_match_at_the_receipts_block_is_rejected(tmp_path):
    """Every chunk arrived and their Merkle root is not the receipt's: wrong
    content, whatever an earlier run saw."""
    ledger, endpoint, daemon = backup_scenario(tmp_path)
    asyncio.run(daemon._process_block_range(SUBMITTED_AT))
    failures = metric("verification_failures_total")
    verifying(daemon, [root_mismatch()])
    ledger.number = FIRST_VALID

    ticks(daemon, 2)

    assert daemon.pending == {} and included(ledger) == []
    assert [level for level, _ in daemon.discord] == ["warning"]
    assert metric("verification_failures_total") == failures + 1


def test_chunks_that_never_arrive_end_in_a_rejection(tmp_path, monkeypatch):
    monkeypatch.setattr(cert_daemon, "MAX_RECEIPT_RPC_RETRIES", 2)
    ledger, endpoint, daemon = backup_scenario(tmp_path)
    asyncio.run(daemon._process_block_range(SUBMITTED_AT))
    failures = metric("verification_failures_total")
    verifying(daemon, [unfetched()] * 10)
    ledger.number = FIRST_VALID

    ticks(daemon, 2)
    assert daemon.pending[RECEIPT_ID].retries == 2 and daemon.discord == []

    ticks(daemon, 2)
    assert daemon.pending == {} and included(ledger) == []
    assert [level for level, _ in daemon.discord] == ["warning"]
    assert metric("verification_failures_total") == failures + 1


def test_a_receipt_that_never_verified_is_rejected_when_its_chunks_do_not_arrive(tmp_path):
    """Its first run failed on the RPC before any verification."""
    ledger, endpoint, daemon = receipt_scenario(tmp_path)
    fail_receipt_reads(daemon, [RPCTimeoutError("no answer")])
    asyncio.run(daemon._process_block_range(SUBMITTED_AT))
    assert daemon.pending[RECEIPT_ID].cert_hash is None
    verifying(daemon, [unfetched()])

    ticks(daemon)

    assert daemon.pending == {} and [level for level, _ in daemon.discord] == ["warning"]


def test_a_refusal_that_a_fresh_read_shows_is_a_backups_early_attest_is_a_wait(tmp_path, caplog):
    """The chain listed the daemon as a backup after its last committee check,
    and its pool refuses the early attest as it refuses any other: 1010, with
    no error name."""
    ledger, endpoint, daemon = receipt_scenario(tmp_path)
    daemon.client.refresh_backup_delay()
    list_as_backup(ledger, daemon.client.keypair.public_key)
    failures = metric("verification_failures_total")

    asyncio.run(daemon._process_block_range(SUBMITTED_AT))

    waiting = daemon.pending[RECEIPT_ID]
    assert (waiting.submitted_at, waiting.retries) == (SUBMITTED_AT, 0)
    assert waiting.cert_hash == canonical_cert_hash(RECEIPT_ID)
    assert signatures(endpoint) == 1 and included(ledger) == []
    assert daemon.discord == [] and noise(caplog) == []
    assert metric("verification_failures_total") == failures
    assert not daemon.cert_store.exists(RECEIPT_ID)

    ledger.number = FIRST_VALID
    ticks(daemon)
    assert included(ledger) == [RECEIPT_ID] and daemon.pending == {} and noise(caplog) == []


@pytest.mark.parametrize("backup_signers", [None, set(OTHER_MEMBERS)], ids=["no-backup-signers", "unlisted"])
def test_an_unsettled_refusal_of_a_signer_that_is_no_backup_costs_one_more_read(tmp_path, caplog, backup_signers):
    ledger, endpoint, daemon = receipt_scenario(tmp_path)
    ledger.backup_signers, ledger.backup_delay, ledger.number = backup_signers, DELAY, SUBMITTED_AT
    write, read = endpoint.opened
    refuse_submits(endpoint, CALL_NOT_EXPECTED)

    asyncio.run(daemon._process_block_range(SUBMITTED_AT))

    assert write.calls == ATTEST + VERDICT
    assert read.calls == BLOCK_AND_RECEIPT + ["query"]
    deferred = daemon.pending[RECEIPT_ID]
    assert (deferred.retries, deferred.submitted_at) == (1, None)
    assert deferred.last_error.startswith("AttestUnsettled") and deferred.cert_hash == canonical_cert_hash(RECEIPT_ID)
    assert [message for message in noise(caplog) if "retry 1/" in message] == noise(caplog) != []
    assert daemon.discord == []


def test_an_unsettled_refusal_stays_one_when_the_backup_read_fails(tmp_path, caplog):
    """The listing was read before, so the signer is known not to be a backup
    as of that read."""
    ledger, endpoint, daemon = receipt_scenario(tmp_path)
    daemon.client.refresh_backup_delay()
    list_as_backup(ledger, daemon.client.keypair.public_key)
    unreadable_listing(endpoint.opened[1], RPCTimeoutError("no answer"))

    asyncio.run(daemon._process_block_range(SUBMITTED_AT))

    deferred = daemon.pending[RECEIPT_ID]
    assert (deferred.retries, deferred.submitted_at) == (1, None)
    assert deferred.last_error.startswith("AttestUnsettled") and deferred.cert_hash == canonical_cert_hash(RECEIPT_ID)
    assert len(noise(caplog)) == 2 and "RPCTimeoutError: no answer" in noise(caplog)[0]
    assert "retry 1/" in noise(caplog)[1]
    assert signatures(endpoint) == 1 and daemon.discord == []


def test_refusals_spend_no_retries_while_the_listing_has_never_been_read(tmp_path, caplog, monkeypatch):
    """The daemon is a backup and cannot know it: every read of the listing
    has failed since it started."""
    monkeypatch.setattr(cert_daemon, "MAX_RECEIPT_RPC_RETRIES", 2)
    ledger, endpoint, daemon = receipt_scenario(tmp_path)
    list_as_backup(ledger, daemon.client.keypair.public_key)
    readable_again = unreadable_listing(endpoint.opened[1], ValueError("decode failed"))
    asyncio.run(daemon._ensure_committee_membership())

    asyncio.run(daemon._process_block_range(SUBMITTED_AT))
    ticks(daemon, 4)

    deferred = daemon.pending[RECEIPT_ID]
    assert deferred.retries == 0 and deferred.last_error.startswith("AttestUnsettled")
    assert criticals(daemon) == [] and "RPC_RETRIES_EXHAUSTED" not in caplog.text

    readable_again()
    ticks(daemon)
    assert daemon.pending[RECEIPT_ID].submitted_at == SUBMITTED_AT
    ledger.number = FIRST_VALID
    ticks(daemon)
    assert included(ledger) == [RECEIPT_ID] and criticals(daemon) == []


def test_a_backups_refusal_at_its_block_is_still_unsettled(tmp_path, caplog):
    """The delay has passed, and the pool refuses the attest for its hash:
    the daemon built its cert against another genesis."""
    ledger, endpoint, daemon = backup_scenario(tmp_path)
    daemon._live_chain_genesis = OTHER_GENESIS
    ledger.number = FIRST_VALID

    asyncio.run(daemon._process_block_range(SUBMITTED_AT))

    deferred = daemon.pending[RECEIPT_ID]
    assert (deferred.retries, deferred.submitted_at) == (1, None)
    assert deferred.last_error.startswith("AttestUnsettled")
    assert signatures(endpoint) == 1 and len(noise(caplog)) == 1


@pytest.mark.parametrize("kind", ["locator-wait", "rpc-failure"])
def test_the_head_is_read_for_waits_only(tmp_path, kind):
    ledger, endpoint, daemon = receipt_scenario(tmp_path)
    ledger.number = SUBMITTED_AT
    daemon.client.refresh_backup_delay()
    if kind == "locator-wait":
        daemon.locator = NoLocator()
    else:
        fail_receipt_reads(daemon, [RPCTimeoutError("no answer")] * 3)
    asyncio.run(daemon._process_block_range(SUBMITTED_AT))
    assert RECEIPT_ID in daemon.pending

    ticks(daemon, 2)

    assert RECEIPT_ID in daemon.pending
    assert "get_block_header" not in [call for si in endpoint.opened for call in si.calls]


def test_a_receipt_waiting_on_its_locator_is_processed_when_it_is_pushed(tmp_path):
    ledger, endpoint, daemon = receipt_scenario(tmp_path)
    ledger.number = SUBMITTED_AT
    locator, daemon.locator = daemon.locator, NoLocator()
    asyncio.run(daemon._process_block_range(SUBMITTED_AT))
    assert daemon.pending[RECEIPT_ID].last_error == ""
    daemon.locator = locator

    asyncio.run(daemon.process_receipt(RECEIPT_ID))

    assert included(ledger) == [RECEIPT_ID]


def test_a_pushed_receipt_waits_without_a_warning_and_its_block_does_not_verify_it_again(
        tmp_path, caplog, monkeypatch):
    ledger, endpoint, daemon = backup_scenario(tmp_path)
    daemon.save_state()
    verified = verifications(daemon)
    pushes = [[{"receiptId": RECEIPT_ID}]]
    monkeypatch.setattr(cert_daemon, "drain_notifications", lambda: pushes.pop() if pushes else [])
    for factory in ("evidence_submitter", "settle_claim_attestor", "expire_policy_attestor", "slash_watcher"):
        monkeypatch.setattr(cert_daemon, f"maybe_create_{factory}", lambda *_args: None)

    async def idle():
        await asyncio.Event().wait()

    async def one_tick(_seconds):
        daemon._running = False

    daemon._keep_write_conn_alive = idle
    monkeypatch.setattr(cert_daemon.asyncio, "sleep", one_tick)

    asyncio.run(daemon.run())

    assert daemon.last_processed_block == SUBMITTED_AT and verified == [RECEIPT_ID]
    assert daemon.pending[RECEIPT_ID].submitted_at == SUBMITTED_AT
    assert persisted(daemon)["deferred"][RECEIPT_ID]["submitted_at"] == SUBMITTED_AT
    assert included(ledger) == [] and noise(caplog) == []
