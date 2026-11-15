"""A signer the chain lists as a backup lets the other members attest first.

``OrinqReceipts.BackupSigners`` lists committee accounts that are unpaid
backups: the chain refuses a listed signer's attest of a receipt before block
``ReceiptSubmittedAt + BackupSignerDelay``. The daemon reads both from the
chain with its committee check. While it is listed with a non-zero delay it
verifies each receipt as ever, then parks it, with the block its attest is
taken from, in the deferred receipts the state file keeps; from that block the
deferred retry rechecks the chain and attests only a receipt still
uncertified.

A signer the chain does not list, and every signer on a runtime without
backup signers, attests exactly as before.
"""
import asyncio
import gzip
import json
import logging
import queue
from pathlib import Path
from types import SimpleNamespace

import pytest
from substrateinterface import SubstrateInterface

from daemon import cert_daemon
from daemon import substrate_client as sc
from daemon.substrate_client import RPCTimeoutError
from tests.fake_chain import (
    CALL_NOT_EXPECTED, OTHER_MEMBERS, RECEIPT_ID, FakeChain, attest_daemon, canonical_cert_hash, certify_by_others,
    connected_client, criticals, dialling_client, fake_endpoint, included, metric, receipt_scenario, refuse_submits,
    signatures,
)

SPEC_242_METADATA = Path(__file__).parent / "fixtures/custody/materios/metadata_spec242.hex.gz"

DELAY = 10
SUBMITTED_AT = 100
NOT_BEFORE = SUBMITTED_AT + DELAY
ATTEST = ["compose_call", "create_signed_extrinsic", "submit_extrinsic"]
VERDICT = ["get_chain_head", "query", "query", "query"]
BLOCK_AND_RECEIPT = ["get_block_hash", "get_events", "query"]
TOO_EARLY = {"type": "Module", "name": "BackupSignerTooEarly",
             "docs": ["A backup signer attested before its delay had passed."]}


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


def chain_calls(endpoint):
    return [call for si in endpoint.opened for call in si.calls]


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


def test_a_signer_the_chain_does_not_list_has_no_delay(caplog):
    ledger = FakeChain()
    list_as_backup(ledger, OTHER_MEMBERS[0])
    client, _ = connected_client(ledger)

    client.refresh_backup_delay()

    assert client._backup_delay == 0
    assert backup_notes(caplog) == [] and noise(caplog) == []


def test_a_listed_signer_follows_the_chains_delay(caplog):
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
    client.refresh_backup_delay()
    assert client._backup_delay == 0 and noise(caplog) == []


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


def test_the_committee_check_reads_the_backup_delay(tmp_path):
    ledger, _, daemon = receipt_scenario(tmp_path)
    list_as_backup(ledger, daemon.client.keypair.public_key)

    asyncio.run(daemon._ensure_committee_membership())
    assert daemon.client._backup_delay == DELAY

    ledger.backup_delay = 2 * DELAY
    asyncio.run(daemon._ensure_committee_membership())
    assert daemon.client._backup_delay == 2 * DELAY


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


def test_a_signer_the_chain_does_not_list_attests_as_on_a_runtime_without_backup_signers(tmp_path):
    def calls_processing_the_block(backup_signers):
        data_dir = tmp_path / ("unlisted" if backup_signers else "no-backup-signers")
        data_dir.mkdir()
        ledger, endpoint, daemon = receipt_scenario(data_dir)
        ledger.backup_signers, ledger.backup_delay, ledger.number = backup_signers, DELAY, SUBMITTED_AT
        daemon.client.refresh_backup_delay()
        before = len(chain_calls(endpoint))
        asyncio.run(daemon._process_block_range(SUBMITTED_AT))
        assert included(ledger) == [RECEIPT_ID]
        return endpoint.opened[0].calls, chain_calls(endpoint)[before:]

    listing_others = calls_processing_the_block(set(OTHER_MEMBERS))

    assert listing_others == calls_processing_the_block(None)
    assert listing_others[0] == ATTEST


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
    assert (waiting.not_before, waiting.retries) == (NOT_BEFORE, 0)
    assert persisted(daemon)["deferred"][RECEIPT_ID]["not_before"] == NOT_BEFORE
    assert not daemon.cert_store.exists(RECEIPT_ID)
    assert daemon.discord == [] and noise(caplog) == []
    assert metric("verification_failures_total") == failures


def test_a_waiting_receipt_does_not_hold_up_one_without_a_delay(tmp_path):
    """A receipt with no ReceiptSubmittedAt entry has no delay."""
    ledger, _, daemon = backup_scenario(tmp_path)
    undelayed = "0x" + "cd" * 32
    ledger.add_receipt(undelayed, block=SUBMITTED_AT)
    del ledger.submitted_at[undelayed]

    asyncio.run(daemon._process_block_range(SUBMITTED_AT))

    assert included(ledger) == [undelayed] and list(daemon.pending) == [RECEIPT_ID]


def test_a_waiting_receipt_is_left_alone_until_its_block(tmp_path):
    ledger, endpoint, daemon = backup_scenario(tmp_path)
    asyncio.run(daemon._process_block_range(SUBMITTED_AT))
    verified = verifications(daemon)
    write_calls = list(endpoint.opened[0].calls)
    ledger.number = NOT_BEFORE - 1

    asyncio.run(daemon.retry_pending())

    assert verified == [] and endpoint.opened[0].calls == write_calls
    assert daemon.pending[RECEIPT_ID].not_before == NOT_BEFORE


def test_a_backup_signer_attests_a_receipt_still_uncertified_at_its_block(tmp_path, caplog):
    ledger, _, daemon = backup_scenario(tmp_path)
    asyncio.run(daemon._process_block_range(SUBMITTED_AT))
    submitted = metric("certs_submitted_total")
    ledger.number = NOT_BEFORE

    asyncio.run(daemon.retry_pending())

    assert included(ledger) == [RECEIPT_ID] and daemon.pending == {}
    assert persisted(daemon)["deferred"] == {}
    assert metric("certs_submitted_total") == submitted + 1
    assert noise(caplog) == []


def test_a_backup_signer_skips_a_receipt_the_others_certified_meanwhile(tmp_path, caplog):
    ledger, endpoint, daemon = backup_scenario(tmp_path)
    asyncio.run(daemon._process_block_range(SUBMITTED_AT))
    verified = verifications(daemon)
    done, submitted = metric("certs_already_on_chain_total"), metric("certs_submitted_total")
    certify_by_others(ledger)
    ledger.number = NOT_BEFORE

    asyncio.run(daemon.retry_pending())

    assert signatures(endpoint) == 0 and verified == [] and daemon.pending == {}
    assert metric("certs_already_on_chain_total") == done + 1
    assert metric("certs_submitted_total") == submitted
    assert daemon.discord == [] and noise(caplog) == []


def test_a_receipt_certified_meanwhile_under_another_hash_is_a_failure(tmp_path):
    """The waiting daemon built its cert against another genesis, so the hash
    it would have claimed is not the one the receipt was certified under."""
    ledger, endpoint, daemon = backup_scenario(tmp_path)
    daemon._live_chain_genesis = "0x" + "99" * 32
    asyncio.run(daemon._process_block_range(SUBMITTED_AT))
    assert daemon.pending[RECEIPT_ID].cert_hash not in (None, canonical_cert_hash(RECEIPT_ID))
    done = metric("certs_already_on_chain_total")
    certify_by_others(ledger)
    ledger.number = NOT_BEFORE

    asyncio.run(daemon.retry_pending())

    assert len(criticals(daemon)) == 1 and daemon.pending == {}
    assert metric("certs_already_on_chain_total") == done
    assert signatures(endpoint) == 0


def test_waiting_spends_none_of_the_retry_budget(tmp_path, caplog, monkeypatch):
    """Head skew: the read conn's node is at the receipt's block while the
    node the attest is submitted to is still one block short of it."""
    monkeypatch.setattr(cert_daemon, "MAX_RECEIPT_RPC_RETRIES", 1)
    ledger, endpoint, daemon = backup_scenario(tmp_path)
    asyncio.run(daemon._process_block_range(SUBMITTED_AT))
    ledger.number = NOT_BEFORE - 1
    write = endpoint.opened[0]
    write.best = ledger.head()
    ledger.number = NOT_BEFORE

    async def three_ticks():
        for _ in range(3):
            await daemon.retry_pending()

    asyncio.run(three_ticks())

    waiting = daemon.pending[RECEIPT_ID]
    assert (waiting.not_before, waiting.retries) == (NOT_BEFORE, 0)
    assert signatures(endpoint) == 0 and daemon.discord == [] and noise(caplog) == []

    write.best = None
    asyncio.run(daemon.retry_pending())
    assert included(ledger) == [RECEIPT_ID] and daemon.pending == {}


def test_a_waiting_receipt_survives_a_restart(tmp_path):
    ledger, endpoint, daemon = backup_scenario(tmp_path)
    asyncio.run(daemon._process_block_range(SUBMITTED_AT))
    claimed = daemon.pending[RECEIPT_ID].cert_hash

    restarted, fresh = restart(ledger, tmp_path)
    waiting = restarted.pending[RECEIPT_ID]
    assert (waiting.not_before, waiting.cert_hash) == (NOT_BEFORE, claimed)
    verified = verifications(restarted)
    ledger.number = NOT_BEFORE - 1
    asyncio.run(restarted.retry_pending())
    assert verified == [] and fresh.opened[0].calls == []

    ledger.number = NOT_BEFORE
    asyncio.run(restarted.retry_pending())
    assert included(ledger) == [RECEIPT_ID] and restarted.pending == {}
    assert signatures(endpoint) + signatures(fresh) == 1


def test_a_waiting_receipt_survives_both_sockets_dropping(tmp_path):
    ledger, endpoint, daemon = backup_scenario(tmp_path)
    asyncio.run(daemon._process_block_range(SUBMITTED_AT))
    for si in endpoint.opened:
        si.failure = BrokenPipeError(32, "Broken pipe")
    ledger.number = NOT_BEFORE

    asyncio.run(daemon.retry_pending())

    assert len(endpoint.opened) == 4, "each conn was replaced"
    assert included(ledger) == [RECEIPT_ID] and daemon.pending == {}


def test_state_saved_before_backup_signers_still_loads(tmp_path):
    ledger, _, daemon = receipt_scenario(tmp_path)
    with open(daemon.config.state_file, "w") as f:
        json.dump({"last_processed_block": 100, "chain_genesis": "", "deferred": {
            RECEIPT_ID: {"retries": 2, "last_error": "RPCTimeoutError: no answer", "cert_hash": None},
        }}, f)

    daemon.load_state()

    assert (daemon.pending[RECEIPT_ID].retries, daemon.pending[RECEIPT_ID].not_before) == (2, 0)


def dispatch_refuses_early_attests(write, ledger):
    """The node's pool takes a backup's early attest and its dispatch fails
    with BackupSignerTooEarly, as on a runtime that checks the delay only there."""
    submit = write.submit_extrinsic

    def submit_extrinsic(extrinsic, wait_for_inclusion=False):
        if not ledger.too_early(extrinsic.signer, extrinsic.call.params["receipt_id"]):
            return submit(extrinsic, wait_for_inclusion)
        write._use("submit_extrinsic")
        return SimpleNamespace(is_success=False, error_message=TOO_EARLY, block_hash="0x01", triggered_events=[])

    write.submit_extrinsic = submit_extrinsic


def test_a_too_early_dispatch_error_is_a_wait_not_a_failure(tmp_path, caplog):
    """The chain listed the daemon as a backup after its last committee check,
    so it attests at once and the chain answers BackupSignerTooEarly."""
    ledger, endpoint, daemon = receipt_scenario(tmp_path)
    daemon.client.refresh_backup_delay()
    list_as_backup(ledger, daemon.client.keypair.public_key)
    dispatch_refuses_early_attests(endpoint.opened[0], ledger)
    failures = metric("verification_failures_total")

    asyncio.run(daemon._process_block_range(SUBMITTED_AT))

    waiting = daemon.pending[RECEIPT_ID]
    assert (waiting.not_before, waiting.retries) == (SUBMITTED_AT + 1, 0)
    assert signatures(endpoint) == 1 and included(ledger) == []
    assert daemon.discord == [] and noise(caplog) == []
    assert metric("verification_failures_total") == failures
    assert not daemon.cert_store.exists(RECEIPT_ID)

    ledger.number = SUBMITTED_AT + 1
    asyncio.run(daemon.retry_pending())
    assert daemon.pending[RECEIPT_ID].not_before == NOT_BEFORE
    assert signatures(endpoint) == 1, "the delay it read on the refusal holds the next attest back"

    ledger.number = NOT_BEFORE
    asyncio.run(daemon.retry_pending())
    assert included(ledger) == [RECEIPT_ID] and daemon.pending == {} and noise(caplog) == []


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
    assert (waiting.not_before, waiting.retries) == (NOT_BEFORE, 0)
    assert waiting.cert_hash == canonical_cert_hash(RECEIPT_ID)
    assert signatures(endpoint) == 1 and included(ledger) == []
    assert daemon.discord == [] and noise(caplog) == []
    assert metric("verification_failures_total") == failures
    assert not daemon.cert_store.exists(RECEIPT_ID)

    ledger.number = NOT_BEFORE
    asyncio.run(daemon.retry_pending())
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
    assert (deferred.retries, deferred.not_before) == (1, 0)
    assert deferred.last_error.startswith("AttestUnsettled") and deferred.cert_hash == canonical_cert_hash(RECEIPT_ID)
    assert [message for message in noise(caplog) if "retry 1/" in message] == noise(caplog) != []
    assert daemon.discord == []


def test_an_unsettled_refusal_stays_one_when_the_backup_read_fails(tmp_path, caplog):
    ledger, endpoint, daemon = receipt_scenario(tmp_path)
    daemon.client.refresh_backup_delay()
    list_as_backup(ledger, daemon.client.keypair.public_key)
    read = endpoint.opened[1]
    query = read.query

    def backup_read_times_out(module, storage_function, params=None, block_hash=None):
        if storage_function == "BackupSigners":
            raise RPCTimeoutError("no answer")
        return query(module, storage_function, params, block_hash)

    read.query = backup_read_times_out

    asyncio.run(daemon._process_block_range(SUBMITTED_AT))

    deferred = daemon.pending[RECEIPT_ID]
    assert (deferred.retries, deferred.not_before) == (1, 0)
    assert deferred.last_error.startswith("AttestUnsettled") and deferred.cert_hash == canonical_cert_hash(RECEIPT_ID)
    assert len(noise(caplog)) == 2 and "RPCTimeoutError: no answer" in noise(caplog)[0]
    assert "retry 1/" in noise(caplog)[1]
    assert signatures(endpoint) == 1 and daemon.discord == []


def test_a_backups_refusal_at_its_block_is_still_unsettled(tmp_path, caplog):
    """The delay has passed, and the pool refuses the attest for its hash:
    the daemon built its cert against another genesis."""
    ledger, endpoint, daemon = backup_scenario(tmp_path)
    daemon._live_chain_genesis = "0x" + "99" * 32
    ledger.number = NOT_BEFORE

    asyncio.run(daemon._process_block_range(SUBMITTED_AT))

    deferred = daemon.pending[RECEIPT_ID]
    assert (deferred.retries, deferred.not_before) == (1, 0)
    assert deferred.last_error.startswith("AttestUnsettled")
    assert signatures(endpoint) == 1 and len(noise(caplog)) == 1


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
    assert daemon.pending[RECEIPT_ID].not_before == NOT_BEFORE
    assert persisted(daemon)["deferred"][RECEIPT_ID]["not_before"] == NOT_BEFORE
    assert included(ledger) == [] and noise(caplog) == []
