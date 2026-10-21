"""An evidence row is dropped for good only for what its own bytes decide: a
payload the pallet can never accept. A chain call that times out says nothing
about the row, so the row waits for a later tick. 9ae6d22 sent the payload's
SCALE encoding through a read-conn RPC and recorded its timeout as a terminal
skip-bit (#69 review). ``Vec<Bytes>`` encodes the same under every runtime,
so assembling the payload needs no chain at all.
"""
import asyncio
import base64
import threading
from types import SimpleNamespace

from daemon.evidence_submitter import EvidenceSubmitter
from tests.fake_chain import EXTRINSIC_HASH, including_client

GATEWAY = "http://gateway.test"
ROW = {
    "id": 7,
    "receipt_id": "a1" * 32,
    "evidence_type": "arm_trustzone",
    "payload": {"cert_chain_b64": [base64.b64encode(b"\x30\x82\x01\x0a").decode()]},
}
SUBMIT_WORKER = "substrate-rpc-write-submit_extrinsic"


def evidence_submitter(failed_state_path, budget: float = 30.0):
    """An ``EvidenceSubmitter`` on a real ``SubstrateClient`` over
    ``IncludingSI``s that see its chain-write lock, and those (write, read)
    interfaces."""
    client, write, read = including_client(budget)
    submitter = EvidenceSubmitter(
        config=SimpleNamespace(blob_base_url=GATEWAY, blob_gateway_url=GATEWAY),
        substrate_client=client,
        chain_write_lock=asyncio.Lock(),
        gateway_url=GATEWAY,
        submitter_token="tok",
        failed_state_path=str(failed_state_path),
    )
    write.lock = read.lock = submitter._chain_write_lock
    return submitter, write, read


def test_a_chain_call_that_times_out_leaves_the_row_for_the_next_tick(tmp_path):
    """Each chain call the row's submission makes stalls past the budget in
    turn. The attempt gives up, records nothing, and the next one submits."""
    clean, write, read = evidence_submitter(tmp_path / "clean.json")
    assert (asyncio.run(clean.submit_one(ROW)), clean._failed_rows) == (EXTRINSIC_HASH, {})
    methods = list(dict.fromkeys(read.names() + write.names()))
    assert methods == ["query", "compose_call", "create_signed_extrinsic", "submit_extrinsic", "get_block",
                       "get_events"], "payload assembly made a chain call"

    outcomes = {}
    for method in methods:
        submitter, write, read = evidence_submitter(tmp_path / f"{method}.json", budget=0.5)
        release = write.stalls[method] = read.stalls[method] = threading.Event()
        first = asyncio.run(submitter.submit_one(ROW))
        dropped = dict(submitter._failed_rows)
        release.set()
        outcomes[method] = (first, dropped, asyncio.run(submitter.submit_one(ROW)))

    assert outcomes == {method: (None, {}, EXTRINSIC_HASH) for method in methods}


def test_the_outcome_is_read_under_the_chain_write_lock(tmp_path):
    submitter, write, _ = evidence_submitter(tmp_path / "failed.json")

    assert asyncio.run(submitter.submit_one(ROW)) == EXTRINSIC_HASH

    assert [call for call in write.calls if call[0] in ("get_block", "get_events")] == [
        ("get_block", SUBMIT_WORKER, True), ("get_events", SUBMIT_WORKER, True),
    ]
