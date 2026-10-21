"""substrate-interface hands back two results that read the chain later,
through the interface that made them. A submit's ``ExtrinsicReceipt`` reads
its block and events the first time ``is_success``, ``error_message`` or
``triggered_events`` is asked for. A ``query_map`` result reads its next page
as iteration reaches it, one more past its last row. Read after ``_call`` has
returned, those reads run outside its budget, and a receipt's run under the
chain-write lock: one that stalls holds every chain write behind it while the
read conn's successes keep /ready green (#69 review). ``_call`` reads both
out inside the call.
"""
import threading

import pytest

from daemon import substrate_client as sc
from tests.fake_chain import EXTRINSIC_HASH, RECEIPT_ID, including_client

SUBMIT_WORKER = "substrate-rpc-write-submit_extrinsic"


def read_outcome(receipt):
    """What the daemon's callers read off a receipt."""
    return receipt.is_success and receipt.error_message is None and receipt.triggered_events != []


SUBMIT_PATHS = {
    "attest": lambda client: client.submit_availability_cert(RECEIPT_ID, bytes(32)).success,
    "settlement": lambda client: client.submit_attest_settle_envelope(
        bytes(32), [(bytes(32), bytes(64))]) == EXTRINSIC_HASH,
    "bond": lambda client: client.submit_bond(1) == (True, EXTRINSIC_HASH),
    "join": lambda client: read_outcome(client.submit_join_committee()),
    "evidence": lambda client: read_outcome(client.submit_extrinsic(object())),
}


@pytest.mark.parametrize("path", SUBMIT_PATHS)
def test_a_submits_outcome_is_read_inside_its_bounded_call(path):
    client, write, read = including_client()

    assert SUBMIT_PATHS[path](client) is True

    outcome_reads = [(name, thread) for name, thread, _ in write.calls if name in ("get_block", "get_events")]
    assert outcome_reads == [("get_block", SUBMIT_WORKER), ("get_events", SUBMIT_WORKER)]
    assert read.calls == []


def test_an_outcome_read_that_stalls_times_out_at_the_submits_budget():
    client, write, _ = including_client(budget=0.2)
    release = write.stalls["get_events"] = threading.Event()

    try:
        with pytest.raises(sc.RPCTimeoutError):
            client.submit_extrinsic(object())
    finally:
        release.set()

    assert client._write.consecutive_timeouts == 1, "the stall counts toward replacing the write conn"


def test_a_maps_pages_are_read_inside_its_bounded_call():
    client, _, read = including_client()

    rows = client._call("query_map", module="IntentSettlement", storage_function="PolicyExpireRequests",
                        conn=client._read)
    read_by_the_call = list(read.calls)

    assert [value for _, value in rows] == [0, 1, 2]
    assert read.calls == read_by_the_call, "iterating the rows read the chain"
    assert read_by_the_call == [("query_map", "substrate-rpc-read-query_map", False)] * 3
