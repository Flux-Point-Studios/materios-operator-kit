"""A submit waiting for inclusion holds the write conn's websocket, and a call
made on that socket meanwhile waits behind it. 3657b37 on Gemtek and Node-3,
2026-10-21: receipt reads and refusal rechecks ran there beside in-flight
submits and timed out ("RPC 'query' exceeded 30.0s budget on the write conn")
while the node answered in ~7 ms (#69).

Receipt reads run on the read conn. The refusal verdict stays on the write
conn, whose node judged the attest (#66 F2): its recheck takes the
chain-write lock instead, so it never shares that socket with a submit, and it
reads the write node's best block even while the poll loop's node lags.
"""
import asyncio
import threading
import time

from daemon import substrate_client as sc
from tests.fake_chain import (
    CALL_NOT_EXPECTED, RECEIPT_ID, FakeSI, certify_by_others, criticals, included, metric, receipt_scenario,
    refuse_submits, signatures,
)

SECOND_RECEIPT = "0x" + "cd" * 32


def include_once(si: FakeSI, done, within: float) -> None:
    """Include the submit ``si`` holds once ``done()`` is true, or after
    ``within`` seconds."""
    def watch():
        deadline = time.monotonic() + within
        while not done() and time.monotonic() < deadline:
            time.sleep(0.005)
        si.inclusion.set()

    threading.Thread(target=watch, daemon=True).start()


def deferred_refusal(tmp_path, monkeypatch):
    """RECEIPT_ID's attest refused for a reason the chain does not show yet,
    so the receipt waits in ``pending`` for its recheck."""
    monkeypatch.setattr(sc.time, "sleep", lambda _seconds: None)
    ledger, endpoint, daemon = receipt_scenario(tmp_path)
    write = refuse_submits(endpoint, CALL_NOT_EXPECTED)
    asyncio.run(daemon._process_block_range(100))
    assert daemon.pending[RECEIPT_ID].last_error.startswith("AttestUnsettled")
    write.failure = None
    return ledger, endpoint, daemon


def test_receipt_reads_run_on_the_read_conn_while_a_submit_holds_the_write_conn(tmp_path):
    """Two receipts in one block: the first one's submit holds the write conn
    while the second one's receipt is read."""
    ledger, endpoint, daemon = receipt_scenario(tmp_path)
    ledger.add_receipt(SECOND_RECEIPT, block=100)
    write, read = endpoint.opened
    write.inclusion.clear()
    get_receipt = daemon.client.get_receipt

    def read_once_the_first_submit_holds(receipt_id):
        if receipt_id == SECOND_RECEIPT:
            assert write.holding.wait(5)
        return get_receipt(receipt_id)

    daemon.client.get_receipt = read_once_the_first_submit_holds
    include_once(write, lambda: read.calls.count("query") == 2 or bool(write.queued), within=2)

    asyncio.run(daemon._process_block_range(100))

    assert write.queued == [], "a receipt read waited behind the submit on the write conn"
    assert included(ledger) == [RECEIPT_ID, SECOND_RECEIPT] and daemon.pending == {}
    assert "query" not in write.calls


def test_a_recheck_waits_for_the_chain_write_lock_not_behind_a_submit(tmp_path, monkeypatch):
    """Another receipt's submit holds the write conn when the deferred
    receipt's recheck comes due. The recheck runs once that submit is in,
    finds the certification and counts the receipt done."""
    ledger, endpoint, daemon = deferred_refusal(tmp_path, monkeypatch)
    certify_by_others(ledger)
    ledger.add_receipt(SECOND_RECEIPT, block=101)
    write = endpoint.opened[0]
    write.inclusion.clear()
    done = metric("certs_already_on_chain_total")

    async def ticks():
        block = asyncio.create_task(daemon._process_block_range(101))
        assert await asyncio.to_thread(write.holding.wait, 5)
        include_once(write, lambda: bool(write.queued), within=0.5)
        await daemon.retry_pending()
        await block

    asyncio.run(ticks())
    assert write.queued == [], "the recheck waited behind the submit on the write conn"
    assert write.calls[-4:] == ["submit_extrinsic", "get_chain_head", "query", "query"]
    assert daemon.pending == {} and metric("certs_already_on_chain_total") == done + 1
    assert included(ledger)[-1] == SECOND_RECEIPT and signatures(endpoint) == 2


def test_a_recheck_reads_the_write_nodes_best_block_while_the_poll_loops_node_lags(tmp_path, monkeypatch):
    """The read conn's node, which the poll loop follows, stopped before the
    certification landed; the write conn's node has it at its best block."""
    ledger, endpoint, daemon = deferred_refusal(tmp_path, monkeypatch)
    endpoint.opened[1].best = ledger.head()
    certify_by_others(ledger)
    done = metric("certs_already_on_chain_total")

    asyncio.run(daemon.retry_pending())

    assert daemon.pending == {} and criticals(daemon) == []
    assert metric("certs_already_on_chain_total") == done + 1
    assert signatures(endpoint) == 1, "the recheck signs nothing"
