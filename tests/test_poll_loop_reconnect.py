"""The cert daemon's poll loop says when it cannot reach its node.

``SubstrateClient.connect()`` reports a failed dial by returning False, never by
raising. A loop that waits for an exception sets ``substrate_connected``, which the
heartbeat and the watchtower's rpc_down alert read, back to true after every failed
reconnect, and can never send its "Connection lost" critical.
"""
import asyncio
from unittest.mock import AsyncMock, MagicMock

from daemon import cert_daemon
from tests.fake_chain import FakeChain, attest_daemon, criticals, fake_endpoint, metric

real_sleep = asyncio.sleep


def _running_daemon(tmp_path, monkeypatch, on_tick):
    """A daemon over a fake node whose poll loop calls ``on_tick(n)`` after its
    n-th tick and stops when that returns False."""
    ledger = FakeChain()
    endpoint = fake_endpoint(ledger)
    daemon = attest_daemon(ledger, endpoint, tmp_path, last_processed_block=ledger.number)
    daemon.verify_chain_genesis_or_wipe = MagicMock()
    daemon._ensure_committee_membership = AsyncMock()

    async def idle():
        await asyncio.Event().wait()

    daemon._keep_write_conn_alive = idle
    for factory in ("evidence_submitter", "settle_claim_attestor", "expire_policy_attestor", "slash_watcher"):
        monkeypatch.setattr(cert_daemon, f"maybe_create_{factory}", lambda *_args: None)
    monkeypatch.setattr(cert_daemon, "drain_notifications", lambda: [])
    ticks = []

    async def tick(_seconds):
        ticks.append(None)
        daemon._running = on_tick(len(ticks)) is not False
        await real_sleep(0)

    monkeypatch.setattr(cert_daemon.asyncio, "sleep", tick)
    return daemon, endpoint


def _node_down(endpoint):
    endpoint.up = False
    for si in endpoint.opened:
        si.failure = BrokenPipeError(32, "Broken pipe")


def test_a_node_the_poll_loop_cannot_reach_is_reported_once_and_as_not_connected(tmp_path, monkeypatch):
    seen = {}

    def on_tick(n):
        if n in (1, 6):
            _node_down(endpoint)
        if n in (4, 9):
            seen[n] = (metric("substrate_connected"), list(criticals(daemon)))
            endpoint.up = True
        if n in (5, 10):
            seen[n] = (metric("substrate_connected"), list(criticals(daemon)))
        return n < 10

    daemon, endpoint = _running_daemon(tmp_path, monkeypatch, on_tick)
    asyncio.run(daemon.run())

    connected, lost = seen[4]
    assert connected is False, "substrate_connected read true after a reconnect that failed"
    assert len(lost) == 1 and lost[0].startswith("Connection lost: "), "one critical for three failed ticks"
    assert seen[5] == (True, lost), "the node is back: connected again, and nothing more to say"
    connected, lost = seen[9]
    assert connected is False and len(lost) == 2, "the next outage is reported too"
    assert seen[10][0] is True

