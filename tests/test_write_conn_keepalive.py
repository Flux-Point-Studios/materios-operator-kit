"""The write conn idles between receipts, and a proxy that drops idle
websockets (Cloudflare's limit is ~100 s) kills it there. A keepalive exercises
it through the bounded call well inside that window, so its failures replace
the conn as any call's would, and it never waits behind a submit.

The clock is the fake chain's: each keepalive sleep advances it, and a socket
idle past its limit dies.
"""
import asyncio
from unittest.mock import AsyncMock, MagicMock

from daemon import cert_daemon, health_server
from daemon.cert_daemon import CertDaemon
from daemon.config import DaemonConfig
from daemon.substrate_client import SubstrateClient
from tests.fake_chain import FakeChain, connected_client, health

CLOUDFLARE_IDLE_SECS = 100
real_sleep = asyncio.sleep


def keepalive_daemon(client):
    daemon = CertDaemon.__new__(CertDaemon)
    daemon.client = client
    daemon._running = True
    daemon._chain_write_lock = None
    daemon._pending_lock = None
    daemon._concurrency_sem = None
    daemon.config = DaemonConfig()
    return daemon


def run_beats(daemon, ledger, monkeypatch, beats):
    """Run the keepalive loop for ``beats`` sleeps, each advancing the clock."""
    slept = []

    async def sleep(seconds):
        slept.append(seconds)
        ledger.clock += seconds
        daemon._running = len(slept) < beats
        await real_sleep(0)

    monkeypatch.setattr(cert_daemon.asyncio, "sleep", sleep)
    return slept


def test_the_keepalive_interval_is_well_inside_the_proxy_idle_window():
    assert SubstrateClient(DaemonConfig()).keepalive_secs == 30
    assert 3 * 30 < CLOUDFLARE_IDLE_SECS


def test_the_keepalive_interval_follows_its_env_var(monkeypatch):
    monkeypatch.setenv("MATERIOS_RPC_KEEPALIVE_SECS", "15")
    assert SubstrateClient(DaemonConfig()).keepalive_secs == 15


def test_an_idle_write_conn_stays_usable_past_the_proxy_window(monkeypatch):
    ledger = FakeChain()
    client, endpoint = connected_client(ledger, idle_limit=CLOUDFLARE_IDLE_SECS)
    write, read = endpoint.opened
    daemon = keepalive_daemon(client)
    slept = run_beats(daemon, ledger, monkeypatch, beats=12)

    asyncio.run(daemon._keep_write_conn_alive())

    assert ledger.clock == sum(slept) >= 3 * CLOUDFLARE_IDLE_SECS
    assert health(client) == {"peers": 3, "isSyncing": False}
    assert client._write.si is write and write.calls.count("system_health") == 13
    assert read.calls == []


def test_without_the_keepalive_the_idle_write_conn_is_dead_past_the_window():
    ledger = FakeChain()
    client, endpoint = connected_client(ledger, idle_limit=CLOUDFLARE_IDLE_SECS)
    write = endpoint.opened[0]
    ledger.clock += 3 * CLOUDFLARE_IDLE_SECS

    health(client)

    assert client._write.si is not write and write.closed


def test_a_keepalive_on_a_dead_socket_replaces_the_write_conn(monkeypatch, caplog):
    ledger = FakeChain()
    client, endpoint = connected_client(ledger)
    write, read = endpoint.opened
    write.failure = BrokenPipeError(32, "Broken pipe")
    daemon = keepalive_daemon(client)
    run_beats(daemon, ledger, monkeypatch, beats=1)
    before = health_server._metrics.get("ws_transport_replacements_total", 0)

    asyncio.run(daemon._keep_write_conn_alive())

    assert client._write.si is endpoint.opened[2] and write.closed
    assert client._read.si is read and read.calls == []
    assert health_server._metrics.get("ws_transport_replacements_total", 0) == before + 1
    assert "force-replace[write]: transport error BrokenPipeError" in caplog.text


def test_the_keepalive_never_waits_behind_a_submit(monkeypatch):
    ledger = FakeChain()
    client, endpoint = connected_client(ledger)
    write = endpoint.opened[0]
    daemon = keepalive_daemon(client)
    run_beats(daemon, ledger, monkeypatch, beats=3)

    async def submit_holds_the_conn():
        daemon._ensure_concurrency_primitives()
        async with daemon._chain_write_lock:
            await asyncio.wait_for(daemon._keep_write_conn_alive(), 2)

    asyncio.run(submit_holds_the_conn())
    assert write.calls == []


def test_the_daemon_runs_the_keepalive_and_stops_it_on_shutdown(monkeypatch, tmp_path):
    started, cancelled = asyncio.Event(), []

    async def keepalive():
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.append(True)
            raise

    daemon = CertDaemon.__new__(CertDaemon)
    daemon.config = DaemonConfig(state_file=str(tmp_path / "state.json"), checkpoint_enabled=False)
    daemon.client = MagicMock()
    daemon.client.get_best_block_number.return_value = 7
    daemon.client.get_finalized_head_number.return_value = 7
    daemon.pending, daemon._notified_ids, daemon._pruned_warned_blocks = {}, {}, set()
    daemon.last_processed_block = 7
    daemon._running = True
    daemon._chain_write_lock = daemon._pending_lock = daemon._concurrency_sem = None
    daemon.verify_chain_genesis_or_wipe = MagicMock()
    daemon._ensure_committee_membership = AsyncMock()
    daemon._keep_write_conn_alive = keepalive
    for factory in ("evidence_submitter", "settle_claim_attestor", "expire_policy_attestor", "slash_watcher"):
        monkeypatch.setattr(cert_daemon, f"maybe_create_{factory}", lambda *_args: None)

    async def one_tick(_seconds):
        await real_sleep(0)
        assert started.is_set(), "the keepalive runs alongside the poll loop"
        daemon._running = False

    async def run_then_settle():
        await daemon.run()
        await real_sleep(0)
        # asyncio.run cancels leftover tasks on its own; main.py's loop does not.
        return list(cancelled)

    monkeypatch.setattr(cert_daemon.asyncio, "sleep", one_tick)
    assert asyncio.run(run_then_settle()) == [True]
