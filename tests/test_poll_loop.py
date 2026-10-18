"""The settle, expire and slash loops survive a failed genesis read."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from daemon import settle_claim_attestor as sca
from daemon.expire_policy_attestor import ExpirePolicyAttestor
from daemon.settle_claim_attestor import SettleClaimAttestor
from daemon.slash_watcher import SlashWatcher

GENESIS = "0x" + "11" * 32


def build(kind, client):
    common = dict(config=SimpleNamespace(), substrate_client=client, chain_write_lock=asyncio.Lock(),
                  observer=SimpleNamespace(kupo_coverage_gap=AsyncMock(return_value=None), kupo_url="http://kupo"),
                  poll_interval=12, aggregator=None, min_signer_threshold=1)
    if kind == "slash":
        return SlashWatcher(**common)
    cls = SettleClaimAttestor if kind == "settle" else ExpirePolicyAttestor
    return cls(min_finality_depth=15, **common)


@pytest.mark.parametrize("kind", ["settle", "expire", "slash"])
def test_a_failed_genesis_read_is_logged_backed_off_and_retried(kind, monkeypatch, caplog):
    client = MagicMock()
    client.get_genesis_hash.side_effect = [TimeoutError("rpc"), ConnectionError("ws"), GENESIS, GENESIS]
    client.get_settlement_runtime_context.side_effect = ValueError("")
    loop = build(kind, client)
    ticks = []
    loop._tick = AsyncMock(side_effect=lambda chain_id: ticks.append(chain_id))
    sleeps = []

    async def sleep(seconds):
        sleeps.append(seconds)
        loop._running = len(sleeps) < 4
    monkeypatch.setattr(sca.asyncio, "sleep", sleep)
    loop._running = True
    asyncio.run(loop._run_forever())
    assert ticks == [bytes.fromhex("11" * 32)] * 2
    assert sleeps == [24, 48, 12, 12]
    failures = [r for r in caplog.records if "genesis" in r.getMessage() and r.levelname == "ERROR"]
    assert len(failures) == 2 and "TimeoutError" in failures[0].getMessage()


def test_genesis_retries_back_off_no_further_than_the_cap(monkeypatch):
    client = MagicMock()
    client.get_genesis_hash.side_effect = TimeoutError("rpc")
    loop = build("settle", client)
    sleeps = []

    async def sleep(seconds):
        sleeps.append(seconds)
        loop._running = len(sleeps) < 8
    monkeypatch.setattr(sca.asyncio, "sleep", sleep)
    loop._running = True
    asyncio.run(loop._run_forever())
    assert sleeps == [24, 48, 96, 192, sca.GENESIS_RETRY_MAX_SECONDS, sca.GENESIS_RETRY_MAX_SECONDS,
                      sca.GENESIS_RETRY_MAX_SECONDS, sca.GENESIS_RETRY_MAX_SECONDS]


@pytest.mark.parametrize("kind", ["settle", "expire", "slash"])
def test_a_raising_tick_is_logged_and_the_next_poll_still_runs(kind, monkeypatch, caplog):
    client = MagicMock()
    client.get_genesis_hash.return_value = GENESIS
    client.get_settlement_runtime_context.side_effect = ValueError("")
    loop = build(kind, client)
    loop._tick = AsyncMock(side_effect=[RuntimeError("boom"), None])
    sleeps = []

    async def sleep(seconds):
        sleeps.append(seconds)
        loop._running = len(sleeps) < 2
    monkeypatch.setattr(sca.asyncio, "sleep", sleep)
    loop._running = True
    asyncio.run(loop._run_forever())
    assert loop._tick.call_count == 2
    assert any("tick raised RuntimeError" in r.getMessage() for r in caplog.records)
