"""Regression coverage for the collateral/history and mint-cursor review PoCs."""
import json

import pytest

from daemon import custody_rules as rules
from daemon import custody_watch as cw
from tests.test_custody_watch import FakeInsight, _at, _mainnet


@pytest.fixture
def config():
    from tests.test_custody_watch import FIX
    return rules.parse_config(json.loads((FIX / "config.json").read_text()))


def _watch_one_asset(config, tmp_path):
    store = cw.Store(str(tmp_path / "state.db"))
    network = _mainnet(config)
    api = FakeInsight(tip=13_500_000, time=int(_at("2026-09-27T01:00:00")))
    source = cw.CardanoSource(network, api, store, stale_seconds=900)
    unit, policy = network.pool.cmatra_unit, network.policies[0].policy_id
    api.routes[f"/assets/policy/{policy}"] = [{"asset": unit, "quantity": "5"}]
    api.routes[f"/assets/{unit}"] = {"mint_or_burn_count": 1}
    api.routes[f"/assets/{unit}/history"] = [{"tx_hash": "0" * 64, "action": "minted", "amount": "5"}]
    assert source.poll(_at("2026-09-27T01:00:30"))
    return store, api, source, unit


@pytest.mark.parametrize("lagging_history", ["matches", "one-behind", "404"])
def test_a_mint_count_ahead_of_its_history(config, tmp_path, lagging_history):
    store, api, source, unit = _watch_one_asset(config, tmp_path)
    mint_hash = api.add_tx("mint_v2", height=13_500_001)
    full = [{"tx_hash": "0" * 64, "action": "minted", "amount": "5"},
            {"tx_hash": mint_hash, "action": "minted", "amount": "1"}]
    api.routes[f"/assets/{unit}"] = {"mint_or_burn_count": 2}
    api.routes[f"/assets/policy/{unit[:56]}"] = [{"asset": unit, "quantity": "6"}]
    api.routes[f"/assets/{unit}/history"] = {"matches": full, "one-behind": full[:1], "404": None}[lagging_history]
    if lagging_history == "matches":
        assert source.poll(_at("2026-09-27T01:01:30"))
    else:
        with pytest.raises(cw.SourceError):
            source.poll(_at("2026-09-27T01:01:30"))
        assert store.get(f"cursor:cardano-mainnet:asset:{unit}") == "1"
    first = store.findings()
    # The replica catches up; later polls see a consistent provider.
    api.routes[f"/assets/{unit}/history"] = full
    for minute in range(2, 6):
        assert source.poll(_at(f"2026-09-27T01:0{minute}:30"))
    after = store.findings()
    if lagging_history == "matches":
        [finding] = first
        assert finding.severity == rules.CRITICAL and "minted" in finding.text
    else:
        assert first == []
        assert len(after) == 1 and after[0].severity == rules.CRITICAL
        assert store.get(f"cursor:cardano-mainnet:asset:{unit}") == "2"
