"""Skeptic reconstruction on a clean b9b8074 tree with the repo's own FakeInsight (not the
red-team HTTP model): the custody verdict depends only on whether the address history lists
the failed transaction whose collateral was the custody UTxO."""
import pytest

from daemon import custody_rules as rules
from daemon import custody_watch as cw
from tests.test_custody_rules import _with_custody
from tests.test_custody_watch import FakeInsight, _at, _mainnet, config  # noqa: F401


@pytest.mark.parametrize("listed", [True, False], ids=["db-sync-listing", "insight-listing"])
def test_failed_script_collateral_from_custody(config, tmp_path, listed):
    api = FakeInsight(tip=13_500_000, time=int(_at("2026-09-27T01:00:00")))
    tx_hash = api.add_tx("phase2_invalid_collateral", height=13_500_001)
    utxos = api.routes[f"/txs/{tx_hash}/utxos"]
    custody, elsewhere = utxos["inputs"][0]["address"], utxos["outputs"][0]["address"]
    network = _with_custody(_mainnet(config), custody, label="admin_1")
    store = cw.Store(str(tmp_path / "s.db"))
    source = cw.CardanoSource(network, api, store, stale_seconds=900)
    source.start_at(13_500_000)
    api.list_at(elsewhere, tx_hash, 20 * 13_500_001)
    if listed:
        api.list_at(custody, tx_hash, 20 * 13_500_001)
    api.outputs = lambda address, slot: ([{
        "tx_hash": utxos["inputs"][0]["tx_hash"], "output_index": 0,
        "spent_by": {"tx_hash": tx_hash, "slot_no": 20 * 13_500_001}}] if address == custody else [])
    api.routes["/blocks/latest"]["height"] = 13_500_005
    assert source.poll(_at("2026-09-27T01:01:00"))
    findings = store.findings()
    [f] = findings
    assert f.severity == rules.CRITICAL and "outflow from admin_1" in f.details[0]
