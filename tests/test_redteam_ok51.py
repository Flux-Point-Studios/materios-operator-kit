"""Regression coverage for the collateral/history and mint-cursor review PoCs."""
import copy
import http.server
import json
import threading
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

import pytest

from daemon import custody_rules as rules
from daemon import custody_watch as cw

FIX = Path(__file__).parent / "fixtures" / "custody"
SLOTS_PER_BLOCK = 20
CMATRA_UNIT = "7ff33a5565393dc47b48ac47becc12d92c9952e724e8446dfb6adc66634d41545241"


def _at(text):
    return datetime.fromisoformat(text).replace(tzinfo=timezone.utc).timestamp()


class Model(http.server.BaseHTTPRequestHandler):
    tip = 0
    tip_time = 0
    txs: dict = {}
    histories: dict = {}
    seen: list = []

    def do_GET(self):
        cls = type(self)
        url = urllib.parse.urlsplit(self.path)
        q = dict(urllib.parse.parse_qsl(url.query))
        cls.seen.append(url.path)
        status, body = 404, {"status_code": 404}
        p = url.path
        if p == "/api/v0/blocks/latest":
            status, body = 200, {"height": cls.tip, "time": cls.tip_time, "slot": SLOTS_PER_BLOCK * cls.tip}
        elif p.startswith("/api/v0/blocks/"):
            h = int(p.rsplit("/", 1)[1])
            status, body = 200, {"height": h, "slot": SLOTS_PER_BLOCK * h}
        elif p.startswith("/api/v0/assets/policy/"):
            status, body = 200, []
        elif p.startswith("/api/v0/txs/"):
            parts = p.split("/")
            doc = cls.txs.get(parts[4])
            if doc is not None:
                field = parts[5] if len(parts) > 5 else "tx"
                status, body = 200, {"tx": doc["tx"], "utxos": doc["utxos"], "redeemers": doc["redeemers"]}[field]
        elif p.startswith(("/v1/utxos/address/", "/v1/outputs/address/")):
            address = p.rsplit("/", 1)[1]
            rows = []
            for doc in cls.txs.values():
                for inp in doc["utxos"]["inputs"]:
                    if inp["address"] != address:
                        continue
                    rows.append({"tx_hash": inp["tx_hash"], "output_index": inp["output_index"],
                                 "created_at": {"slot_no": 0},
                                 "spent_by": {"tx_hash": doc["tx"]["hash"],
                                              "slot_no": doc["tx"]["block_height"] * SLOTS_PER_BLOCK}})
            if p.startswith("/v1/utxos/"):
                rows = []
            status, body = 200, {"success": True, "data": rows,
                                 "pagination": {"hasMore": False},
                                 "last_updated": {"slot_no": SLOTS_PER_BLOCK * cls.tip}}
        elif p.startswith("/v1/tx/history/"):
            address = p.rsplit("/", 1)[1]
            rows = sorted(cls.histories.get(address, []), key=lambda r: r["slot_no"])
            rows = [r for r in rows if r["slot_no"] >= int(q.get("fromSlot", 0))]
            offset, limit = int(q.get("offset", 0)), int(q.get("limit", 100))
            page = rows[offset:offset + limit]
            status, body = 200, {"success": True, "data": page,
                                 "pagination": {"limit": limit, "offset": offset,
                                                "hasMore": offset + limit < len(rows)},
                                 "last_updated": {"slot_no": SLOTS_PER_BLOCK * cls.tip}}
        raw = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, *args):
        pass


@pytest.fixture
def config():
    return rules.parse_config(json.loads((FIX / "config.json").read_text()))


@pytest.fixture
def server(tmp_path):
    Model.tip, Model.tip_time, Model.txs, Model.histories, Model.seen = 0, 0, {}, {}, []
    srv = http.server.HTTPServer(("127.0.0.1", 0), Model)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    key = tmp_path / "insight.key"
    key.write_text("SECRETKEY\n")
    yield cw.Insight(f"http://127.0.0.1:{srv.server_port}", str(key), min_interval=0)
    srv.shutdown()


def _failed_collateral_tx(reserve: bool):
    """Real preprod tx b9ebe459 (phase-2 failure): its only consumed UTxO is collateral from
    addr_test1vqyaz..; its collateral return pays addr_test1qpe6s.. (someone else)."""
    doc = json.loads((FIX / "cardano" / "phase2_invalid_collateral.json").read_text())
    if reserve:
        for row in (doc["utxos"]["inputs"][0], doc["utxos"]["outputs"][0]):
            row["amount"].append({"unit": CMATRA_UNIT, "quantity": "277500000000000"})
    return doc


def _setup(config, tmp_path, server, reserve, insight_lists_collateral_only):
    mainnet = next(n for n in config.cardano if n.name == "cardano-mainnet")
    doc = _failed_collateral_tx(reserve)
    custody = doc["utxos"]["inputs"][0]["address"]
    thief = doc["utxos"]["outputs"][0]["address"]
    watched = rules.WatchedAddress(label="custody-reserve", address=custody, role="custody")
    network = rules.CardanoNetwork(**{**mainnet.__dict__, "addresses": mainnet.addresses + (watched,)})
    store = cw.Store(str(tmp_path / "state.db"))
    source = cw.CardanoSource(network, server, store, stale_seconds=900)
    Model.tip, Model.tip_time = 13_500_000, int(_at("2026-09-27T01:00:00"))
    source.start_at(13_500_000)
    # The failed tx lands one block later.
    height = 13_500_001
    doc["tx"]["block_height"] = height
    Model.txs[doc["tx"]["hash"]] = doc
    row = {"tx_hash": doc["tx"]["hash"], "slot_no": SLOTS_PER_BLOCK * height, "block_no": height}
    Model.histories.setdefault(thief, []).append(dict(row))
    if insight_lists_collateral_only:
        Model.histories.setdefault(custody, []).append(dict(row))
    Model.tip, Model.tip_time = 13_500_003, int(_at("2026-09-27T01:01:00"))
    return network, store, source, custody, doc["tx"]["hash"]


@pytest.mark.parametrize("reserve", [False, True], ids=["ada-collateral", "reserve-as-collateral"])
def test_control_listed_failed_collateral_spend_pages_critical(config, tmp_path, server, reserve):
    network, store, source, custody, tx_hash = _setup(config, tmp_path, server, reserve, True)
    assert source.poll(_at("2026-09-27T01:01:30"))
    [finding] = store.findings()
    assert finding.severity == rules.CRITICAL
    assert finding.key == f"cardano-mainnet:{tx_hash}"
    assert "outflow from custody-reserve" in finding.details[0]
    if reserve:
        assert "-277,500,000.000000 cMATRA" in finding.details[0]


@pytest.mark.parametrize("reserve", [False, True], ids=["ada-collateral", "reserve-as-collateral"])
def test_attack_insight_omits_failed_collateral_spend_and_watcher_pages(
        config, tmp_path, server, reserve):
    network, store, source, custody, tx_hash = _setup(config, tmp_path, server, reserve, False)
    posts = []
    watch = cw.Watch(config, store, [source], posts.append, clock=lambda: _at("2026-09-27T01:01:30"))
    watch.cycle()
    assert store.get(f"cursor:cardano-mainnet:address:{custody}") == "13500003"
    assert store.get("health:cardano-mainnet:error") is None
    assert any(f.key == f"cardano-mainnet:{tx_hash}" and f.severity == rules.CRITICAL
               for f in store.findings())
    assert store.processed(f"cardano-mainnet:{tx_hash}")
    assert any("@here" in json.dumps(p) for p in posts)
    # Moving past the reorg window must neither erase nor duplicate the page.
    for step in range(1, 5):
        Model.tip = 13_500_003 + step * 40
        Model.tip_time = int(_at("2026-09-27T01:01:00")) + step * 600
        assert source.poll(Model.tip_time + 10)
    assert len(store.findings()) == 1
    assert store.processed(f"cardano-mainnet:{tx_hash}")
    # The history cursor really ran over HTTP against the custody address.
    assert any(p == f"/v1/tx/history/{custody}" for p in Model.seen)


def test_composition_three_drains_page_before_digest(config, tmp_path, server):
    """Three omitted collateral spends must page before the daily digest."""
    for listed in (True, False):
        Model.txs, Model.histories, Model.seen = {}, {}, []
        root = tmp_path / ("listed" if listed else "omitted")
        root.mkdir()
        network, store, source, custody, first = _setup(config, root, server, True, listed)
        posts = []
        clock = [_at("2026-09-27T01:01:30")]
        watch = cw.Watch(config, store, [source], posts.append, clock=lambda: clock[0])
        hashes = [first]
        base = json.loads((FIX / "cardano" / "phase2_invalid_collateral.json").read_text())
        thief = base["utxos"]["outputs"][0]["address"]
        for i in range(1, 3):
            doc = copy.deepcopy(base)
            doc["tx"]["hash"] = f"{i:02x}" * 32
            doc["utxos"]["inputs"][0]["tx_hash"] = f"{i + 10:02x}" * 32
            height = 13_500_003 + 60 * i
            doc["tx"]["block_height"] = height
            Model.txs[doc["tx"]["hash"]] = doc
            row = {"tx_hash": doc["tx"]["hash"], "slot_no": SLOTS_PER_BLOCK * height, "block_no": height}
            Model.histories.setdefault(thief, []).append(dict(row))
            if listed:
                Model.histories.setdefault(custody, []).append(dict(row))
            hashes.append(doc["tx"]["hash"])
        for minute in range(0, 12 * 60 + 15, 5):
            clock[0] = _at("2026-09-27T01:01:30") + minute * 60
            Model.tip = 13_500_003 + minute * 3
            Model.tip_time = int(clock[0]) - 20
            watch.cycle()
        text = "\n".join(p["content"] for p in posts)
        found = [f.key for f in store.findings() if f.severity == rules.CRITICAL]
        assert sorted(found) == sorted(f"cardano-mainnet:{h}" for h in hashes)
        assert text.count("@here") >= 1
