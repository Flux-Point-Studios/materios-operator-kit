import copy
import json
from dataclasses import replace
from pathlib import Path

import pytest

from daemon import custody_rules as rules
from daemon import custody_watch as cw
from tests.test_custody_watch import FakeInsight, FIX, _mainnet


def setup(tmp_path):
    config = rules.parse_config(json.loads((FIX / 'config.json').read_text()))
    doc = json.loads((FIX / 'cardano/phase2_invalid_collateral.json').read_text())
    address = doc['utxos']['inputs'][0]['address']
    network = replace(_mainnet(config), addresses=(rules.WatchedAddress('reserve', address, 'custody'),),
                      policies=(), pool=None)
    api = FakeInsight(100, 1000)
    row = doc['utxos']['inputs'][0]
    output = {'tx_hash': row['tx_hash'], 'output_index': row['output_index'],
              'created_at': {'slot_no': 100}, 'spent_by': None}
    api.utxos = lambda a, s: copy.deepcopy(api.snapshot)
    api.outputs = lambda a, s: copy.deepcopy(api.output_rows)
    api.snapshot = [dict(transaction_id=row['tx_hash'], output_index=row['output_index'])]
    api.output_rows = [output]
    store = cw.Store(str(tmp_path / 'state.db'))
    source = cw.CardanoSource(network, api, store, 900)
    source.poll(1001)
    tx_hash = doc['tx']['hash']
    doc['tx']['block_height'] = 101
    api.routes[f'/txs/{tx_hash}'] = doc['tx']
    api.routes[f'/txs/{tx_hash}/utxos'] = doc['utxos']
    api.routes[f'/txs/{tx_hash}/redeemers'] = []
    api.routes['/blocks/latest']['height'] = 101
    return store, source, api, output, tx_hash


@pytest.mark.parametrize('restart', [False, True])
def test_omitted_collateral_is_discovered_and_snapshot_survives_restart(tmp_path, restart):
    store, source, api, output, h = setup(tmp_path)
    if restart:
        store.close()
        store = cw.Store(str(tmp_path / 'state.db'))
        source = cw.CardanoSource(source._network, api, store, 900)
    api.snapshot = []
    output['spent_by'] = {'tx_hash': h, 'slot_no': 2020}
    assert source.poll(1002)
    assert any(f.key == f'cardano-mainnet:{h}' and f.severity == rules.CRITICAL for f in store.findings())


def test_unexplained_disappearance_fails_closed(tmp_path):
    store, source, api, output, h = setup(tmp_path)
    api.snapshot = []
    api.output_rows = []
    assert source.poll(1002)
    assert any(f.severity == rules.CRITICAL and 'unexplained' in f.kind for f in store.findings())


def test_created_and_spent_between_polls_is_not_missed(tmp_path):
    store, source, api, output, h = setup(tmp_path)
    output['tx_hash'] = 'a' * 64
    output['created_at']['slot_no'] = 2001
    output['spent_by'] = {'tx_hash': h, 'slot_no': 2020}
    assert source.poll(1002)
    assert any(f.key == f'cardano-mainnet:{h}' for f in store.findings())


def test_history_lag_preserves_snapshot_then_catches_collateral(tmp_path):
    store, source, api, output, h = setup(tmp_path)
    api.snapshot = []
    output['spent_by'] = {'tx_hash': h, 'slot_no': 2020}
    api.indexed = 2000
    with pytest.raises(cw.SourceError):
        source.poll(1002)
    assert store.findings() == []
    assert store.get(source._address_key(source._network.addresses[0].address)) == '100'
    api.indexed = None
    assert source.poll(1003)
    assert any(f.key == f'cardano-mainnet:{h}' for f in store.findings())


@pytest.mark.parametrize('history', [None, []])
def test_short_mint_history_does_not_commit_cursor(tmp_path, history):
    store, source, api, _, _ = setup(tmp_path)
    config = rules.parse_config(json.loads((FIX / 'config.json').read_text()))
    network = _mainnet(config)
    source._network = replace(source._network, policies=network.policies)
    policy = network.policies[0].policy_id
    unit = network.pool.cmatra_unit
    store.put(source._policy_key(policy), 'baselined')
    store.put(f'cursor:{source.name}:asset:{unit}', '1')
    api.routes[f'/assets/policy/{policy}'] = [{'asset': unit, 'quantity': '6'}]
    api.routes[f'/assets/{unit}'] = {'mint_or_burn_count': 2}
    api.routes[f'/assets/{unit}/history'] = history
    with pytest.raises(cw.SourceError):
        source.poll(1002)
    assert store.get(f'cursor:{source.name}:asset:{unit}') == '1'


def test_block_watcher_runs_unprivileged_outside_home():
    unit = Path('ops/cardano-bp-block-watcher.service').read_text()
    assert 'DynamicUser=yes' in unit
    assert '/usr/local/lib/cardano-bp-block-watcher/block-watcher.py' in unit
    assert '/home/' not in unit


def test_listed_classified_consumer_explains_disappearance_without_output_row(tmp_path):
    store, source, api, output, h = setup(tmp_path)
    api.snapshot = []
    api.output_rows = []
    api.list_at(source._network.addresses[0].address, h, 2020)
    assert source.poll(1002)
    assert len(store.findings()) == 1
    assert store.findings()[0].key == f'cardano-mainnet:{h}'


def test_spent_by_must_actually_consume_missing_utxo(tmp_path):
    store, source, api, output, h = setup(tmp_path)
    api.snapshot = []
    output['spent_by'] = {'tx_hash': h, 'slot_no': 2020}
    api.routes[f'/txs/{h}/utxos']['inputs'] = []
    assert source.poll(1002)
    assert any(f.kind == 'unexplained-custody-outflow' for f in store.findings())


@pytest.mark.parametrize('prefix', ['71cbcd71', '98ca8f02', 'e4a63b57', 'b9ebe459'])
def test_live_invalid_fixtures_omitted_from_history_page(tmp_path, prefix):
    store, source, api, output, h = setup(tmp_path)
    doc = json.loads((FIX / f'cardano/invalid_{prefix}.json').read_text())
    h = doc['tx']['hash']
    address = doc['utxos']['inputs'][0]['address']
    source._network = replace(source._network, addresses=(rules.WatchedAddress('real collateral', address, 'custody'),))
    source._addresses = [address]
    source.start_at(100)
    api.routes[f'/txs/{h}'] = doc['tx']
    api.routes[f'/txs/{h}/utxos'] = doc['utxos']
    api.routes[f'/txs/{h}/redeemers'] = []
    output['spent_by'] = {'tx_hash': h, 'slot_no': 2020}
    assert source.poll(1002)
    assert any(f.key == f'cardano-mainnet:{h}' and f.severity == rules.CRITICAL for f in store.findings())


@pytest.mark.parametrize('method', ['utxos', 'outputs'])
@pytest.mark.parametrize('failure', ['lag', '404', 'empty-more', 'duplicate'])
def test_native_output_reads_fail_closed(tmp_path, method, failure):
    key = tmp_path / 'key'
    key.write_text('test')
    api = cw.Insight('http://unused', str(key))
    page = {'success': True, 'data': [], 'pagination': {'hasMore': False}, 'last_updated': {'slot_no': 100}}
    if failure == 'lag':
        page['last_updated']['slot_no'] = 99
    elif failure == '404':
        page = None
    elif failure == 'empty-more':
        page['pagination']['hasMore'] = True
    else:
        page['data'] = [{'transaction_id': 'a', 'output_index': 0}] * 2
    api._read = lambda *args: copy.deepcopy(page)
    with pytest.raises(cw.SourceError):
        getattr(api, method)('address', 100)


def test_native_utxo_paging_pins_every_page(tmp_path):
    key = tmp_path / 'key'
    key.write_text('test')
    api = cw.Insight('http://unused', str(key))
    calls = []
    def read(path, params):
        calls.append(params)
        offset = params['offset']
        return {'success': True, 'data': [{'transaction_id': str(offset), 'output_index': 0}],
                'pagination': {'hasMore': offset < 2}, 'last_updated': {'slot_no': 100 + offset}}
    api._read = read
    assert len(api.utxos('address', 100)) == 3
    assert [c['offset'] for c in calls] == [0, 1, 2]
    assert all(c['atSlot'] == 100 for c in calls)


def test_output_lag_does_not_alarm_or_lose_snapshot(tmp_path):
    store, source, api, output, h = setup(tmp_path)
    api.snapshot = []
    def lag(*args):
        raise cw.SourceError('outputs lag')
    api.outputs = lag
    with pytest.raises(cw.SourceError):
        source.poll(1002)
    assert store.findings() == []
    api.outputs = lambda *args: []
    assert source.poll(1003)
    assert any(f.kind == 'unexplained-custody-outflow' for f in store.findings())


def test_tip_replica_behind_snapshot_does_not_false_alarm(tmp_path):
    store, source, api, output, h = setup(tmp_path)
    api.routes['/blocks/latest']['height'] = 99
    api.snapshot = []
    with pytest.raises(cw.SourceError):
        source.poll(1002)
    assert store.findings() == []


def test_unexplained_disappearance_uses_custody_pager_priority(tmp_path):
    store, source, api, output, h = setup(tmp_path)
    api.snapshot = []
    api.output_rows = []
    source.poll(1002)
    assert store.findings()[0].authority


@pytest.mark.parametrize('history', [None, []])
def test_persistent_mint_history_failure_pages_stale_without_advancing(tmp_path, history):
    from tests.test_redteam_ok51_asset import _watch_one_asset
    config = rules.parse_config(json.loads((FIX / 'config.json').read_text()))
    store, api, source, unit = _watch_one_asset(config, tmp_path)
    api.routes[f'/assets/{unit}'] = {'mint_or_burn_count': 2}
    api.routes[f'/assets/{unit}/history'] = history
    posts = []
    clock = [api.routes['/blocks/latest']['time'] + 30]
    watch = cw.Watch(config, store, [source], posts.append, clock=lambda: clock[0])
    watch.cycle()
    clock[0] += config.source_stale_seconds + 1
    api.routes['/blocks/latest']['time'] = clock[0]
    watch.cycle()
    assert store.get(f'cursor:{source.name}:asset:{unit}') == '1'
    assert any('@here' in p['content'] for p in posts)
    assert store.get(f'health:{source.name}:error') is not None


def test_snapshot_and_cursor_commit_is_atomic(tmp_path):
    store, source, api, output, h = setup(tmp_path)
    api.snapshot = []
    api.output_rows = []
    put = store.put
    def fail(name, value):
        if name.startswith('snapshot:'):
            raise RuntimeError('disk write failed')
        put(name, value)
    store.put = fail
    with pytest.raises(RuntimeError, match='disk write failed'):
        source.poll(1002)
    assert store.get(source._address_key(source._network.addresses[0].address)) == '100'
    assert store.findings() == []
    store.put = put
    assert source.poll(1003)
    assert any(f.kind == 'unexplained-custody-outflow' for f in store.findings())


def test_history_404_is_source_error_at_pagination_boundary(tmp_path):
    store, source, api, output, h = setup(tmp_path)
    with pytest.raises(cw.SourceError, match='serves no history'):
        list(source._pages('/assets/missing/history'))
