import asyncio
import json
from dataclasses import asdict, replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from substrateinterface import Keypair

from daemon import slash_watcher as sw
from daemon.substrate_client import SubstrateClient
from tests.test_slash_watcher import (
    CHAIN_ID, _default_pending_request, _make_observation,
    _make_substrate_client_stub,
)
from tests.test_multisig_aggregator import _with_gateway

FIXTURES = Path(__file__).parent / 'fixtures' / 'slash_runtime'


def context(protocol='FRAU', block=101, ttl=10):
    return dict(protocol=protocol, block_number=block, ttl=ttl,
                spec_version=239, block_hash='0x' + 'aa' * 32)


def setup_watcher(ctx=None, aggregator=None):
    client = _make_substrate_client_stub()
    client.get_slash_runtime_context = MagicMock(return_value=ctx or context())
    client.keypair = MagicMock(wraps=Keypair.create_from_uri('//Alice'))
    client.keypair.public_key = Keypair.create_from_uri('//Alice').public_key
    observer = SimpleNamespace(observe=AsyncMock(return_value=_make_observation(matched_lovelace=4)))
    watcher = sw.SlashWatcher(config=SimpleNamespace(), substrate_client=client,
                              chain_write_lock=asyncio.Lock(), observer=observer,
                              aggregator=aggregator, min_signer_threshold=2 if aggregator else 1)
    request = _default_pending_request(requester=Keypair.create_from_uri('//Alice').ss58_address)
    return watcher, client, observer, request


def metadata(protocol='frau', ttl=10):
    docs = json.loads((FIXTURES / f'{protocol}_docs.json').read_text())
    result = SimpleNamespace(value=("0x6d657461", {'V14': {
        'pallets': [{'name': 'IntentSettlement', 'calls': {'ty': 7}, 'constants': [
            {'name': 'SettlementRequestTtl', 'type': 4, 'value': ttl.to_bytes(4, 'little').decode('latin1')}
        ]}],
        'types': {'types': [
            {'id': 4, 'type': {'def': {'primitive': 'u32'}}},
            {'id': 7, 'type': {'def': {'variant': {'variants': [
                {'name': 'slash_bad_settlement_evidence', 'index': 20,
                 'fields': [], 'docs': docs}
            ]}}}},
        ]},
    }}))
    def pallet(name):
        assert name == 'IntentSettlement'
        constants = result.value[1]['V14']['pallets'][0]['constants']
        return SimpleNamespace(constants=[SimpleNamespace(name=c['name'], constant_value=c['value'].encode('latin1')) for c in constants])
    result.get_metadata_pallet = pallet
    return result


def rpc_client(meta=None, spec=239, spec_name='materios'):
    client = object.__new__(SubstrateClient)
    client._read = object()
    def call(method, *args, **kwargs):
        assert kwargs['conn'] is client._read
        if method == 'get_chain_head':
            return '0x' + 'ab' * 32
        assert args == ('0x' + 'ab' * 32,), 'all reads must use the same head'
        if method == 'get_block_runtime_version':
            return {'specName': spec_name, 'specVersion': spec}
        if method == 'get_block_metadata':
            return meta or metadata()
        if method == 'get_block_number':
            return 101
        raise AssertionError(method)
    client._call = MagicMock(side_effect=call)
    return client


@pytest.mark.parametrize('protocol', ['frau', 'frv2'])
def test_runtime_detects_exact_metadata_at_head(protocol):
    client = rpc_client(metadata(protocol))
    result = client.get_slash_runtime_context()
    assert result == context(protocol.upper()) | {'block_hash': '0x' + 'ab' * 32}
    assert client._call.call_count == 4


def test_wrong_chain_spec_refuses():
    with pytest.raises(ValueError, match='slash runtime'):
        rpc_client(spec_name='other').get_slash_runtime_context()


@pytest.mark.parametrize('damage', ['docs', 'empty_docs', 'no_call', 'no_pallet', 'ttl_missing', 'ttl_short', 'ttl_type', 'metadata_version'])
def test_unknown_metadata_refuses(damage):
    meta = metadata()
    body = meta.value[1]['V14']
    if damage == 'docs':
        body['types']['types'][1]['type']['def']['variant']['variants'][0]['docs'].append('FRV3')
    elif damage == 'empty_docs':
        body['types']['types'][1]['type']['def']['variant']['variants'][0]['docs'] = []
    elif damage == 'no_call':
        body['types']['types'][1]['type']['def']['variant']['variants'] = []
    elif damage == 'no_pallet':
        body['pallets'] = []
    elif damage == 'ttl_missing':
        body['pallets'][0]['constants'] = []
    elif damage == 'ttl_short':
        body['pallets'][0]['constants'][0]['value'] = ''
    elif damage == 'ttl_type':
        body['types']['types'][0]['type']['def']['primitive'] = 'u64'
    else:
        meta.value[1]['V99'] = meta.value[1].pop('V14')
    with pytest.raises(ValueError, match='slash runtime'):
        rpc_client(meta).get_slash_runtime_context()


@pytest.mark.parametrize('error', [ValueError('unknown slash runtime'), RuntimeError(''), TimeoutError()])
def test_unavailable_context_does_not_sign_and_alerts(error, caplog):
    watcher, client, observer, req = setup_watcher()
    client.get_slash_runtime_context.side_effect = error
    verdict = asyncio.run(watcher.process_one(req, CHAIN_ID))
    assert verdict.detail == 'runtime_unavailable'
    client.keypair.sign.assert_not_called()
    observer.observe.assert_not_called()
    client.submit_slash_bad_settlement_evidence.assert_not_called()
    assert 'slash runtime unavailable' in caplog.text


@pytest.mark.parametrize('protocol', ['FRAU', 'FRV2'])
@pytest.mark.parametrize('block,expected', [(109, True), (110, True), (111, False)])
def test_expiry_boundary(protocol, block, expected):
    watcher, client, observer, req = setup_watcher(context(protocol, block))
    verdict = asyncio.run(watcher.process_one(req, CHAIN_ID))
    assert client.submit_slash_bad_settlement_evidence.called is expected
    assert client.keypair.sign.called is expected
    assert observer.observe.called is expected
    if not expected:
        assert verdict.detail == 'request_expired'


def test_expired_rows_never_repeat_submits_or_take_write_lock():
    watcher, client, observer, req = setup_watcher(context(block=111))
    client.list_pending_settlement_requests.return_value = [asdict(req)]
    watcher.process_one = AsyncMock()
    async def run():
        for _ in range(4):
            await watcher._tick(CHAIN_ID)
    asyncio.run(run())
    watcher.process_one.assert_not_called()
    client.keypair.sign.assert_not_called()
    client.submit_slash_bad_settlement_evidence.assert_not_called()


@pytest.mark.parametrize('when', ['observe', 'aggregate', 'lock'])
@pytest.mark.parametrize('change', ['expired', 'upgrade', 'unavailable'])
def test_runtime_or_expiry_changes_defer(when, change, caplog):
    agg = SimpleNamespace(assemble_envelope=AsyncMock())
    watcher, client, observer, req = setup_watcher(aggregator=agg)
    def mutate():
        if change == 'unavailable':
            client.get_slash_runtime_context.side_effect = ValueError('')
        else:
            client.get_slash_runtime_context.return_value = context('FRV2' if change == 'upgrade' else 'FRAU', 111 if change == 'expired' else 102)
    async def observe(*args):
        if when == 'observe':
            mutate()
        return _make_observation(matched_lovelace=4)
    async def assemble(*args, **kwargs):
        if when == 'aggregate':
            mutate()
        return [(client.keypair.public_key, kwargs['my_sig']), (b'b' * 32, b's' * 64)]
    class Lock:
        async def __aenter__(self):
            if when == 'lock':
                mutate()
        async def __aexit__(self, *args):
            return False
    watcher._chain_write_lock = Lock()
    observer.observe.side_effect = observe
    agg.assemble_envelope.side_effect = assemble
    verdict = asyncio.run(watcher.process_one(req, CHAIN_ID))
    if when == 'observe' and change == 'upgrade':
        assert client.submit_slash_bad_settlement_evidence.called
        digest = client.keypair.sign.call_args.args[0]
        assert digest == sw.compute_frau_digest(sw.build_frv2_preimage(CHAIN_ID, req, sw.WrongAmount(4)))
    else:
        client.submit_slash_bad_settlement_evidence.assert_not_called()
        assert not verdict.extrinsic_hash
        assert verdict.detail in ('request_expired', 'runtime_changed', 'runtime_unavailable')
    if when == 'observe' and change != 'upgrade':
        client.keypair.sign.assert_not_called()


@pytest.mark.parametrize('protocol', ['FRAU', 'FRV2'])
def test_signs_live_genesis_for_each_protocol(protocol):
    watcher, client, _, req = setup_watcher(context(protocol))
    chain_id = bytes(range(32))
    asyncio.run(watcher.process_one(req, chain_id))
    proof = sw.WrongAmount(4)
    preimage = (sw.build_frau_preimage(chain_id, req.claim_id, proof) if protocol == 'FRAU'
                else sw.build_frv2_preimage(chain_id, req, proof))
    digest = sw.compute_frau_digest(preimage)
    assert client.keypair.sign.call_args.args == (digest,)
    signature = client.submit_slash_bad_settlement_evidence.call_args.args[2][0][1]
    assert client.keypair.verify(digest, signature)


def test_partial_rollout_defers_then_recovers(caplog):
    async def run(gw, agg, session):
        watcher, client, _, req = setup_watcher(context('FRV2'), agg)
        peer = Keypair.create_from_uri('//Bob')
        old_digest = sw.compute_frau_digest(sw.build_frau_preimage(CHAIN_ID, req.claim_id, sw.WrongAmount(4)))
        new_digest = sw.compute_frau_digest(sw.build_frv2_preimage(CHAIN_ID, req, sw.WrongAmount(4)))
        # Return every row even when the client requested just one digest.
        old = dict(pubkey=peer.public_key.hex(), sig=peer.sign(old_digest).hex(), digest=old_digest.hex())
        gw.canned_get_body = {'sigs': [old]}
        verdict = await watcher.process_one(req, CHAIN_ID)
        assert verdict.detail == 'awaiting_peer_sigs:1/2'
        client.submit_slash_bad_settlement_evidence.assert_not_called()
        gw.canned_get_body['sigs'].append(dict(pubkey=peer.public_key.hex(), sig=peer.sign(new_digest).hex(), digest=new_digest.hex()))
        verdict = await watcher.process_one(req, CHAIN_ID)
        assert verdict.extrinsic_hash
        envelope = client.submit_slash_bad_settlement_evidence.call_args.args[2]
        assert len(envelope) == 2
        assert all(Keypair(public_key=pub, ss58_format=42).verify(new_digest, sig) for pub, sig in envelope)
    asyncio.run(_with_gateway(run))


@pytest.mark.parametrize('field,value', [
    ('protocol', 'FRV3'), ('protocol', ''), ('protocol', None),
    ('ttl', None), ('ttl', ''), ('ttl', True), ('ttl', -1), ('ttl', 2**32),
    ('block_number', ''), ('block_number', -1), ('spec_version', None),
])
def test_invalid_context_refuses_before_signing(field, value, caplog):
    ctx = context()
    ctx[field] = value
    watcher, client, observer, req = setup_watcher(ctx)
    verdict = asyncio.run(watcher.process_one(req, CHAIN_ID))
    assert verdict.detail == 'runtime_unavailable'
    client.keypair.sign.assert_not_called()
    client.submit_slash_bad_settlement_evidence.assert_not_called()
    assert 'slash runtime unavailable' in caplog.text


def test_unknown_runtime_stops_entire_tick(caplog):
    watcher, client, observer, req = setup_watcher()
    client.get_slash_runtime_context.side_effect = ValueError('')
    client.list_pending_settlement_requests.return_value = [asdict(req)]
    watcher.process_one = AsyncMock()
    asyncio.run(watcher._tick(CHAIN_ID))
    watcher.process_one.assert_not_called()
    assert 'slash runtime unavailable' in caplog.text


def test_zero_ttl_is_read_from_chain_and_only_same_block_is_valid():
    assert rpc_client(metadata(ttl=0)).get_slash_runtime_context()['ttl'] == 0
    watcher, client, _, req = setup_watcher(context(block=101, ttl=0))
    assert asyncio.run(watcher.process_one(req, CHAIN_ID)).detail == 'request_expired'
    client.submit_slash_bad_settlement_evidence.assert_not_called()
    client.get_slash_runtime_context.return_value = context(block=100, ttl=0)
    assert asyncio.run(watcher.process_one(req, CHAIN_ID)).extrinsic_hash
