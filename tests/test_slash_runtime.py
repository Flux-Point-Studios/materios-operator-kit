import asyncio
import json
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from substrateinterface import Keypair

from daemon import slash_watcher as sw
from daemon import substrate_client as sc
from daemon.substrate_client import SubstrateClient
from tests.test_slash_watcher import (
    CHAIN_ID, _default_pending_request, _make_observation,
    _make_substrate_client_stub,
)
from tests.test_multisig_aggregator import _with_gateway

FIXTURES = Path(__file__).parent / 'fixtures' / 'slash_runtime'


def context(protocol='FRAU', block=101, ttl=10):
    return dict(protocol=protocol, block_number=block, ttl=ttl,
                spec_version=239, block_hash='0x' + 'aa' * 32,
                supersede=protocol == 'FRV2', min_finality_depth=15)


def setup_watcher(ctx=None, aggregator=None):
    client = _make_substrate_client_stub()
    client.get_settlement_runtime_context = MagicMock(return_value=ctx or context())
    client.keypair = MagicMock(wraps=Keypair.create_from_uri('//Alice'))
    client.keypair.public_key = Keypair.create_from_uri('//Alice').public_key
    observer = SimpleNamespace(observe=AsyncMock(return_value=_make_observation(matched_lovelace=4)),
                               kupo_coverage_gap=AsyncMock(return_value=None), kupo_url="http://kupo.test")
    watcher = sw.SlashWatcher(config=SimpleNamespace(), substrate_client=client,
                              chain_write_lock=asyncio.Lock(), observer=observer,
                              aggregator=aggregator, min_signer_threshold=2 if aggregator else 1)
    request = _default_pending_request(requester=Keypair.create_from_uri('//Alice').ss58_address)
    return watcher, client, observer, request


def fixture(protocol):
    return json.loads((FIXTURES / f'{protocol}_metadata.json').read_text())['v14']


def intent_settlement(v14):
    return next(p for p in v14['pallets'] if p['name'] == 'IntentSettlement')


def registry_type(v14, type_id):
    return next(entry['type'] for entry in v14['types']['types'] if entry['id'] == type_id)


def named_type(v14, name):
    return next(entry for entry in v14['types']['types'] if entry['type']['path'][-1:] == [name])


def call_variant(v14, name):
    variants = registry_type(v14, intent_settlement(v14)['calls']['ty'])['def']['variant']['variants']
    return next(v for v in variants if v['name'] == name)


def storage_entry(v14, name):
    return next(e for e in intent_settlement(v14)['storage']['entries'] if e['name'] == name)


def ttl_constant(v14):
    return next(c for c in intent_settlement(v14)['constants'] if c['name'] == 'SettlementRequestTtl')


def add_type(v14, definition):
    type_id = max(entry['id'] for entry in v14['types']['types']) + 1
    v14['types']['types'].append({'id': type_id, 'type': {'path': [], 'params': [], 'def': definition, 'docs': []}})
    return type_id


def primitive(v14, name):
    return add_type(v14, {'primitive': name})


def metadata(protocol='frau', ttl=10, v14=None):
    """Real trimmed V14 metadata; ``ttl=None`` keeps the captured value."""
    if v14 is None:
        v14 = fixture(protocol)
    if ttl is not None:
        ttl_constant(v14)['value'] = '0x' + ttl.to_bytes(4, 'little').hex()
    result = SimpleNamespace(value=("0x6d657461", {'V14': v14}))
    def pallet(name):
        assert name == 'IntentSettlement'
        return SimpleNamespace(constants=[
            SimpleNamespace(name=c['name'], constant_value=bytes.fromhex(c['value'][2:]))
            for c in intent_settlement(v14)['constants']])
    result.get_metadata_pallet = pallet
    return result


HEAD = '0x' + 'ab' * 32
CODE = '0x' + 'c0' * 32


def rpc_client(meta=None, spec=239, spec_name='materios', code=CODE):
    """A client whose node serves ``head`` (metadata, ``:code`` hash) at one head."""
    client = object.__new__(SubstrateClient)
    client._read = object()
    client._settlement_contract = None
    client._settlement_reported = None
    client.head = SimpleNamespace(meta=meta or metadata(), code=code)
    def call(method, *args, **kwargs):
        assert kwargs['conn'] is client._read
        if method == 'get_chain_head':
            return HEAD
        if method == 'rpc_request':
            assert args == ('state_getStorageHash', ['0x3a636f6465', HEAD]), 'the code hash must be read at the head'
            return {'jsonrpc': '2.0', 'result': client.head.code}
        assert args == (HEAD,), 'all reads must use the same head'
        if method == 'get_block_runtime_version':
            return {'specName': spec_name, 'specVersion': spec}
        if method == 'get_block_metadata':
            return client.head.meta
        if method == 'get_block_number':
            return 101
        raise AssertionError(method)
    client._call = MagicMock(side_effect=call)
    return client


def methods(client):
    return [c.args[0] for c in client._call.call_args_list]


@pytest.mark.parametrize('protocol', ['frau', 'frv2'])
def test_runtime_detects_exact_metadata_at_head(protocol):
    client = rpc_client(metadata(protocol))
    result = client.get_settlement_runtime_context()
    assert result == context(protocol.upper()) | {'block_hash': HEAD, 'fingerprint': FINGERPRINTS[protocol]}
    assert methods(client) == ['get_chain_head', 'get_block_runtime_version', 'get_block_number',
                               'rpc_request', 'get_block_metadata']


def test_one_runtime_code_is_parsed_once_and_an_upgrade_is_parsed_again():
    client = rpc_client(metadata('frau'))
    assert client.get_settlement_runtime_context()['protocol'] == 'FRAU'
    client._call.reset_mock()
    assert client.get_settlement_runtime_context()['protocol'] == 'FRAU'
    assert 'get_block_metadata' not in methods(client)
    client.head = SimpleNamespace(meta=metadata('frv2'), code='0x' + 'c1' * 32)
    upgraded = client.get_settlement_runtime_context()
    assert (upgraded['protocol'], upgraded['supersede']) == ('FRV2', True)
    assert methods(client).count('get_block_metadata') == 1


@pytest.mark.parametrize('code', [None, ''])
def test_without_a_code_hash_every_read_parses_metadata(code):
    client = rpc_client(metadata('frv2'), code=code)
    for _ in range(2):
        assert client.get_settlement_runtime_context()['protocol'] == 'FRV2'
    assert methods(client).count('get_block_metadata') == 2


def test_a_refused_runtime_is_not_remembered():
    v14 = fixture('frv2')
    set_constant(v14, 'SlashSettlementTag', b'FRV3')
    client = rpc_client(metadata(v14=v14))
    for _ in range(2):
        with pytest.raises(ValueError, match='settlement runtime'):
            client.get_settlement_runtime_context()
    assert methods(client).count('get_block_metadata') == 2


def test_the_spec_name_is_checked_on_every_read():
    client = rpc_client(metadata('frau'))
    client.get_settlement_runtime_context()
    client._call.side_effect = (lambda call: lambda method, *a, **kw: (
        {'specName': 'other', 'specVersion': 239} if method == 'get_block_runtime_version' else call(method, *a, **kw)
    ))(client._call.side_effect)
    with pytest.raises(ValueError, match='settlement runtime'):
        client.get_settlement_runtime_context()


@pytest.mark.parametrize('protocol', ['frau', 'frv2'])
def test_captured_runtimes_select_protocol_and_ttl(protocol):
    context = rpc_client(metadata(protocol, ttl=None)).get_settlement_runtime_context()
    assert (context['protocol'], context['ttl']) == (protocol.upper(), 2400)


def constant(v14, name):
    return next((c for c in intent_settlement(v14)['constants'] if c['name'] == name), None)


def set_constant(v14, name, value):
    """Replace ``name`` with a ``[u8; len(value)]`` constant, or drop it when ``value`` is None."""
    pallet = intent_settlement(v14)
    pallet['constants'] = [c for c in pallet['constants'] if c['name'] != name]
    if value is not None:
        array = add_type(v14, {'array': {'len': len(value), 'type': primitive(v14, 'u8')}})
        pallet['constants'].append({'name': name, 'type': array, 'value': '0x' + value.hex(), 'documentation': []})


def test_captured_runtimes_carry_the_reviewed_digest_tags():
    frau, frv2 = fixture('frau'), fixture('frv2')
    assert constant(frau, 'SlashSettlementTag') is None
    assert constant(frau, 'AttestSettleTag') is None
    assert bytes.fromhex(constant(frv2, 'SlashSettlementTag')['value'][2:]) == b'FRV2'
    assert bytes.fromhex(constant(frv2, 'AttestSettleTag')['value'][2:]) == b'STCA'


def test_slash_tag_and_frv2_pin_select_frv2():
    assert rpc_client(metadata('frv2')).get_settlement_runtime_context()['protocol'] == 'FRV2'


@pytest.mark.parametrize('tag', [b'FRAU', b'FRV3', b'frv2', b'\x00' * 4, b'FRV', b'FRV2\x00'])
def test_any_other_slash_tag_refuses_and_names_it(tag, caplog):
    v14 = fixture('frv2')
    set_constant(v14, 'SlashSettlementTag', tag)
    with pytest.raises(ValueError, match='settlement runtime'):
        rpc_client(metadata(v14=v14)).get_settlement_runtime_context()
    assert f'SlashSettlementTag 0x{tag.hex()}' in caplog.text


def test_tagless_runtime_signs_only_on_the_frau_pin():
    assert rpc_client(metadata('frau')).get_settlement_runtime_context()['protocol'] == 'FRAU'
    v14 = fixture('frv2')
    set_constant(v14, 'SlashSettlementTag', None)
    assert sc.SLASH_PROTOCOLS[sc.slash_contract_fingerprint(v14)] == 'FRV2'
    with pytest.raises(ValueError, match='settlement runtime'):
        rpc_client(metadata(v14=v14)).get_settlement_runtime_context()


def test_frv2_tag_on_the_frau_contract_refuses():
    frau_contract = fixture('frv2')
    intent_settlement(frau_contract)['storage']['entries'].remove(storage_entry(frau_contract, 'SlashedSettlementRequests'))
    tagged_frau = fixture('frau')
    set_constant(tagged_frau, 'SlashSettlementTag', b'FRV2')
    for v14 in (frau_contract, tagged_frau):
        assert sc.SLASH_PROTOCOLS[sc.slash_contract_fingerprint(v14)] == 'FRAU'
        with pytest.raises(ValueError, match='settlement runtime'):
            rpc_client(metadata(v14=v14)).get_settlement_runtime_context()


def test_supersede_is_offered_only_by_the_runtime_that_has_it():
    old = rpc_client(metadata('frau')).get_settlement_runtime_context()
    new = rpc_client(metadata('frv2')).get_settlement_runtime_context()
    assert (old['supersede'], old['min_finality_depth']) == (False, 15)
    assert (new['supersede'], new['min_finality_depth']) == (True, 15)


@pytest.mark.parametrize('protocol', ['frau', 'frv2'])
def test_min_finality_depth_is_read_from_the_head(protocol):
    v14 = fixture(protocol)
    constant(v14, 'MinFinalityDepth')['value'] = '0x' + (40).to_bytes(4, 'little').hex()
    assert rpc_client(metadata(v14=v14)).get_settlement_runtime_context()['min_finality_depth'] == 40


def rename_supersede_arg(v14):
    call_variant(v14, 'supersede_settlement_request')['fields'][2]['name'] = 'attestation_evidence'


@pytest.mark.parametrize('edit', [
    lambda v14: set_constant(v14, 'AttestSettleTag', None),
    lambda v14: set_constant(v14, 'AttestSettleTag', b'STCB'),
    rename_supersede_arg,
    lambda v14: set_constant(v14, 'MinFinalityDepth', b'\x0f\x00'),
])
def test_unreviewed_supersede_contract_refuses(edit):
    v14 = fixture('frv2')
    edit(v14)
    with pytest.raises(ValueError, match='settlement runtime'):
        rpc_client(metadata(v14=v14)).get_settlement_runtime_context()


def test_request_scoped_slashes_are_the_only_contract_difference():
    frau, frv2 = fixture('frau'), fixture('frv2')
    assert sc.slash_contract_fingerprint(frau) != sc.slash_contract_fingerprint(frv2)
    entries = intent_settlement(frv2)['storage']['entries']
    entries.remove(storage_entry(frv2, 'SlashedSettlementRequests'))
    assert sc.slash_contract_fingerprint(frv2) == sc.slash_contract_fingerprint(frau)


def rewrite_docs(node, docs):
    if isinstance(node, dict):
        for key, value in node.items():
            if key in ('docs', 'documentation'):
                node[key] = list(docs)
            else:
                rewrite_docs(value, docs)
    elif isinstance(node, list):
        for item in node:
            rewrite_docs(item, docs)


@pytest.mark.parametrize('protocol', ['frau', 'frv2'])
@pytest.mark.parametrize('docs', [[], [' '], ['canonical FRV3 digest: blake2_256("FRV3" || chain_id)']])
def test_docs_at_every_level_do_not_change_protocol(protocol, docs):
    v14 = fixture(protocol)
    assert call_variant(v14, 'slash_bad_settlement_evidence')['docs']
    assert ttl_constant(v14)['documentation']
    rewrite_docs(v14, docs)
    assert rpc_client(metadata(v14=v14)).get_settlement_runtime_context()['protocol'] == protocol.upper()
    assert sc.slash_contract_fingerprint(v14) == sc.slash_contract_fingerprint(fixture(protocol))


def test_round_one_settlement_docs_keep_frv2():
    v14 = fixture('frv2')
    call = call_variant(v14, 'slash_bad_settlement_evidence')
    round_one = json.loads((FIXTURES / 'frv2_74cd340_docs.json').read_text())
    assert call['docs'] != round_one
    call['docs'] = round_one
    assert rpc_client(metadata(v14=v14)).get_settlement_runtime_context()['protocol'] == 'FRV2'


def type_fields(kind, body):
    if kind == 'composite':
        return body['fields']
    if kind == 'variant':
        return [f for v in body['variants'] for f in v['fields']]
    return []


def renumber(v14, shift):
    for entry in v14['types']['types']:
        entry['id'] += shift
        (kind, body), = entry['type']['def'].items()
        if kind == 'tuple':
            entry['type']['def'][kind] = [item + shift for item in body]
        elif kind in ('sequence', 'compact', 'array'):
            body['type'] += shift
        for field in type_fields(kind, body):
            field['type'] += shift
    pallet = intent_settlement(v14)
    pallet['calls']['ty'] += shift
    for entry in pallet['storage']['entries']:
        entry['type']['Map']['key'] += shift
        entry['type']['Map']['value'] += shift
    for constant in pallet['constants']:
        constant['type'] += shift
    v14['types']['types'].reverse()


@pytest.mark.parametrize('protocol', ['frau', 'frv2'])
def test_type_registry_positions_do_not_change_protocol(protocol):
    v14 = fixture(protocol)
    renumber(v14, 1000)
    assert rpc_client(metadata(v14=v14)).get_settlement_runtime_context()['protocol'] == protocol.upper()
    assert sc.slash_contract_fingerprint(v14) == sc.slash_contract_fingerprint(fixture(protocol))


def rename_paths(v14):
    for entry in v14['types']['types']:
        entry['type']['path'] = ['renamed'] + entry['type']['path']
        entry['type']['params'] = [{'name': 'T', 'type': None}]
        (kind, body), = entry['type']['def'].items()
        for field in type_fields(kind, body):
            field['typeName'] = 'Renamed'


UNRELATED_EDITS = {
    'new_call': lambda v14: registry_type(v14, intent_settlement(v14)['calls']['ty'])['def']['variant']['variants'].append(
        {'name': 'new_call', 'fields': [], 'index': 99, 'docs': []}),
    'request_settle_args': lambda v14: call_variant(v14, 'request_settle')['fields'].append(
        {'name': 'extra', 'type': primitive(v14, 'u8'), 'typeName': 'u8', 'docs': []}),
    'new_storage': lambda v14: intent_settlement(v14)['storage']['entries'].append(
        {'name': 'Other', 'modifier': 'Optional', 'type': {'Map': {'hashers': ['Identity'], 'key': primitive(v14, 'u8'), 'value': primitive(v14, 'u8')}},
         'default': '0x00', 'documentation': []}),
    'legacy_tombstone_removed': lambda v14: intent_settlement(v14)['storage']['entries'].remove(
        storage_entry(v14, 'BondSlashedRequests')),
    'ttl_value': lambda v14: ttl_constant(v14).update(value='0x01000000'),
    'other_constant_type': lambda v14: next(
        c for c in intent_settlement(v14)['constants'] if c['name'] == 'SlashWatcherShareBps').update(type=primitive(v14, 'u64')),
    'pallet_index': lambda v14: intent_settlement(v14).update(index=intent_settlement(v14)['index'] + 1),
    'storage_hashers': lambda v14: storage_entry(v14, 'ClaimSettlementRequests')['type']['Map'].update(hashers=['Twox64Concat']),
    'type_paths': rename_paths,
}


@pytest.mark.parametrize('protocol', ['frau', 'frv2'])
@pytest.mark.parametrize('edit', UNRELATED_EDITS)
def test_unrelated_metadata_changes_do_not_change_protocol(protocol, edit):
    v14 = fixture(protocol)
    UNRELATED_EDITS[edit](v14)
    assert rpc_client(metadata(v14=v14)).get_settlement_runtime_context()['protocol'] == protocol.upper()
    assert sc.slash_contract_fingerprint(v14) == sc.slash_contract_fingerprint(fixture(protocol))


def slash_field(v14, name):
    return next(f for f in call_variant(v14, 'slash_bad_settlement_evidence')['fields'] if f['name'] == name)


def struct_field(v14, struct, name):
    fields = named_type(v14, struct)['type']['def']['composite']['fields']
    return next(f for f in fields if f['name'] == name)


def proof_variant(v14, name):
    return next(v for v in named_type(v14, 'FraudProof')['type']['def']['variant']['variants'] if v['name'] == name)


def widen_signatures(v14):
    pair = registry_type(v14, registry_type(v14, slash_field(v14, 'signatures')['type'])['def']['sequence']['type'])
    pair['def']['tuple'] = [pair['def']['tuple'][0], add_type(v14, {'array': {'len': 65, 'type': primitive(v14, 'u8')}})]


def swap_record_fields(v14):
    fields = named_type(v14, 'SettlementRequestRecord')['type']['def']['composite']['fields']
    fields[0], fields[1] = fields[1], fields[0]


CONTRACT_EDITS = {
    'call_index': lambda v14: call_variant(v14, 'slash_bad_settlement_evidence').update(index=21),
    'arg_renamed': lambda v14: slash_field(v14, 'claim_id').update(name='claim'),
    'arg_added': lambda v14: call_variant(v14, 'slash_bad_settlement_evidence')['fields'].append(
        {'name': 'request_id', 'type': primitive(v14, 'u8'), 'typeName': 'u8', 'docs': []}),
    'arg_type': lambda v14: slash_field(v14, 'fraud_proof').update(type=primitive(v14, 'u8')),
    'proof_field_width': lambda v14: proof_variant(v14, 'WrongAmount')['fields'][0].update(type=primitive(v14, 'u64')),
    'proof_variant_index': lambda v14: proof_variant(v14, 'TxNotFound').update(index=7),
    'signature_width': widen_signatures,
    'record_block_width': lambda v14: struct_field(v14, 'SettlementRequestRecord', 'submitted_block').update(type=primitive(v14, 'u64')),
    'record_field_order': swap_record_fields,
    'evidence_field_width': lambda v14: struct_field(v14, 'SettlementEvidence', 'amount_lovelace').update(type=primitive(v14, 'u128')),
    'request_key': lambda v14: storage_entry(v14, 'ClaimSettlementRequests')['type']['Map'].update(key=primitive(v14, 'u64')),
    'ttl_type': lambda v14: ttl_constant(v14).update(type=primitive(v14, 'u64')),
}


@pytest.mark.parametrize('protocol', ['frau', 'frv2'])
@pytest.mark.parametrize('edit', CONTRACT_EDITS)
def test_signing_contract_changes_refuse(protocol, edit):
    v14 = fixture(protocol)
    CONTRACT_EDITS[edit](v14)
    with pytest.raises(ValueError, match='settlement runtime'):
        rpc_client(metadata(v14=v14)).get_settlement_runtime_context()
    changed = sc.slash_contract_fingerprint(v14)
    assert changed != sc.slash_contract_fingerprint(fixture(protocol))
    assert changed not in sc.SLASH_PROTOCOLS


def test_request_slash_key_change_refuses():
    v14 = fixture('frv2')
    layout = storage_entry(v14, 'SlashedSettlementRequests')['type']['Map']
    claim_key = registry_type(v14, layout['key'])['def']['tuple'][0]
    narrow = add_type(v14, {'array': {'len': 28, 'type': primitive(v14, 'u8')}})
    layout['key'] = add_type(v14, {'tuple': [claim_key, narrow]})
    with pytest.raises(ValueError, match='settlement runtime'):
        rpc_client(metadata(v14=v14)).get_settlement_runtime_context()


def test_wrong_chain_spec_refuses():
    with pytest.raises(ValueError, match='settlement runtime'):
        rpc_client(spec_name='other').get_settlement_runtime_context()


def make_recursive(v14):
    proof = named_type(v14, 'FraudProof')
    proof_variant(v14, 'WrongAmount')['fields'][0]['type'] = proof['id']


def make_bitsequence(v14):
    u8 = primitive(v14, 'u8')
    slash_field(v14, 'fraud_proof')['type'] = add_type(v14, {'bitsequence': {'bit_store_type': u8, 'bit_order_type': u8}})


@pytest.mark.parametrize('edit,error', [(make_recursive, 'recursive'), (make_bitsequence, 'unsupported bitsequence')])
def test_types_without_canonical_form_have_no_fingerprint(edit, error):
    v14 = fixture('frv2')
    edit(v14)
    with pytest.raises(ValueError, match=error):
        sc.slash_contract_fingerprint(v14)


@pytest.mark.parametrize('damage', [
    'no_call', 'no_pallet', 'no_request_storage', 'plain_request_storage', 'dangling_type',
    'recursive_type', 'bitsequence_type', 'ttl_missing', 'ttl_short', 'metadata_version',
])
def test_unknown_metadata_refuses(damage):
    meta = metadata()
    v14 = meta.value[1]['V14']
    if damage == 'no_call':
        variants = registry_type(v14, intent_settlement(v14)['calls']['ty'])['def']['variant']['variants']
        variants.remove(call_variant(v14, 'slash_bad_settlement_evidence'))
    elif damage == 'no_pallet':
        v14['pallets'] = []
    elif damage == 'no_request_storage':
        intent_settlement(v14)['storage']['entries'].remove(storage_entry(v14, 'ClaimSettlementRequests'))
    elif damage == 'plain_request_storage':
        storage_entry(v14, 'ClaimSettlementRequests')['type'] = {'Plain': primitive(v14, 'u8')}
    elif damage == 'dangling_type':
        slash_field(v14, 'fraud_proof')['type'] = 10**6
    elif damage == 'recursive_type':
        make_recursive(v14)
    elif damage == 'bitsequence_type':
        make_bitsequence(v14)
    elif damage == 'ttl_missing':
        intent_settlement(v14)['constants'].remove(ttl_constant(v14))
    elif damage == 'ttl_short':
        ttl_constant(v14)['value'] = '0x'
    else:
        meta.value[1]['V99'] = meta.value[1].pop('V14')
    with pytest.raises(ValueError, match='settlement runtime'):
        rpc_client(meta).get_settlement_runtime_context()


@pytest.mark.parametrize('error', [ValueError('unknown settlement runtime'), RuntimeError(''), TimeoutError()])
def test_unavailable_context_does_not_sign_and_alerts(error, caplog):
    watcher, client, observer, req = setup_watcher()
    client.get_settlement_runtime_context.side_effect = error
    verdict = asyncio.run(watcher.process_one(req, CHAIN_ID))
    assert verdict.detail == 'runtime_unavailable'
    client.keypair.sign.assert_not_called()
    observer.observe.assert_not_called()
    client.submit_slash_bad_settlement_evidence.assert_not_called()
    assert 'settlement runtime unavailable' in caplog.text


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
            client.get_settlement_runtime_context.side_effect = ValueError('')
        else:
            client.get_settlement_runtime_context.return_value = context('FRV2' if change == 'upgrade' else 'FRAU', 111 if change == 'expired' else 102)
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
    asyncio.run(_with_gateway(run, committee=frozenset(
        {Keypair.create_from_uri('//Alice').public_key, Keypair.create_from_uri('//Bob').public_key})))


@pytest.mark.parametrize('field,value', [
    ('protocol', 'FRV3'), ('protocol', ''), ('protocol', None),
    ('ttl', None), ('ttl', ''), ('ttl', True), ('ttl', -1), ('ttl', 2**32),
    ('block_number', ''), ('block_number', -1), ('spec_version', None),
    ('supersede', None), ('supersede', 1), ('min_finality_depth', None), ('min_finality_depth', 2**32),
])
def test_invalid_context_refuses_before_signing(field, value, caplog):
    ctx = context()
    ctx[field] = value
    watcher, client, observer, req = setup_watcher(ctx)
    verdict = asyncio.run(watcher.process_one(req, CHAIN_ID))
    assert verdict.detail == 'runtime_unavailable'
    client.keypair.sign.assert_not_called()
    client.submit_slash_bad_settlement_evidence.assert_not_called()
    assert 'settlement runtime unavailable' in caplog.text


def test_unknown_runtime_stops_entire_tick(caplog):
    watcher, client, observer, req = setup_watcher()
    client.get_settlement_runtime_context.side_effect = ValueError('')
    client.list_pending_settlement_requests.return_value = [asdict(req)]
    watcher.process_one = AsyncMock()
    asyncio.run(watcher._tick(CHAIN_ID))
    watcher.process_one.assert_not_called()
    assert 'settlement runtime unavailable' in caplog.text


def test_zero_ttl_is_read_from_chain_and_only_same_block_is_valid():
    assert rpc_client(metadata(ttl=0)).get_settlement_runtime_context()['ttl'] == 0
    watcher, client, _, req = setup_watcher(context(block=101, ttl=0))
    assert asyncio.run(watcher.process_one(req, CHAIN_ID)).detail == 'request_expired'
    client.submit_slash_bad_settlement_evidence.assert_not_called()
    client.get_settlement_runtime_context.return_value = context(block=100, ttl=0)
    assert asyncio.run(watcher.process_one(req, CHAIN_ID)).extrinsic_hash


# --- what an operator sees -----------------------------------------------------

from daemon import health_server  # noqa: E402

FINGERPRINTS = {"frau": "96dfe9b41a5471b578e065d9e3055a359e27e9a6576f94b1f932bd668219e85c",
                "frv2": "693ba3e8bc9106fb036f96bc73b3c7797d314c36ef95c0564c715ce58113eeb7"}


def detections(caplog):
    return [r.getMessage() for r in caplog.records if "settlement runtime" in r.getMessage()]


@pytest.mark.parametrize("protocol, supersede, gauge", [("frau", False, 1), ("frv2", True, 2)])
def test_each_runtime_is_announced_once_with_what_it_selects(protocol, supersede, gauge, caplog):
    caplog.set_level("INFO")
    client = rpc_client(metadata(protocol), spec=242 if supersede else 239)
    for _ in range(3):
        client.get_settlement_runtime_context()
    [line] = detections(caplog)
    assert f"protocol={protocol.upper()}" in line and f"supersede={supersede}" in line
    assert f"fingerprint={FINGERPRINTS[protocol]}" in line and f"spec={242 if supersede else 239}" in line
    assert "ttl=10" in line and "min_finality_depth=15" in line
    assert health_server._metrics["settlement_slash_protocol"] == gauge
    assert health_server._metrics["settlement_supersede_offered"] == int(supersede)
    assert health_server._metrics["settlement_runtime_spec_version"] == (242 if supersede else 239)


def test_an_upgrade_is_announced_and_a_refusal_is_reported_once_per_runtime(caplog):
    caplog.set_level("INFO")
    client = rpc_client(metadata('frau'))
    client.get_settlement_runtime_context()
    v14 = fixture('frv2')
    set_constant(v14, 'SlashSettlementTag', b'FRV3')
    client.head = SimpleNamespace(meta=metadata(v14=v14), code='0x' + 'c1' * 32)
    for _ in range(3):
        with pytest.raises(ValueError, match='settlement runtime'):
            client.get_settlement_runtime_context()
    refusals = [r for r in caplog.records if r.levelname == "ERROR"]
    assert len(refusals) == 1 and "0x46525633" in refusals[0].getMessage()
    assert health_server._metrics["settlement_slash_protocol"] == 0
    assert health_server._metrics["settlement_supersede_offered"] == 0
    client.head = SimpleNamespace(meta=metadata('frv2'), code='0x' + 'c2' * 32)
    client.get_settlement_runtime_context()
    assert "protocol=FRV2" in detections(caplog)[-1]
    assert health_server._metrics["settlement_slash_protocol"] == 2


def test_the_watcher_reports_runtime_and_kupo_coverage_on_change_even_when_idle(caplog):
    caplog.set_level("INFO")
    watcher, client, observer, _ = setup_watcher(context('FRV2'))
    observer.kupo_coverage_gap = AsyncMock(side_effect=["patterns_without_*:4", "patterns_without_*:4", None])
    for _ in range(3):
        asyncio.run(watcher._report_status())
    assert client.get_settlement_runtime_context.call_count == 3
    coverage = [r for r in caplog.records if "Kupo" in r.getMessage()]
    assert [r.levelname for r in coverage] == ["WARNING", "INFO"]
    assert "patterns_without_*:4" in coverage[0].getMessage()
    assert health_server._metrics["slash_kupo_coverage_ok"] == 1


def test_a_failing_status_probe_is_logged_and_survived(caplog):
    watcher, client, observer, _ = setup_watcher()
    client.get_settlement_runtime_context.side_effect = ValueError('')
    observer.kupo_coverage_gap = AsyncMock(side_effect=OSError('kupo down'))
    for _ in range(2):
        asyncio.run(watcher._report_status())
    assert health_server._metrics["slash_kupo_coverage_ok"] == 0
    assert sum("settlement runtime unavailable" in r.getMessage() for r in caplog.records) == 1


def test_every_poll_reports_status_before_looking_at_requests(monkeypatch):
    watcher, client, _, _ = setup_watcher()
    order = []
    watcher._report_status = AsyncMock(side_effect=lambda: order.append("status"))
    watcher._tick = AsyncMock(side_effect=lambda chain_id: order.append("tick"))
    client.get_genesis_hash = MagicMock(return_value="0x" + "11" * 32)

    async def stop_after_one_poll(seconds):
        watcher._running = False
    monkeypatch.setattr(sw.asyncio, "sleep", stop_after_one_poll)
    watcher._running = True
    asyncio.run(watcher._run_forever())
    assert order == ["status", "tick"]
