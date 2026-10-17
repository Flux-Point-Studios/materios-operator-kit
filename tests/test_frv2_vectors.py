import hashlib
import json
from dataclasses import replace
from pathlib import Path

import pytest
from scalecodec.utils.ss58 import ss58_encode

from daemon import slash_watcher as sw

VECTORS = json.loads((Path(__file__).parent / 'fixtures/frv2/vectors.json').read_text())


def request(vector):
    data = {key: vector[key] for key in (
        'claim_id', 'requester', 'submitted_block', 'settled_direct', 'cardano_tx_hash',
        'observed_at_depth', 'observed_slot', 'beneficiary_addr_hash',
        'amount_lovelace', 'mainchain_genesis_hash',
    )}
    for key in ('claim_id', 'cardano_tx_hash', 'beneficiary_addr_hash', 'mainchain_genesis_hash'):
        data[key] = bytes.fromhex(data[key])
    data['requester'] = ss58_encode(data['requester'], ss58_format=42)
    return sw.PendingBondedRequest(**data, voucher_digest=b'v' * 32, bond_amount=10_000_000)


def proof(vector):
    raw = bytes.fromhex(vector['proof_scale'])
    if raw[0] == 0:
        return sw.WrongAmount(int.from_bytes(raw[1:], 'little'))
    if raw[0] == 1:
        return sw.TxNotFound()
    return sw.WrongBeneficiary(raw[1:])


@pytest.mark.parametrize('vector', VECTORS)
def test_rust_pallet_vectors(vector):
    req = request(vector)
    identity = sw.encode_request_identity(req)
    assert identity.hex() == vector['request_preimage']
    assert hashlib.blake2b(identity, digest_size=32).hexdigest() == vector['request_id']
    preimage = sw.build_frv2_preimage(bytes.fromhex(vector['chain_id']), req, proof(vector))
    assert sw.encode_fraud_proof(proof(vector)).hex() == vector['proof_scale']
    assert sw.compute_frau_digest(preimage).hex() == vector['digest']
    assert len(identity) == 153
    assert sw.encode_request_identity(replace(req, requester='0x' + vector['requester'])) == identity


def test_u32_and_u64_submitted_blocks_normalize_to_same_identity():
    assert VECTORS[0]['block_scale_width'] == 4
    assert VECTORS[1]['block_scale_width'] == 8
    assert VECTORS[0]['request_id'] == VECTORS[1]['request_id']
    assert sw.encode_request_identity(request(VECTORS[0])) == sw.encode_request_identity(request(VECTORS[1]))


@pytest.mark.parametrize('field,value', [
    ('requester', '0x' + '99' * 32), ('submitted_block', 101), ('settled_direct', True),
    ('cardano_tx_hash', b'x' * 32), ('observed_at_depth', 7), ('observed_slot', 7),
    ('beneficiary_addr_hash', b'y' * 28), ('amount_lovelace', 7), ('mainchain_genesis_hash', b'z' * 32),
])
def test_each_identity_field_changes_digest(field, value):
    req = request(VECTORS[0])
    digest = sw.compute_frau_digest(sw.build_frv2_preimage(b'c' * 32, req, sw.TxNotFound()))
    changed = replace(req, **{field: value})
    assert sw.compute_frau_digest(sw.build_frv2_preimage(b'c' * 32, changed, sw.TxNotFound())) != digest


def test_bond_and_voucher_are_not_part_of_request_identity():
    req = request(VECTORS[0])
    assert sw.encode_request_identity(replace(req, bond_amount=0, voucher_digest=b'q' * 32)) == sw.encode_request_identity(req)


@pytest.mark.parametrize('field,value', [
    ('requester', ''), ('requester', '0x12'), ('submitted_block', -1),
    ('submitted_block', 2**64), ('submitted_block', True), ('observed_at_depth', 2**32),
    ('observed_slot', -1), ('amount_lovelace', 2**64), ('settled_direct', 1),
    ('cardano_tx_hash', b''), ('beneficiary_addr_hash', b'a' * 32),
    ('mainchain_genesis_hash', 'a' * 32),
])
def test_invalid_identity_refuses(field, value):
    with pytest.raises(ValueError):
        sw.encode_request_identity(replace(request(VECTORS[0]), **{field: value}))
