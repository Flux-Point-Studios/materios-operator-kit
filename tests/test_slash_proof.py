"""What the slash watcher must see on Cardano before it signs a fraud proof.

The pallet checks only that a proof is internally consistent; TxNotFound
always is. Whatever the watchers sign is what slashes, so every proof here
runs the real ``CardanoSlashObserver`` against a scripted Ogmios and Kupo.
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from substrateinterface import Keypair

from daemon import slash_watcher as sw
from daemon.settle_claim_attestor import CARDANO_PREPROD_GENESIS_HASH
from tests.test_slash_watcher import (
    CHAIN_ID, OBSERVED_SLOT, _default_pending_request, _make_watcher,
)

ALICE = Keypair.create_from_uri("//Alice")
TIP_SLOT = OBSERVED_SLOT + 10_000
TIP_HEIGHT = 1_000_000


def context(protocol="FRV2", block=101, ttl=2400):
    return dict(protocol=protocol, block_number=block, ttl=ttl, spec_version=242,
                block_hash="0x" + "ab" * 32, supersede=protocol == "FRV2", min_finality_depth=15)


def ogmios(method, params=None):
    return {
        "queryLedgerState/tip": {"slot": TIP_SLOT, "id": "ff" * 32},
        "queryNetwork/tip": {"slot": TIP_SLOT, "id": "ff" * 32},
        "queryNetwork/blockHeight": TIP_HEIGHT,
        "queryNetwork/genesisConfiguration": {"era": "shelley", "networkMagic": 1, "network": "testnet"},
    }[method]


def observer(matches):
    obs = sw.CardanoSlashObserver(ogmios_url="http://ogmios.test", kupo_url="http://kupo.test")
    obs._ogmios_rpc = AsyncMock(side_effect=lambda session, method, params=None: ogmios(method, params))
    obs._kupo_get = AsyncMock(return_value=matches)
    obs.kupo_checkpoint_slot = AsyncMock(return_value=TIP_SLOT)
    return obs


def watcher(obs):
    client = SimpleNamespace(
        keypair=MagicMock(wraps=ALICE, public_key=ALICE.public_key),
        get_settlement_runtime_context=MagicMock(return_value=context()),
        submit_slash_bad_settlement_evidence=MagicMock(return_value="0x" + "ee" * 32),
        list_pending_settlement_requests=MagicMock(return_value=[]),
    )
    return _make_watcher(substrate_client=client, observer=obs), client


def bonded_request(**overrides):
    return _default_pending_request(requester=ALICE.ss58_address,
                                    mainchain_genesis_hash=CARDANO_PREPROD_GENESIS_HASH, **overrides)


def test_a_failed_kupo_lookup_is_never_signed_as_tx_not_found():
    obs = observer(None)
    slash_watcher, client = watcher(obs)
    verdict = asyncio.run(slash_watcher.process_one(bonded_request(), CHAIN_ID))
    assert verdict.fraud_proof is None
    assert "kupo_matches_unavailable" in (verdict.detail or "")
    client.keypair.sign.assert_not_called()
    client.submit_slash_bad_settlement_evidence.assert_not_called()
