"""What the slash watcher must see on Cardano before it signs a fraud proof.

The pallet checks only that a proof is internally consistent; TxNotFound
always is. Whatever the watchers sign is what slashes, so every proof here
runs the real ``CardanoSlashObserver`` against a scripted Ogmios and Kupo.
"""
from __future__ import annotations

import asyncio
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from substrateinterface import Keypair

from daemon import health_server
from daemon import slash_watcher as sw
from daemon.settle_claim_attestor import CARDANO_PREPROD_GENESIS_HASH, CARDANO_PREVIEW_GENESIS_HASH
from tests.test_slash_watcher import (
    AMOUNT_LOVELACE, BENE_HASH_28, CARDANO_TX_HASH, CHAIN_ID, OBSERVED_SLOT, OTHER_HASH_28,
    _default_pending_request, _make_kupo_match, _make_testnet_addr_with_payment_hash, _make_watcher,
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
    return _default_pending_request(**{"requester": ALICE.ss58_address,
                                       "mainchain_genesis_hash": CARDANO_PREPROD_GENESIS_HASH, **overrides})


def test_a_failed_kupo_lookup_is_never_signed_as_tx_not_found():
    obs = observer(None)
    slash_watcher, client = watcher(obs)
    verdict = asyncio.run(slash_watcher.process_one(bonded_request(), CHAIN_ID))
    assert verdict.fraud_proof is None
    assert "kupo_matches_unavailable" in (verdict.detail or "")
    client.keypair.sign.assert_not_called()
    client.submit_slash_bad_settlement_evidence.assert_not_called()


# --- what proves a transaction absent ----------------------------------------

CANARY_TX, CANARY_INDEX = sw.SPENT_CANARIES[CARDANO_PREPROD_GENESIS_HASH]
SPENT = {"slot_no": 10608917, "header_hash": "cd" * 32}
TX_LOOKUP = f"/matches/*@{CARDANO_TX_HASH.hex()}"
CANARY = [dict(transaction_id=CANARY_TX, output_index=CANARY_INDEX, spent_at=SPENT)]
# Ogmios queryNetwork/startTime and eraSummaries on preprod: 2022-06-01T00:00:00Z,
# 86400 Byron slots of 20 s, then 1 s slots.
PREPROD_CLOCK = (86400, 1654041600 + 86400 * 20)


class Cardano:
    """Ogmios and Kupo as the observer reads them, scripted per test."""

    def __init__(self, tx_rows=(), patterns=["*"], canary=CANARY, checkpoint=None, magic=1):
        self.tx_rows = list(tx_rows)
        self.base_rows = None
        self.patterns = patterns
        self.canary = canary
        self.tip_slot, self.tip_height = TIP_SLOT, TIP_HEIGHT
        # The network's current slot by the wall clock; a healthy node's tip
        # trails it by a block or so.
        self.wall_slot = TIP_SLOT + 20
        self.checkpoint = checkpoint
        self.magic = magic
        self.reads = []

    def ogmios(self, method):
        self.reads.append(method)
        return {
            "queryLedgerState/tip": {"slot": self.tip_slot, "id": "ff" * 32},
            "queryNetwork/tip": {"slot": self.tip_slot, "id": "ff" * 32},
            "queryNetwork/blockHeight": self.tip_height,
            "queryNetwork/genesisConfiguration": {"era": "shelley", "networkMagic": self.magic},
        }[method]

    async def kupo(self, session, path, params):
        assert params == {}, f"{path} must be an indexed lookup, not a scan: {params}"
        self.reads.append(path)
        if path == "/patterns":
            return self.patterns
        if path == f"/matches/{CANARY_INDEX}@{CANARY_TX}":
            return self.canary
        if path == TX_LOOKUP:
            rows = self.base_rows if self.base_rows is not None and self.reads.count(TX_LOOKUP) == 1 else self.tx_rows
            return rows
        raise AssertionError(f"unexpected Kupo read {path}")

    async def health(self):
        self.reads.append("/health")
        return self.tip_slot if self.checkpoint is None else self.checkpoint

    def advance(self, blocks, slots_per_block=20):
        self.tip_height += blocks
        self.tip_slot += slots_per_block * blocks
        self.wall_slot += slots_per_block * blocks
        self.reads.clear()


def scripted(cardano):
    obs = sw.CardanoSlashObserver(ogmios_url="http://ogmios.test", kupo_url="http://kupo.test")
    obs._ogmios_rpc = AsyncMock(side_effect=lambda session, method, params=None: cardano.ogmios(method))
    obs._kupo_get = AsyncMock(side_effect=cardano.kupo)
    obs.kupo_checkpoint_slot = AsyncMock(side_effect=cardano.health)
    obs.get_block_height_by_hash = AsyncMock(return_value=TIP_HEIGHT - 20)
    shelley_slot, shelley_unix = PREPROD_CLOCK
    obs.wall_clock = lambda: shelley_unix + cardano.wall_slot - shelley_slot + 0.5
    return obs


def run(slash_watcher, request):
    return asyncio.run(slash_watcher.process_one(request, CHAIN_ID))


def signed(client):
    return client.submit_slash_bad_settlement_evidence.call_count


def test_tx_not_found_needs_two_proofs_at_least_the_confirmation_depth_apart():
    cardano = Cardano()
    slash_watcher, client = watcher(scripted(cardano))
    request = bonded_request()
    first = run(slash_watcher, request)
    assert first.fraud_proof is None and "absence_unconfirmed" in first.detail
    cardano.advance(sw.TX_ABSENCE_CONFIRM_BLOCKS - 1)
    assert run(slash_watcher, request).fraud_proof is None
    client.keypair.sign.assert_not_called()
    cardano.advance(1)
    verdict = run(slash_watcher, request)
    assert isinstance(verdict.fraud_proof, sw.TxNotFound)
    assert signed(client) == 1
    digest = sw.compute_frau_digest(sw.build_frv2_preimage(CHAIN_ID, request, sw.TxNotFound()))
    assert client.keypair.sign.call_args.args == (digest,)


def test_kupo_is_proven_fresh_before_the_lookup_that_finds_nothing():
    cardano = Cardano()
    slash_watcher, _ = watcher(scripted(cardano))
    run(slash_watcher, bonded_request())
    proof_reads = cardano.reads[cardano.reads.index("queryNetwork/tip"):]
    assert proof_reads.index("/health") < proof_reads.index(TX_LOOKUP)
    assert proof_reads.index("/patterns") < proof_reads.index(TX_LOOKUP)


@pytest.mark.parametrize("lag", [sw.KUPO_TIP_LAG_MAX_SLOTS + 1, 5_000])
def test_a_kupo_behind_the_cardano_tip_proves_nothing_whatever_the_requested_slot(lag):
    # Past the requester's slot by far more than the old 20-slot margin.
    cardano = Cardano(checkpoint=TIP_SLOT - lag)
    assert cardano.checkpoint > OBSERVED_SLOT + 20
    slash_watcher, client = watcher(scripted(cardano))
    request = bonded_request()
    for _ in range(3):
        verdict = run(slash_watcher, request)
        assert "kupo_behind_tip" in verdict.detail
        cardano.advance(sw.TX_ABSENCE_CONFIRM_BLOCKS)
        cardano.checkpoint = cardano.tip_slot - lag
    client.keypair.sign.assert_not_called()


def test_a_kupo_at_the_lag_bound_is_fresh():
    cardano = Cardano(checkpoint=TIP_SLOT - sw.KUPO_TIP_LAG_MAX_SLOTS)
    slash_watcher, _ = watcher(scripted(cardano))
    assert "absence_unconfirmed" in run(slash_watcher, bonded_request()).detail


def paying(beneficiary_hash, coins, index=0):
    return _make_kupo_match(output_index=index, address=_make_testnet_addr_with_payment_hash(beneficiary_hash),
                            coins=coins)


FRAUDS = {
    "tx_not_found": [],
    "wrong_amount": [paying(BENE_HASH_28, AMOUNT_LOVELACE - 1)],
    "wrong_beneficiary": [paying(OTHER_HASH_28, AMOUNT_LOVELACE)],
}
GAPS = {
    "narrow_patterns": dict(patterns=["70cd1c6fbbbd7b1e855f589abd842f433ec0d7b46c7a9e437194e931.*"]),
    "no_patterns": dict(patterns=[]),
    "patterns_unreadable": dict(patterns=None),
    "pruned": dict(canary=[]),
    "canary_unspent": dict(canary=[dict(transaction_id=CANARY_TX, output_index=CANARY_INDEX, spent_at=None)]),
    "canary_unreadable": dict(canary=None),
    "canary_wrong_shape": dict(canary={"spent_at": SPENT}),
}


@pytest.mark.parametrize("fraud", FRAUDS)
@pytest.mark.parametrize("gap", GAPS)
def test_no_fraud_proof_from_a_kupo_that_misses_addresses_or_spent_outputs(fraud, gap):
    cardano = Cardano(tx_rows=FRAUDS[fraud], **GAPS[gap])
    slash_watcher, client = watcher(scripted(cardano))
    request = bonded_request()
    for _ in range(2):
        verdict = run(slash_watcher, request)
        assert verdict.fraud_proof is None
        assert "kupo_coverage" in verdict.detail
        cardano.advance(sw.TX_ABSENCE_CONFIRM_BLOCKS)
    client.keypair.sign.assert_not_called()


@pytest.mark.parametrize("fraud, proof", [
    ("wrong_amount", sw.WrongAmount(AMOUNT_LOVELACE - 1)),
    ("wrong_beneficiary", sw.WrongBeneficiary(OTHER_HASH_28)),
])
def test_a_present_tx_is_proven_wrong_from_a_kupo_that_indexes_everything(fraud, proof):
    cardano = Cardano(tx_rows=FRAUDS[fraud])
    slash_watcher, client = watcher(scripted(cardano))
    verdict = run(slash_watcher, bonded_request())
    assert verdict.fraud_proof == proof
    assert signed(client) == 1
    assert {"/patterns", f"/matches/{CANARY_INDEX}@{CANARY_TX}"} <= set(cardano.reads)


def test_a_network_without_a_spent_canary_cannot_prove_absence(monkeypatch):
    monkeypatch.setitem(sw.NETWORK_CLOCKS, CARDANO_PREVIEW_GENESIS_HASH, PREPROD_CLOCK)
    cardano = Cardano(magic=2)
    slash_watcher, client = watcher(scripted(cardano))
    request = bonded_request(mainchain_genesis_hash=CARDANO_PREVIEW_GENESIS_HASH)
    for _ in range(2):
        assert "no_spent_canary" in run(slash_watcher, request).detail
        cardano.advance(sw.TX_ABSENCE_CONFIRM_BLOCKS)
    client.keypair.sign.assert_not_called()


@pytest.mark.parametrize("fraud", FRAUDS)
def test_an_observer_on_another_cardano_network_proves_nothing(fraud):
    cardano = Cardano(tx_rows=FRAUDS[fraud])
    slash_watcher, client = watcher(scripted(cardano))
    request = bonded_request(mainchain_genesis_hash=CARDANO_PREVIEW_GENESIS_HASH)
    for _ in range(2):
        assert "cardano_network_mismatch" in run(slash_watcher, request).detail
        cardano.advance(sw.TX_ABSENCE_CONFIRM_BLOCKS)
    client.keypair.sign.assert_not_called()


def test_a_tx_that_appears_between_proofs_restarts_the_count():
    tx = [paying(BENE_HASH_28, AMOUNT_LOVELACE)]
    cardano = Cardano()
    slash_watcher, client = watcher(scripted(cardano))
    request = bonded_request()
    run(slash_watcher, request)
    cardano.advance(sw.TX_ABSENCE_CONFIRM_BLOCKS)
    # The tx lands between the observation and the proof's own lookup.
    cardano.base_rows, cardano.tx_rows = [], tx
    assert "tx_found" in run(slash_watcher, request).detail
    cardano.advance(sw.TX_ABSENCE_CONFIRM_BLOCKS)
    cardano.base_rows, cardano.tx_rows = None, []
    assert "absence_unconfirmed" in run(slash_watcher, request).detail
    client.keypair.sign.assert_not_called()


def test_a_replacement_request_does_not_inherit_an_absence():
    cardano = Cardano()
    slash_watcher, client = watcher(scripted(cardano))
    run(slash_watcher, bonded_request())
    cardano.advance(sw.TX_ABSENCE_CONFIRM_BLOCKS)
    replacement = bonded_request(submitted_block=101)
    assert "absence_unconfirmed" in run(slash_watcher, replacement).detail
    client.keypair.sign.assert_not_called()


def test_absences_are_forgotten_once_the_request_leaves_the_pending_set():
    cardano = Cardano()
    slash_watcher, client = watcher(scripted(cardano))
    request = bonded_request()
    client.list_pending_settlement_requests.return_value = [asdict(request)]
    asyncio.run(slash_watcher._tick(CHAIN_ID))
    assert len(slash_watcher._absences) == 1
    client.list_pending_settlement_requests.return_value = []
    asyncio.run(slash_watcher._tick(CHAIN_ID))
    assert slash_watcher._absences == {}


def test_the_node_tip_is_not_kupos_checkpoint():
    assert sw._extract_kupo_checkpoint_slot({"most_recent_node_tip": TIP_SLOT}) is None
    assert sw._extract_kupo_checkpoint_slot({"most_recent_checkpoint": TIP_SLOT - 9, "most_recent_node_tip": TIP_SLOT}) \
        == TIP_SLOT - 9


def test_every_tx_lookup_uses_the_output_reference_index():
    cardano = Cardano(tx_rows=[paying(BENE_HASH_28, AMOUNT_LOVELACE)])
    obs = asyncio.run(scripted(cardano).observe(CARDANO_TX_HASH.hex(), BENE_HASH_28))
    assert obs.mismatches == [] and obs.matched_address_lovelace == AMOUNT_LOVELACE
    assert cardano.reads.count(TX_LOOKUP) == 1


# --- the node must be on the network's chain, at the network's time ----------


def test_the_network_clock_is_pinned_per_cardano_genesis():
    assert sw.NETWORK_CLOCKS == {CARDANO_PREPROD_GENESIS_HASH: PREPROD_CLOCK}


def test_a_node_behind_the_network_proves_nothing():
    """materios review of 219176c: the shared node is catching up, its tip
    still before the honest tx's slot, while Kupo keeps pace with it."""
    cardano = Cardano()
    cardano.tip_slot = OBSERVED_SLOT - 3000
    cardano.tip_height = 900_000
    slash_watcher, client = watcher(scripted(cardano))
    request = bonded_request()
    for _ in range(3):
        verdict = run(slash_watcher, request)
        assert verdict.fraud_proof is None
        assert "cardano_tip_behind_wallclock" in verdict.detail
        cardano.advance(sw.TX_ABSENCE_CONFIRM_BLOCKS)
    client.keypair.sign.assert_not_called()


@pytest.mark.parametrize("lag, fresh", [(sw.CARDANO_TIP_LAG_MAX_SLOTS, True), (sw.CARDANO_TIP_LAG_MAX_SLOTS + 1, False)])
def test_the_tip_may_trail_the_wall_clock_by_the_bound(lag, fresh):
    cardano = Cardano()
    cardano.wall_slot = cardano.tip_slot + lag
    slash_watcher, _ = watcher(scripted(cardano))
    detail = run(slash_watcher, bonded_request()).detail
    assert ("absence_unconfirmed" in detail) is fresh
    assert ("cardano_tip_behind_wallclock" in detail) is not fresh


def test_a_network_without_a_pinned_clock_proves_nothing(monkeypatch):
    monkeypatch.setitem(sw.SPENT_CANARIES, CARDANO_PREVIEW_GENESIS_HASH, (CANARY_TX, CANARY_INDEX))
    cardano = Cardano(magic=2)
    slash_watcher, client = watcher(scripted(cardano))
    request = bonded_request(mainchain_genesis_hash=CARDANO_PREVIEW_GENESIS_HASH)
    for _ in range(2):
        assert "no_network_clock" in run(slash_watcher, request).detail
        cardano.advance(sw.TX_ABSENCE_CONFIRM_BLOCKS)
    client.keypair.sign.assert_not_called()


def test_a_sparse_chain_between_the_proofs_restarts_the_count():
    """A minority fork advances, but far more slowly than the network."""
    cardano = Cardano()
    slash_watcher, client = watcher(scripted(cardano))
    request = bonded_request()
    run(slash_watcher, request)
    cardano.advance(sw.TX_ABSENCE_CONFIRM_BLOCKS, slots_per_block=200)
    assert "cardano_chain_sparse" in run(slash_watcher, request).detail
    client.keypair.sign.assert_not_called()
    # The count restarted at the sparse proof; dense blocks since then prove it.
    cardano.advance(sw.TX_ABSENCE_CONFIRM_BLOCKS)
    assert isinstance(run(slash_watcher, request).fraud_proof, sw.TxNotFound)


@pytest.mark.parametrize("slots_per_block, dense", [(sw.CHAIN_SLOTS_PER_BLOCK_MAX, True),
                                                    (sw.CHAIN_SLOTS_PER_BLOCK_MAX + 1, False)])
def test_the_chain_between_the_proofs_may_average_the_bound(slots_per_block, dense):
    cardano = Cardano()
    slash_watcher, client = watcher(scripted(cardano))
    request = bonded_request()
    run(slash_watcher, request)
    cardano.advance(sw.TX_ABSENCE_CONFIRM_BLOCKS, slots_per_block=slots_per_block)
    verdict = run(slash_watcher, request)
    assert isinstance(verdict.fraud_proof, sw.TxNotFound) is dense
    assert signed(client) == int(dense)


def test_a_tx_seen_by_the_observation_restarts_the_count():
    tx = [paying(BENE_HASH_28, AMOUNT_LOVELACE)]
    cardano = Cardano()
    slash_watcher, client = watcher(scripted(cardano))
    request = bonded_request()
    assert "absence_unconfirmed" in run(slash_watcher, request).detail
    cardano.advance(1)
    cardano.tx_rows = tx
    assert run(slash_watcher, request).outcome == sw.ClassifierOutcome.NOT_FRAUD
    cardano.advance(sw.TX_ABSENCE_CONFIRM_BLOCKS)
    cardano.tx_rows = []
    assert "absence_unconfirmed" in run(slash_watcher, request).detail
    client.keypair.sign.assert_not_called()


def test_the_watcher_exports_how_far_the_node_trails_the_network():
    cardano = Cardano()
    cardano.wall_slot = cardano.tip_slot + 37
    slash_watcher, _ = watcher(scripted(cardano))
    asyncio.run(slash_watcher._report_status())
    assert health_server._metrics["slash_cardano_tip_lag_slots"] == 37
