"""Supersede driver: committee attestors replace an unattestable settlement
request with evidence they verified on Cardano, through the same STCA digest
``attest_settle`` checks.

The chain below verifies a supersede the way ``supersede_settlement_request``
does at settlement 0757de3: the evidence passes the request checks, differs
from the pending request, and carries a threshold of distinct committee
signatures over the STCA digest that include the caller's.
"""
from __future__ import annotations

import asyncio
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from substrateinterface import Keypair

from daemon import settle_claim_attestor as sca
from daemon.settle_claim_attestor import SettlementEvidence, SettleClaimAttestor
from tests.test_multisig_aggregator import _with_gateway
from tests.test_settle_claim_attestor import (
    AMOUNT_LOVELACE, BENE_HASH_28, CHAIN_ID, CLAIM_ID, OBSERVED_SLOT,
    PREPROD_GENESIS, VOUCHER_DIGEST, _default_pending_request, _make_observer_stub,
)

ALICE = Keypair.create_from_uri("//Alice")
BOB = Keypair.create_from_uri("//Bob")
TTL = 2400
MIN_DEPTH = 15
# The request's slot is wrong: no FraudProof covers it and no attestor signs it.
JUNK = dict(observed_slot=OBSERVED_SLOT + 7, submitted_block=1000)
CORRECT = SettlementEvidence(
    cardano_tx_hash=_default_pending_request().cardano_tx_hash,
    observed_at_depth=MIN_DEPTH,
    observed_slot=OBSERVED_SLOT,
    beneficiary_addr_hash=BENE_HASH_28,
    amount_lovelace=AMOUNT_LOVELACE,
    mainchain_genesis_hash=PREPROD_GENESIS,
)


def context(block=1100, supersede=True, ttl=TTL, min_depth=MIN_DEPTH):
    return dict(protocol="FRV2" if supersede else "FRAU", block_number=block, ttl=ttl,
                spec_version=242 if supersede else 239, block_hash="0x" + "ab" * 32,
                supersede=supersede, min_finality_depth=min_depth)


class Chain:
    """Settlement state shared by every attestor's client."""

    def __init__(self, request, committee=(ALICE, BOB), threshold=2):
        self.requests = {request.claim_id: asdict(request)}
        self.committee = {k.public_key for k in committee}
        self.threshold = threshold
        self.context = context()
        self.voucher = {"amount_lovelace": AMOUNT_LOVELACE, "beneficiary_addr_blake2_224": BENE_HASH_28}
        self.voucher_digest = VOUCHER_DIGEST
        self.settled = {}
        self.submissions = []
        self.fail_submissions = False

    def client(self, keypair):
        return SimpleNamespace(
            keypair=MagicMock(wraps=keypair, public_key=keypair.public_key),
            get_settlement_runtime_context=MagicMock(side_effect=lambda: dict(self.context)),
            list_pending_settlement_requests=MagicMock(side_effect=lambda: [dict(r) for r in self.requests.values()]),
            get_settlement_request=MagicMock(side_effect=lambda claim: dict(self.requests[claim]) if claim in self.requests else None),
            get_voucher_digest=MagicMock(side_effect=lambda claim: self.voucher_digest),
            get_voucher=MagicMock(side_effect=lambda claim: self.voucher),
            get_genesis_hash=MagicMock(return_value="0x" + CHAIN_ID.hex()),
            submit_attest_settle=MagicMock(return_value="0x" + "ee" * 32),
            submit_attest_settle_envelope=MagicMock(return_value="0x" + "ee" * 32),
            submit_supersede_settlement_request=MagicMock(
                side_effect=lambda *args: self.supersede(keypair.public_key, *args)),
        )

    def supersede(self, caller, claim_id, settled_direct, evidence, signatures):
        self.submissions.append((caller, claim_id, settled_direct, evidence, list(signatures)))
        if self.fail_submissions:
            return None
        request = self.requests.get(claim_id)
        assert request is not None, "SettlementRequestMissing"
        assert (SettlementEvidence.of(SimpleNamespace(**request)), request["settled_direct"]) != (evidence, settled_direct), \
            "SupersedeMatchesRequest"
        assert evidence.mainchain_genesis_hash == PREPROD_GENESIS
        assert MIN_DEPTH <= evidence.observed_at_depth <= 2160
        assert (evidence.amount_lovelace, evidence.beneficiary_addr_hash) == (
            self.voucher["amount_lovelace"], self.voucher["beneficiary_addr_blake2_224"])
        digest = sca.stca_digest(CHAIN_ID, claim_id, self.voucher_digest, settled_direct, evidence)
        signers = [pub for pub, _ in signatures]
        assert len(signatures) >= self.threshold, "InsufficientSignatures"
        assert caller in signers, "InsufficientSignatures"
        assert len(set(signers)) == len(signers), "DuplicateSigner"
        assert set(signers) <= self.committee, "SignerNotCommitteeMember"
        for pub, sig in signatures:
            assert Keypair(public_key=pub, ss58_format=42).verify(digest, sig), "InvalidSignature"
        del self.requests[claim_id]
        self.settled[claim_id] = (evidence, settled_direct)
        return "0x" + "5e" * 32


def attestor(client, aggregator=None, threshold=1, min_finality_depth=MIN_DEPTH):
    return SettleClaimAttestor(
        config=SimpleNamespace(), substrate_client=client, chain_write_lock=asyncio.Lock(),
        observer=_make_observer_stub(), min_finality_depth=min_finality_depth,
        aggregator=aggregator, min_signer_threshold=threshold,
    )


def junk_request(**overrides):
    return _default_pending_request(**{**JUNK, **overrides})


def test_two_attestors_supersede_a_junk_request_at_m2():
    request = junk_request()
    chain = Chain(request)

    async def run(gw, agg, session):
        alice = attestor(chain.client(ALICE), agg, threshold=2)
        bob = attestor(chain.client(BOB), agg, threshold=2)
        first = await alice.process_one(request, CHAIN_ID)
        assert first.refusal_reason == sca.RefusalReason.SLOT_MISMATCH
        assert first.supersede == "awaiting_peer_sigs:1/2"
        assert chain.submissions == []
        second = await bob.process_one(request, CHAIN_ID)
        assert second.supersede == "superseded"
        assert second.extrinsic_hash == "0x" + "5e" * 32
        await alice._tick(CHAIN_ID)
        return alice, bob

    alice, bob = asyncio.run(_with_gateway(run, committee=chain.committee))
    assert chain.settled == {CLAIM_ID: (CORRECT, request.settled_direct)}
    [(caller, _, _, evidence, envelope)] = chain.submissions
    assert caller == BOB.public_key
    assert {pub for pub, _ in envelope} == {ALICE.public_key, BOB.public_key}
    for side in (alice, bob):
        side.client.submit_attest_settle.assert_not_called()
        side.client.submit_attest_settle_envelope.assert_not_called()


def test_an_outsider_signature_on_the_bulletin_board_does_not_block_the_supersede():
    request = junk_request()
    chain = Chain(request)
    outsider = Keypair.create_from_uri("//Mallory")

    async def run(gw, agg, session):
        alice = attestor(chain.client(ALICE), agg, threshold=2)
        bob = attestor(chain.client(BOB), agg, threshold=2)
        await alice.process_one(request, CHAIN_ID)
        # Anyone can read the digest and post a valid signature of their own.
        [(kind, key, digest, _)] = list(gw.store)
        gw.store[(kind, key, digest, outsider.public_key.hex())] = outsider.sign(bytes.fromhex(digest)).hex()
        return await bob.process_one(request, CHAIN_ID)

    assert asyncio.run(_with_gateway(run, committee=chain.committee)).supersede == "superseded"
    [(_, _, _, _, envelope)] = chain.submissions
    assert {pub for pub, _ in envelope} == {ALICE.public_key, BOB.public_key}


@pytest.mark.parametrize("junk", [
    dict(observed_slot=OBSERVED_SLOT + 7),
    # A depth the transaction has not reached; the observer sees 20.
    dict(observed_at_depth=2160),
])
def test_supersede_signs_only_the_evidence_it_observed(junk):
    request = _default_pending_request(submitted_block=1000, **junk)
    chain = Chain(request, committee=(ALICE,), threshold=1)
    client = chain.client(ALICE)
    verdict = asyncio.run(attestor(client).process_one(request, CHAIN_ID))
    assert verdict.supersede == "superseded"
    digest = sca.stca_digest(CHAIN_ID, CLAIM_ID, VOUCHER_DIGEST, request.settled_direct, CORRECT)
    assert [c.args for c in client.keypair.sign.call_args_list] == [(digest,)]
    assert chain.settled[CLAIM_ID] == (CORRECT, request.settled_direct)


@pytest.mark.parametrize("request_fields", [
    dict(),
    dict(observed_at_depth=MIN_DEPTH),
    dict(observed_at_depth=20),
])
def test_an_attestable_request_is_attested_not_superseded(request_fields):
    request = _default_pending_request(submitted_block=1000, **request_fields)
    chain = Chain(request, committee=(ALICE,), threshold=1)
    client = chain.client(ALICE)
    verdict = asyncio.run(attestor(client).process_one(request, CHAIN_ID))
    assert verdict.signed is True
    assert verdict.supersede is None
    client.submit_attest_settle.assert_called_once()
    client.submit_supersede_settlement_request.assert_not_called()
    client.get_settlement_runtime_context.assert_not_called()


def unobserved_depth(chain, att):
    att.observer = _make_observer_stub(tx_block_no=None)


def shallow(chain, att):
    att.observer = _make_observer_stub(tip_block_no=1_000_000, tx_block_no=1_000_000 - (MIN_DEPTH - 1))


def stricter_local_floor(chain, att):
    att.min_finality_depth = 25


def voucher_digest_changed(chain, att):
    chain.voucher_digest = bytes.fromhex("99" * 32)


def voucher_unreadable(chain, att):
    chain.voucher = None


def voucher_without_beneficiary(chain, att):
    chain.voucher = {"amount_lovelace": AMOUNT_LOVELACE}


def voucher_other_beneficiary(chain, att):
    chain.voucher = {"amount_lovelace": AMOUNT_LOVELACE, "beneficiary_addr_blake2_224": bytes(28)}


def underpaid(chain, att):
    att.observer = _make_observer_stub(matched_lovelace=AMOUNT_LOVELACE - 1)


def tx_missing(chain, att):
    att.observer = _make_observer_stub(mismatches=["kupo_no_matches_for_tx"], matched_lovelace=None, observed_slot=None)


def other_network(chain, att):
    att.observer = _make_observer_stub(genesis_hash=bytes.fromhex("66" * 32))


def runtime_unreadable(chain, att):
    att.client.get_settlement_runtime_context.side_effect = ValueError("unknown settlement runtime")


def runtime_without_supersede(chain, att):
    chain.context = context(supersede=False)


def expired(chain, att):
    chain.context = context(block=1000 + TTL + 1)


@pytest.mark.parametrize("damage", [
    unobserved_depth, shallow, stricter_local_floor, voucher_digest_changed, voucher_unreadable,
    voucher_without_beneficiary, voucher_other_beneficiary, underpaid, tx_missing, other_network,
    runtime_unreadable, runtime_without_supersede, expired,
])
def test_no_signature_without_verified_evidence_on_a_runtime_that_supersedes(damage):
    request = junk_request()
    chain = Chain(request, committee=(ALICE,), threshold=1)
    att = attestor(chain.client(ALICE))
    damage(chain, att)
    verdict = asyncio.run(att.process_one(request, CHAIN_ID))
    assert verdict.signed is False
    assert verdict.supersede != "superseded"
    att.client.keypair.sign.assert_not_called()
    att.client.submit_supersede_settlement_request.assert_not_called()
    assert chain.settled == {}


def test_the_last_fresh_block_still_supersedes():
    request = junk_request()
    chain = Chain(request, committee=(ALICE,), threshold=1)
    chain.context = context(block=1000 + TTL)
    assert asyncio.run(attestor(chain.client(ALICE)).process_one(request, CHAIN_ID)).supersede == "superseded"


def test_failed_supersedes_back_off_doubling_up_to_the_ttl():
    request = junk_request()
    chain = Chain(request, committee=(ALICE,), threshold=1)
    chain.fail_submissions = True
    att = attestor(chain.client(ALICE))

    def attempt(req, block):
        chain.requests[CLAIM_ID] = asdict(req)
        chain.context = context(block=block, ttl=30)
        return asyncio.run(att.process_one(req, CHAIN_ID)).supersede

    assert [b for b in range(1000, 1031) if attempt(request, b) == "submit_failed"] == [1000, 1010, 1030]
    # The junk is re-filed once it expires; the third failure waits min(40, ttl) = 30 blocks.
    refiled = replace(request, submitted_block=1031)
    assert attempt(refiled, 1059) == "backoff"
    assert attempt(refiled, 1060) == "submit_failed"
    assert len(chain.submissions) == 4


def test_no_duplicate_submission_once_a_peer_has_superseded():
    request = junk_request()
    chain = Chain(request)

    async def run(gw, agg, session):
        alice = attestor(chain.client(ALICE), agg, threshold=2)
        bob = attestor(chain.client(BOB), agg, threshold=2)
        await alice.process_one(request, CHAIN_ID)
        assert (await bob.process_one(request, CHAIN_ID)).supersede == "superseded"
        # Alice still holds the stale row; her envelope is complete now.
        return await alice.process_one(request, CHAIN_ID)

    verdict = asyncio.run(_with_gateway(run, committee=chain.committee))
    assert verdict.supersede == "request_changed"
    assert len(chain.submissions) == 1


@pytest.mark.parametrize("change", ["replaced", "expired", "unsupported", "unreadable"])
def test_request_or_runtime_changes_before_submission_defer(change):
    request = junk_request()
    chain = Chain(request, committee=(ALICE,), threshold=1)
    att = attestor(chain.client(ALICE))

    class Lock:
        async def __aenter__(self):
            if change == "replaced":
                chain.requests[CLAIM_ID] = asdict(replace(request, submitted_block=1050))
            elif change == "expired":
                chain.context = context(block=1000 + TTL + 1)
            elif change == "unsupported":
                chain.context = context(supersede=False)
            else:
                att.client.get_settlement_runtime_context.side_effect = ValueError("")

        async def __aexit__(self, *args):
            return False

    att._chain_write_lock = Lock()
    verdict = asyncio.run(att.process_one(request, CHAIN_ID))
    assert verdict.supersede in ("request_changed", "request_expired", "supersede_unsupported", "runtime_unavailable")
    att.client.submit_supersede_settlement_request.assert_not_called()


def test_backoff_is_forgotten_once_the_claim_leaves_the_pending_set():
    request = junk_request()
    chain = Chain(request, committee=(ALICE,), threshold=1)
    chain.fail_submissions = True
    att = attestor(chain.client(ALICE))
    asyncio.run(att._tick(CHAIN_ID))
    assert CLAIM_ID in att._supersede_backoff
    chain.requests.clear()
    asyncio.run(att._tick(CHAIN_ID))
    assert att._supersede_backoff == {}


# --- SubstrateClient --------------------------------------------------------

from daemon.substrate_client import SubstrateClient  # noqa: E402


class Receipt(SimpleNamespace):
    pass


def rpc(results):
    """A SubstrateClient whose RPC layer answers from ``results`` by method."""
    client = object.__new__(SubstrateClient)
    client.config = SimpleNamespace(signer_uri="//Alice", tx_max_retries=3)
    client._read = object()

    def call(method, *args, **kwargs):
        answer = results[method]
        return answer(*args, **kwargs) if callable(answer) else answer

    client._call = MagicMock(side_effect=call)
    return client


SUPERSEDE_PARAMS = {
    "claim_id": "0x" + CLAIM_ID.hex(),
    "settled_direct": True,
    "evidence": {
        "cardano_tx_hash": "0x" + CORRECT.cardano_tx_hash.hex(),
        "observed_at_depth": MIN_DEPTH,
        "observed_slot": OBSERVED_SLOT,
        "beneficiary_addr_hash": "0x" + BENE_HASH_28.hex(),
        "amount_lovelace": AMOUNT_LOVELACE,
        "mainchain_genesis_hash": "0x" + PREPROD_GENESIS.hex(),
    },
    "signatures": [("0x" + "aa" * 32, "0x" + "bb" * 64), ("0x" + "cc" * 32, "0x" + "dd" * 64)],
}
ENVELOPE = [(b"\xaa" * 32, b"\xbb" * 64), (b"\xcc" * 32, b"\xdd" * 64)]


def test_supersede_extrinsic_carries_the_signed_evidence():
    client = rpc({
        "compose_call": "call",
        "create_signed_extrinsic": "xt",
        "submit_extrinsic": Receipt(is_success=True, extrinsic_hash="0x" + "5e" * 32),
    })
    assert client.submit_supersede_settlement_request(CLAIM_ID, True, CORRECT, ENVELOPE) == "0x" + "5e" * 32
    compose, sign, submit = client._call.call_args_list
    assert compose.args == ("compose_call",)
    assert compose.kwargs == dict(call_module="IntentSettlement", call_function="supersede_settlement_request",
                                  call_params=SUPERSEDE_PARAMS)
    assert sign.kwargs["call"] == "call"
    assert sign.kwargs["keypair"].public_key == ALICE.public_key
    assert submit.args == ("submit_extrinsic", "xt") and submit.kwargs == {"wait_for_inclusion": True}


def test_a_refused_supersede_is_not_retried():
    client = rpc({
        "compose_call": "call",
        "create_signed_extrinsic": "xt",
        "submit_extrinsic": Receipt(is_success=False, error_message={"name": "SupersedeMatchesRequest"}),
    })
    assert client.submit_supersede_settlement_request(CLAIM_ID, True, CORRECT, ENVELOPE) is None
    assert [c.args[0] for c in client._call.call_args_list] == ["compose_call", "create_signed_extrinsic", "submit_extrinsic"]


@pytest.mark.parametrize("args", [
    (CLAIM_ID[:31], True, CORRECT, ENVELOPE),
    (CLAIM_ID, True, CORRECT, []),
    (CLAIM_ID, True, CORRECT, [(b"\xaa" * 31, b"\xbb" * 64)]),
    (CLAIM_ID, True, CORRECT, [(b"\xaa" * 32, b"\xbb" * 63)]),
    (CLAIM_ID, True, replace(CORRECT, beneficiary_addr_hash=bytes(32)), ENVELOPE),
])
def test_malformed_supersedes_are_refused_before_composing(args):
    client = rpc({})
    with pytest.raises(ValueError):
        client.submit_supersede_settlement_request(*args)
    client._call.assert_not_called()


def stored_request(**evidence):
    return SimpleNamespace(value={
        "requester": "5DummyAccountId1",
        "evidence": {
            "cardano_tx_hash": "0x" + CORRECT.cardano_tx_hash.hex(),
            "observed_at_depth": 20,
            "observed_slot": OBSERVED_SLOT,
            "beneficiary_addr_hash": "0x" + BENE_HASH_28.hex(),
            "amount_lovelace": AMOUNT_LOVELACE,
            "mainchain_genesis_hash": "0x" + PREPROD_GENESIS.hex(),
            **evidence,
        },
        "settled_direct": True,
        "submitted_block": 1000,
        "bond_amount": 7,
    })


def test_one_pending_request_reads_like_a_listed_row():
    client = rpc({
        "query": lambda *a, **kw: stored_request(),
        "query_map": [(SimpleNamespace(value="0x" + CLAIM_ID.hex()), stored_request())],
    })
    client.get_voucher_digest = MagicMock(return_value=VOUCHER_DIGEST)
    row = client.get_settlement_request(CLAIM_ID)
    assert row == client.list_pending_settlement_requests()[0]
    assert row == asdict(_default_pending_request(submitted_block=1000, bond_amount=7))
    query = next(c for c in client._call.call_args_list if c.args[0] == "query")
    assert query.kwargs == dict(module="IntentSettlement", storage_function="ClaimSettlementRequests",
                                params=["0x" + CLAIM_ID.hex()])


def test_an_absent_request_reads_as_none():
    client = rpc({"query": SimpleNamespace(value=None)})
    assert client.get_settlement_request(CLAIM_ID) is None


TYPE0 = bytes([0x01]) + BENE_HASH_28 + b"\x77" * 28


@pytest.mark.parametrize("address, beneficiary", [
    ("0x" + TYPE0.hex(), BENE_HASH_28),
    (list(TYPE0), BENE_HASH_28),
    ("0x" + (bytes([0x61]) + BENE_HASH_28).hex(), None),
    ("0x" + TYPE0[:-1].hex(), None),
])
def test_voucher_beneficiary_is_the_payment_key_hash_the_pallet_checks(address, beneficiary):
    client = rpc({"query": SimpleNamespace(value={"amount_ada": AMOUNT_LOVELACE, "beneficiary_cardano_addr": address})})
    voucher = client.get_voucher(CLAIM_ID)
    assert voucher.get("beneficiary_addr_blake2_224") == beneficiary
    assert voucher["amount_lovelace"] == AMOUNT_LOVELACE


def test_the_supersede_call_encodes_against_the_spec_242_runtime():
    import gzip
    from pathlib import Path
    from daemon.custody_rules import RuntimeDecoder

    metadata = gzip.decompress(
        (Path(__file__).parent / "fixtures/custody/materios/metadata_spec242.hex.gz").read_bytes()).decode()
    runtime = RuntimeDecoder(metadata)
    client = rpc({
        "compose_call": "call",
        "create_signed_extrinsic": "xt",
        "submit_extrinsic": Receipt(is_success=True, extrinsic_hash="0x" + "5e" * 32),
    })
    client.submit_supersede_settlement_request(CLAIM_ID, True, CORRECT, ENVELOPE)
    composed = client._call.call_args_list[0].kwargs
    call = runtime._config.create_scale_object("Call", metadata=runtime._metadata)
    data = call.encode({"call_module": composed["call_module"], "call_function": composed["call_function"],
                        "call_args": composed["call_params"]})
    assert data.to_hex()[:6] == "0x1316"  # IntentSettlement (19), call 22
    decoded = runtime._config.create_scale_object("Call", data=data, metadata=runtime._metadata).decode()
    args = {a["name"]: a["value"] for a in decoded["call_args"]}
    assert args["evidence"] == SUPERSEDE_PARAMS["evidence"]
    assert args["settled_direct"] is True
    assert [tuple(pair) for pair in args["signatures"]] == SUPERSEDE_PARAMS["signatures"]


def test_each_reason_not_to_supersede_is_logged_once_per_claim(caplog):
    caplog.set_level("INFO")
    request = junk_request()
    chain = Chain(request, committee=(ALICE,), threshold=1)
    chain.context = context(supersede=False)
    att = attestor(chain.client(ALICE))
    for _ in range(3):
        asyncio.run(att.process_one(request, CHAIN_ID))
    chain.voucher = None
    chain.context = context()
    for _ in range(2):
        asyncio.run(att.process_one(request, CHAIN_ID))
    notes = [r.getMessage() for r in caplog.records if "not superseding" in r.getMessage()]
    assert len(notes) == 2
    assert "supersede_unsupported" in notes[0] and "unverified:voucher" in notes[1]



@pytest.mark.parametrize("error", [TimeoutError("rpc"), ConnectionError("ws"), ValueError("decode")])
def test_a_failed_listing_raises_instead_of_reading_as_no_requests(error):
    def failing(*args, **kwargs):
        raise error
    client = rpc({"query_map": failing})
    with pytest.raises(type(error)):
        client.list_pending_settlement_requests()


def test_a_failed_listing_keeps_every_backoff():
    request = junk_request()
    chain = Chain(request, committee=(ALICE,), threshold=1)
    chain.fail_submissions = True
    att = attestor(chain.client(ALICE))
    asyncio.run(att._tick(CHAIN_ID))
    before = dict(att._supersede_backoff)
    assert CLAIM_ID in before
    att.client.list_pending_settlement_requests.side_effect = TimeoutError("rpc")
    asyncio.run(att._tick(CHAIN_ID))
    assert att._supersede_backoff == before


def test_supersede_rereads_under_the_write_lock_are_bounded(monkeypatch):
    import time as clock
    monkeypatch.setattr(sca, "LOCKED_REREAD_SECONDS", 0.2)
    request = junk_request()
    chain = Chain(request, committee=(ALICE,), threshold=1)
    att = attestor(chain.client(ALICE))
    att.client.get_settlement_request.side_effect = lambda claim: clock.sleep(1) or asdict(request)

    async def run():
        started = clock.monotonic()
        verdict = await att.process_one(request, CHAIN_ID)
        return verdict, clock.monotonic() - started, att._chain_write_lock.locked()
    verdict, elapsed, held = asyncio.run(run())
    assert verdict.supersede == "reread_timed_out"
    assert elapsed < 0.8 and not held
    att.client.submit_supersede_settlement_request.assert_not_called()
