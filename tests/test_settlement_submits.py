"""Every committee-signed IntentSettlement submission composes its call,
signs with the daemon key, treats a refused dispatch as final for the tick and
retries only transport errors."""
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from substrateinterface import Keypair
from substrateinterface.exceptions import SubstrateRequestException

from daemon.slash_watcher import TxNotFound, WrongAmount, WrongBeneficiary
from daemon.substrate_client import SubstrateClient

ID = b"\x11" * 32
PUB, SIG = b"\xaa" * 32, b"\xbb" * 64
SIGS_PARAM = [("0x" + PUB.hex(), "0x" + SIG.hex())]
OK = SimpleNamespace(is_success=True, extrinsic_hash="0x" + "5e" * 32)

SUBMITS = [
    ("submit_attest_settle", (ID, PUB, SIG), "attest_settle",
     {"claim_id": "0x" + ID.hex(), "signatures": SIGS_PARAM}),
    ("submit_attest_settle_envelope", (ID, [(PUB, SIG)]), "attest_settle",
     {"claim_id": "0x" + ID.hex(), "signatures": SIGS_PARAM}),
    ("submit_attest_expire_policy", (ID, PUB, SIG), "attest_expire_policy",
     {"intent_id": "0x" + ID.hex(), "signatures": SIGS_PARAM}),
    ("submit_attest_expire_policy_envelope", (ID, [(PUB, SIG)]), "attest_expire_policy",
     {"intent_id": "0x" + ID.hex(), "signatures": SIGS_PARAM}),
    ("submit_slash_bad_settlement_evidence", (ID, WrongAmount(4), [(PUB, SIG)]), "slash_bad_settlement_evidence",
     {"claim_id": "0x" + ID.hex(), "fraud_proof": {"WrongAmount": {"actual_lovelace": 4}}, "signatures": SIGS_PARAM}),
    ("submit_slash_bad_settlement_evidence", (ID, TxNotFound(), [(PUB, SIG)]), "slash_bad_settlement_evidence",
     {"claim_id": "0x" + ID.hex(), "fraud_proof": "TxNotFound", "signatures": SIGS_PARAM}),
    ("submit_slash_bad_settlement_evidence", (ID, WrongBeneficiary(b"\x22" * 28), [(PUB, SIG)]),
     "slash_bad_settlement_evidence",
     {"claim_id": "0x" + ID.hex(), "fraud_proof": {"WrongBeneficiary": {"actual_payment_hash": "0x" + "22" * 28}},
      "signatures": SIGS_PARAM}),
]


def client(*receipts):
    c = object.__new__(SubstrateClient)
    c.config = SimpleNamespace(signer_uri="//Alice", tx_max_retries=3)
    c._ensure_wrapper_state()
    answers = iter(receipts)

    def call(method, *args, **kwargs):
        if method == "submit_extrinsic":
            answer = next(answers)
            if isinstance(answer, Exception):
                raise answer
            return answer
        return method

    c._call = MagicMock(side_effect=call)
    return c


@pytest.mark.parametrize("method, args, function, params", SUBMITS)
def test_submission_composes_signs_and_returns_the_hash(method, args, function, params):
    c = client(OK)
    assert getattr(c, method)(*args) == "0x" + "5e" * 32
    compose, sign, submit = c._call.call_args_list
    assert compose.kwargs == dict(call_module="IntentSettlement", call_function=function, call_params=params,
                                  conn=c._write)
    assert sign.kwargs == dict(call="compose_call", keypair=sign.kwargs["keypair"], conn=c._write)
    assert sign.kwargs["keypair"].public_key == Keypair.create_from_uri("//Alice").public_key
    assert submit.args == ("submit_extrinsic", "create_signed_extrinsic")
    assert submit.kwargs == {"wait_for_inclusion": True, "conn": c._write}


@pytest.mark.parametrize("method, args, function, params", SUBMITS)
def test_a_refused_dispatch_is_final_for_the_tick(method, args, function, params):
    c = client(SimpleNamespace(is_success=False, error_message={"name": "AlreadySettled"}))
    assert getattr(c, method)(*args) is None
    assert c._call.call_count == 3


@pytest.mark.parametrize("method, args, function, params", SUBMITS)
def test_transport_errors_retry_then_give_up(method, args, function, params):
    c = client(*[SubstrateRequestException("dropped")] * 3)
    with patch("daemon.substrate_client.time.sleep") as sleep:
        assert getattr(c, method)(*args) is None
    assert [s.args for s in sleep.call_args_list] == [(1,), (2,)]
    assert c._call.call_count == 9
    c = client(SubstrateRequestException("dropped"), OK)
    with patch("daemon.substrate_client.time.sleep"):
        assert getattr(c, method)(*args) == "0x" + "5e" * 32


@pytest.mark.parametrize("method, args, function, params", SUBMITS)
def test_unexpected_errors_stop_at_once(method, args, function, params):
    c = client(RuntimeError("decode"))
    assert getattr(c, method)(*args) is None
    assert c._call.call_count == 3


@pytest.mark.parametrize("sigs", [[], [(b"\xaa" * 31, SIG)], [(PUB, b"\xbb" * 63)]])
@pytest.mark.parametrize("method", ["submit_attest_settle_envelope", "submit_attest_expire_policy_envelope"])
def test_malformed_envelopes_never_reach_the_chain(method, sigs):
    c = client()
    with pytest.raises(ValueError):
        getattr(c, method)(ID, sigs)
    with pytest.raises(ValueError):
        getattr(c, method)(ID[:31], [(PUB, SIG)])
    c._call.assert_not_called()


def test_committee_pubkeys_are_the_raw_account_bytes_of_the_orinq_committee():
    alice, bob = Keypair.create_from_uri("//Alice"), Keypair.create_from_uri("//Bob")
    c = object.__new__(SubstrateClient)
    c._read = object()
    c._call = MagicMock(return_value=SimpleNamespace(value=[alice.ss58_address, "0x" + bob.public_key.hex()]))
    assert c.get_committee_pubkeys() == frozenset({alice.public_key, bob.public_key})
    assert c._call.call_args.args == ("query", "OrinqReceipts", "CommitteeMembers")
    assert c._call.call_args.kwargs == {"conn": c._read}


@pytest.mark.parametrize("factory", ["settle", "expire", "slash"])
def test_every_attestor_reads_the_committee_its_envelopes_are_checked_against(factory, monkeypatch):
    from daemon.expire_policy_attestor import maybe_create_expire_policy_attestor
    from daemon.settle_claim_attestor import maybe_create_settle_claim_attestor
    from daemon.slash_watcher import maybe_create_slash_watcher
    create = {"settle": maybe_create_settle_claim_attestor, "expire": maybe_create_expire_policy_attestor,
              "slash": maybe_create_slash_watcher}[factory]
    monkeypatch.setenv("KUPO_URL", "http://kupo.test")
    client = MagicMock()
    client.get_min_signer_threshold.return_value = 2
    client.get_min_finality_depth.return_value = 15
    config = SimpleNamespace(ogmios_url="http://ogmios.test", kupo_url="http://kupo.test",
                             blob_gateway_url="http://gateway.test")
    import asyncio
    attestor = create(config, client, asyncio.Lock())
    client.get_committee_pubkeys.return_value = frozenset({b"\x01" * 32})
    assert attestor.aggregator._committee() == frozenset({b"\x01" * 32})
