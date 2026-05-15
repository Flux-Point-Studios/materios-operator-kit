"""Tests for ``daemon.expire_policy_attestor`` (task #284 / spec-221).

These exercise the second M-of-N attestation type (expire_policy)
end-to-end at the dispatcher seam, with mocks at the substrate-interface +
Ogmios/Kupo boundaries. No live chain or Cardano follower is required.

Coverage matrix:

    EXPP preimage builder (PR #34 §3.2):
      - byte order pinned exactly
      - total length 176B (4B tag + 172B content)
      - rejects wrong widths / out-of-range integers
      - byte-exact parity vector ``fixture G`` (digest
        ``0x773fa47732e9af0d07dc6e7acb81e8d6c4c94e4f93f5f1ba8d5ff92da34defd6``)

    ExpirePolicyAttestor.process_one (the dispatcher):
      - happy path → sign + submit
      - refusal: genesis mismatch
      - refusal: tx not found
      - refusal: slot mismatch
      - refusal: depth below MinFinalityDepth
      - refusal: policy_id_witness mismatch
      - refusal: intent not found
      - refusal: intent already terminal (Settled / Expired / Refunded)
      - refusal: RefundCredit intent (no policy_id resolvable)

    Concurrency:
      - max_concurrent semaphore caps parallel dispatch

    Factory (maybe_create_expire_policy_attestor):
      - missing ogmios_url → None
      - missing kupo_url → None
      - both present → ExpirePolicyAttestor
      - MinFinalityDepth pulled from chain when available
      - falls back to env default when chain constant unreadable
      - default max_concurrent = 8

    _tick dict→dataclass conversion (same regression class the settle
    attestor closed in security-review post-PR #266):
      - well-formed dict row → PendingExpiryRequest, process_one called
      - malformed row skipped without killing batch

    SubstrateClient helpers (task #284 direct unit tests):
      - get_intent_status: bare string, dict envelope, int encoding,
        absent intent
      - get_policy_id_for_intent: BuyPolicy → product_id,
        RequestPayout → policy_id, RefundCredit → None, unknown variant
        → None
"""

from __future__ import annotations

import asyncio
import hashlib
import os
from types import SimpleNamespace
from typing import Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from substrateinterface import Keypair

from daemon.expire_policy_attestor import (
    EXPP_CONTENT_LEN,
    EXPP_PREIMAGE_LEN,
    ExpirePolicyAttestor,
    PendingExpiryRequest,
    RefusalReason,
    TAG_EXPP,
    build_expp_preimage,
    compute_expp_digest,
    maybe_create_expire_policy_attestor,
)
from daemon.settle_claim_attestor import (
    CARDANO_MAINNET_GENESIS_HASH,
    CARDANO_PREPROD_GENESIS_HASH,
    CardanoTxObservation,
)


# ---------------------------------------------------------------------------
# Helpers — mirror the settle_claim test harness exactly so the auditor
# can grep the two side by side.
# ---------------------------------------------------------------------------


def _run(coro):
    """Mirror the harness used by test_settle_claim_attestor.py — repo
    doesn't ship pytest-asyncio so each async test owns its event loop.
    """
    return asyncio.new_event_loop().run_until_complete(coro)


CHAIN_ID = bytes.fromhex("11" * 32)
INTENT_ID = bytes.fromhex("22" * 32)
POLICY_ID = bytes.fromhex("33" * 32)
CARDANO_TX_HASH = bytes.fromhex("44" * 32)
MAINNET_GENESIS = bytes.fromhex("66" * 32)
PREPROD_GENESIS = bytes.fromhex("77" * 32)
OBSERVED_SLOT = 123_456
OBSERVED_DEPTH = 20


def _default_pending_request(**overrides) -> PendingExpiryRequest:
    base = dict(
        intent_id=INTENT_ID,
        requester="5DummyAccountId1",
        submitted_block=100,
        cardano_tx_hash=CARDANO_TX_HASH,
        observed_at_depth=OBSERVED_DEPTH,
        observed_slot=OBSERVED_SLOT,
        mainchain_genesis_hash=PREPROD_GENESIS,
        policy_id_witness=POLICY_ID,
    )
    base.update(overrides)
    return PendingExpiryRequest(**base)


def _make_substrate_client_stub(
    *,
    intent_status: Optional[str] = "Pending",
    resolved_policy_id: Optional[bytes] = POLICY_ID,
    submit_ext_hash: Optional[str] = "0x" + ("ee" * 32),
    genesis_hex: str = "0x" + ("aa" * 32),
):
    """Build a SubstrateClient-compatible stub for the dispatcher to call.

    Mirrors the methods the attestor exercises:
      - keypair (sr25519, real Keypair so signatures are real bytes)
      - get_intent_status(intent_id) -> str | None
      - get_policy_id_for_intent(intent_id) -> bytes | None
      - submit_attest_expire_policy(intent_id, pubkey, sig) -> str | None
      - get_genesis_hash() -> hex str
      - list_pending_expiry_requests() -> list (unused per-test)
    """
    kp = Keypair.create_from_uri("//Alice")
    stub = SimpleNamespace(
        keypair=kp,
        get_intent_status=MagicMock(return_value=intent_status),
        get_policy_id_for_intent=MagicMock(return_value=resolved_policy_id),
        submit_attest_expire_policy=MagicMock(return_value=submit_ext_hash),
        get_genesis_hash=MagicMock(return_value=genesis_hex),
        list_pending_expiry_requests=MagicMock(return_value=[]),
    )
    return stub


def _make_observer_stub(
    *,
    tip_block_no: Optional[int] = 1_000_000,
    tx_block_no: Optional[int] = 1_000_000 - OBSERVED_DEPTH,
    observed_slot: Optional[int] = OBSERVED_SLOT,
    genesis_hash: bytes = PREPROD_GENESIS,
    mismatches: Optional[list[str]] = None,
):
    """Build a CardanoTxObserver-shaped stub with controllable returns.

    Note: the expire path does NOT bind to a beneficiary or an amount,
    so we don't populate ``matched_address_lovelace`` /
    ``beneficiary_addr_blake2_224`` even when the source observer would
    — the dispatcher never reads them. The stub uses ``None`` for both
    so a future regression that accidentally reads them dies loudly.
    """
    obs = CardanoTxObservation(tx_hash_hex=CARDANO_TX_HASH.hex())
    obs.cardano_tip_block_no = tip_block_no
    obs.tx_block_no = tx_block_no
    obs.observed_slot = observed_slot
    obs.matched_address_lovelace = None   # expire path doesn't read this
    obs.beneficiary_addr_blake2_224 = None  # expire path doesn't read this
    obs.mainchain_genesis_hash = genesis_hash
    obs.mismatches = list(mismatches or [])
    observer = SimpleNamespace(
        observe=AsyncMock(return_value=obs),
        get_genesis_hash=AsyncMock(return_value=genesis_hash),
        ogmios_url="http://ogmios.test",
        kupo_url="http://kupo.test",
    )
    return observer


# ---------------------------------------------------------------------------
# EXPP preimage builder
# ---------------------------------------------------------------------------


class TestExppPreimage:
    def test_total_length_is_176_bytes(self):
        preimage = build_expp_preimage(
            chain_id=CHAIN_ID,
            intent_id=INTENT_ID,
            policy_id=POLICY_ID,
            cardano_tx_hash=CARDANO_TX_HASH,
            observed_at_depth=OBSERVED_DEPTH,
            observed_slot=OBSERVED_SLOT,
            mainchain_genesis_hash=PREPROD_GENESIS,
        )
        assert len(preimage) == EXPP_PREIMAGE_LEN == 176
        # The content body (everything after the 4-byte tag) is 172
        # bytes — pinned for forward-compat with any future docstring
        # change.
        assert EXPP_PREIMAGE_LEN - len(TAG_EXPP) == EXPP_CONTENT_LEN == 172

    def test_tag_prefix_is_EXPP(self):
        preimage = build_expp_preimage(
            chain_id=CHAIN_ID,
            intent_id=INTENT_ID,
            policy_id=POLICY_ID,
            cardano_tx_hash=CARDANO_TX_HASH,
            observed_at_depth=OBSERVED_DEPTH,
            observed_slot=OBSERVED_SLOT,
            mainchain_genesis_hash=PREPROD_GENESIS,
        )
        assert preimage[:4] == b"EXPP"

    def test_byte_order_exact_per_pr_34_section_3_2(self):
        """Pin the byte order against PR #34 §3.2 spec.

        Layout:
            tag (4) || chain_id (32) || intent_id (32) || policy_id (32)
            || cardano_tx_hash (32) || depth_le (4) || slot_le (8)
            || mc_genesis (32)
        """
        preimage = build_expp_preimage(
            chain_id=CHAIN_ID,
            intent_id=INTENT_ID,
            policy_id=POLICY_ID,
            cardano_tx_hash=CARDANO_TX_HASH,
            observed_at_depth=OBSERVED_DEPTH,
            observed_slot=OBSERVED_SLOT,
            mainchain_genesis_hash=MAINNET_GENESIS,
        )
        pos = 0
        assert preimage[pos:pos + 4] == b"EXPP"
        pos += 4
        assert preimage[pos:pos + 32] == CHAIN_ID
        pos += 32
        assert preimage[pos:pos + 32] == INTENT_ID
        pos += 32
        assert preimage[pos:pos + 32] == POLICY_ID
        pos += 32
        assert preimage[pos:pos + 32] == CARDANO_TX_HASH
        pos += 32
        assert preimage[pos:pos + 4] == OBSERVED_DEPTH.to_bytes(4, "little")
        pos += 4
        assert preimage[pos:pos + 8] == OBSERVED_SLOT.to_bytes(8, "little")
        pos += 8
        assert preimage[pos:pos + 32] == MAINNET_GENESIS
        pos += 32
        assert pos == EXPP_PREIMAGE_LEN

    def test_rejects_wrong_chain_id_length(self):
        with pytest.raises(ValueError, match="chain_id"):
            build_expp_preimage(
                chain_id=b"\x00" * 31,  # 31 not 32
                intent_id=INTENT_ID, policy_id=POLICY_ID,
                cardano_tx_hash=CARDANO_TX_HASH,
                observed_at_depth=0, observed_slot=0,
                mainchain_genesis_hash=PREPROD_GENESIS,
            )

    def test_rejects_wrong_intent_id_length(self):
        with pytest.raises(ValueError, match="intent_id"):
            build_expp_preimage(
                chain_id=CHAIN_ID,
                intent_id=b"\x00" * 16,  # 16 not 32
                policy_id=POLICY_ID,
                cardano_tx_hash=CARDANO_TX_HASH,
                observed_at_depth=0, observed_slot=0,
                mainchain_genesis_hash=PREPROD_GENESIS,
            )

    def test_rejects_wrong_policy_id_length(self):
        with pytest.raises(ValueError, match="policy_id"):
            build_expp_preimage(
                chain_id=CHAIN_ID, intent_id=INTENT_ID,
                policy_id=b"\x00" * 28,  # 28 not 32
                cardano_tx_hash=CARDANO_TX_HASH,
                observed_at_depth=0, observed_slot=0,
                mainchain_genesis_hash=PREPROD_GENESIS,
            )

    def test_rejects_overflowing_depth(self):
        with pytest.raises(ValueError, match="observed_at_depth"):
            build_expp_preimage(
                chain_id=CHAIN_ID, intent_id=INTENT_ID,
                policy_id=POLICY_ID,
                cardano_tx_hash=CARDANO_TX_HASH,
                observed_at_depth=2**32,  # one past u32 cap
                observed_slot=0,
                mainchain_genesis_hash=PREPROD_GENESIS,
            )

    def test_rejects_overflowing_slot(self):
        with pytest.raises(ValueError, match="observed_slot"):
            build_expp_preimage(
                chain_id=CHAIN_ID, intent_id=INTENT_ID,
                policy_id=POLICY_ID,
                cardano_tx_hash=CARDANO_TX_HASH,
                observed_at_depth=0, observed_slot=2**64,
                mainchain_genesis_hash=PREPROD_GENESIS,
            )

    def test_distinct_inputs_produce_distinct_preimages(self):
        a = build_expp_preimage(
            chain_id=CHAIN_ID, intent_id=INTENT_ID,
            policy_id=POLICY_ID, cardano_tx_hash=CARDANO_TX_HASH,
            observed_at_depth=OBSERVED_DEPTH, observed_slot=OBSERVED_SLOT,
            mainchain_genesis_hash=PREPROD_GENESIS,
        )
        # Same inputs but a different intent_id.
        b = build_expp_preimage(
            chain_id=CHAIN_ID, intent_id=bytes.fromhex("99" * 32),
            policy_id=POLICY_ID, cardano_tx_hash=CARDANO_TX_HASH,
            observed_at_depth=OBSERVED_DEPTH, observed_slot=OBSERVED_SLOT,
            mainchain_genesis_hash=PREPROD_GENESIS,
        )
        assert a != b
        assert compute_expp_digest(a) != compute_expp_digest(b)


class TestExppDigest:
    def test_digest_is_32_bytes_blake2(self):
        preimage = build_expp_preimage(
            chain_id=CHAIN_ID, intent_id=INTENT_ID,
            policy_id=POLICY_ID, cardano_tx_hash=CARDANO_TX_HASH,
            observed_at_depth=OBSERVED_DEPTH, observed_slot=OBSERVED_SLOT,
            mainchain_genesis_hash=PREPROD_GENESIS,
        )
        digest = compute_expp_digest(preimage)
        assert len(digest) == 32
        expected = hashlib.blake2b(preimage, digest_size=32).digest()
        assert digest == expected

    def test_digest_rejects_wrong_preimage_length(self):
        with pytest.raises(ValueError, match="preimage"):
            compute_expp_digest(b"\x00" * 100)


class TestExppFixtureG:
    """PINNED parity vector from PR #34 (materios-intent-settlement
    sister PR at rev ``01952c69…``). The pallet's pinned proptest
    fixture G and the daemon-side digest MUST agree byte-exact —
    any drift between them would silently break attest_expire_policy
    (every committee sig would be rejected as a wrong-digest).

    Fixture G inputs:
      - TEST_CHAIN_ID = 32×0x73
      - intent_id = 32×0x07
      - policy_id = 32×0x09
      - tx_hash = 32×0x33
      - depth = 15
      - slot = 12_345_678
      - mc_genesis = 32×0x65

    Expected digest:
        0x773fa47732e9af0d07dc6e7acb81e8d6c4c94e4f93f5f1ba8d5ff92da34defd6

    This vector was independently computed against the pallet's
    ``expire_policy_attested_payload`` byte order at PR #34 build time
    and verified once at task #284 implementation time (2026-05-15). If
    this test ever fails, the FIRST thing to check is whether the byte
    order in :func:`build_expp_preimage` drifted from the pallet —
    typically a field reorder or width change introduced by a careless
    refactor.
    """
    TEST_CHAIN_ID = bytes([0x73]) * 32
    INTENT_ID_G = bytes([0x07]) * 32
    POLICY_ID_G = bytes([0x09]) * 32
    TX_HASH_G = bytes([0x33]) * 32
    DEPTH_G = 15
    SLOT_G = 12_345_678
    MC_GENESIS_G = bytes([0x65]) * 32

    EXPECTED_DIGEST_G_HEX = (
        "773fa47732e9af0d07dc6e7acb81e8d6c4c94e4f93f5f1ba8d5ff92da34defd6"
    )

    def test_fixture_g_byte_exact_match(self):
        preimage = build_expp_preimage(
            chain_id=self.TEST_CHAIN_ID,
            intent_id=self.INTENT_ID_G,
            policy_id=self.POLICY_ID_G,
            cardano_tx_hash=self.TX_HASH_G,
            observed_at_depth=self.DEPTH_G,
            observed_slot=self.SLOT_G,
            mainchain_genesis_hash=self.MC_GENESIS_G,
        )
        # Preimage shape pin
        assert len(preimage) == EXPP_PREIMAGE_LEN == 176
        # Digest byte-exact parity
        digest = compute_expp_digest(preimage)
        assert digest.hex() == self.EXPECTED_DIGEST_G_HEX, (
            f"EXPP digest drift detected — "
            f"got {digest.hex()}, "
            f"expected {self.EXPECTED_DIGEST_G_HEX}. "
            f"This means daemon-side build_expp_preimage has drifted "
            f"from pallet ``expire_policy_attested_payload`` byte "
            f"order — every attest_expire_policy sig will be silently "
            f"rejected until this is fixed."
        )

    def test_fixture_g_preimage_bytes_pinned(self):
        """Extra defense: pin the EXACT 176 bytes of the preimage,
        not just the digest. A bug that mangled the preimage but
        ended up with the same blake2 output by sheer chance would
        fool the digest check but not this one. (Such collisions are
        cryptographically infeasible, but the pin documents the
        canonical layout for an auditor reading this file.)"""
        preimage = build_expp_preimage(
            chain_id=self.TEST_CHAIN_ID,
            intent_id=self.INTENT_ID_G,
            policy_id=self.POLICY_ID_G,
            cardano_tx_hash=self.TX_HASH_G,
            observed_at_depth=self.DEPTH_G,
            observed_slot=self.SLOT_G,
            mainchain_genesis_hash=self.MC_GENESIS_G,
        )
        expected = (
            b"EXPP"
            + self.TEST_CHAIN_ID
            + self.INTENT_ID_G
            + self.POLICY_ID_G
            + self.TX_HASH_G
            + self.DEPTH_G.to_bytes(4, "little")
            + self.SLOT_G.to_bytes(8, "little")
            + self.MC_GENESIS_G
        )
        assert preimage == expected


# ---------------------------------------------------------------------------
# Dispatcher process_one — happy + refusal paths
# ---------------------------------------------------------------------------


def _make_attestor(
    *,
    substrate_client,
    observer,
    min_finality_depth: int = 15,
    max_concurrent: Optional[int] = None,
) -> ExpirePolicyAttestor:
    """Build an ExpirePolicyAttestor wired against the supplied mocks."""
    config = SimpleNamespace(
        ogmios_url="http://ogmios.test",
        kupo_url="http://kupo.test",
    )
    chain_lock = asyncio.Lock()
    return ExpirePolicyAttestor(
        config=config,
        substrate_client=substrate_client,
        chain_write_lock=chain_lock,
        observer=observer,
        min_finality_depth=min_finality_depth,
        poll_interval=12,
        max_concurrent=max_concurrent,
    )


class TestDispatcherHappyPath:
    def test_signs_and_submits_when_all_facts_agree(self):
        client = _make_substrate_client_stub()
        observer = _make_observer_stub()
        attestor = _make_attestor(
            substrate_client=client, observer=observer,
        )
        req = _default_pending_request()
        verdict = _run(attestor.process_one(req, CHAIN_ID))
        assert verdict.signed is True
        assert verdict.refusal_reason is None
        assert verdict.extrinsic_hash is not None
        # submit_attest_expire_policy was called with a 32B pubkey + 64B sig.
        client.submit_attest_expire_policy.assert_called_once()
        call_args = client.submit_attest_expire_policy.call_args
        # positional: (intent_id, pubkey, sig)
        assert call_args.args[0] == INTENT_ID
        assert len(call_args.args[1]) == 32
        assert len(call_args.args[2]) == 64

    def test_signs_with_observer_depth_not_request_depth(self):
        """PR #34 §3.2 makes ``observed_at_depth`` the attestor's
        independent observation. The dispatcher MUST commit to its own
        observer.depth, NOT echo back request.observed_at_depth.

        Mirrors the same property the STCA path established. An
        attacker who lied about their observed depth cannot influence
        the EXPP digest the committee signs — the digest binds to
        what the attestor saw, not what the requester claimed.
        """
        # observer reports depth = 30, request says 20.
        observer = _make_observer_stub(
            tip_block_no=1_000_000, tx_block_no=1_000_000 - 30,
        )
        client = _make_substrate_client_stub()
        attestor = _make_attestor(
            substrate_client=client, observer=observer,
        )
        # Request says 20 but observer sees 30. Both are >= 15
        # MinFinalityDepth so the attestor signs — but it signs over a
        # preimage with depth=30, not 20.
        req = _default_pending_request(observed_at_depth=20)
        verdict = _run(attestor.process_one(req, CHAIN_ID))
        assert verdict.signed is True

        # Reconstruct what the dispatcher SHOULD have signed and verify
        # the signature is valid against that preimage.
        expected_preimage = build_expp_preimage(
            chain_id=CHAIN_ID, intent_id=INTENT_ID,
            policy_id=POLICY_ID, cardano_tx_hash=CARDANO_TX_HASH,
            observed_at_depth=30,  # attestor's observation, NOT 20
            observed_slot=OBSERVED_SLOT,
            mainchain_genesis_hash=PREPROD_GENESIS,
        )
        expected_digest = compute_expp_digest(expected_preimage)
        # Pull the signature off submit_attest_expire_policy's call args.
        call_args = client.submit_attest_expire_policy.call_args
        sig = call_args.args[2]
        pubkey = call_args.args[1]
        # Verify sig over the digest using the same Keypair.
        kp = Keypair(public_key=pubkey, ss58_format=42)
        assert kp.verify(expected_digest, sig) is True

    def test_signs_with_chain_resolved_policy_id_not_request_witness(self):
        """The EXPP digest commits to the chain-state-resolved policy_id
        (from ``Intents[intent_id].kind``), NOT the requester's witness.
        Even if the witness matches today, the dispatcher MUST fetch
        the resolved value and commit to THAT in the preimage — closing
        the recycling attack class where a colluding requester binds an
        Expire-redeemer tx for one policy onto a different intent.

        Mirrors the same chain-state-binding property the STCA path
        established for ``voucher_digest``.
        """
        # Set up: request says POLICY_ID, chain returns a DIFFERENT id.
        # The dispatcher should refuse (POLICY_ID_WITNESS_MISMATCH), not
        # silently sign over the wrong digest.
        observer = _make_observer_stub()
        chain_resolved = bytes.fromhex("ab" * 32)
        client = _make_substrate_client_stub(
            resolved_policy_id=chain_resolved,
        )
        attestor = _make_attestor(
            substrate_client=client, observer=observer,
        )
        req = _default_pending_request(policy_id_witness=POLICY_ID)
        verdict = _run(attestor.process_one(req, CHAIN_ID))
        assert verdict.signed is False
        assert verdict.refusal_reason == RefusalReason.POLICY_ID_WITNESS_MISMATCH


class TestDispatcherRefusals:
    def test_refuses_on_genesis_mismatch(self):
        # Observer reports mainnet, request claims preprod.
        observer = _make_observer_stub(genesis_hash=MAINNET_GENESIS)
        client = _make_substrate_client_stub()
        attestor = _make_attestor(
            substrate_client=client, observer=observer,
        )
        req = _default_pending_request(
            mainchain_genesis_hash=PREPROD_GENESIS,
        )
        verdict = _run(attestor.process_one(req, CHAIN_ID))
        assert verdict.signed is False
        assert verdict.refusal_reason == RefusalReason.GENESIS_MISMATCH
        client.submit_attest_expire_policy.assert_not_called()

    def test_refuses_on_tx_not_found(self):
        observer = _make_observer_stub(
            mismatches=["kupo_no_matches_for_tx"],
            observed_slot=None,
        )
        client = _make_substrate_client_stub()
        attestor = _make_attestor(
            substrate_client=client, observer=observer,
        )
        req = _default_pending_request()
        verdict = _run(attestor.process_one(req, CHAIN_ID))
        assert verdict.signed is False
        assert verdict.refusal_reason == RefusalReason.TX_NOT_FOUND
        client.submit_attest_expire_policy.assert_not_called()

    def test_refuses_on_slot_mismatch(self):
        observer = _make_observer_stub(observed_slot=999_999)
        client = _make_substrate_client_stub()
        attestor = _make_attestor(
            substrate_client=client, observer=observer,
        )
        req = _default_pending_request(observed_slot=123_456)
        verdict = _run(attestor.process_one(req, CHAIN_ID))
        assert verdict.signed is False
        assert verdict.refusal_reason == RefusalReason.SLOT_MISMATCH
        client.submit_attest_expire_policy.assert_not_called()

    def test_refuses_when_depth_below_min_finality(self):
        # Observer reports depth = 5 (= 1000 tip - 995 tx).
        observer = _make_observer_stub(
            tip_block_no=1_000, tx_block_no=995,
        )
        client = _make_substrate_client_stub()
        attestor = _make_attestor(
            substrate_client=client, observer=observer,
            min_finality_depth=15,
        )
        req = _default_pending_request()
        verdict = _run(attestor.process_one(req, CHAIN_ID))
        assert verdict.signed is False
        assert verdict.refusal_reason == RefusalReason.FINALITY_BELOW_MIN
        assert "observed_depth=5" in (verdict.refusal_detail or "")
        client.submit_attest_expire_policy.assert_not_called()

    def test_refuses_when_intent_not_found(self):
        observer = _make_observer_stub()
        client = _make_substrate_client_stub(intent_status=None)
        attestor = _make_attestor(
            substrate_client=client, observer=observer,
        )
        req = _default_pending_request()
        verdict = _run(attestor.process_one(req, CHAIN_ID))
        assert verdict.signed is False
        assert verdict.refusal_reason == RefusalReason.INTENT_NOT_FOUND
        client.submit_attest_expire_policy.assert_not_called()

    def test_refuses_when_intent_already_expired(self):
        """Already-Expired is idempotent SKIP per pallet contract.
        Same legacy semantic ``settle_claim_attestor`` preserves for
        AlreadySettled — refuse with a structured reason but the
        operator does NOT retry."""
        observer = _make_observer_stub()
        client = _make_substrate_client_stub(intent_status="Expired")
        attestor = _make_attestor(
            substrate_client=client, observer=observer,
        )
        req = _default_pending_request()
        verdict = _run(attestor.process_one(req, CHAIN_ID))
        assert verdict.signed is False
        assert verdict.refusal_reason == RefusalReason.INTENT_ALREADY_TERMINAL
        assert "Expired" in (verdict.refusal_detail or "")
        client.submit_attest_expire_policy.assert_not_called()

    def test_refuses_when_intent_already_settled(self):
        """Settle-or-expire dichotomy: a settled intent cannot also
        expire. Pallet enforces with ``IntentNotEligibleForExpiry``;
        daemon side surfaces the same property structurally."""
        observer = _make_observer_stub()
        client = _make_substrate_client_stub(intent_status="Settled")
        attestor = _make_attestor(
            substrate_client=client, observer=observer,
        )
        req = _default_pending_request()
        verdict = _run(attestor.process_one(req, CHAIN_ID))
        assert verdict.signed is False
        assert verdict.refusal_reason == RefusalReason.INTENT_ALREADY_TERMINAL
        assert "Settled" in (verdict.refusal_detail or "")
        client.submit_attest_expire_policy.assert_not_called()

    def test_refuses_when_intent_already_refunded(self):
        observer = _make_observer_stub()
        client = _make_substrate_client_stub(intent_status="Refunded")
        attestor = _make_attestor(
            substrate_client=client, observer=observer,
        )
        req = _default_pending_request()
        verdict = _run(attestor.process_one(req, CHAIN_ID))
        assert verdict.signed is False
        assert verdict.refusal_reason == RefusalReason.INTENT_ALREADY_TERMINAL
        assert "Refunded" in (verdict.refusal_detail or "")
        client.submit_attest_expire_policy.assert_not_called()

    def test_refuses_when_policy_id_witness_mismatches(self):
        # Chain-state resolved policy_id = X; request witness = Y. Refuse.
        observer = _make_observer_stub()
        chain_resolved = bytes.fromhex("aa" * 32)
        request_witness = bytes.fromhex("bb" * 32)
        client = _make_substrate_client_stub(
            resolved_policy_id=chain_resolved,
        )
        attestor = _make_attestor(
            substrate_client=client, observer=observer,
        )
        req = _default_pending_request(policy_id_witness=request_witness)
        verdict = _run(attestor.process_one(req, CHAIN_ID))
        assert verdict.signed is False
        assert verdict.refusal_reason == RefusalReason.POLICY_ID_WITNESS_MISMATCH
        client.submit_attest_expire_policy.assert_not_called()

    def test_refuses_when_intent_is_refund_credit(self):
        """``RefundCredit`` intents have no Cardano-side policy and
        cannot be expire-redeemed. Pallet rejects via
        ``IntentNotEligibleForExpiry``;
        ``get_policy_id_for_intent`` returns None for this variant,
        which the dispatcher surfaces as POLICY_ID_WITNESS_MISMATCH /
        detail ``intent_kind_has_no_policy_id``."""
        observer = _make_observer_stub()
        client = _make_substrate_client_stub(resolved_policy_id=None)
        attestor = _make_attestor(
            substrate_client=client, observer=observer,
        )
        req = _default_pending_request()
        verdict = _run(attestor.process_one(req, CHAIN_ID))
        assert verdict.signed is False
        assert verdict.refusal_reason == RefusalReason.POLICY_ID_WITNESS_MISMATCH
        assert "no_policy_id" in (verdict.refusal_detail or "")
        client.submit_attest_expire_policy.assert_not_called()

    def test_refuses_when_observer_genesis_unavailable(self):
        """Observer's genesis lookup failed (Ogmios down). Soft refusal
        — operator-unavailable rather than safety mismatch."""
        observer = _make_observer_stub()
        observer.get_genesis_hash = AsyncMock(return_value=None)
        client = _make_substrate_client_stub()
        attestor = _make_attestor(
            substrate_client=client, observer=observer,
        )
        req = _default_pending_request()
        verdict = _run(attestor.process_one(req, CHAIN_ID))
        assert verdict.signed is False
        assert verdict.refusal_reason == RefusalReason.OBSERVER_UNAVAILABLE
        client.submit_attest_expire_policy.assert_not_called()


class TestDispatcherConcurrency:
    def test_semaphore_caps_concurrent_dispatch(self):
        """Cap parallel attestation submissions per memo §6 OQ#10.

        We construct an ExpirePolicyAttestor with max_concurrent=2 and
        fire 5 simultaneous process_one calls. The dispatcher should
        serialise them through a semaphore of 2 — mirroring the same
        property the settle_claim path proves.
        """
        observer = _make_observer_stub()
        client = _make_substrate_client_stub()
        attestor = _make_attestor(
            substrate_client=client, observer=observer,
            max_concurrent=2,
        )

        # Track concurrent in-flight count by patching the observer's
        # observe method to track entry/exit.
        in_flight = 0
        max_in_flight = 0
        gate = asyncio.Event()  # holds all coroutines at the same instant

        async def slow_observe(tx_hex, bene_hash):
            nonlocal in_flight, max_in_flight
            in_flight += 1
            max_in_flight = max(max_in_flight, in_flight)
            await gate.wait()
            in_flight -= 1
            obs = CardanoTxObservation(tx_hash_hex=tx_hex)
            obs.cardano_tip_block_no = 1_000_000
            obs.tx_block_no = 1_000_000 - OBSERVED_DEPTH
            obs.observed_slot = OBSERVED_SLOT
            obs.matched_address_lovelace = None
            obs.beneficiary_addr_blake2_224 = None
            obs.mainchain_genesis_hash = PREPROD_GENESIS
            return obs

        observer.observe = slow_observe

        async def driver():
            req = _default_pending_request()
            coros = [attestor.process_one(req, CHAIN_ID) for _ in range(5)]
            tasks = [asyncio.create_task(c) for c in coros]
            # Yield enough to let the bounded set enter the slow region.
            for _ in range(10):
                await asyncio.sleep(0)
            assert max_in_flight <= 2
            gate.set()
            await asyncio.gather(*tasks)
            return max_in_flight

        observed_peak = _run(driver())
        # The sem cap MUST hold: never more than 2 concurrent processings.
        assert observed_peak <= 2
        assert observed_peak >= 1


# ---------------------------------------------------------------------------
# Factory: soft-disable and config wiring
# ---------------------------------------------------------------------------


class TestFactorySoftDisable:
    def test_returns_none_when_ogmios_unset(self):
        config = SimpleNamespace(ogmios_url="", kupo_url="http://kupo")
        client = _make_substrate_client_stub()
        chain_lock = asyncio.Lock()
        with patch.dict(os.environ, {"KUPO_URL": ""}, clear=False):
            result = maybe_create_expire_policy_attestor(
                config, client, chain_lock,
            )
        assert result is None

    def test_returns_none_when_kupo_unset(self):
        config = SimpleNamespace(
            ogmios_url="http://ogmios.test", kupo_url="",
        )
        client = _make_substrate_client_stub()
        chain_lock = asyncio.Lock()
        with patch.dict(os.environ, {"KUPO_URL": ""}, clear=False):
            result = maybe_create_expire_policy_attestor(
                config, client, chain_lock,
            )
        assert result is None

    def test_returns_attestor_when_both_set(self):
        config = SimpleNamespace(
            ogmios_url="http://ogmios.test",
            kupo_url="http://kupo.test",
        )
        client = _make_substrate_client_stub()
        client.get_min_finality_depth = MagicMock(return_value=15)
        chain_lock = asyncio.Lock()
        with patch.dict(
            os.environ, {"KUPO_URL": "http://kupo.test"}, clear=False,
        ):
            result = maybe_create_expire_policy_attestor(
                config, client, chain_lock,
            )
        assert isinstance(result, ExpirePolicyAttestor)
        assert result.min_finality_depth == 15

    def test_factory_uses_chain_min_finality_depth_when_available(self):
        config = SimpleNamespace(
            ogmios_url="http://ogmios.test",
            kupo_url="http://kupo.test",
        )
        client = _make_substrate_client_stub()
        client.get_min_finality_depth = MagicMock(return_value=30)
        chain_lock = asyncio.Lock()
        with patch.dict(
            os.environ,
            {
                "KUPO_URL": "http://kupo.test",
                "EXPIRE_ATTESTOR_MIN_FINALITY_DEPTH": "5",  # ignored
            },
            clear=False,
        ):
            result = maybe_create_expire_policy_attestor(
                config, client, chain_lock,
            )
        # On-chain value wins over env fallback.
        assert result is not None
        assert result.min_finality_depth == 30

    def test_factory_falls_back_to_env_when_chain_constant_missing(self):
        config = SimpleNamespace(
            ogmios_url="http://ogmios.test",
            kupo_url="http://kupo.test",
        )
        client = _make_substrate_client_stub()
        client.get_min_finality_depth = MagicMock(return_value=None)
        chain_lock = asyncio.Lock()
        with patch.dict(
            os.environ,
            {
                "KUPO_URL": "http://kupo.test",
                "EXPIRE_ATTESTOR_MIN_FINALITY_DEPTH": "25",
            },
            clear=False,
        ):
            result = maybe_create_expire_policy_attestor(
                config, client, chain_lock,
            )
        assert result is not None
        assert result.min_finality_depth == 25

    def test_factory_caps_concurrent_at_8_by_default(self):
        config = SimpleNamespace(
            ogmios_url="http://ogmios.test",
            kupo_url="http://kupo.test",
        )
        client = _make_substrate_client_stub()
        client.get_min_finality_depth = MagicMock(return_value=15)
        chain_lock = asyncio.Lock()
        with patch.dict(
            os.environ,
            {"KUPO_URL": "http://kupo.test"},
            clear=False,
        ):
            # ensure env var doesn't accidentally pre-set this in CI
            os.environ.pop("EXPIRE_ATTESTOR_MAX_CONCURRENT", None)
            result = maybe_create_expire_policy_attestor(
                config, client, chain_lock,
            )
        assert result is not None
        # Sem cap should default to 8 (memo §6 OQ#10).
        assert result._sem._value == 8  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# _tick dict-to-dataclass conversion (same regression class the settle
# attestor closed in security-review post-PR #266 — keep parity here so
# the EXPP path is robust against the same shape of bug).
# ---------------------------------------------------------------------------


class TestTickDictToDataclassConversion:
    def test_tick_converts_dict_rows_to_pending_expiry_request(self):
        """_tick must convert each list_pending_expiry_requests dict
        into a PendingExpiryRequest before calling process_one — the
        substrate_client deliberately returns dicts (see its docstring)
        but process_one is typed as the dataclass and uses attribute
        access. Without this conversion, the daemon AttributeErrors on
        every real pending request from chain."""
        observer = _make_observer_stub()
        client = _make_substrate_client_stub()
        dict_row = dict(
            intent_id=INTENT_ID,
            requester="5DummyAccountId1",
            submitted_block=100,
            cardano_tx_hash=CARDANO_TX_HASH,
            observed_at_depth=OBSERVED_DEPTH,
            observed_slot=OBSERVED_SLOT,
            mainchain_genesis_hash=PREPROD_GENESIS,
            policy_id_witness=POLICY_ID,
        )
        client.list_pending_expiry_requests = MagicMock(
            return_value=[dict_row]
        )
        attestor = _make_attestor(
            substrate_client=client, observer=observer,
        )

        captured: list = []
        original_process_one = attestor.process_one

        async def capture(req, chain_id):
            captured.append(req)
            return await original_process_one(req, chain_id)

        attestor.process_one = capture  # type: ignore[method-assign]

        _run(attestor._tick(CHAIN_ID))

        assert len(captured) == 1
        assert isinstance(captured[0], PendingExpiryRequest)
        assert captured[0].intent_id == INTENT_ID
        assert captured[0].cardano_tx_hash == CARDANO_TX_HASH
        assert captured[0].policy_id_witness == POLICY_ID

    def test_tick_skips_malformed_row_without_killing_batch(self):
        """A dict missing required dataclass fields is logged + skipped;
        well-formed rows in the same batch still get processed."""
        observer = _make_observer_stub()
        client = _make_substrate_client_stub()
        good_row = dict(
            intent_id=INTENT_ID,
            requester="5DummyAccountId1",
            submitted_block=100,
            cardano_tx_hash=CARDANO_TX_HASH,
            observed_at_depth=OBSERVED_DEPTH,
            observed_slot=OBSERVED_SLOT,
            mainchain_genesis_hash=PREPROD_GENESIS,
            policy_id_witness=POLICY_ID,
        )
        bad_row = {"intent_id": INTENT_ID}  # missing every other field
        client.list_pending_expiry_requests = MagicMock(
            return_value=[bad_row, good_row]
        )
        attestor = _make_attestor(
            substrate_client=client, observer=observer,
        )

        captured: list = []
        original_process_one = attestor.process_one

        async def capture(req, chain_id):
            captured.append(req)
            return await original_process_one(req, chain_id)

        attestor.process_one = capture  # type: ignore[method-assign]

        _run(attestor._tick(CHAIN_ID))

        assert len(captured) == 1
        assert isinstance(captured[0], PendingExpiryRequest)
        assert captured[0].intent_id == INTENT_ID


# ---------------------------------------------------------------------------
# SubstrateClient helpers — direct unit tests for the new methods added
# alongside the attestor. The dispatcher's behavior is tested above with
# stubs; these unit tests make sure the helpers handle the substrate-
# interface decoder's various shapes correctly (a SCALE enum can decode
# as bare string, dict envelope, or integer depending on the type
# registry).
# ---------------------------------------------------------------------------


class TestSubstrateClientGetIntentStatus:
    def _client(self, query_result_value):
        """Build a minimal SubstrateClient with `substrate.query` returning
        a SimpleNamespace(.value=...) shape (mirrors the substrate-interface
        QueryMapResult convention)."""
        from daemon.substrate_client import SubstrateClient
        from daemon.config import DaemonConfig

        config = DaemonConfig(
            rpc_url="ws://localhost",
            signer_uri="//Alice",
            data_dir="/tmp",
            state_file="/tmp/state.json",
        )
        client = SubstrateClient.__new__(SubstrateClient)
        client.config = config
        client.substrate = SimpleNamespace(
            query=MagicMock(
                return_value=SimpleNamespace(value=query_result_value)
            ),
        )
        return client

    def test_decodes_bare_string_variant(self):
        """SCALE enum sometimes decodes as bare string `"Pending"`."""
        client = self._client({"status": "Pending", "kind": {}})
        assert client.get_intent_status(b"\x00" * 32) == "Pending"

    def test_decodes_dict_envelope_variant(self):
        """SCALE enum more commonly decodes as `{"Pending": null}`."""
        client = self._client({"status": {"Vouchered": None}, "kind": {}})
        assert client.get_intent_status(b"\x00" * 32) == "Vouchered"

    def test_decodes_integer_variant(self):
        """Rare codepath — substrate-interface in some versions emits an
        int discriminant for compact enums."""
        client = self._client({"status": 4, "kind": {}})  # Expired
        assert client.get_intent_status(b"\x00" * 32) == "Expired"

    def test_returns_none_when_intent_absent(self):
        client = self._client(None)
        assert client.get_intent_status(b"\x00" * 32) is None

    def test_returns_none_when_status_field_missing(self):
        client = self._client({"kind": {}})  # missing status
        assert client.get_intent_status(b"\x00" * 32) is None


class TestSubstrateClientGetPolicyIdForIntent:
    def _client(self, query_result_value):
        from daemon.substrate_client import SubstrateClient
        from daemon.config import DaemonConfig

        config = DaemonConfig(
            rpc_url="ws://localhost",
            signer_uri="//Alice",
            data_dir="/tmp",
            state_file="/tmp/state.json",
        )
        client = SubstrateClient.__new__(SubstrateClient)
        client.config = config
        client.substrate = SimpleNamespace(
            query=MagicMock(
                return_value=SimpleNamespace(value=query_result_value)
            ),
        )
        return client

    def test_buy_policy_returns_product_id(self):
        """`BuyPolicy { product_id, .. }` — product_id IS the policy_id."""
        product_id = bytes([0xAB] * 32)
        client = self._client({
            "status": "Pending",
            "kind": {
                "BuyPolicy": {
                    "product_id": "0x" + product_id.hex(),
                    "strike": 100,
                    "term_slots": 10,
                    "premium_ada": 1_000_000,
                    "beneficiary_cardano_addr": [],
                },
            },
        })
        result = client.get_policy_id_for_intent(b"\x00" * 32)
        assert result == product_id

    def test_request_payout_returns_policy_id(self):
        """`RequestPayout { policy_id, .. }` — policy_id direct."""
        policy_id = bytes([0xCD] * 32)
        client = self._client({
            "status": "Pending",
            "kind": {
                "RequestPayout": {
                    "policy_id": "0x" + policy_id.hex(),
                    "oracle_evidence": [],
                },
            },
        })
        result = client.get_policy_id_for_intent(b"\x00" * 32)
        assert result == policy_id

    def test_refund_credit_returns_none(self):
        """`RefundCredit { amount_ada }` — no Cardano-side policy."""
        client = self._client({
            "status": "Pending",
            "kind": {
                "RefundCredit": {"amount_ada": 5_000_000},
            },
        })
        assert client.get_policy_id_for_intent(b"\x00" * 32) is None

    def test_unknown_variant_returns_none(self):
        """An unknown IntentKind variant (e.g. a future spec adds
        another kind we don't recognize yet) MUST refuse to fabricate
        a policy id rather than guess."""
        client = self._client({
            "status": "Pending",
            "kind": {"FutureKind": {"foo": "bar"}},
        })
        assert client.get_policy_id_for_intent(b"\x00" * 32) is None

    def test_intent_absent_returns_none(self):
        client = self._client(None)
        assert client.get_policy_id_for_intent(b"\x00" * 32) is None
