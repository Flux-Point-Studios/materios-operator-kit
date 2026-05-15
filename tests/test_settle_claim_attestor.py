"""Tests for `daemon.settle_claim_attestor` (task #266).

These exercise the cardano-tx-confirmed attestation type end-to-end at
the dispatcher seam, with mocks at the substrate-interface + aiohttp
+ Ogmios/Kupo boundaries. No live chain or Cardano follower is required.

Coverage matrix:

    STCA preimage builder (memo §3.2):
      - byte order pinned exactly
      - total length 213B (4B tag + 209B content)
      - rejects wrong widths / out-of-range integers
      - matches a pinned reference vector

    Observer (CardanoTxObserver):
      - Ogmios tip + genesis-hash queries
      - Kupo tx lookup by hash
      - blake2_224 of bech32 address
      - depth = tip_block_no - tx_block_no
      - mismatches populated when tx not found

    SettleClaimAttestor.process_one (the dispatcher):
      - happy path → sign + submit
      - refusal: genesis mismatch
      - refusal: tx not found
      - refusal: amount mismatch
      - refusal: slot mismatch
      - refusal: depth below MinFinalityDepth
      - refusal: voucher_digest mismatch (chain-state vs request)
      - refusal: voucher_digest unavailable (no voucher row)

    Concurrency:
      - max_concurrent semaphore caps parallel dispatch

    Factory (maybe_create_settle_claim_attestor):
      - missing ogmios_url → None
      - missing kupo_url → None
      - both present → SettleClaimAttestor
      - MinFinalityDepth pulled from chain when available
      - falls back to env default when chain constant unreadable
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

from daemon.settle_claim_attestor import (
    CardanoTxObservation,
    PendingSettlementRequest,
    RefusalReason,
    SettleClaimAttestor,
    STCA_CONTENT_LEN,
    STCA_PREIMAGE_LEN,
    TAG_STCA,
    blake2_224_of_cardano_address,
    build_stca_preimage,
    compute_stca_digest,
    maybe_create_settle_claim_attestor,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run(coro):
    """Mirror the harness used by test_evidence_submitter.py — repo
    doesn't ship pytest-asyncio so each async test owns its event loop.
    """
    return asyncio.new_event_loop().run_until_complete(coro)


CHAIN_ID = bytes.fromhex("11" * 32)
CLAIM_ID = bytes.fromhex("22" * 32)
VOUCHER_DIGEST = bytes.fromhex("33" * 32)
CARDANO_TX_HASH = bytes.fromhex("44" * 32)
BENE_HASH_28 = bytes.fromhex("55" * 28)
MAINNET_GENESIS = bytes.fromhex("66" * 32)
PREPROD_GENESIS = bytes.fromhex("77" * 32)
AMOUNT_LOVELACE = 5_000_000  # 5 ADA
OBSERVED_SLOT = 123_456
OBSERVED_DEPTH = 20


def _default_pending_request(**overrides) -> PendingSettlementRequest:
    base = dict(
        claim_id=CLAIM_ID,
        requester="5DummyAccountId1",
        submitted_block=100,
        settled_direct=True,
        cardano_tx_hash=CARDANO_TX_HASH,
        observed_at_depth=OBSERVED_DEPTH,
        observed_slot=OBSERVED_SLOT,
        beneficiary_addr_hash=BENE_HASH_28,
        amount_lovelace=AMOUNT_LOVELACE,
        mainchain_genesis_hash=PREPROD_GENESIS,
        voucher_digest=VOUCHER_DIGEST,
    )
    base.update(overrides)
    return PendingSettlementRequest(**base)


def _make_substrate_client_stub(
    *,
    voucher_digest: Optional[bytes] = VOUCHER_DIGEST,
    voucher: Optional[dict] = None,
    submit_ext_hash: Optional[str] = "0x" + ("ee" * 32),
    genesis_hex: str = "0x" + ("aa" * 32),
):
    """Build a SubstrateClient-compatible stub for the dispatcher to call.

    Mirrors the methods the attestor exercises:
      - keypair (sr25519, real Keypair so signatures are real bytes)
      - get_voucher_digest(claim_id) -> bytes | None
      - get_voucher(claim_id) -> dict | None
      - submit_attest_settle(claim_id, pubkey, sig) -> str | None
      - get_genesis_hash() -> hex str
      - list_pending_settlement_requests() -> list (unused per-test)
    """
    kp = Keypair.create_from_uri("//Alice")
    stub = SimpleNamespace(
        keypair=kp,
        get_voucher_digest=MagicMock(return_value=voucher_digest),
        get_voucher=MagicMock(return_value=voucher),
        submit_attest_settle=MagicMock(return_value=submit_ext_hash),
        get_genesis_hash=MagicMock(return_value=genesis_hex),
        list_pending_settlement_requests=MagicMock(return_value=[]),
    )
    return stub


def _make_observer_stub(
    *,
    tip_block_no: Optional[int] = 1_000_000,
    tx_block_no: Optional[int] = 1_000_000 - OBSERVED_DEPTH,
    observed_slot: Optional[int] = OBSERVED_SLOT,
    matched_lovelace: Optional[int] = AMOUNT_LOVELACE,
    genesis_hash: bytes = PREPROD_GENESIS,
    mismatches: Optional[list[str]] = None,
):
    """Build a CardanoTxObserver-shaped stub with controllable returns."""
    obs = CardanoTxObservation(tx_hash_hex=CARDANO_TX_HASH.hex())
    obs.cardano_tip_block_no = tip_block_no
    obs.tx_block_no = tx_block_no
    obs.observed_slot = observed_slot
    obs.matched_address_lovelace = matched_lovelace
    obs.beneficiary_addr_blake2_224 = BENE_HASH_28
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
# STCA preimage builder
# ---------------------------------------------------------------------------


class TestStcaPreimage:
    def test_total_length_is_213_bytes(self):
        preimage = build_stca_preimage(
            chain_id=CHAIN_ID,
            claim_id=CLAIM_ID,
            voucher_digest=VOUCHER_DIGEST,
            cardano_tx_hash=CARDANO_TX_HASH,
            settled_direct=True,
            beneficiary_addr_blake2_224=BENE_HASH_28,
            amount_lovelace=AMOUNT_LOVELACE,
            observed_at_depth=OBSERVED_DEPTH,
            observed_slot=OBSERVED_SLOT,
            mainchain_genesis_hash=PREPROD_GENESIS,
        )
        assert len(preimage) == STCA_PREIMAGE_LEN == 213
        # The memo quotes "209 bytes preimage" which is the CONTENT
        # (everything after the 4-byte tag).
        assert STCA_PREIMAGE_LEN - len(TAG_STCA) == STCA_CONTENT_LEN == 209

    def test_tag_prefix_is_STCA(self):
        preimage = build_stca_preimage(
            chain_id=CHAIN_ID,
            claim_id=CLAIM_ID,
            voucher_digest=VOUCHER_DIGEST,
            cardano_tx_hash=CARDANO_TX_HASH,
            settled_direct=False,
            beneficiary_addr_blake2_224=BENE_HASH_28,
            amount_lovelace=AMOUNT_LOVELACE,
            observed_at_depth=OBSERVED_DEPTH,
            observed_slot=OBSERVED_SLOT,
            mainchain_genesis_hash=PREPROD_GENESIS,
        )
        assert preimage[:4] == b"STCA"

    def test_byte_order_exact_per_memo_section_3_2(self):
        """Pin the byte order against the memo §3.2 spec.

        Layout:
            tag (4B) || chain_id (32) || claim_id (32) || voucher_digest (32)
            || cardano_tx_hash (32) || settled_direct (1) || bene (28)
            || amount_le (8) || depth_le (4) || slot_le (8) || mc_genesis (32)
        """
        preimage = build_stca_preimage(
            chain_id=CHAIN_ID,
            claim_id=CLAIM_ID,
            voucher_digest=VOUCHER_DIGEST,
            cardano_tx_hash=CARDANO_TX_HASH,
            settled_direct=True,
            beneficiary_addr_blake2_224=BENE_HASH_28,
            amount_lovelace=AMOUNT_LOVELACE,
            observed_at_depth=OBSERVED_DEPTH,
            observed_slot=OBSERVED_SLOT,
            mainchain_genesis_hash=MAINNET_GENESIS,
        )
        # offsets per memo §3.2
        pos = 0
        assert preimage[pos:pos + 4] == b"STCA"
        pos += 4
        assert preimage[pos:pos + 32] == CHAIN_ID
        pos += 32
        assert preimage[pos:pos + 32] == CLAIM_ID
        pos += 32
        assert preimage[pos:pos + 32] == VOUCHER_DIGEST
        pos += 32
        assert preimage[pos:pos + 32] == CARDANO_TX_HASH
        pos += 32
        assert preimage[pos] == 1  # settled_direct=True
        pos += 1
        assert preimage[pos:pos + 28] == BENE_HASH_28
        pos += 28
        assert preimage[pos:pos + 8] == AMOUNT_LOVELACE.to_bytes(8, "little")
        pos += 8
        assert preimage[pos:pos + 4] == OBSERVED_DEPTH.to_bytes(4, "little")
        pos += 4
        assert preimage[pos:pos + 8] == OBSERVED_SLOT.to_bytes(8, "little")
        pos += 8
        assert preimage[pos:pos + 32] == MAINNET_GENESIS
        pos += 32
        assert pos == STCA_PREIMAGE_LEN

    def test_settled_direct_false_writes_zero(self):
        preimage = build_stca_preimage(
            chain_id=CHAIN_ID, claim_id=CLAIM_ID,
            voucher_digest=VOUCHER_DIGEST, cardano_tx_hash=CARDANO_TX_HASH,
            settled_direct=False, beneficiary_addr_blake2_224=BENE_HASH_28,
            amount_lovelace=AMOUNT_LOVELACE,
            observed_at_depth=OBSERVED_DEPTH, observed_slot=OBSERVED_SLOT,
            mainchain_genesis_hash=PREPROD_GENESIS,
        )
        # settled_direct byte is at offset 4 + 32*4 = 132
        assert preimage[132] == 0

    def test_rejects_wrong_chain_id_length(self):
        with pytest.raises(ValueError, match="chain_id"):
            build_stca_preimage(
                chain_id=b"\x00" * 31,  # 31 not 32
                claim_id=CLAIM_ID, voucher_digest=VOUCHER_DIGEST,
                cardano_tx_hash=CARDANO_TX_HASH, settled_direct=True,
                beneficiary_addr_blake2_224=BENE_HASH_28,
                amount_lovelace=0, observed_at_depth=0,
                observed_slot=0, mainchain_genesis_hash=PREPROD_GENESIS,
            )

    def test_rejects_wrong_beneficiary_hash_length(self):
        with pytest.raises(ValueError, match="beneficiary"):
            build_stca_preimage(
                chain_id=CHAIN_ID, claim_id=CLAIM_ID,
                voucher_digest=VOUCHER_DIGEST,
                cardano_tx_hash=CARDANO_TX_HASH, settled_direct=True,
                beneficiary_addr_blake2_224=b"\x00" * 32,  # blake2_256 not 224
                amount_lovelace=0, observed_at_depth=0,
                observed_slot=0, mainchain_genesis_hash=PREPROD_GENESIS,
            )

    def test_rejects_overflowing_amount(self):
        with pytest.raises(ValueError, match="amount_lovelace"):
            build_stca_preimage(
                chain_id=CHAIN_ID, claim_id=CLAIM_ID,
                voucher_digest=VOUCHER_DIGEST,
                cardano_tx_hash=CARDANO_TX_HASH, settled_direct=True,
                beneficiary_addr_blake2_224=BENE_HASH_28,
                amount_lovelace=2**64,  # one past u64 cap
                observed_at_depth=0, observed_slot=0,
                mainchain_genesis_hash=PREPROD_GENESIS,
            )

    def test_distinct_inputs_produce_distinct_preimages(self):
        a = build_stca_preimage(
            chain_id=CHAIN_ID, claim_id=CLAIM_ID,
            voucher_digest=VOUCHER_DIGEST,
            cardano_tx_hash=CARDANO_TX_HASH, settled_direct=True,
            beneficiary_addr_blake2_224=BENE_HASH_28,
            amount_lovelace=AMOUNT_LOVELACE,
            observed_at_depth=OBSERVED_DEPTH, observed_slot=OBSERVED_SLOT,
            mainchain_genesis_hash=PREPROD_GENESIS,
        )
        # Same inputs but a different claim_id.
        b = build_stca_preimage(
            chain_id=CHAIN_ID, claim_id=bytes.fromhex("99" * 32),
            voucher_digest=VOUCHER_DIGEST,
            cardano_tx_hash=CARDANO_TX_HASH, settled_direct=True,
            beneficiary_addr_blake2_224=BENE_HASH_28,
            amount_lovelace=AMOUNT_LOVELACE,
            observed_at_depth=OBSERVED_DEPTH, observed_slot=OBSERVED_SLOT,
            mainchain_genesis_hash=PREPROD_GENESIS,
        )
        assert a != b
        assert compute_stca_digest(a) != compute_stca_digest(b)


class TestStcaDigest:
    def test_digest_is_32_bytes_blake2(self):
        preimage = build_stca_preimage(
            chain_id=CHAIN_ID, claim_id=CLAIM_ID,
            voucher_digest=VOUCHER_DIGEST,
            cardano_tx_hash=CARDANO_TX_HASH, settled_direct=True,
            beneficiary_addr_blake2_224=BENE_HASH_28,
            amount_lovelace=AMOUNT_LOVELACE,
            observed_at_depth=OBSERVED_DEPTH, observed_slot=OBSERVED_SLOT,
            mainchain_genesis_hash=PREPROD_GENESIS,
        )
        digest = compute_stca_digest(preimage)
        assert len(digest) == 32
        # Confirm it really is blake2b-256 of the preimage.
        expected = hashlib.blake2b(preimage, digest_size=32).digest()
        assert digest == expected

    def test_digest_rejects_wrong_preimage_length(self):
        with pytest.raises(ValueError, match="preimage"):
            compute_stca_digest(b"\x00" * 100)


# ---------------------------------------------------------------------------
# Observer helpers
# ---------------------------------------------------------------------------


class TestObserverHelpers:
    def test_address_extraction_is_28_bytes_and_matches_payment_hash(self):
        """Post-PR #272 the legacy `blake2_224_of_cardano_address` is a
        thin shim that delegates to `extract_payment_hash_from_cardano_address`.
        The returned bytes are the 28-byte payment-key hash from
        CIP-0019 [1..29], NOT a blake2_224 hash of the bech32 string.

        Verified via round-trip: construct a known CIP-0019 type-0
        testnet address with payment_hash = 0x11*28, encode to bech32,
        decode via the shim, assert we get back exactly 0x11*28.
        """
        # Construct a CIP-0019 type-0 testnet address: header(1) + payment(28) + stake(28) = 57B.
        from tests.test_cardano_address import _bech32_encode_addr_test
        header = bytes([0x00])  # type 0 base address, testnet (network nibble = 0)
        payment_hash = b"\x11" * 28
        stake_hash = b"\x22" * 28
        raw = header + payment_hash + stake_hash
        addr = _bech32_encode_addr_test(raw)
        got = blake2_224_of_cardano_address(addr)
        assert len(got) == 28
        assert got == payment_hash, (
            f"expected payment-key extraction at [1..29], "
            f"got {got.hex()} vs {payment_hash.hex()}"
        )

    def test_observation_ok_property_when_complete(self):
        obs = CardanoTxObservation(tx_hash_hex="aa" * 32)
        obs.cardano_tip_block_no = 100
        obs.tx_block_no = 80
        obs.observed_slot = 1234
        obs.matched_address_lovelace = 5_000_000
        obs.beneficiary_addr_blake2_224 = BENE_HASH_28
        obs.mainchain_genesis_hash = PREPROD_GENESIS
        assert obs.ok is True
        assert obs.depth == 20

    def test_observation_not_ok_when_mismatches_present(self):
        obs = CardanoTxObservation(tx_hash_hex="aa" * 32)
        obs.cardano_tip_block_no = 100
        obs.tx_block_no = 80
        obs.observed_slot = 1234
        obs.matched_address_lovelace = 5_000_000
        obs.beneficiary_addr_blake2_224 = BENE_HASH_28
        obs.mainchain_genesis_hash = PREPROD_GENESIS
        obs.mismatches.append("kupo_no_matches_for_tx")
        assert obs.ok is False

    def test_depth_is_none_when_tip_or_block_unknown(self):
        obs = CardanoTxObservation(tx_hash_hex="aa" * 32)
        assert obs.depth is None
        obs.cardano_tip_block_no = 100
        assert obs.depth is None
        obs.tx_block_no = 50
        assert obs.depth == 50

    def test_depth_floor_is_zero(self):
        """Negative depth (tx in the future relative to our tip) clamps to
        zero so the attestor refuses with FINALITY_BELOW_MIN instead of
        silently passing a negative integer through to the u32 field."""
        obs = CardanoTxObservation(tx_hash_hex="aa" * 32)
        obs.cardano_tip_block_no = 50
        obs.tx_block_no = 100
        assert obs.depth == 0


# ---------------------------------------------------------------------------
# Dispatcher process_one — happy + refusal paths
# ---------------------------------------------------------------------------


def _make_attestor(
    *,
    substrate_client,
    observer,
    min_finality_depth: int = 15,
    max_concurrent: Optional[int] = None,
) -> SettleClaimAttestor:
    """Build a SettleClaimAttestor wired against the supplied mocks."""
    config = SimpleNamespace(
        ogmios_url="http://ogmios.test",
        kupo_url="http://kupo.test",
    )
    chain_lock = asyncio.Lock()
    return SettleClaimAttestor(
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
        # submit_attest_settle was called with a 32B pubkey + 64B sig.
        client.submit_attest_settle.assert_called_once()
        call_args = client.submit_attest_settle.call_args
        # positional: (claim_id, pubkey, sig)
        assert call_args.args[0] == CLAIM_ID
        assert len(call_args.args[1]) == 32
        assert len(call_args.args[2]) == 64

    def test_signs_with_observer_depth_not_request_depth(self):
        """Memo §3.2 makes ``observed_at_depth`` the attestor's
        independent observation. The dispatcher MUST commit to its own
        observer.depth, NOT echo back request.observed_at_depth."""
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
        expected_preimage = build_stca_preimage(
            chain_id=CHAIN_ID, claim_id=CLAIM_ID,
            voucher_digest=VOUCHER_DIGEST,
            cardano_tx_hash=CARDANO_TX_HASH, settled_direct=True,
            beneficiary_addr_blake2_224=BENE_HASH_28,
            amount_lovelace=AMOUNT_LOVELACE,
            observed_at_depth=30,  # attestor's observation, NOT 20
            observed_slot=OBSERVED_SLOT,
            mainchain_genesis_hash=PREPROD_GENESIS,
        )
        expected_digest = compute_stca_digest(expected_preimage)
        # Pull the signature off submit_attest_settle's call args.
        call_args = client.submit_attest_settle.call_args
        sig = call_args.args[2]
        pubkey = call_args.args[1]
        # Verify sig over the digest using the same Keypair.
        kp = Keypair(public_key=pubkey, ss58_format=42)
        assert kp.verify(expected_digest, sig) is True


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
        client.submit_attest_settle.assert_not_called()

    def test_refuses_on_tx_not_found(self):
        observer = _make_observer_stub(
            mismatches=["kupo_no_matches_for_tx"],
            matched_lovelace=None,
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
        client.submit_attest_settle.assert_not_called()

    def test_refuses_on_amount_mismatch(self):
        # Observer says 4 ADA was paid; request claims 5.
        observer = _make_observer_stub(matched_lovelace=4_000_000)
        client = _make_substrate_client_stub()
        attestor = _make_attestor(
            substrate_client=client, observer=observer,
        )
        req = _default_pending_request(amount_lovelace=5_000_000)
        verdict = _run(attestor.process_one(req, CHAIN_ID))
        assert verdict.signed is False
        assert verdict.refusal_reason == RefusalReason.AMOUNT_MISMATCH
        assert "4000000" in (verdict.refusal_detail or "")
        client.submit_attest_settle.assert_not_called()

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
        client.submit_attest_settle.assert_not_called()

    def test_refuses_when_depth_below_min_finality(self):
        # Observer reports depth = 5 (= 1_000 tip - 995 tx).
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
        client.submit_attest_settle.assert_not_called()

    def test_refuses_when_voucher_digest_unavailable(self):
        observer = _make_observer_stub()
        client = _make_substrate_client_stub(voucher_digest=None)
        attestor = _make_attestor(
            substrate_client=client, observer=observer,
        )
        req = _default_pending_request()
        verdict = _run(attestor.process_one(req, CHAIN_ID))
        assert verdict.signed is False
        assert verdict.refusal_reason == RefusalReason.VOUCHER_DIGEST_MISMATCH
        assert verdict.refusal_detail == "no_voucher_for_claim_id"
        client.submit_attest_settle.assert_not_called()

    def test_refuses_when_chain_voucher_digest_differs_from_request(self):
        # Chain state says voucher_digest=A; request claims B. Refuse.
        observer = _make_observer_stub()
        chain_vd = bytes.fromhex("aa" * 32)
        request_vd = bytes.fromhex("bb" * 32)
        client = _make_substrate_client_stub(voucher_digest=chain_vd)
        attestor = _make_attestor(
            substrate_client=client, observer=observer,
        )
        req = _default_pending_request(voucher_digest=request_vd)
        verdict = _run(attestor.process_one(req, CHAIN_ID))
        assert verdict.signed is False
        assert verdict.refusal_reason == RefusalReason.VOUCHER_DIGEST_MISMATCH
        client.submit_attest_settle.assert_not_called()

    def test_refuses_when_voucher_amount_disagrees_with_evidence(self):
        # Both chain-state digest matches request AND chain-state
        # voucher.amount_lovelace disagrees with request.amount_lovelace.
        observer = _make_observer_stub()
        client = _make_substrate_client_stub(
            voucher_digest=VOUCHER_DIGEST,
            voucher={"amount_lovelace": 7_000_000},
        )
        attestor = _make_attestor(
            substrate_client=client, observer=observer,
        )
        req = _default_pending_request(amount_lovelace=5_000_000)
        # observer's matched_lovelace defaults to 5_000_000 to match req.
        # cross-check (fact 6) catches the mismatch before sig.
        verdict = _run(attestor.process_one(req, CHAIN_ID))
        assert verdict.signed is False
        assert verdict.refusal_reason == RefusalReason.VOUCHER_AMOUNT_MISMATCH
        client.submit_attest_settle.assert_not_called()


class TestDispatcherConcurrency:
    def test_semaphore_caps_concurrent_dispatch(self):
        """Cap parallel attestation submissions per memo §6 OQ#10.

        We construct a SettleClaimAttestor with max_concurrent=2 and
        fire 5 simultaneous process_one calls. The dispatcher should
        serialise them through a semaphore of 2.
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
            obs.matched_address_lovelace = AMOUNT_LOVELACE
            obs.beneficiary_addr_blake2_224 = bene_hash
            obs.mainchain_genesis_hash = PREPROD_GENESIS
            return obs

        observer.observe = slow_observe

        async def driver():
            req = _default_pending_request()
            coros = [attestor.process_one(req, CHAIN_ID) for _ in range(5)]
            # Schedule everything, let them block on `gate`, then check
            # max_in_flight, then release.
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
            result = maybe_create_settle_claim_attestor(
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
            result = maybe_create_settle_claim_attestor(
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
            os.environ, {"KUPO_URL": "http://kupo.test"}, clear=False
        ):
            result = maybe_create_settle_claim_attestor(
                config, client, chain_lock,
            )
        assert isinstance(result, SettleClaimAttestor)
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
                "SETTLE_ATTESTOR_MIN_FINALITY_DEPTH": "5",  # ignored
            },
            clear=False,
        ):
            result = maybe_create_settle_claim_attestor(
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
                "SETTLE_ATTESTOR_MIN_FINALITY_DEPTH": "25",
            },
            clear=False,
        ):
            result = maybe_create_settle_claim_attestor(
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
            os.environ.pop("SETTLE_ATTESTOR_MAX_CONCURRENT", None)
            result = maybe_create_settle_claim_attestor(
                config, client, chain_lock,
            )
        assert result is not None
        # Sem cap should default to 8 (memo §6 OQ#10).
        assert result._sem._value == 8  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# SubstrateClient helpers (task #266) — direct unit tests
# ---------------------------------------------------------------------------


class TestSubstrateClientSettleHelpers:
    def test_to_bytes_exact_validates_length(self):
        from daemon.substrate_client import _to_bytes_exact

        # 28-byte beneficiary
        h = _to_bytes_exact(list(range(28)), 28)
        assert len(h) == 28
        assert h == bytes(range(28))

        # Wrong length raises
        with pytest.raises(ValueError, match="expected 28"):
            _to_bytes_exact(list(range(20)), 28)

    def test_to_bytes_exact_accepts_hex(self):
        from daemon.substrate_client import _to_bytes_exact

        h = _to_bytes_exact("0x" + "ab" * 28, 28)
        assert h == bytes.fromhex("ab" * 28)

    def test_to_bytes_exact_accepts_bytes(self):
        from daemon.substrate_client import _to_bytes_exact

        h = _to_bytes_exact(bytes.fromhex("cd" * 28), 28)
        assert h == bytes.fromhex("cd" * 28)


# ---------------------------------------------------------------------------
# Regression: _tick converts dicts from substrate_client to dataclass at the
# dispatcher boundary. The pre-fix code passed dicts straight to process_one,
# which accesses attributes — daemon would AttributeError on first real
# pending request from chain. Caught in security review.
# ---------------------------------------------------------------------------
class TestTickDictToDataclassConversion:

    def test_tick_converts_dict_rows_to_pending_settlement_request(self):
        """_tick must convert each list_pending_settlement_requests dict
        into a PendingSettlementRequest before calling process_one — the
        substrate_client deliberately returns dicts (see its docstring)
        but process_one is typed as the dataclass and uses attribute access.

        Without this conversion, the daemon AttributeErrors on every real
        pending request from chain (caught by sec-review, not by the earlier
        tests which all bypass _tick).
        """
        observer = _make_observer_stub()
        client = _make_substrate_client_stub()
        dict_row = dict(
            claim_id=CLAIM_ID,
            requester="5DummyAccountId1",
            submitted_block=100,
            settled_direct=True,
            cardano_tx_hash=CARDANO_TX_HASH,
            observed_at_depth=OBSERVED_DEPTH,
            observed_slot=OBSERVED_SLOT,
            beneficiary_addr_hash=BENE_HASH_28,
            amount_lovelace=AMOUNT_LOVELACE,
            mainchain_genesis_hash=PREPROD_GENESIS,
            voucher_digest=VOUCHER_DIGEST,
        )
        client.list_pending_settlement_requests = MagicMock(return_value=[dict_row])
        attestor = _make_attestor(substrate_client=client, observer=observer)

        captured: list = []
        original_process_one = attestor.process_one

        async def capture(req, chain_id):
            captured.append(req)
            return await original_process_one(req, chain_id)

        attestor.process_one = capture  # type: ignore[method-assign]

        _run(attestor._tick(CHAIN_ID))

        assert len(captured) == 1
        assert isinstance(captured[0], PendingSettlementRequest)
        assert captured[0].claim_id == CLAIM_ID
        assert captured[0].cardano_tx_hash == CARDANO_TX_HASH
        assert captured[0].voucher_digest == VOUCHER_DIGEST

    def test_tick_skips_malformed_row_without_killing_batch(self):
        """A dict missing required dataclass fields is logged + skipped;
        well-formed rows in the same batch still get processed."""
        observer = _make_observer_stub()
        client = _make_substrate_client_stub()
        good_row = dict(
            claim_id=CLAIM_ID,
            requester="5DummyAccountId1",
            submitted_block=100,
            settled_direct=True,
            cardano_tx_hash=CARDANO_TX_HASH,
            observed_at_depth=OBSERVED_DEPTH,
            observed_slot=OBSERVED_SLOT,
            beneficiary_addr_hash=BENE_HASH_28,
            amount_lovelace=AMOUNT_LOVELACE,
            mainchain_genesis_hash=PREPROD_GENESIS,
            voucher_digest=VOUCHER_DIGEST,
        )
        bad_row = {"claim_id": CLAIM_ID}  # missing every other field
        client.list_pending_settlement_requests = MagicMock(
            return_value=[bad_row, good_row]
        )
        attestor = _make_attestor(substrate_client=client, observer=observer)

        captured: list = []
        original_process_one = attestor.process_one

        async def capture(req, chain_id):
            captured.append(req)
            return await original_process_one(req, chain_id)

        attestor.process_one = capture  # type: ignore[method-assign]

        _run(attestor._tick(CHAIN_ID))

        # bad row was skipped, good row processed
        assert len(captured) == 1
        assert isinstance(captured[0], PendingSettlementRequest)
        assert captured[0].claim_id == CLAIM_ID
        assert captured[0].cardano_tx_hash == CARDANO_TX_HASH
