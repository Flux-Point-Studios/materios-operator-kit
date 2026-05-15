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
    CARDANO_MAINNET_GENESIS_HASH,
    CARDANO_PREPROD_GENESIS_HASH,
    CARDANO_PREVIEW_GENESIS_HASH,
    CardanoTxObservation,
    CardanoTxObserver,
    NETWORK_MAGIC_TO_GENESIS_HASH,
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
# Observer.get_genesis_hash — networkMagic-keyed resolution (task #280).
#
# Ogmios `queryNetwork/genesisConfiguration` returns the SHELLEY GENESIS
# PARAMETERS (`{era, networkMagic, startTime, slotLength, ...}`), NOT the
# 32-byte blake2b-256 hash of the genesis file. Pre-task-#280 the daemon
# searched for a `hash`/`genesisHash` field that does not exist in modern
# Ogmios payloads and refused every attest_settle with
# `ogmios_genesis_hash_unavailable`.
#
# The fix keys off `networkMagic` (which IS in the response) and looks
# the canonical blake2b-256 up in a pinned table. This preserves the
# preprod-vs-mainnet domain separation that the original mainchain_genesis_hash
# check exists for, without depending on a non-existent Ogmios field.
# ---------------------------------------------------------------------------


class TestNetworkMagicGenesisTable:
    """The pinned table is the trust anchor for the preprod/mainnet check.
    These values are blake2b-256 of the canonical IOG-published Shelley
    genesis JSON for each network. A wrong value here silently breaks
    every settle attestation, so they are tested directly.
    """

    def test_preprod_hash_pinned(self):
        # blake2b-256 of
        # https://book.world.dev.cardano.org/environments/preprod/shelley-genesis.json
        # (verified 2026-05-15 in task #280).
        assert CARDANO_PREPROD_GENESIS_HASH == bytes.fromhex(
            "162d29c4e1cf6b8a84f2d692e67a3ac6bc7851bc3e6e4afe64d15778bed8bd86"
        )
        assert len(CARDANO_PREPROD_GENESIS_HASH) == 32

    def test_mainnet_hash_pinned(self):
        # blake2b-256 of
        # https://book.world.dev.cardano.org/environments/mainnet/shelley-genesis.json
        assert CARDANO_MAINNET_GENESIS_HASH == bytes.fromhex(
            "1a3be38bcbb7911969283716ad7aa550250226b76a61fc51cc9a9a35d9276d81"
        )
        assert len(CARDANO_MAINNET_GENESIS_HASH) == 32

    def test_preview_hash_pinned(self):
        # blake2b-256 of
        # https://book.world.dev.cardano.org/environments/preview/shelley-genesis.json
        assert CARDANO_PREVIEW_GENESIS_HASH == bytes.fromhex(
            "363498d1024f84bb39d3fa9593ce391483cb40d479b87233f868d6e57c3a400d"
        )
        assert len(CARDANO_PREVIEW_GENESIS_HASH) == 32

    def test_table_covers_three_networks(self):
        # The three networks we operate against: preprod (1), preview (2),
        # mainnet (764824073). Adding a new network (e.g. sanchonet) is a
        # code change — not a runtime concern.
        assert NETWORK_MAGIC_TO_GENESIS_HASH[1] == CARDANO_PREPROD_GENESIS_HASH
        assert NETWORK_MAGIC_TO_GENESIS_HASH[2] == CARDANO_PREVIEW_GENESIS_HASH
        assert (
            NETWORK_MAGIC_TO_GENESIS_HASH[764824073]
            == CARDANO_MAINNET_GENESIS_HASH
        )


class TestObserverGetGenesisHash:
    """The Ogmios `queryNetwork/genesisConfiguration` parser, post-#280.

    Each test patches `CardanoTxObserver._ogmios_rpc` to return a canned
    Ogmios payload and asserts the function returns (a) the right 32-byte
    blake2b-256 for known networkMagic values, (b) None for unknown
    networkMagic, and (c) None for malformed payloads / transport errors.
    """

    def _patched_observer(self, rpc_return):
        """Build a CardanoTxObserver with `_ogmios_rpc` replaced to return
        `rpc_return` on every call (the real method is async)."""
        observer = CardanoTxObserver(
            ogmios_url="http://ogmios.test",
            kupo_url="http://kupo.test",
        )

        async def fake_rpc(session, method, params=None):
            return rpc_return

        observer._ogmios_rpc = fake_rpc  # type: ignore[method-assign]
        return observer

    def test_returns_preprod_hash_for_networkMagic_1(self):
        """The live preprod parity vector: Ogmios on Node-3 (192.168.0.133:1337)
        returns `{networkMagic: 1, network: "testnet", ...}` (verified
        2026-05-15). The function MUST resolve that to the pinned preprod
        blake2b-256, NOT refuse with ogmios_genesis_hash_unavailable."""
        live_preprod_payload = {
            "era": "shelley",
            "startTime": "2022-06-01T00:00:00Z",
            "networkMagic": 1,
            "network": "testnet",
            "slotLength": {"milliseconds": 1000},
        }
        observer = self._patched_observer(live_preprod_payload)

        async def driver():
            import aiohttp
            async with aiohttp.ClientSession() as session:
                return await observer.get_genesis_hash(session)

        result = _run(driver())
        assert result == CARDANO_PREPROD_GENESIS_HASH
        # And it caches: a second call should return the same bytes
        # without re-querying.
        assert observer._cached_genesis == CARDANO_PREPROD_GENESIS_HASH

    def test_returns_mainnet_hash_for_networkMagic_764824073(self):
        observer = self._patched_observer({
            "era": "shelley",
            "networkMagic": 764824073,
            "network": "mainnet",
        })

        async def driver():
            import aiohttp
            async with aiohttp.ClientSession() as session:
                return await observer.get_genesis_hash(session)

        result = _run(driver())
        assert result == CARDANO_MAINNET_GENESIS_HASH

    def test_returns_preview_hash_for_networkMagic_2(self):
        observer = self._patched_observer({
            "era": "shelley",
            "networkMagic": 2,
            "network": "testnet",
        })

        async def driver():
            import aiohttp
            async with aiohttp.ClientSession() as session:
                return await observer.get_genesis_hash(session)

        result = _run(driver())
        assert result == CARDANO_PREVIEW_GENESIS_HASH

    def test_returns_none_for_unknown_networkMagic(self):
        """An unknown magic (e.g. a private testnet, sanchonet before we
        add it) MUST refuse to fabricate a hash. The caller treats None
        as observer_unavailable and refuses the attest — the safe default."""
        observer = self._patched_observer({
            "era": "shelley",
            "networkMagic": 999,
            "network": "testnet",
        })

        async def driver():
            import aiohttp
            async with aiohttp.ClientSession() as session:
                return await observer.get_genesis_hash(session)

        result = _run(driver())
        assert result is None
        # Nothing got cached either — a later call could succeed if the
        # operator points at a known network.
        assert observer._cached_genesis is None

    def test_returns_none_when_networkMagic_field_missing(self):
        """A payload missing `networkMagic` (e.g. an unknown Ogmios
        version) MUST refuse rather than silently treating it as
        preprod or mainnet."""
        observer = self._patched_observer({
            "era": "shelley",
            "network": "testnet",
            # NO networkMagic field
        })

        async def driver():
            import aiohttp
            async with aiohttp.ClientSession() as session:
                return await observer.get_genesis_hash(session)

        result = _run(driver())
        assert result is None

    def test_returns_none_when_networkMagic_is_not_int(self):
        """Defensive: a non-int networkMagic (string, dict, list) must
        refuse — never try to index the table with a non-int."""
        observer = self._patched_observer({
            "era": "shelley",
            "networkMagic": "1",  # str, not int — malformed
            "network": "testnet",
        })

        async def driver():
            import aiohttp
            async with aiohttp.ClientSession() as session:
                return await observer.get_genesis_hash(session)

        result = _run(driver())
        assert result is None

    def test_returns_none_when_ogmios_call_fails(self):
        """The RPC layer returns None on timeout/transport error; the
        function MUST propagate that as None (preserves existing
        refusal semantics — was the only behavior tested pre-#280)."""
        observer = self._patched_observer(None)

        async def driver():
            import aiohttp
            async with aiohttp.ClientSession() as session:
                return await observer.get_genesis_hash(session)

        result = _run(driver())
        assert result is None
        # Cache is NOT poisoned by a failed call — a retry can succeed.
        assert observer._cached_genesis is None

    def test_caches_successful_result_across_calls(self):
        """Genesis hash is invariant per network — the resolver caches
        after the first successful lookup so subsequent observations
        don't re-roundtrip Ogmios. (Behavior preserved from pre-#280.)"""
        call_count = 0
        observer = CardanoTxObserver(
            ogmios_url="http://ogmios.test",
            kupo_url="http://kupo.test",
        )

        async def counting_rpc(session, method, params=None):
            nonlocal call_count
            call_count += 1
            return {
                "era": "shelley",
                "networkMagic": 1,
                "network": "testnet",
            }

        observer._ogmios_rpc = counting_rpc  # type: ignore[method-assign]

        async def driver():
            import aiohttp
            async with aiohttp.ClientSession() as session:
                a = await observer.get_genesis_hash(session)
                b = await observer.get_genesis_hash(session)
                return (a, b)

        a, b = _run(driver())
        assert a == b == CARDANO_PREPROD_GENESIS_HASH
        assert call_count == 1  # second call served from cache


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

    def test_signs_with_request_depth_not_observer_depth(self):
        """Task #287: ``observed_at_depth`` in the signed STCA preimage
        MUST be ``request.observed_at_depth`` (the value pinned on
        chain at request_settle time), NOT the daemon's fresh
        ``obs.depth``. The pallet rebuilds the preimage from the pinned
        value; signing over a different one produces InvalidSignature.

        The daemon's local obs.depth is used as an INDEPENDENT
        reality-check (`obs.depth >= request.observed_at_depth` must
        hold or the daemon refuses), but is NOT what gets signed.
        """
        # observer reports depth = 30, request says 20. Reality has
        # caught up, so the daemon signs — but it signs over depth=20
        # (the pinned value), not 30.
        observer = _make_observer_stub(
            tip_block_no=1_000_000, tx_block_no=1_000_000 - 30,
        )
        client = _make_substrate_client_stub()
        attestor = _make_attestor(
            substrate_client=client, observer=observer,
        )
        req = _default_pending_request(observed_at_depth=20)
        verdict = _run(attestor.process_one(req, CHAIN_ID))
        assert verdict.signed is True

        expected_preimage = build_stca_preimage(
            chain_id=CHAIN_ID, claim_id=CLAIM_ID,
            voucher_digest=VOUCHER_DIGEST,
            cardano_tx_hash=CARDANO_TX_HASH, settled_direct=True,
            beneficiary_addr_blake2_224=BENE_HASH_28,
            amount_lovelace=AMOUNT_LOVELACE,
            observed_at_depth=20,  # request.observed_at_depth, NOT 30
            observed_slot=OBSERVED_SLOT,
            mainchain_genesis_hash=PREPROD_GENESIS,
        )
        expected_digest = compute_stca_digest(expected_preimage)
        call_args = client.submit_attest_settle.call_args
        sig = call_args.args[2]
        pubkey = call_args.args[1]
        kp = Keypair(public_key=pubkey, ss58_format=42)
        assert kp.verify(expected_digest, sig) is True

    def test_refuses_when_reality_below_pinned_depth(self):
        """Task #287: if reality hasn't caught up to the requester's
        pinned depth, REFUSE rather than sign over a stale claim."""
        # observer reports depth = 10, but request claims 25.
        observer = _make_observer_stub(
            tip_block_no=1_000_000, tx_block_no=1_000_000 - 10,
        )
        client = _make_substrate_client_stub()
        attestor = _make_attestor(
            substrate_client=client, observer=observer,
        )
        req = _default_pending_request(observed_at_depth=25)
        verdict = _run(attestor.process_one(req, CHAIN_ID))
        assert verdict.signed is False
        assert verdict.refusal_reason == RefusalReason.DEPTH_UNDERSHOOT
        assert "reality has not caught up" in (verdict.refusal_detail or "")


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
        # Observer reports depth = 5 (= 1_000 tip - 995 tx). The pinned
        # request depth must match reality (5) so the depth-undershoot
        # check passes, exposing the min-finality refusal underneath
        # (task #287 layered the two checks).
        observer = _make_observer_stub(
            tip_block_no=1_000, tx_block_no=995,
        )
        client = _make_substrate_client_stub()
        attestor = _make_attestor(
            substrate_client=client, observer=observer,
            min_finality_depth=15,
        )
        req = _default_pending_request(observed_at_depth=5)
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


# ---------------------------------------------------------------------------
# get_block_height_by_hash (task #283).
#
# Kupo's /matches/* response carries `created_at: {slot_no, header_hash}` —
# but NOT a block number / height. Pre-#283 the daemon scanned for a
# `block_no` / `blockNo` / `height` field that does not exist in Kupo 2.x
# payloads and refused every settle attestation with
# `depth_observation_unavailable` because tx_block_no stayed None.
#
# Ogmios 6.x does NOT expose a direct "block-hash → height" RPC under
# queryNetwork/* or queryLedgerState/*. But the chain-sync mini-protocol
# DOES carry block height: findIntersection at the target (slot, hash)
# confirms the block exists, then nextBlock x2 yields a RollForward whose
# block.height = our_height + 1 and whose block.ancestor MUST equal our
# header_hash (cryptographic verification: an attacker-supplied Ogmios
# can't fake a chain that descends from a block it didn't produce).
#
# This avoids a new pip dep (aiohttp already supports ws_connect) and
# any new env var (the existing OGMIOS_URL upgrades to ws via the same
# host:port). No operator action required to activate.
# ---------------------------------------------------------------------------


class _FakeWebSocket:
    """Records sent JSON-RPC requests and replays a scripted response
    sequence. Mirrors the methods of ``aiohttp.ClientWebSocketResponse``
    that ``get_block_height_by_hash`` calls.

    Each ``send_json`` call pops the next entry from ``response_script``
    and stashes it for the next ``receive_json`` to return — that
    one-in/one-out sequencing matches Ogmios's request-correlated id
    semantics.
    """

    def __init__(self, response_script):
        self.sent = []
        self._script = list(response_script)
        self._pending = None
        self.closed = False

    async def send_json(self, payload):
        self.sent.append(payload)
        nxt = self._script.pop(0)
        if callable(nxt):
            nxt = nxt(payload)
        self._pending = nxt

    async def receive_json(self, timeout=None):  # noqa: ARG002
        if self._pending is None:
            raise AssertionError("receive_json called before send_json")
        out = self._pending
        self._pending = None
        if isinstance(out, BaseException):
            raise out
        return out

    async def close(self):
        self.closed = True


class _FakeWsContextManager:
    """Async context manager wrapping a _FakeWebSocket so tests can use
    ``async with session.ws_connect(...) as ws: ...``."""

    def __init__(self, ws_or_exc):
        self._inner = ws_or_exc

    async def __aenter__(self):
        if isinstance(self._inner, BaseException):
            raise self._inner
        return self._inner

    async def __aexit__(self, *args):
        if not isinstance(self._inner, BaseException):
            await self._inner.close()
        return False


class _FakeSession:
    """Stub ``aiohttp.ClientSession`` that returns a scripted WS handle
    from ``.ws_connect(...)``. We bypass the real network by overriding
    just the one method the SUT calls."""

    def __init__(self, ws_or_exc):
        self._ws_or_exc = ws_or_exc
        self.connect_url = None

    def ws_connect(self, url, **kwargs):
        self.connect_url = url
        return _FakeWsContextManager(self._ws_or_exc)


# A pinned live-preprod parity vector (Node-3 Ogmios @ 192.168.0.133:1337,
# verified 2026-05-15). The Kupo /matches/* lookup for the existing
# attest_settle exemplar tx returns this header_hash + slot pair; the
# Ogmios chain-sync walk-forward returns the next block whose ancestor
# is the same header_hash and whose height is OUR height + 1.
LIVE_TX_HASH_HEX = "157a215d8eb9dae711dc3044741875947baf23400f0bba7be1d8ee1c0afe0609"
LIVE_HEADER_HASH_HEX = "d1742ce015040d48327608969b7a8b9dffe5b8c67e63911d93015394478d096d"
LIVE_SLOT = 123170608
LIVE_NEXT_BLOCK_HEIGHT = 4712149  # Ogmios block AFTER the one carrying our tx
LIVE_OUR_HEIGHT = LIVE_NEXT_BLOCK_HEIGHT - 1  # = 4712148


def _intersection_ok_response(req):
    return {
        "jsonrpc": "2.0",
        "method": "findIntersection",
        "result": {
            "intersection": {
                "slot": LIVE_SLOT,
                "id": LIVE_HEADER_HASH_HEX,
            },
            "tip": {
                "slot": LIVE_SLOT + 1000,
                "id": "ff" * 32,
                "height": LIVE_OUR_HEIGHT + 100,
            },
        },
        "id": req.get("id", "f"),
    }


def _intersection_not_found_response(req):
    return {
        "jsonrpc": "2.0",
        "method": "findIntersection",
        "error": {
            "code": 1000,
            "message": "No intersection found.",
            "data": {
                "tip": {
                    "slot": LIVE_SLOT + 2000,
                    "id": "ff" * 32,
                    "height": LIVE_OUR_HEIGHT + 200,
                },
            },
        },
        "id": req.get("id", "f"),
    }


def _next_block_backward_response(req):
    return {
        "jsonrpc": "2.0",
        "method": "nextBlock",
        "result": {
            "direction": "backward",
            "point": {"slot": LIVE_SLOT, "id": LIVE_HEADER_HASH_HEX},
            "tip": {
                "slot": LIVE_SLOT + 1000,
                "id": "ff" * 32,
                "height": LIVE_OUR_HEIGHT + 100,
            },
        },
        "id": req.get("id", "n1"),
    }


def _next_block_forward_response(req, *, ancestor_hex=LIVE_HEADER_HASH_HEX,
                                  next_height=LIVE_NEXT_BLOCK_HEIGHT):
    return {
        "jsonrpc": "2.0",
        "method": "nextBlock",
        "result": {
            "direction": "forward",
            "block": {
                "type": "praos",
                "era": "conway",
                "id": "ab" * 32,
                "height": next_height,
                "slot": LIVE_SLOT + 16,
                "ancestor": ancestor_hex,
            },
            "tip": {
                "slot": LIVE_SLOT + 1000,
                "id": "ff" * 32,
                "height": next_height + 100,
            },
        },
        "id": req.get("id", "n2"),
    }


def _make_testnet_addr_with_payment_hash(payment_hash: bytes) -> str:
    """CIP-0019 type-0 testnet base address whose payment hash is the
    supplied 28-byte value. Used by the observe() integration tests to
    confirm the address-decoder path still works end-to-end."""
    assert len(payment_hash) == 28
    from tests.test_cardano_address import _bech32_encode_addr_test
    header = bytes([0x00])
    stake_hash = b"\x99" * 28
    return _bech32_encode_addr_test(header + payment_hash + stake_hash)


class TestGetBlockHeightByHash:
    """Resolve a Cardano block-hash to block-height via Ogmios chain-sync.

    The pre-#283 implementation scanned Kupo's ``created_at`` for a
    ``block_no``/``blockNo``/``height`` field — none of which exist in
    Kupo 2.x output (only ``slot_no`` + ``header_hash``). This left
    ``tx_block_no`` None, made ``obs.depth`` always None, and the
    daemon refused every settle with ``depth_observation_unavailable``.

    The fix opens a WS to the same Ogmios URL, performs a chain-sync
    ``findIntersection`` at the target (slot, hash), then ``nextBlock``
    twice (first RollBackward = "you are here", second RollForward = the
    block AFTER ours, whose ``block.ancestor`` MUST equal our hash and
    whose ``block.height - 1`` IS our height).
    """

    def _observer(self):
        return CardanoTxObserver(
            ogmios_url="http://ogmios.test:1337",
            kupo_url="http://kupo.test",
        )

    def test_happy_path_resolves_height_from_ogmios_chain_sync(self):
        ws = _FakeWebSocket([
            _intersection_ok_response,
            _next_block_backward_response,
            _next_block_forward_response,
        ])
        session = _FakeSession(ws)
        observer = self._observer()

        async def driver():
            return await observer.get_block_height_by_hash(
                session, LIVE_SLOT, LIVE_HEADER_HASH_HEX,
            )

        height = _run(driver())
        assert height == LIVE_OUR_HEIGHT
        # Verify the JSON-RPC sequence is correct.
        assert len(ws.sent) == 3
        assert ws.sent[0]["method"] == "findIntersection"
        points = ws.sent[0]["params"]["points"]
        assert points[0]["slot"] == LIVE_SLOT
        assert points[0]["id"] == LIVE_HEADER_HASH_HEX
        assert ws.sent[1]["method"] == "nextBlock"
        assert ws.sent[2]["method"] == "nextBlock"

    def test_returns_none_when_intersection_not_found(self):
        ws = _FakeWebSocket([_intersection_not_found_response])
        session = _FakeSession(ws)
        observer = self._observer()

        async def driver():
            return await observer.get_block_height_by_hash(
                session, LIVE_SLOT, "00" * 32,
            )

        result = _run(driver())
        assert result is None
        assert "00" * 32 not in getattr(observer, "_block_height_cache", {})

    def test_returns_none_when_next_block_ancestor_mismatches(self):
        ws = _FakeWebSocket([
            _intersection_ok_response,
            _next_block_backward_response,
            lambda req: _next_block_forward_response(
                req, ancestor_hex="de" * 32,
            ),
        ])
        session = _FakeSession(ws)
        observer = self._observer()

        async def driver():
            return await observer.get_block_height_by_hash(
                session, LIVE_SLOT, LIVE_HEADER_HASH_HEX,
            )

        result = _run(driver())
        assert result is None

    def test_returns_none_on_ws_connection_error(self):
        session = _FakeSession(ConnectionRefusedError("ogmios down"))
        observer = self._observer()

        async def driver():
            return await observer.get_block_height_by_hash(
                session, LIVE_SLOT, LIVE_HEADER_HASH_HEX,
            )

        result = _run(driver())
        assert result is None

    def test_returns_none_on_timeout_at_tip(self):
        ws = _FakeWebSocket([
            _intersection_ok_response,
            _next_block_backward_response,
            asyncio.TimeoutError(),
        ])
        session = _FakeSession(ws)
        observer = self._observer()

        async def driver():
            return await observer.get_block_height_by_hash(
                session, LIVE_SLOT, LIVE_HEADER_HASH_HEX,
            )

        result = _run(driver())
        assert result is None

    def test_returns_none_on_malformed_header_hash_hex(self):
        """Defensive: a non-64-char or empty hex string must short-circuit
        to None without opening a WS — protects against arg-drift bugs
        upstream."""
        observer = self._observer()
        bad_session = _FakeSession(ConnectionRefusedError("must not call"))

        async def driver():
            return await observer.get_block_height_by_hash(
                bad_session, LIVE_SLOT, "abc123",
            )

        result = _run(driver())
        assert result is None
        assert bad_session.connect_url is None

    def test_caches_successful_result_across_calls(self):
        observer = self._observer()
        ws1 = _FakeWebSocket([
            _intersection_ok_response,
            _next_block_backward_response,
            _next_block_forward_response,
        ])
        session1 = _FakeSession(ws1)

        async def first_call():
            return await observer.get_block_height_by_hash(
                session1, LIVE_SLOT, LIVE_HEADER_HASH_HEX,
            )

        first = _run(first_call())
        assert first == LIVE_OUR_HEIGHT
        assert len(ws1.sent) == 3

        # Cache must serve the next call WITHOUT opening a connection.
        bad_session = _FakeSession(ConnectionRefusedError("must not call"))

        async def second_call():
            return await observer.get_block_height_by_hash(
                bad_session, LIVE_SLOT, LIVE_HEADER_HASH_HEX,
            )

        second = _run(second_call())
        assert second == LIVE_OUR_HEIGHT
        assert bad_session.connect_url is None

    def test_failed_resolution_is_not_cached(self):
        observer = self._observer()
        ws1 = _FakeWebSocket([_intersection_not_found_response])
        session1 = _FakeSession(ws1)

        async def first_call():
            return await observer.get_block_height_by_hash(
                session1, LIVE_SLOT, LIVE_HEADER_HASH_HEX,
            )

        first = _run(first_call())
        assert first is None

        ws2 = _FakeWebSocket([
            _intersection_ok_response,
            _next_block_backward_response,
            _next_block_forward_response,
        ])
        session2 = _FakeSession(ws2)

        async def second_call():
            return await observer.get_block_height_by_hash(
                session2, LIVE_SLOT, LIVE_HEADER_HASH_HEX,
            )

        second = _run(second_call())
        assert second == LIVE_OUR_HEIGHT
        assert len(ws2.sent) == 3


class TestObserveResolvesTxBlockNoViaOgmiosWalk:
    """End-to-end of observe() with a Kupo response that has only
    ``header_hash`` (the modern Kupo 2.x shape — no ``block_no``).
    Pre-#283 this returned tx_block_no=None and the dispatcher refused
    with depth_observation_unavailable. Post-#283 observe() falls back
    to the Ogmios chain-sync walk.
    """

    def test_observe_populates_tx_block_no_from_ogmios_when_kupo_lacks_height(self):
        observer = CardanoTxObserver(
            ogmios_url="http://ogmios.test:1337",
            kupo_url="http://kupo.test",
        )
        observer._ogmios_rpc = AsyncMock(  # type: ignore[method-assign]
            side_effect=[
                {"slot": LIVE_SLOT + 1000, "id": "ff" * 32},
                LIVE_OUR_HEIGHT + 100,
                {
                    "era": "shelley",
                    "networkMagic": 1,
                    "network": "testnet",
                },
            ]
        )
        observer._kupo_get = AsyncMock(  # type: ignore[method-assign]
            return_value=[
                {
                    "transaction_id": LIVE_TX_HASH_HEX,
                    "output_index": 0,
                    "address": _make_testnet_addr_with_payment_hash(BENE_HASH_28),
                    "value": {"coins": AMOUNT_LOVELACE, "assets": {}},
                    "created_at": {
                        "slot_no": LIVE_SLOT,
                        "header_hash": LIVE_HEADER_HASH_HEX,
                    },
                    "spent_at": None,
                },
            ]
        )
        observer.get_block_height_by_hash = AsyncMock(  # type: ignore[method-assign]
            return_value=LIVE_OUR_HEIGHT,
        )

        async def driver():
            return await observer.observe(LIVE_TX_HASH_HEX, BENE_HASH_28)

        obs = _run(driver())
        assert obs.tx_block_no == LIVE_OUR_HEIGHT, (
            f"observe() failed to resolve tx_block_no via the new "
            f"chain-sync path; got tx_block_no={obs.tx_block_no}, "
            f"mismatches={obs.mismatches}"
        )
        assert obs.observed_slot == LIVE_SLOT
        assert obs.cardano_tip_block_no == LIVE_OUR_HEIGHT + 100
        assert obs.matched_address_lovelace == AMOUNT_LOVELACE
        observer.get_block_height_by_hash.assert_called_once()
        call = observer.get_block_height_by_hash.call_args
        assert call.args[1] == LIVE_SLOT
        assert call.args[2] == LIVE_HEADER_HASH_HEX

    def test_observe_refuses_when_kupo_has_no_header_hash(self):
        observer = CardanoTxObserver(
            ogmios_url="http://ogmios.test:1337",
            kupo_url="http://kupo.test",
        )
        observer._ogmios_rpc = AsyncMock(  # type: ignore[method-assign]
            side_effect=[
                {"slot": LIVE_SLOT + 1000, "id": "ff" * 32},
                LIVE_OUR_HEIGHT + 100,
                {"era": "shelley", "networkMagic": 1, "network": "testnet"},
            ]
        )
        observer._kupo_get = AsyncMock(  # type: ignore[method-assign]
            return_value=[
                {
                    "transaction_id": LIVE_TX_HASH_HEX,
                    "output_index": 0,
                    "address": _make_testnet_addr_with_payment_hash(BENE_HASH_28),
                    "value": {"coins": AMOUNT_LOVELACE, "assets": {}},
                    "created_at": {
                        "slot_no": LIVE_SLOT,
                    },
                    "spent_at": None,
                },
            ]
        )
        observer.get_block_height_by_hash = AsyncMock(  # type: ignore[method-assign]
            return_value=None,
        )

        async def driver():
            return await observer.observe(LIVE_TX_HASH_HEX, BENE_HASH_28)

        obs = _run(driver())
        assert obs.tx_block_no is None
        assert "kupo_match_missing_block_no" in obs.mismatches
        # No header_hash means nothing to resolve — resolver MUST NOT be called.
        observer.get_block_height_by_hash.assert_not_called()

    def test_observe_uses_legacy_block_no_when_kupo_provides_one(self):
        """Backwards-compat: if a Kupo build DOES emit ``block_no`` in
        ``created_at`` (older versions, custom forks), observe() takes
        that directly without the WS roundtrip."""
        observer = CardanoTxObserver(
            ogmios_url="http://ogmios.test:1337",
            kupo_url="http://kupo.test",
        )
        observer._ogmios_rpc = AsyncMock(  # type: ignore[method-assign]
            side_effect=[
                {"slot": LIVE_SLOT + 1000, "id": "ff" * 32},
                LIVE_OUR_HEIGHT + 100,
                {"era": "shelley", "networkMagic": 1, "network": "testnet"},
            ]
        )
        observer._kupo_get = AsyncMock(  # type: ignore[method-assign]
            return_value=[
                {
                    "transaction_id": LIVE_TX_HASH_HEX,
                    "output_index": 0,
                    "address": _make_testnet_addr_with_payment_hash(BENE_HASH_28),
                    "value": {"coins": AMOUNT_LOVELACE, "assets": {}},
                    "created_at": {
                        "slot_no": LIVE_SLOT,
                        "block_no": LIVE_OUR_HEIGHT,
                        "header_hash": LIVE_HEADER_HASH_HEX,
                    },
                    "spent_at": None,
                },
            ]
        )
        observer.get_block_height_by_hash = AsyncMock(  # type: ignore[method-assign]
            return_value=999_999,
        )

        async def driver():
            return await observer.observe(LIVE_TX_HASH_HEX, BENE_HASH_28)

        obs = _run(driver())
        assert obs.tx_block_no == LIVE_OUR_HEIGHT
        observer.get_block_height_by_hash.assert_not_called()

    def test_observe_refuses_when_chain_sync_resolution_fails(self):
        """If ``header_hash`` IS present but the chain-sync resolver
        fails (Ogmios down, intersection-not-found, ancestor mismatch,
        tip timeout) — observe() must surface the same
        ``kupo_match_missing_block_no`` mismatch as if there were no
        ``header_hash`` at all. The dispatcher then refuses safely with
        ``depth_observation_unavailable`` on the next gate."""
        observer = CardanoTxObserver(
            ogmios_url="http://ogmios.test:1337",
            kupo_url="http://kupo.test",
        )
        observer._ogmios_rpc = AsyncMock(  # type: ignore[method-assign]
            side_effect=[
                {"slot": LIVE_SLOT + 1000, "id": "ff" * 32},
                LIVE_OUR_HEIGHT + 100,
                {"era": "shelley", "networkMagic": 1, "network": "testnet"},
            ]
        )
        observer._kupo_get = AsyncMock(  # type: ignore[method-assign]
            return_value=[
                {
                    "transaction_id": LIVE_TX_HASH_HEX,
                    "output_index": 0,
                    "address": _make_testnet_addr_with_payment_hash(BENE_HASH_28),
                    "value": {"coins": AMOUNT_LOVELACE, "assets": {}},
                    "created_at": {
                        "slot_no": LIVE_SLOT,
                        "header_hash": LIVE_HEADER_HASH_HEX,
                    },
                    "spent_at": None,
                },
            ]
        )
        observer.get_block_height_by_hash = AsyncMock(  # type: ignore[method-assign]
            return_value=None,
        )

        async def driver():
            return await observer.observe(LIVE_TX_HASH_HEX, BENE_HASH_28)

        obs = _run(driver())
        assert obs.tx_block_no is None
        assert "kupo_match_missing_block_no" in obs.mismatches
        observer.get_block_height_by_hash.assert_called_once()
