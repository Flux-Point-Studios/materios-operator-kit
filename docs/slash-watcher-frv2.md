# Slash signatures across runtime upgrades

The watcher keeps signing FRAU on the current runtime and switches to FRV2
when the slash call's metadata advertises the new contract. No environment
switch, guessed spec-version threshold, or service restart is required.

## Detection and refusal

`SubstrateClient.get_slash_runtime_context` pins the current best head and
reads its runtime version, block number and V14 metadata. The complete
`slash_bad_settlement_evidence` call documentation is SHA-256 fingerprinted
against the FRAU and FRV2 fixtures in `tests/fixtures/slash_runtime`.
This identifies the **signature protocol**, independently of unrelated
runtime spec-version bumps. Any unrecognized fingerprint, metadata format,
missing call, wrong spec name, or missing/malformed TTL stops signing and
submission with an ERROR: `slash runtime unavailable`.

The FRAU fixture was captured from live Materios spec 239. The FRV2 fixture
comes from settlement PR #55 at
`74cd340bbec3e80d2209bfd00855e9a552bc66c5`. This relies on the runtime's
metadata accurately describing its implementation, as do other metadata
consumers. Changing that documentation requires review and a new fingerprint;
stripped docs fail closed. Release preflight must compare the enacted
runtime's metadata with these fixtures. Never add a fallback protocol.

The direct `get_block_metadata(head)` read is deliberate: substrate-interface
1.x convenience metadata methods initialize from the *parent* runtime.
Direct reads see the new protocol at the upgrade block's post-state.
Independent watchers reaching that same state select the same protocol;
lagging watchers can temporarily select different protocols.

`SettlementRequestTtl` is decoded as u32 from that same metadata. A request
is expired exactly when `submitted_block + ttl < current_block`; equality
remains eligible. Expired rows are filtered before dispatch and checked
again before signing and after acquiring the shared write lock. They cannot
repeatedly send failed extrinsics while occupying that lock.

## Quorum and upgrade races

The existing `slash` bulletin-board channel and claim-id key are unchanged.
The aggregator requests the locally computed digest and independently
filters returned rows by digest. FRAU, FRV2 and replacement-request
signatures cannot mix into a quorum. Tests exercise a real local HTTP
bulletin board with real sr25519 signatures, including a server returning
both old and new digests.

With one old binary in the current 2-of-2 committee, both watchers continue
using FRAU before enactment. After enactment the upgraded watcher logs
`awaiting_peer_sigs:1/2` and retries each poll (12 seconds by default).
It resumes once the second watcher supplies FRV2. An old-only quorum on a
larger committee could still submit FRAU; the upgraded pallet rejects it.
Deploy both active watchers before enactment.

If the protocol changes during observation, signing uses the refreshed
protocol. If it changes while collecting signatures or waiting for the
write lock, the bundle is discarded with `runtime_changed` and retried on
the next poll. A change or expiry after the final read can still race
inclusion: the pallet rejects the extrinsic, the client logs the failure,
and the next tick re-reads the runtime/TTL. No success is reported for that
failure. There is no atomic read-and-submit RPC.

The digest continues to use live `chain_getBlockHash(0)`. It does not use
the `MateriosChainId` constant or an environment-supplied chain ID.

## Rust provenance and validation

`tests/fixtures/frv2/generate.rs` calls the actual pallet's
`slash_bad_settlement_evidence_payload`, `account_to_bytes`, SCALE Encode
and `sp_io::hashing::blake2_256`. It produced `vectors.json` from settlement
PR #55 head `74cd340bbec3e80d2209bfd00855e9a552bc66c5`.
The six cases cover all FraudProof variants, both settlement modes, u32/u64
block normalization, zero and maximum integer widths. Python checks the
153-byte identity preimage, request ID, proof encoding and final digest.

To reproduce in a separate settlement checkout at that commit, copy
`generate.rs` to `pallets/intent-settlement/examples/frv2_vectors.rs`, then:

```sh
nice -n 10 env CARGO_BUILD_JOBS=2 CARGO_TARGET_DIR=/path/to/unique-target \
  cargo +stable run --locked -p pallet-intent-settlement --example frv2_vectors
nice -n 10 python3 -B -m pytest -q -p no:cacheprovider
nice -n 10 python3 scripts/check-slash-mutations.py
```

The mutation harness checks unknown-contract and malformed-context refusals,
TTL source/type/width/boundary, expiry filtering, post-observation and
post-lock checks, protocol-change rejection, peer-digest filtering and
request encoding. It runs isolated copies and rejects surviving or invalid
mutants.

## Rollout order

1. Review and release this operator-kit change.
2. Deploy the release to both active slash watchers: Gemtek and Node-3.
   Verify both detect FRAU and the live TTL before enactment.
3. Enact the reviewed runtime containing settlement #55 and the live-genesis
   chain-ID fix, after checking its FRV2 metadata fingerprint. Verify both
   watchers detect FRV2 and can converge on the same request digest.

This PR does not deploy or restart services. The draft PR with green CI is
the implementation handoff; materios performs the cross-model review.
