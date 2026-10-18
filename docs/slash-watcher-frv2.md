# Slash signatures across runtime upgrades

The watcher keeps signing FRAU on the current runtime and switches to FRV2
when the runtime's metadata names FRV2 and carries the FRV2 signing contract.
No environment switch, guessed spec-version threshold, or service restart is
required.

## Detection and refusal

`SubstrateClient.get_settlement_runtime_context` pins the current best head
and reads its runtime version, block number and V14 metadata. From that one
metadata it reads the `IntentSettlement` constants and selects the protocol
with `select_slash_protocol`:

| `SlashSettlementTag` | Contract fingerprint | Protocol |
| --- | --- | --- |
| absent | FRAU pin | FRAU |
| absent | anything else, the FRV2 pin included | refuse |
| `FRV2` | FRV2 pin | FRV2 |
| `FRV2` | anything else, the FRAU pin included | refuse |
| any other value or width | any | refuse, naming the tag |

Live preprod (spec 239) has no tag constants. The spec-242 runtime (Materios
`67ec3d9`, settlement `0757de3`) exposes all sixteen digest tags; its
`SlashSettlementTag` is `FRV2` and the pallet's verifier hashes the same
constant. The tag says which digest to sign; the type-only fingerprint
cross-checks that the call, request record and request-scoped slash map are
the reviewed ones.

The same read reports whether the runtime offers `supersede_settlement_request`
(see [Supersede driver](#supersede-driver)), `SettlementRequestTtl` and
`MinFinalityDepth`. An unknown tag, a fingerprint outside the table, a
supersede call with other arguments or an `AttestSettleTag` other than `STCA`,
a missing item, a wrong spec name, or a constant that is not a u32 stops
signing and submission. The client logs `settlement runtime at <head>
refused: <reason>` and each consumer logs `settlement runtime unavailable`.
Never add a fallback protocol.

### What is hashed

SHA-256 of compact, key-sorted JSON with four entries, all read from the
`IntentSettlement` pallet:

| Entry | Content | Why it is in the contract |
| --- | --- | --- |
| `call` | `slash_bad_settlement_evidence`: call index, name, each argument's name and resolved type | The extrinsic the watcher composes. `FraudProof`'s type is `SCALE(proof)` in both digests; `ClaimId`'s type is the `claim_id` bytes. |
| `requests` | `ClaimSettlementRequests` key and value types | FRV2's `request_id` is `blake2_256(requester ‖ u64_le(submitted_block) ‖ SCALE(evidence) ‖ settled_direct)`. All four inputs are fields of this record, and the watcher decodes the whole row. |
| `request_slashes` | `SlashedSettlementRequests` key and value types, or `null` when absent | The only metadata item stating that a slash is bound to one request identity, keyed `(ClaimId, request_id)`. It is what separates FRV2 from FRAU. |
| `ttl` | `SettlementRequestTtl` name and resolved type | The expiry filter decodes it. |

A resolved type is the type's structure, followed recursively: composite
fields as `(name, type)`, variants sorted by index as `(index, name, fields)`,
sequence, compact, array `(length, type)`, tuple and primitive. Any other
shape, and any recursive type, raises and refuses.

Excluded: docs at every level (call, arguments, variants, types, storage,
constants), Rust type paths, type parameters and type names, registry ids,
the pallet index, storage hashers, modifiers and defaults, the TTL value
(read live), and every other call, storage item, event, error and constant.

### Why this set

*Necessary.* Each entry feeds the extrinsic the watcher builds, the bytes of a
digest, or the expiry check, so changing any of them can make a correctly
labelled signature wrong; it must force a review. Without `request_slashes`,
FRAU and FRV2 hash identically: the slash call, the request record and the
TTL are byte-identical across settlement #55.
`test_request_scoped_slashes_are_the_only_contract_difference` shows this
on the captured metadata.

*Sufficient for the reviewed contracts.* Nothing excluded reaches the
extrinsic encoding, the digest bytes or the expiry check. Doc edits, registry
renumbering, path renames and unrelated pallet changes leave the fingerprint
unchanged, and each has a test. Real builds confirm it: settlement 5fd5f54
with its round-1 slash docs swapped back in, and Materios spec 240 (whose
registry ids differ from spec 239), both keep their fingerprints.

### Limit

Metadata types do not carry the digest's preimage layout, so a runtime can
change the digest without moving any of the four entries. Settlement #55
round 1 (`74cd340`) was such a runtime: it already required FRV2, but its
contract fingerprint is FRAU's (measured on a real build). The tag closes the
selection half of that gap: a tagless runtime is refused unless it is the FRAU
contract, and FRV2 is selected only when the runtime itself says FRV2. A
change of layout under an unchanged tag is still invisible to metadata, so
the enactment preflight keeps checking the settlement revision (see Rollout).

### Pins and provenance

| Protocol | Fingerprint | Source |
| --- | --- | --- |
| FRAU | `96dfe9b41a5471b578e065d9e3055a359e27e9a6576f94b1f932bd668219e85c` | Live preprod spec 239, `state_getMetadata` at block 2330118 (`0x2743fc10…5311`), read-only. A native build of Materios `a9f14a8` (spec 240, settlement `c60b98f`) gives the same value. |
| FRV2 | `693ba3e8bc9106fb036f96bc73b3c7797d314c36ef95c0564c715ce58113eeb7` | Native build of Materios `67ec3d9` (spec 242, transaction version 6, settlement `0757de3`), `Runtime::metadata_at_version(14)` under `sp_io::TestExternalities`. The earlier Materios `a9f14a8` build with settlement `5fd5f54` gives the same value, as does the adoption agent's subwasm read of the spec-242 WASM. |

`tests/fixtures/slash_runtime/{frau,frv2}_metadata.json` hold the
`IntentSettlement` items the fingerprint and context read (the slash and
supersede calls, `ClaimSettlementRequests`, `SlashedSettlementRequests`,
`SettlementRequestTtl`, `MinFinalityDepth`, `SlashSettlementTag`,
`AttestSettleTag`), plus `request_settle`, `BondSlashedRequests` and
`SlashWatcherShareBps` as unrelated controls, and the types they reach. They
are cut from the decoded metadata named in each file's `provenance`, which
records the SHA-256 of the full metadata. Constant values are stored as hex.
`frv2_metadata.json` is the spec-242 runtime.

To pin a new contract, decode the runtime's metadata with scalecodec's
`MetadataVersioned` (from `state_getMetadata` on a node running it, or
natively from `Runtime::metadata_at_version(14)` with `SKIP_WASM_BUILD=1`,
inside `sp_io::TestExternalities` because `MateriosChainId` reads storage),
pass its `V14` body to `slash_contract_fingerprint`, and add the value only
after reviewing the new digest against the pallet source.

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
the next poll. Because no RPC reads state and submits atomically, a change
or expiry after the final read can still race inclusion: the pallet rejects
the extrinsic, the client logs it as a failure, and the next tick re-reads
the runtime/TTL.

The digest continues to use live `chain_getBlockHash(0)`. It does not use
the `MateriosChainId` constant or an environment-supplied chain ID.

## Rust provenance and validation

`tests/fixtures/frv2/generate.rs` calls the pallet's own
`slash_bad_settlement_evidence_payload`, `settlement_request_id` and
`account_to_bytes`, and asserts that the identity preimage it records hashes
to the pallet's request ID. It produced `vectors.json` at settlement PR #55
round 2, `5fd5f5428a678fc952163b72813e69a509b74ddb`, with the repository's
pinned rustc 1.85.0. The output is byte-identical to the round-1 (`74cd340`)
vectors (SHA-256 `0e307b4b…8d8b`), so round 2 kept the request ID and FRV2
layout. The six cases cover all FraudProof variants, both settlement modes,
u32/u64 block normalization, zero and maximum integer widths. Python checks
the 153-byte identity preimage, request ID, proof encoding and final digest.

To reproduce in a separate settlement checkout at that commit, copy
`generate.rs` to `pallets/intent-settlement/examples/frv2_vectors.rs`, then:

```sh
flock /tmp/materios-cargo-build.lock nice -n 10 env CARGO_TARGET_DIR=/path/to/unique-target \
  cargo run --locked -p pallet-intent-settlement --example frv2_vectors
nice -n 10 python3 -B -m pytest -q -p no:cacheprovider
nice -n 10 python3 scripts/check-slash-mutations.py
```

The mutation harness checks unknown-contract and malformed-context refusals,
each part of the contract fingerprint (docs excluded; call index, argument
types, variant indexes, array lengths, request record, request-scoped slashes
and TTL type included; no fingerprint for recursive or unsupported types),
TTL source/width/boundary, expiry filtering, post-observation and post-lock
checks, protocol-change rejection, peer-digest filtering and request encoding.
It runs isolated copies and rejects surviving or invalid mutants.

## Rollout order

1. Review and release this operator-kit change.
2. Deploy the release to both active slash watchers: Gemtek and Node-3.
   Verify both detect FRAU and the live TTL before enactment.
3. Enact the reviewed runtime containing settlement #55 and the live-genesis
   chain-ID fix. Before enactment, check both: the runtime pins
   `pallet-intent-settlement` at `5fd5f54` (or at a revision whose fingerprint
   and vectors were re-derived here), and its metadata fingerprints to the
   FRV2 pin. The fingerprint alone cannot tell a digest change that leaves
   types untouched (see Limit). After enactment, verify both watchers detect
   FRV2 and can converge on the same request digest.

This PR does not deploy or restart services. The draft PR with green CI is
the implementation handoff; materios performs the cross-model review.
