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

## Supersede driver

Some unattestable requests cannot be slashed: no `FraudProof` covers a wrong
`observed_slot` or a depth the transaction has not reached. On the spec-242
runtime `supersede_settlement_request` (call 22) settles such a claim on
evidence the committee supplies, refunds the displaced bond, and verifies the
same STCA digest `attest_settle` verifies for a request carrying that
evidence. At M > 1 the settle attestors drive it.

`SettleClaimAttestor` runs its usual checks in order: Cardano network, the
transaction found by Kupo, the amount paid to the beneficiary, the voucher
digest from chain state, then slot and depth. A request refused for
`slot_mismatch` or `depth_undershoot` has passed everything before it, so its
transaction is real and pays the voucher. The attestor then supersedes only
when all of these hold:

- the head's context says the runtime offers the reviewed supersede (call
  arguments `claim_id, settled_direct, evidence, signatures` and
  `AttestSettleTag` = `STCA`); live spec 239 does not, so nothing happens there;
- the request is unexpired (`submitted_block + SettlementRequestTtl >= head`);
- the claim is not backing off after a failed supersede;
- the observed depth reaches both the runtime's `MinFinalityDepth` and the
  attestor's own floor;
- the chain-state voucher shows the request's amount and beneficiary payment
  key (the 57-byte type-0 address split the pallet uses).

The evidence it signs keeps the request's transaction, amount, beneficiary,
network and settlement mode, takes the slot Kupo reports, and claims exactly
`MinFinalityDepth`. Every honest attestor derives the same bytes, so their
signatures aggregate. It always differs from the request: a slot mismatch
changes the slot, and an undershot depth exceeds the observed depth.

Signatures go to the existing `settle` bulletin-board channel under the claim
id. The gateway keys rows by digest, so supersede and attest signatures sit
side by side; no gateway change is needed. Anyone can post to the board, and
the pallet fails a whole bundle on one non-member or one bad signature, so
every envelope (attest, supersede, expire, slash) admits a peer row only when
its key is in `OrinqReceipts.CommitteeMembers`, read when the envelope is
assembled, and it is a raw sr25519 signature over the local digest. An
unreadable committee admits no peer. Once the envelope reaches `MinSignerThreshold`, the
attestor takes the shared write lock, re-reads the runtime context and the
pending request, and submits only if the request is unchanged, unexpired and
still supersedable. A peer that already superseded leaves no request, so the
attestor stops with `request_changed` instead of sending a duplicate. A failed
submission backs the claim off 10 blocks, doubling per consecutive failure up
to the TTL, and the backoff is dropped once the claim leaves the pending set.
The pallet charges no fee for supersede (`Pays::No`), and these rules also
keep failed extrinsics out of blocks. The context parse is cached per
`:code` hash, so a flood of junk requests costs three small RPCs a read, not a
metadata download each.

Out of reach: a request whose transaction does not exist, or pays another
amount. The attestor knows no correct transaction for that claim, so it
neither attests nor supersedes; the request waits out its TTL or is slashed
when bonded. STCA does not bind a transaction to one claim, so two claims with
the same beneficiary and amount can be settled by one payout. That holds for
`attest_settle` today; supersede signs only what `attest_settle` would sign
for a request carrying the same evidence and adds no new reach.

## What a fraud proof needs

The pallet checks only that a proof is internally consistent, and
`TxNotFound` always is. What the committee signs is what slashes, so the
watcher signs only what its Cardano view can prove.

- **One lookup, read whole.** `/matches/*@<tx>` reads Kupo's output-reference
  index; `*?transaction_id=` scans every match and times out on a Kupo that
  indexes all addresses. A failed lookup (`kupo_matches_unavailable`) is never
  an empty one: the watcher defers and the attestors refuse it as observer
  unavailable. The amount sum, the shape check and the actual payee all read
  the rows of that one lookup.
- **The same network.** The observer's Cardano genesis must equal the
  request's `mainchain_genesis_hash`.
- **A Kupo that sees everything**, for every proof: `GET /patterns` holds `*`,
  and Kupo returns the network's spent canary with `spent_at`. A Kupo with
  narrow patterns misses beneficiaries and change outputs, and one started
  with `--prune-utxo` forgets spent outputs; either turns an honest payout
  into TxNotFound, WrongAmount or WrongBeneficiary. The canary (`SPENT_CANARIES`,
  per Shelley genesis) is an output created and spent at preprod slot
  10608917, so it also proves Kupo has indexed that far back. A network
  without a canary, mainnet today, cannot prove fraud.
- **TxNotFound needs two absence proofs.** Each proof reads the Ogmios tip,
  then Kupo's checkpoint, which must be within `KUPO_TIP_LAG_MAX_SLOTS` (60)
  of that tip, then the coverage checks, then the lookup, which must succeed
  and be empty. The requester's slot plays no part. The two proofs must be
  at least `TX_ABSENCE_CONFIRM_BLOCKS` (10) Cardano blocks apart; a tx seen in
  between restarts the count, and a replacement request starts its own.
  Ogmios has no historical tx lookup and no daemon has an Insight or Koios
  endpoint configured, so the second proof is a second Kupo lookup.

### The deployed Kupo cannot prove fraud yet

Both cert-daemons read the validation Kupo on Node-3 (`:1442`; Gemtek as
`192.168.0.133:1442`, Node-3's container as `172.17.0.1:1442`). Read on
2026-10-18:

| Kupo | `/patterns` holds `*` | keeps spent outputs | fraud proofs |
| --- | --- | --- | --- |
| validation `:1442` (v2.9) | no: 4 stored patterns | yes: no `--prune-utxo`, spent rows have `spent_at` | refused (`patterns_without_*:4`) |
| all-address replica `:1444` (v2.12, read-only, writer `--match *`) | yes | yes: no `--prune-utxo`, canary returned spent | allowed |

So on today's deployment the watcher signs no slash at all; every candidate
is deferred with `kupo_coverage:patterns_without_*:4`. Slashing needs this
infrastructure change: on Node-3, `ufw allow from 172.17.0.0/16 to any port
1444 proto tcp` (Gemtek is already allowed), then `KUPO_URL` on both
cert-daemons set to `http://192.168.0.133:1444`. The settle and expire
attestors read the same `KUPO_URL` and work against either Kupo, since the
tx lookup uses the index; on `:1444` they also see payouts to addresses the
validation patterns miss.

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

The mutation harness checks the tag table (an unknown tag, a tagless FRV2
contract and a tag without its pin all refuse), the supersede contract check,
unknown-contract and malformed-context refusals, each part of the contract
fingerprint (docs excluded; call index, argument types, variant indexes, array
lengths, request record, request-scoped slashes and TTL type included; no
fingerprint for recursive or unsupported types), the per-code cache, TTL and
`MinFinalityDepth` sources and widths, expiry filtering, post-observation and
post-lock checks, protocol-change rejection, peer-digest filtering, request
encoding, and each supersede guard (runtime support, expiry, backoff and its
doubling and cap, depth floors, voucher, signed slot and depth, quorum, the
lock-time re-reads). It runs isolated copies and rejects surviving or invalid
mutants.

`test_merged_settlement_vector` checks the Python FRV2 digest against the
vector settlement `0757de3` asserts in `integration.rs`.

## Rollout order

1. Review and release this operator-kit change.
2. Deploy it to the cert-daemons that reach Cardano (slash watcher and
   settle attestor run in the same process): Gemtek and Node-3, the M=2
   committee for these paths. Deploy it to the custody watcher on node2.
   Verify each cert-daemon detects FRAU, `supersede=False`, TTL 2400 and
   `MinFinalityDepth` 15 on live spec 239. Rollback before enactment is the
   previous image or release. After enactment the previous builds' slashes
   fail (they sign FRAU), their attestors cannot supersede and the previous
   custody watcher pages on every supersede, so roll forward instead.
3. Enact the reviewed runtime (Materios `67ec3d9`, spec 242, transaction
   version 6, settlement `0757de3`). Before enactment, check that its
   metadata carries `SlashSettlementTag` = `FRV2` and fingerprints to the FRV2
   pin; the tag cannot reveal a layout change under an unchanged tag (see
   Limit), so also check the settlement revision. After enactment, verify
   every daemon detects FRV2 and `supersede=True`.

This PR does not deploy or restart services. The draft PR with green CI is
the implementation handoff; materios performs the cross-model review.
