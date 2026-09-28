"""Classify custody and authority moves on Materios and Cardano.

Pure functions over decoded chain data; ``custody_watch`` does the fetching.

Materios: every extrinsic of a finalized block is decoded against runtime
metadata and its call tree walked through Sudo, Multisig, Utility and Recovery
wrappers. Root calls, runtime-code changes, forced balance moves, recovery,
treasury spends, finality overrides and committee-gate levers are CRITICAL.

Cardano: a transaction is classified by what it spends and produces at watched
addresses and what it mints under watched policies. A surrender-pool spend is
INFO only when it has every mark of a surrender (the surrender redeemer, the
pool's continuing output keeping its inline datum, a payout equal to the rate-table
entitlement for the legacy assets deposited at the quarantine address, and each
wallet that gave up those assets paid exactly the entitlement of its own). An
underpayment or an asset outside the rate table is an ALERT; any other departure
is CRITICAL.
"""

from __future__ import annotations

import enum
import hashlib
import json
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from scalecodec.base import RuntimeConfigurationObject, ScaleBytes
from scalecodec.type_registry import load_type_registry_preset
from scalecodec.types import GenericCall
from substrateinterface.utils.ss58 import ss58_decode, ss58_encode

from daemon.cardano_address import decode_cardano_address

SS58_FORMAT = 42
DAY = 86_400


class Severity(enum.IntEnum):
    INFO = 0
    ALERT = 1
    CRITICAL = 2


INFO, ALERT, CRITICAL = Severity.INFO, Severity.ALERT, Severity.CRITICAL


@dataclass(frozen=True)
class Finding:
    severity: Severity
    key: str
    headline: str
    details: tuple[str, ...] = ()
    kind: str = "event"
    amount: int = 0
    # Pending findings that share a group are paged as one message; None pages alone.
    group: str | None = None

    def render(self) -> str:
        return "\n".join([f"[{self.severity.name}] {self.headline}", *(f"  {d}" for d in self.details)])


# --- configuration -------------------------------------------------------------


@dataclass(frozen=True)
class WatchedAddress:
    label: str
    address: str
    role: str
    severity: Severity = ALERT


@dataclass(frozen=True)
class WatchedPolicy:
    label: str
    policy_id: str
    severity: Severity = CRITICAL


@dataclass(frozen=True)
class Redemption:
    key: str
    numerator: int
    denominator: int
    unit: str | None = None
    policy_id: str | None = None
    # For a redemption by policy, the asset names (hex) it redeems: a collection policy
    # that can still mint must not make a fresh name redeemable.
    asset_names: frozenset[str] = frozenset()


@dataclass(frozen=True)
class PinnedUnit:
    """A legacy unit as the merger's redemption pin records it: what may still be
    surrendered is its supply less the team waiver less what quarantine holds."""
    unit: str
    asset: str
    supply: int
    waiver: int
    quarantined: int


@dataclass(frozen=True)
class PoolCoverage:
    """What the surrender pool still owes: every pinned unit, priced at its asset's
    rate-table rate, against a floor and a deadline."""
    units: tuple[PinnedUnit, ...]
    rates: dict[str, tuple[int, int]]
    deadline: float
    floor_percent: int
    runout_page_days: int


@dataclass(frozen=True)
class SurrenderPool:
    label: str
    address: str
    cmatra_unit: str
    cmatra_decimals: int
    quarantine_address: str | None
    redeemers: dict
    surrender_redeemer: str
    redemptions: tuple[Redemption, ...]
    max_payout: int | None = None
    coverage: PoolCoverage | None = None


@dataclass(frozen=True)
class CardanoNetwork:
    name: str
    blockfrost_url: str
    project_id_file: str
    poll_seconds: int
    reorg_depth_blocks: int
    addresses: tuple[WatchedAddress, ...]
    policies: tuple[WatchedPolicy, ...]
    pool: SurrenderPool | None


@dataclass(frozen=True)
class MateriosConfig:
    name: str
    rpc_url: str
    poll_seconds: int
    state_pruning_blocks: int
    start_block: int | None
    # Accounts besides Sudo.Key whose moves are authority moves: the sudo multisig's
    # signatories, committee operators.
    authority_accounts: tuple[str, ...] = ()


@dataclass(frozen=True)
class WatchConfig:
    state_db: str
    digest_hour_utc: int
    source_stale_seconds: int
    materios: MateriosConfig | None
    cardano: tuple[CardanoNetwork, ...]
    # A file holding the webhook of the watcher's own channel; without one it pages
    # through DISCORD_WEBHOOK_URL.
    discord_webhook_file: str | None = None


ADDRESS_ROLES = ("custody", "contract")


def parse_config(doc: dict, base_dir: Path | None = None) -> WatchConfig:
    """``base_dir`` anchors relative key paths, so a config handed over by systemd
    ``LoadCredential`` can name its sibling key files."""
    materios = doc.get("materios")
    webhook_file = doc.get("discord_webhook_file")
    return WatchConfig(
        state_db=doc["state_db"],
        digest_hour_utc=int(doc.get("digest_hour_utc", 13)),
        source_stale_seconds=int(doc.get("source_stale_seconds", 900)),
        materios=MateriosConfig(
            name=materios["name"],
            rpc_url=materios["rpc_url"],
            poll_seconds=int(materios.get("poll_seconds", 6)),
            state_pruning_blocks=int(materios.get("state_pruning_blocks", 256)),
            start_block=materios.get("start_block"),
            authority_accounts=tuple(materios.get("authority_accounts", ())),
        ) if materios else None,
        cardano=tuple(_parse_network(n, base_dir) for n in doc.get("cardano", [])),
        discord_webhook_file=_relative(webhook_file, base_dir) if webhook_file else None,
    )


def _relative(path: str, base_dir: Path | None) -> str:
    return str(base_dir / path) if base_dir else path


def _parse_network(doc: dict, base_dir: Path | None) -> CardanoNetwork:
    addresses = []
    for a in doc.get("addresses", []):
        if a["role"] not in ADDRESS_ROLES:
            raise ValueError(f"{doc['name']}: address {a['label']} has unknown role {a['role']!r}")
        addresses.append(WatchedAddress(a["label"], a["address"], a["role"],
                                        Severity[a.get("severity", "ALERT")]))
    pool = doc.get("surrender_pool")
    redemptions = tuple(_parse_redemption(doc["name"], r) for r in pool.get("redemptions", [])) if pool else ()
    coverage = pool.get("coverage") if pool else None
    if coverage and not pool.get("quarantine_address"):
        raise ValueError(f"{doc['name']}: surrender_pool coverage needs the quarantine_address it counts")
    return CardanoNetwork(
        name=doc["name"],
        blockfrost_url=doc["blockfrost_url"].rstrip("/"),
        project_id_file=_relative(doc["project_id_file"], base_dir),
        poll_seconds=int(doc.get("poll_seconds", 60)),
        reorg_depth_blocks=int(doc.get("reorg_depth_blocks", 30)),
        addresses=tuple(addresses),
        policies=tuple(WatchedPolicy(p["label"], p["policy_id"], Severity[p.get("severity", "CRITICAL")])
                       for p in doc.get("policies", [])),
        pool=SurrenderPool(
            label=pool["label"],
            address=pool["address"],
            cmatra_unit=pool["cmatra_unit"],
            cmatra_decimals=int(pool.get("cmatra_decimals", 6)),
            quarantine_address=pool.get("quarantine_address"),
            redeemers={int(k): v for k, v in pool["redeemers"].items()},
            surrender_redeemer=pool["surrender_redeemer"],
            redemptions=redemptions,
            max_payout=pool.get("max_payout"),
            coverage=_parse_coverage(doc["name"], coverage, redemptions, base_dir) if coverage else None,
        ) if pool else None,
    )


def _parse_coverage(network: str, doc: dict, redemptions: tuple[Redemption, ...],
                    base_dir: Path | None) -> PoolCoverage:
    """The merger's redemption pin and rate table, as it publishes them. Every pinned
    asset must have a rate, and every redemption the classifier checks surrenders
    against must be priced as the rate table prices it."""
    pin = json.loads(Path(_relative(doc["redemption_pin_file"], base_dir)).read_text())
    table = json.loads(Path(_relative(doc["rate_table_file"], base_dir)).read_text())["tokens"]
    rates = {}
    for asset in pin["assets"]:
        if asset not in table:
            raise ValueError(f"{network}: pinned asset {asset} has no rate in the rate table")
        rates[asset] = (int(table[asset]["rate_numerator"]), int(table[asset]["rate_denominator"]))
    for r in redemptions:
        if r.key in rates and rates[r.key] != (r.numerator, r.denominator):
            raise ValueError(f"{network}: redemption {r.key} is priced {r.numerator}/{r.denominator}, "
                             f"the rate table {rates[r.key][0]}/{rates[r.key][1]}")
    units = tuple(PinnedUnit(entry["policy_id"] + name, asset, int(row["supply"]), int(row["waiver"]),
                             int(row["quarantined"]))
                  for asset, entry in pin["assets"].items() for name, row in entry["units"].items())
    deadline = datetime.fromisoformat(doc["deadline_utc"])
    return PoolCoverage(units, rates, deadline.timestamp(), int(doc.get("floor_percent", 90)),
                        int(doc.get("runout_page_days", 14)))


def outstanding(coverage: PoolCoverage, held: dict[str, int]) -> dict[str, int]:
    """cMATRA the pool still owes per asset: each unit's remaining at the pin, less what
    quarantine has received of it since, summed per asset and priced floor(count *
    numerator / denominator), as the merger's compute_redemption prices a surrender."""
    counts: dict[str, int] = defaultdict(int)
    for u in coverage.units:
        pinned = u.supply - u.waiver - u.quarantined
        counts[u.asset] += max(0, min(pinned, u.supply - u.waiver - held.get(u.unit, 0)))
    return {asset: count * coverage.rates[asset][0] // coverage.rates[asset][1] for asset, count in counts.items()}


def _parse_redemption(network: str, doc: dict) -> Redemption:
    names = frozenset(doc.get("asset_names", ()))
    if doc.get("policy_id") and not names:
        raise ValueError(f"{network}: redemption {doc['key']} is by policy but pins no asset_names")
    return Redemption(doc["key"], int(doc["numerator"]), int(doc["denominator"]),
                      doc.get("unit"), doc.get("policy_id"), names)


# --- Materios --------------------------------------------------------------------


# A runtime decodes an extrinsic whose calls nest up to MAX_EXTRINSIC_DEPTH (256) deep,
# and scalecodec recurses about 13 frames per nested call, so Python's default limit of
# 1000 would stop at about 75 and leave a deeper call unread.
DECODE_RECURSION_LIMIT = 10_000
# The values a block's extrinsics, and separately its events, may decode into.
# scalecodec builds a Python object of a few hundred bytes, in about 15 microseconds,
# for every element it reads, and an element can be a single byte: a block filled to
# its length limit would take gigabytes and minutes. At this budget a block costs at
# most about a second and a few megabytes. A routine block needs under a hundred values,
# and an extrinsic the budget does not reach pages CRITICAL as undecoded.
DECODE_BUDGET = 50_000


class DecodeBudgetExceeded(Exception):
    """A decode stopped at the values its block's budget had left."""

    def __str__(self) -> str:
        return f"stopped at the budget of {DECODE_BUDGET:,} decoded values per block"


class _MeteredConfig(RuntimeConfigurationObject):
    """Counts every value scalecodec builds, since it builds each one through
    ``create_scale_object``, and stops a decode that would build more than ``stop_at``."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.values = 0
        self.stop_at: int | None = None
        self.stopped = False

    def create_scale_object(self, type_string, data=None, **kwargs):
        if self.stop_at is not None and self.values >= self.stop_at:
            self.stopped = True
            raise DecodeBudgetExceeded()
        self.values += 1
        return super().create_scale_object(type_string, data, **kwargs)


def _bytes_read(scale_object) -> bytearray:
    """The bytes ``scale_object`` has read so far, which are all of its own once its decode is done."""
    end = scale_object.data.offset if scale_object.data_end_offset is None else scale_object.data_end_offset
    return scale_object.data.data[scale_object.data_start_offset:end]


def _envelope(extrinsic_hex: str) -> tuple[bool, bytes | None]:
    """Whether an extrinsic carries a signature, and the account that signed it, read
    from its fixed-layout header without decoding its call: the version byte after the
    compact length prefix has its top bit set when signed, and a ``MultiAddress::Id``
    signer follows it as tag 0 and 32 bytes. The signer is None for any other address."""
    first = int(extrinsic_hex[2:4], 16)
    prefix = 1 << (first & 3) if first & 3 < 3 else (first >> 2) + 5
    at = 2 + 2 * prefix
    version = extrinsic_hex[at:at + 2]
    if version and not int(version, 16) & 0x80:
        return False, None
    signer = extrinsic_hex[at + 4:at + 68] if extrinsic_hex[at + 2:at + 4] == "00" else ""
    return True, bytes.fromhex(signer) if len(signer) == 64 else None


def accountable(sudo_key: bytes | None, authorities: frozenset[bytes]) -> frozenset[bytes]:
    """The accounts whose moves are authority moves: Sudo.Key and the configured authorities."""
    return authorities | ({sudo_key} if sudo_key else frozenset())


class RuntimeDecoder:
    """Decodes extrinsics and ``System.Events`` against one runtime's metadata.

    Holding the metadata here, rather than inside a websocket client, lets a block
    whose state the node has already pruned be decoded against a pinned runtime.
    """

    def __init__(self, metadata_hex: str):
        self._config = _MeteredConfig(ss58_format=SS58_FORMAT)
        self._config.update_type_registry(load_type_registry_preset("core"))
        self._metadata = self._config.create_scale_object("MetadataVersioned", data=ScaleBytes(metadata_hex))
        self._metadata.decode()
        self._config.add_portable_registry(self._metadata)
        # scalecodec hashes a call while decoding it, before its end offset is set, and so
        # hashes everything after the call too: a batch's decode would grow with its length
        # squared. The runtime's call types are classes of this registry alone.
        for name, decoder_class in self._config.type_registry["types"].items():
            if name.startswith("scale_info::") and issubclass(decoder_class, GenericCall):
                decoder_class.get_used_bytes = _bytes_read
        self._events_type = (self._metadata.get_metadata_pallet("System")
                             .get_storage_function("Events").get_value_type_string())

    @property
    def values(self) -> int:
        """The values this decoder has built so far; what a block cost is the difference."""
        return self._config.values

    def extrinsics(self, extrinsics_hex: list[str], accountable: frozenset[bytes] = frozenset(),
                   ahead: tuple[frozenset[bytes], ...] = ()) -> list[dict]:
        """Each extrinsic decoded, or ``{"undecodable": hex, "error": ...}`` for one this
        runtime's metadata cannot read or its DECODE_BUDGET does not reach, so a call the
        watcher has not read is reported rather than silently skipped.

        Extrinsics signed by an ``accountable`` account decode first, against a budget of
        their own: any funded account can fill a block with extrinsics smaller than a
        multisig leg, but none can sign as these accounts, so an account anyone can become
        never belongs there. The rest share a second budget: unsigned extrinsics first, the
        inherents among them, then those signed by each set of ``ahead`` in turn, then
        everyone else's, smallest first within each, so a large extrinsic cannot spend what
        the inherents and a small privileged call need."""
        envelopes = [_envelope(x) for x in extrinsics_hex]

        def rank(signed: bool, signer: bytes | None) -> int:
            if signer in accountable:
                return 0
            if not signed:
                return 1
            return 2 + next((n for n, accounts in enumerate(ahead) if signer in accounts), len(ahead))

        ranks = [rank(*envelope) for envelope in envelopes]
        decoded: list[dict] = [{} for _ in extrinsics_hex]
        budgets = {True: DECODE_BUDGET, False: DECODE_BUDGET}
        for index in sorted(range(len(extrinsics_hex)), key=lambda i: (ranks[i], len(extrinsics_hex[i]))):
            x = extrinsics_hex[index]
            reserved = ranks[index] == 0
            before = self.values
            try:
                decoded[index] = self._decode("Extrinsic", x, budgets[reserved])
            except Exception as e:  # scalecodec raises any type on bytes it cannot place
                decoded[index] = {"undecodable": x, "error": f"{type(e).__name__}: {e}"[:200]}
            budgets[reserved] -= self.values - before
        return decoded

    def storage(self, pallet: str, item: str, value_hex: str):
        """A value of ``pallet``'s storage ``item``, decoded within DECODE_BUDGET."""
        function = self._metadata.get_metadata_pallet(pallet).get_storage_function(item)
        return self._decode(function.get_value_type_string(), value_hex, DECODE_BUDGET)

    def events(self, events_hex: str) -> dict[int, list[str]]:
        grouped: dict[int, list[str]] = defaultdict(list)
        for record in self._decode(self._events_type, events_hex, DECODE_BUDGET):
            if record.get("phase") == "ApplyExtrinsic":
                grouped[record["extrinsic_idx"]].append(_event_text(record))
        return dict(grouped)

    def _decode(self, type_string: str, data_hex: str, budget: int):
        """``data_hex`` decoded as ``type_string``, building at most ``budget`` values."""
        config = self._config
        config.stop_at, config.stopped = config.values + budget, False
        limit = sys.getrecursionlimit()
        sys.setrecursionlimit(max(limit, DECODE_RECURSION_LIMIT))
        try:
            obj = config.create_scale_object(type_string, data=ScaleBytes(data_hex), metadata=self._metadata)
            value = obj.decode(check_remaining=True)
        finally:
            sys.setrecursionlimit(limit)
            config.stop_at = None
        # scalecodec's opaque-call wrappers catch every error, a stopped decode's
        # included, and return the bytes they did not read as though decoded.
        if config.stopped:
            raise DecodeBudgetExceeded()
        return value


def _event_text(record: dict) -> str:
    name = f"{record['module_id']}.{record['event_id']}"
    attributes = record.get("attributes")
    if not isinstance(attributes, dict):
        return name
    results = {k: v for k, v in attributes.items() if k.endswith("result") or k == "dispatch_error"}
    if not results:
        return name
    return name + "(" + ", ".join(f"{k}={_result_text(v)}" for k, v in results.items()) + ")"


def _result_text(value) -> str:
    if isinstance(value, dict) and set(value) == {"Ok"}:
        return "Ok"
    if isinstance(value, dict) and set(value) == {"Err"}:
        return "Err:" + json.dumps(value["Err"], default=str)[:120]
    return json.dumps(value, default=str)[:120]


EVENT_NOISE = ("Motra.", "TransactionPayment.", "Balances.Withdraw", "Balances.Deposit")

_ANY = "*"
# Gated by ensure_root or an EnsureRoot origin in the spec-238 runtime and its pallets:
# these take effect only under Root, and only Sudo gives Root.
_ROOT_GATED = frozenset({
    *(("System", f) for f in (
        "set_heap_pages", "set_code", "set_code_without_checks", "set_storage", "kill_storage",
        "kill_prefix", "authorize_upgrade", "authorize_upgrade_without_checks")),
    *(("Balances", f) for f in (
        "force_transfer", "force_unreserve", "force_set_balance", "force_adjust_total_issuance")),
    *(("Treasury", f) for f in ("spend_local", "remove_approval", "spend", "void_spend")),
    *(("Vesting", f) for f in ("force_vested_transfer", "force_remove_vesting_schedule")),
    ("Grandpa", "note_stalled"),
    ("Recovery", "set_recovered"),
    ("Utility", "dispatch_as"),
    ("Utility", "with_weight"),
    ("SessionCommitteeManagement", "set_main_chain_scripts"),
    ("NativeTokenManagement", "set_main_chain_scripts"),
    *(("OrinqReceipts", f) for f in (
        "set_availability_cert", "set_committee", "join_committee", "leave_committee",
        "set_attestation_reward_per_signer", "set_era_cap_base", "set_era_cap_baseline_attestor_count",
        "slash_attestor", "set_bond_requirement", "set_receipt_submission_fee",
        "set_receipt_submission_fee_floor", "set_receipt_expiry_blocks", "set_bad_attest_slash_threshold",
        "reset_candidate_liveness", "set_core_eviction_enabled", "set_contribution_window_enabled",
        "set_break_glass_floor_enabled", "set_slack_invariant_enabled", "set_break_glass_aura_keys",
        "set_pinned_committee", "clear_pinned_committee")),
    ("Motra", "set_params"),
    ("TeeAttestation", "set_disabled"),
    ("Billing", "governance_set_endpoint_price"),
    ("Billing", "governance_set_debits_enabled"),
    ("PerpEngine", "governance_set_market"),
    ("Oracle", "register_attestor"),
    ("IntentSettlement", "set_pool_utilization"),
    ("IntentSettlement", "set_min_signer_threshold"),
})
_CALL_SEVERITY: dict[tuple[str, str], Severity] = {
    **dict.fromkeys(_ROOT_GATED, CRITICAL),
    ("Sudo", _ANY): CRITICAL,
    ("System", "apply_authorized_upgrade"): CRITICAL,
    ("Treasury", "payout"): CRITICAL,
    # Any account may start, vouch for or claim a recovery; the walk raises one that
    # names an authority account to CRITICAL.
    ("Recovery", _ANY): ALERT,
    ("Grandpa", "report_equivocation"): ALERT,
    ("Grandpa", "report_equivocation_unsigned"): ALERT,
    ("NativeTokenManagement", "transfer_tokens"): ALERT,
    ("PalletSession", "set_keys"): ALERT,
    ("PalletSession", "purge_keys"): ALERT,
    ("RootTimelock", _ANY): CRITICAL,
}
# Calls of the spec-238 runtime that move no custody or authority. A call in neither
# table pages as an ALERT, so one a runtime upgrade adds is reviewed, not ignored.
_ROUTINE = frozenset({
    ("System", "remark"), ("System", "remark_with_event"),
    ("Timestamp", "set"),
    *(("Balances", f) for f in (
        "transfer_allow_death", "transfer_keep_alive", "transfer_all", "upgrade_accounts", "burn")),
    *(("Multisig", f) for f in ("as_multi_threshold_1", "as_multi", "approve_as_multi", "cancel_as_multi")),
    *(("Utility", f) for f in ("batch", "as_derivative", "batch_all", "force_batch")),
    ("Treasury", "check_status"),
    *(("Vesting", f) for f in ("vest", "vest_other", "vested_transfer", "merge_schedules")),
    *(("OrinqReceipts", f) for f in (
        "submit_receipt", "attest_availability_cert", "submit_anchor", "submit_receipt_v2", "bond", "unbond",
        "expire_receipt_fee")),
    ("Motra", "set_delegatee"), ("Motra", "claim_motra"),
    ("SessionCommitteeManagement", "set"),
    ("BlockRewards", "set_current_block_beneficiary"),
    *(("IntentSettlement", f) for f in (
        "submit_intent", "attest_intent", "request_voucher", "request_credit_refund", "settle_claim",
        "expire_policy_mirror", "credit_deposit", "settle_batch_atomic", "attest_batch_intents",
        "request_batch_vouchers", "submit_batch_intents", "request_settle", "attest_settle",
        "request_batch_settle", "attest_batch_settle", "request_expire_policy", "attest_expire_policy",
        "post_settlement_bond", "slash_bad_settlement_evidence", "release_settlement_bond")),
    ("TeeAttestation", "submit_evidence"),
    *(("Billing", f) for f in (
        "topup_self", "topup_for", "pay_request", "request_withdrawal", "execute_withdrawal", "cancel_withdrawal",
        "prune_paid_requests")),
    ("Oracle", "submit_price"),
    *(("PerpEngine", f) for f in (
        "open_position", "close_position", "deposit_margin", "withdraw_margin", "liquidate", "settle_funding",
        "adjust_leverage", "reserve_keeper_bond", "release_keeper_bond")),
})
_MULTISIG_CALLS = {"as_multi", "as_multi_threshold_1", "approve_as_multi", "cancel_as_multi"}
_BATCH_CALLS = {"batch", "batch_all", "force_batch"}


def account_bytes(account: str) -> bytes:
    if account.startswith("0x"):
        return bytes.fromhex(account[2:])
    return bytes.fromhex(ss58_decode(account))


def _account(value) -> bytes | None:
    """The account an AccountId or MultiAddress value names, or None for the Index, Raw,
    Address20 and Address32 variants (which the runtime's lookup rejects) and for any
    value that names no account."""
    if isinstance(value, dict) and set(value) == {"Id"}:
        value = value["Id"]
    if not isinstance(value, str):
        return None
    try:
        raw = account_bytes(value)
    except ValueError:
        return None
    return raw if len(raw) == 32 else None


def render_account(account: bytes) -> str:
    return ss58_encode(account.hex(), SS58_FORMAT)


def _compact(n: int) -> bytes:
    if n < 1 << 6:
        return bytes([n << 2])
    if n < 1 << 14:
        return ((n << 2) | 1).to_bytes(2, "little")
    if n < 1 << 30:
        return ((n << 2) | 2).to_bytes(4, "little")
    raw = n.to_bytes((n.bit_length() + 7) // 8, "little")
    return bytes([((len(raw) - 4) << 2) | 3]) + raw


def multisig_account(signatories: list[bytes], threshold: int) -> bytes:
    """``pallet_multisig::multi_account_id``: blake2_256 over the sorted signatories."""
    ordered = sorted(signatories)
    preimage = b"modlpy/utilisuba" + _compact(len(ordered)) + b"".join(ordered) + threshold.to_bytes(2, "little")
    return hashlib.blake2b(preimage, digest_size=32).digest()


def _args(call: dict) -> dict:
    return {a["name"]: a["value"] for a in call.get("call_args", [])}


def _is_call(value) -> bool:
    return isinstance(value, dict) and "call_module" in value


def hex_digest(hex_text: str) -> str:
    """The length and blake2_256 of the bytes that 0x-prefixed ``hex_text`` spells, read a
    chunk at a time so a block-sized value costs no copy of itself."""
    digest = hashlib.blake2b(digest_size=32)
    for start in range(2, len(hex_text), 1 << 16):
        digest.update(bytes.fromhex(hex_text[start:start + (1 << 16)]))
    return f"{(len(hex_text) - 2) // 2} bytes blake2_256 0x{digest.hexdigest()}"


# One character class repeated: a repeated group would keep a backtracking mark per
# repetition, over 100 bytes of memory for each byte of a block-sized argument.
_HEX_DIGITS = re.compile(r"[0-9a-fA-F]*")
_LONG_HEX_BYTES = 65


def _render_value(value) -> str:
    """A decoded argument as one line. Byte strings too long to read are shown as their
    hash; everything else goes through JSON, so text from the chain keeps its quotes
    and its newlines stay escaped."""
    if (isinstance(value, str) and value.startswith("0x") and len(value) % 2 == 0
            and len(value) >= 2 + 2 * _LONG_HEX_BYTES and _HEX_DIGITS.fullmatch(value, 2)):
        return f"<{hex_digest(value)}>"
    # A string's first 401 characters encode to the same first 400 as the whole of it.
    text = json.dumps(value[:401] if isinstance(value, str) else value, default=str)
    return text if len(text) <= 400 else text[:400] + "\u2026"


def _holds_calls(value) -> bool:
    return _is_call(value) or (isinstance(value, list) and bool(value) and all(_is_call(x) for x in value))


# An origin during a walk is an account (bytes), _ROOT, or None for one the walk cannot name.
_ROOT = "Root"


MAX_PATH = 200
MAX_SITE_ARGS = 300
MAX_SITE_LINES = 20
MAX_UNDECODED_LINES = 20
MAX_TREE_LINES = 200
MAX_INDENT = 16


def _indent(depth: int) -> str:
    """A tree line's indent; past MAX_INDENT levels it stops growing, so a call nested
    hundreds deep costs only its line's text."""
    return "  " * min(depth, MAX_INDENT)


# What must hold for a call to dispatch, as (fact, value): the call is blocked once a
# block's state proves the fact holds another value. SUDO is Sudo.Key; ("proxy", rescuer)
# is Recovery.Proxy(rescuer), the account the rescuer may act as. NEVER, a root-gated call
# reached from an account, needs no proof: Root comes only from Sudo.
SUDO = ("sudo",)
NEVER = (("never",), None)


def proxy_fact(rescuer: bytes) -> tuple:
    return ("proxy", rescuer)


@dataclass(frozen=True)
class _Site:
    """A privileged call in an extrinsic's call tree, with its own arguments rendered;
    ``conditions`` for its dispatch, ``unlisted`` when it is in neither call table."""
    path: str
    args: str
    depth: int
    severity: Severity
    conditions: frozenset
    unlisted: bool

    def blocked(self, facts: dict) -> bool:
        """Whether ``facts`` (fact -> the value the chain's state proves it held) show this
        call could not dispatch."""
        return any(c == NEVER or (c[0] in facts and facts[c[0]] != c[1]) for c in self.conditions)

    def line(self, counted: bool, blocked: bool) -> str:
        notes = [" (cannot take effect from this origin)"] if blocked and not counted else []
        if self.unlisted:
            notes.append(" (in neither the severity table nor the routine list)")
        args = self.args if len(self.args) <= MAX_SITE_ARGS else self.args[:MAX_SITE_ARGS] + "\u2026"
        return f"{self.severity.name}: {self.path}({args}){''.join(notes)}"


@dataclass
class _Tree:
    """What walking one extrinsic's call tree found."""
    sudo_key: bytes | None
    authority: frozenset[bytes]
    lines: list[str] = field(default_factory=list)
    hidden: int = 0
    sites: list[_Site] = field(default_factory=list)
    involved: bool = False

    def line(self, text: str) -> None:
        """Add a line of the call tree, or count it once MAX_TREE_LINES are held, so a
        batch of a block's worth of calls keeps only what a page can show."""
        if len(self.lines) < MAX_TREE_LINES:
            self.lines.append(text)
        else:
            self.hidden += 1


def _multisig_origin(function: str, args: dict, origin) -> bytes | None:
    """The multisig account a Multisig call dispatches as, or None when its signatories
    or threshold do not name one."""
    threshold = 1 if function == "as_multi_threshold_1" else args.get("threshold")
    others = args.get("other_signatories")
    if (not isinstance(origin, bytes) or not isinstance(threshold, int) or not 0 <= threshold < 1 << 16
            or not isinstance(others, list)):
        return None
    signatories = [origin, *(_account(s) for s in others)]
    return None if None in signatories else multisig_account(signatories, threshold)


def _dispatched_origin(value):
    """The origin ``Utility.dispatch_as`` names: Root, a signed account, or None."""
    system = value.get("system") if isinstance(value, dict) else None
    if system == "Root":
        return _ROOT
    return _account(system.get("Signed")) if isinstance(system, dict) else None


def _inner_origin(module: str, function: str, args: dict, origin, tree: _Tree, depth: int):
    """The origin the calls wrapped by this one dispatch with."""
    if module == "Utility" and (function in _BATCH_CALLS or function == "with_weight"):
        return origin
    if (module, function) == ("Utility", "as_derivative"):
        index = args.get("index")
        if not isinstance(origin, bytes) or not isinstance(index, int) or not 0 <= index < 1 << 16:
            return None
        # pallet_utility::derivative_account_id
        return hashlib.blake2b(b"modlpy/utilisuba" + origin + index.to_bytes(2, "little"), digest_size=32).digest()
    if (module, function) == ("Utility", "dispatch_as"):
        return _dispatched_origin(args.get("as_origin"))
    if module == "Sudo" and function in ("sudo", "sudo_unchecked_weight"):
        return _ROOT if origin == _ROOT or (isinstance(origin, bytes) and origin == tree.sudo_key) else None
    if (module, function) == ("Sudo", "sudo_as"):
        return _account(args.get("who"))
    if (module, function) == ("Recovery", "as_recovered"):
        return _account(args.get("account"))
    if module == "Multisig" and function in _MULTISIG_CALLS:
        account = _multisig_origin(function, args, origin)
        if account is not None:
            label = "is Sudo.Key " if account == tree.sudo_key else ""
            tree.line(f"{_indent(depth + 1)}multisig account {label}{render_account(account)}")
        return account
    return None


def _severity(module: str, function: str, args: dict, origin, inner, tree: _Tree) -> Severity | None:
    if module == "Multisig" and inner is not None and inner == tree.sudo_key:
        return CRITICAL
    severity = _CALL_SEVERITY.get((module, function), _CALL_SEVERITY.get((module, _ANY)))
    if module == "Recovery" and severity == ALERT:
        named = [origin, *(_account(args.get(n)) for n in ("account", "lost", "rescuer"))]
        friends = args.get("friends")
        named += [_account(f) for f in friends] if isinstance(friends, list) else []
        if any(isinstance(a, bytes) and a in tree.authority for a in named):
            return CRITICAL
    return severity


# Wrappers whose inner origin follows from the outer one alone. Any other wrapper
# dispatches as an account its caller names, which proves nothing about who signed.
_DERIVED = frozenset({*(("Utility", f) for f in (*_BATCH_CALLS, "with_weight", "as_derivative")),
                      *(("Multisig", f) for f in _MULTISIG_CALLS)})


def _abridged(path: tuple[str, ...]) -> str:
    """A call path short enough for a headline; the outermost and innermost calls
    survive however deep the nesting."""
    text = " > ".join(path)
    if len(text) <= MAX_PATH:
        return text
    return f"{path[0]} > \u2026 {len(path) - 2} calls \u2026 > {path[-1]}"


def _walk(call: dict, origin, tree: _Tree, depth: int, parents: tuple[str, ...], conditions: frozenset,
          proven: bool) -> None:
    """Record ``call`` and every call it wraps. From an account's origin a root-gated call
    never dispatches, a Sudo call dispatches only if that account holds Sudo.Key, and
    Recovery.as_recovered, with everything it wraps, only if its Recovery.Proxy is the
    account named; a subtree carries the conditions of every call above it. ``proven``
    holds while the origin follows from the signer alone; only such an origin can make
    an authority's attempt."""
    module, function = call["call_module"], call["call_function"]
    args = _args(call)
    path = (*parents, f"{module}.{function}")
    rendered = ", ".join(f"{k}={_render_value(v)}" for k, v in args.items() if not _holds_calls(v))
    tree.line(f"{_indent(depth)}{module}.{function}({rendered})")
    if isinstance(origin, bytes):
        if (module, function) in _ROOT_GATED:
            conditions = conditions | {NEVER}
        elif module == "Sudo":
            conditions = conditions | {(SUDO, origin)}
        elif (module, function) == ("Recovery", "as_recovered") and (lost := _account(args.get("account"))):
            conditions = conditions | {(proxy_fact(origin), lost)}
    inner = _inner_origin(module, function, args, origin, tree, depth)
    inner_proven = proven and (module, function) in _DERIVED
    if any(p and isinstance(o, bytes) and o in tree.authority for o, p in ((origin, proven), (inner, inner_proven))):
        tree.involved = True
    severity = _severity(module, function, args, origin, inner, tree)
    unlisted = severity is None and (module, function) not in _ROUTINE
    if unlisted:
        severity = ALERT
    if severity is not None:
        tree.sites.append(_Site(_abridged(path), rendered, depth, severity, conditions, unlisted))
    for value in args.values():
        for child in ([value] if _is_call(value) else value if isinstance(value, list) else []):
            if _is_call(child):
                _walk(child, inner, tree, depth + 1, path, conditions, inner_proven)


RUNTIME_ENVIRONMENT_UPDATED = "0x08"


def runtime_upgraded(header: dict) -> bool:
    """Whether this block changed the runtime, so the blocks after it need new metadata."""
    return RUNTIME_ENVIRONMENT_UPDATED in header["digest"]["logs"]


def runtime_changed(chain: str, number: int) -> Finding:
    """The CRITICAL for a RuntimeEnvironmentUpdated digest, read from the header alone, so
    no extrinsic left undecoded can hide a new runtime."""
    return Finding(CRITICAL, f"{chain}:{number}:runtime",
                   f"{chain} #{number}: the runtime environment changed (a new runtime or heap pages)",
                   details=("the block's header carries a RuntimeEnvironmentUpdated digest; the blocks after it "
                            "are decoded with the new runtime's metadata",))


def plural(n: int, word: str) -> str:
    return f"{n:,} {word}" + ("" if n == 1 else "s")


def unclassifiable(key: str, what: str, error: Exception, *details: str, group: str | None = None) -> Finding:
    """The CRITICAL raised in place of a classification that failed, so the item is
    still paged and its cursor still moves."""
    return Finding(CRITICAL, key, f"{what} could not be classified",
                   details=(*details, f"{type(error).__name__}: {error}"[:200]), group=group)


def _undecoded_line(index: int, ext: dict) -> str:
    signed, signer = _envelope(ext["undecodable"])
    who = f"signer {render_account(signer)}" if signer else "signer names no account" if signed else "unsigned"
    return f"extrinsic {index}: {who}: {hex_digest(ext['undecodable'])}: {ext['error']}"


def classify_materios_block(chain: str, number: int, extrinsics: list[dict],
                            read_events: Callable[[], dict[int, list[str]] | None], sudo_key: bytes | None,
                            authorities: frozenset[bytes] = frozenset(),
                            read_state: Callable[[frozenset[bytes]], dict | None] = lambda rescuers: None,
                            ) -> list[Finding]:
    """Findings for a block's extrinsics, each call tree walked once. ``read_events``
    returns the block's events by extrinsic, or None when they could not be read, and is
    called once, only when an extrinsic has something to report. Without an extrinsic's
    events its calls page as though they took effect, except those the chain's state
    proves could not: ``read_state`` is called at most once, only then, with the
    rescuers whose Recovery.Proxy matters, and returns the facts (SUDO, and
    ("proxy", rescuer)) that held the same value at the block's parent and at the
    block, or None when that state is gone too."""
    findings = []
    accounts = accountable(sudo_key, authorities)
    # What cannot be read may hide any call, so it pages CRITICAL: grouped per source
    # when anyone could have sent it, alone when Sudo.Key or an authority signed it. One
    # finding a block for each, however many extrinsics it could not read.
    unreadable = f"{chain} unclassifiable"
    undecoded: dict[bytes | None, list[tuple[int, dict]]] = defaultdict(list)
    for index, ext in enumerate(extrinsics):
        if "undecodable" in ext:
            signer = _envelope(ext["undecodable"])[1]
            undecoded[signer if signer in accounts else None].append((index, ext))
    for signer, items in undecoded.items():
        listed = [_undecoded_line(index, ext) for index, ext in items[:MAX_UNDECODED_LINES]]
        if len(items) > MAX_UNDECODED_LINES:
            listed.append(f"... {len(items) - MAX_UNDECODED_LINES:,} more")
        what = f"{chain} #{number}: {plural(len(items), 'extrinsic')} could not be decoded"
        if signer is None:
            findings.append(Finding(CRITICAL, f"{chain}:{number}:undecoded", what, details=tuple(listed),
                                    group=unreadable))
        else:
            role = "Sudo.Key" if signer == sudo_key else "an authority account"
            findings.append(Finding(CRITICAL, f"{chain}:{number}:undecoded:{render_account(signer)}",
                                    f"{what}, signed by {role}", details=tuple(listed)))
    reported = []
    for index, ext in enumerate(extrinsics):
        if "undecodable" in ext:
            continue
        key = f"{chain}:{number}:{index}"
        signer = _account(ext.get("address"))
        tree = _Tree(sudo_key, accounts)
        try:
            _walk(ext["call"], signer, tree, 0, (), frozenset(), True)
        except Exception as e:  # argument values are the signer's choice; none may stall the block
            findings.append(unclassifiable(key, f"{chain} #{number} extrinsic {index}", e,
                                           f"extrinsic hash {ext.get('extrinsic_hash')}", group=unreadable))
            continue
        if tree.sites or (signer is not None and signer == sudo_key):
            reported.append((index, ext, signer, tree))
    events = read_events() if reported else None
    unverified = [tree for index, _, signer, tree in reported
                  if (events is None or events.get(index) is None) and signer not in (None, sudo_key)
                  and not tree.involved]
    rescuers = frozenset(fact[1] for tree in unverified for site in tree.sites
                         for fact, _ in site.conditions if fact[0] == "proxy")
    facts = read_state(rescuers) if unverified else None
    findings.extend(_report(chain, number, index, ext, signer, tree, events, facts)
                    for index, ext, signer, tree in reported)
    return findings


def _report(chain: str, number: int, index: int, ext: dict, signer: bytes | None, tree: _Tree,
            events: dict[int, list[str]] | None, facts: dict | None) -> Finding:
    address = ext.get("address")
    # Every applied extrinsic has events, so one with none is as unverified as a block
    # whose events could not be read.
    own = None if events is None else events.get(index)
    # pallet-sudo emits events only for a caller that passed its key check, whatever
    # key this watcher last read.
    if own is not None and any(e.startswith("Sudo.") for e in own):
        tree.involved = True
    by_sudo = signer is not None and signer == tree.sudo_key

    if address is None:
        notes = ["unsigned"]
    elif signer is None:
        notes = [f"signer {_render_value(address)} names no account"]
    else:
        notes = [f"signer {render_account(signer)}"]
    if by_sudo:
        notes.append("signed by Sudo.Key")
    # An authority's attempts page whatever their outcome; anyone else's page only when
    # they could have taken effect.
    counted, proof = tree.sites, {}
    ordinary = not tree.involved and signer is not None
    if own is None:
        notes.append("events unavailable (state pruned or undecodable): dispatch result not verified")
        if ordinary:
            proof = facts or {}
            counted = [s for s in tree.sites if not s.blocked(proof)]
            notes.append("Sudo.Key and Recovery.Proxy, the same at the block and its parent, decide what could "
                         "take effect" if facts is not None else "state at the block unavailable too")
    else:
        shown = [e for e in own if not e.startswith(EVENT_NOISE)]
        notes.append("result: " + (", ".join(shown) if shown else "no events"))
        proof = {SUDO: tree.sudo_key}
        if not tree.involved and any(e.startswith("System.ExtrinsicFailed") for e in own):
            counted = []
            notes.append("dispatch failed: nothing took effect")
        elif not tree.involved:
            counted = [s for s in tree.sites if not s.blocked(proof)]
    severity = CRITICAL if by_sudo else max((s.severity for s in counted), default=INFO)
    # Calls that count lead, most severe and then innermost first, so the headline and
    # the top of a truncated page name the call that matters.
    live = {id(s) for s in counted}
    ranked = sorted(tree.sites, key=lambda s: (id(s) not in live, -s.severity, -s.depth))
    call = ext["call"]
    top = ranked[0].path if ranked else f"{call['call_module']}.{call['call_function']}"
    sites = [s.line(id(s) in live, s.blocked(proof)) for s in ranked[:MAX_SITE_LINES]]
    if len(ranked) > MAX_SITE_LINES:
        sites.append(f"... {len(ranked) - MAX_SITE_LINES:,} more privileged calls")
    lines = tree.lines + ([f"... {tree.hidden:,} more lines of the call tree"] if tree.hidden else [])
    return Finding(
        severity=severity,
        key=f"{chain}:{number}:{index}",
        headline=f"{chain} #{number} extrinsic {index}: {top}",
        details=tuple(notes + sites + ["call tree:"] + lines),
        group=(None if not ordinary else f"{chain} unverified" if own is None
               else f"{chain} signer {render_account(signer)}"),
    )


def describe_recovery(item: str, accounts: tuple[bytes, ...], value, who: Callable[[bytes], str]) -> str:
    """One line for a decoded Recovery storage entry: ``Recoverable`` (account), its
    friends, threshold and delay; ``ActiveRecoveries`` (lost, rescuer), who has vouched;
    ``Proxy`` (rescuer), the account it may act as."""
    if item == "Recoverable":
        return (f"Recovery.Recoverable({who(accounts[0])}): threshold {value['threshold']} of "
                f"{plural(len(value['friends']), 'friend')}, delay {value['delay_period']:,} blocks: "
                + ", ".join(value["friends"]))
    if item == "ActiveRecoveries":
        vouched = ", ".join(value["friends"]) or "no friend yet"
        return (f"Recovery.ActiveRecoveries(lost {who(accounts[0])}, rescuer {who(accounts[1])}): "
                f"started #{value['created']}, vouched by {vouched}")
    return f"Recovery.Proxy({who(accounts[0])}): acts as {who(account_bytes(value))}"


def recovery_friends(value) -> list[bytes]:
    """The friends a decoded ``Recoverable`` or ``ActiveRecoveries`` entry names."""
    return [account_bytes(f) for f in value["friends"]]


def committee_of(extrinsics: list[dict]) -> tuple[tuple[str, ...], ...] | None:
    for ext in extrinsics:
        call = ext.get("call")
        if call is None:
            continue
        if (call["call_module"], call["call_function"]) == ("SessionCommitteeManagement", "set"):
            members = []
            for member in _args(call)["validators"]:
                cross_chain, keys = member[0], member[1]
                members.append((cross_chain, *(f"{k}={keys[k]}" for k in sorted(keys))))
            return tuple(sorted(members))
    return None


def committee_change(chain: str, number: int, previous: tuple, current: tuple) -> Finding:
    key = f"{chain}:{number}:committee"
    if previous == current:
        return Finding(INFO, key, f"{chain} #{number}: committee rotation, membership unchanged", kind="committee")
    added = sorted(set(current) - set(previous))
    removed = sorted(set(previous) - set(current))
    return Finding(
        ALERT, key, f"{chain} #{number}: validator committee membership changed",
        details=tuple([f"+ {' '.join(m)}" for m in added] + [f"- {' '.join(m)}" for m in removed]),
        kind="committee",
    )


# --- Cardano ---------------------------------------------------------------------


def _value(utxos: list[dict]) -> dict[str, int]:
    total: dict[str, int] = defaultdict(int)
    for u in utxos:
        for a in u["amount"]:
            total[a["unit"]] += int(a["quantity"])
    return total


def _delta(after: dict[str, int], before: dict[str, int]) -> dict[str, int]:
    units = set(after) | set(before)
    return {u: after.get(u, 0) - before.get(u, 0) for u in units if after.get(u, 0) != before.get(u, 0)}


def _payment_credential(address: str) -> tuple[bool, bytes] | None:
    """(is_script, credential) for a Shelley address; None for anything else."""
    try:
        _hrp, raw = decode_cardano_address(address)
    except ValueError:
        return None
    if len(raw) < 29 or raw[0] >> 4 > 7:
        return None
    return bool((raw[0] >> 4) & 1), raw[1:29]


def _script_hash(address: str) -> str | None:
    credential = _payment_credential(address)
    return credential[1].hex() if credential and credential[0] else None


class _Names:
    def __init__(self, network: CardanoNetwork):
        self._names: dict[str, tuple[str, int]] = {"lovelace": ("ADA", 6)}
        if network.pool:
            self._names[network.pool.cmatra_unit] = ("cMATRA", network.pool.cmatra_decimals)
            for r in network.pool.redemptions:
                if r.unit:
                    self._names[r.unit] = (r.key, 0)
        self._policies = {r.policy_id: r.key for r in network.pool.redemptions if r.policy_id} if network.pool else {}

    def quantity(self, unit: str, quantity: int) -> str:
        if unit in self._names:
            name, decimals = self._names[unit]
        elif unit[:56] in self._policies:
            name, decimals = f"{self._policies[unit[:56]]}#{bytes.fromhex(unit[56:]).hex()[:16]}", 0
        else:
            name, decimals = f"{unit[:16]}\u2026", 0
        sign = "-" if quantity < 0 else ""
        whole, frac = divmod(abs(quantity), 10 ** decimals) if decimals else (abs(quantity), 0)
        text = f"{whole:,}" + (f".{frac:0{decimals}d}" if decimals else "")
        return f"{sign}{text} {name}"

    def value(self, value: dict[str, int]) -> str:
        parts = [self.quantity(u, q) for u, q in sorted(value.items(), key=lambda kv: kv[0] != "lovelace") if q]
        return ", ".join(parts) if parts else "nothing"


def _redemption_of(unit: str, redemptions: tuple[Redemption, ...]) -> Redemption | None:
    return next((r for r in redemptions if unit == r.unit or (r.policy_id and unit[:56] == r.policy_id)), None)


def _entitlement(deposited: dict[str, int], redemptions: tuple[Redemption, ...]
                 ) -> tuple[int, dict[str, int], dict[str, int], dict[str, int]]:
    """(entitlement, count per redemption key, units no redemption covers, units under a
    redeemable policy but outside its pinned names) for a quarantine deposit."""
    counts: dict[str, int] = defaultdict(int)
    unknown: dict[str, int] = {}
    unpinned: dict[str, int] = {}
    for unit, quantity in deposited.items():
        if unit == "lovelace" or quantity <= 0:
            continue
        rule = _redemption_of(unit, redemptions)
        if rule is None:
            unknown[unit] = quantity
        elif rule.policy_id and unit[56:] not in rule.asset_names:
            unpinned[unit] = quantity
        else:
            counts[rule.key] += quantity
    rates = {r.key: r for r in redemptions}
    total = sum(counts[k] * rates[k].numerator // rates[k].denominator for k in counts)
    return total, dict(counts), unknown, unpinned


def redemption_overruns(network: CardanoNetwork, holding: dict[str, int]) -> list[Finding]:
    """An ALERT for each redemption of which the quarantine address, where every
    surrendered unit stays, holds more than the supply its rate was set for."""
    held: dict[str, int] = defaultdict(int)
    for unit, quantity in holding.items():
        rule = _redemption_of(unit, network.pool.redemptions)
        if rule is not None:
            held[rule.key] += quantity
    return [Finding(ALERT, f"{network.name}:redeemed:{r.key}:{held[r.key]}",
                    f"{network.name}: {held[r.key]:,} {r.key} surrendered against a rate-table supply of "
                    f"{r.denominator:,}",
                    details=("each surrender past that supply is paid from the other holders' share of the pool",))
            for r in network.pool.redemptions if held[r.key] > r.denominator]


def pool_outflow(pool: SurrenderPool, utxos: dict) -> int:
    """The cMATRA a transaction took out of the pool, net of what it put back there. A
    failed script's rows are its collateral, which a script address never provides."""
    def at_pool(rows: list[dict]) -> int:
        return _value([u for u in rows if u["address"] == pool.address and not u.get("reference")
                       and not u.get("collateral")]).get(pool.cmatra_unit, 0)
    return max(0, at_pool(utxos["inputs"]) - at_pool(utxos["outputs"]))


def pool_coverage(network: CardanoNetwork, balance: int, held: dict[str, int], week: int, unread: int,
                  now: float) -> tuple[list[str], Finding | None]:
    """The digest's lines on whether the pool, holding ``balance``, can pay everything
    still redeemable with ``held`` at the quarantine address, having paid ``week`` in the
    last seven days (``unread`` of whose transactions were not read). An ALERT, once a
    day, while before the deadline the pool covers less than the floor, or at that pace
    runs out within ``runout_page_days`` and before the deadline."""
    pool, coverage = network.pool, network.pool.coverage
    names = _Names(network)

    def cmatra(quantity: int) -> str:
        return names.quantity(pool.cmatra_unit, quantity)

    owed = sum(outstanding(coverage, held).values())
    deadline = datetime.fromtimestamp(coverage.deadline, timezone.utc).date().isoformat()
    left = (coverage.deadline - now) / DAY
    covered = f"{balance * 10_000 // owed / 100:.2f}%" if owed else None
    head = f"{network.name} surrender pool: {cmatra(balance)} against {cmatra(owed)} outstanding at the pinned rates"
    if covered:
        head += f", {covered} covered" + (f" ({cmatra(owed - balance)} short)" if balance < owed else "")
    redeemed = f"at least {cmatra(week)} ({plural(unread, 'transaction')} left unread)" if unread else cmatra(week)
    pace = [f"redeemed in the last 7 days: {redeemed}",
            f"{int(left)} days to the {deadline} deadline" if left > 0 else f"the {deadline} deadline has passed"]
    lasts = balance * 7 / week if week else None
    if lasts is not None:
        pace.append(f"at that pace the pool lasts {int(lasts)} days")
    lines = [head, "  " + "; ".join(pace)]
    reasons = []
    if left > 0 and covered and balance * 100 < coverage.floor_percent * owed:
        reasons.append(f"covers {covered} of what is outstanding, below the {coverage.floor_percent}% floor")
    if left > 0 and lasts is not None and lasts < left and lasts <= coverage.runout_page_days:
        reasons.append(f"runs out in about {round(lasts)} days at the last 7 days' pace, "
                       f"before the {deadline} deadline")
    if not reasons:
        return lines, None
    day = datetime.fromtimestamp(now, timezone.utc).date().isoformat()
    return lines, Finding(ALERT, f"{network.name}:coverage:{day}",
                          f"{network.name} surrender pool " + " and ".join(reasons), details=tuple(lines))


def _redeemer_name(pool: SurrenderPool, json_value) -> str:
    constructor = json_value.get("constructor") if isinstance(json_value, dict) else None
    if isinstance(constructor, int) and constructor in pool.redeemers:
        return pool.redeemers[constructor]
    return f"unrecognized redeemer {json.dumps(json_value, default=str)[:80]}"


def _misdirected(pool: SurrenderPool, spent: list[dict], produced: list[dict], names: _Names) -> list[str]:
    """What keeps a pool spend from paying each wallet exactly the entitlement of the
    legacy units that wallet gave up: cMATRA to a wallet that gave up none, more cMATRA
    than a wallet's own units are entitled to, and legacy units reaching a wallet rather
    than the quarantine address. A wallet is a payment credential, since that alone
    decides who can spend what it is paid; every movement is net of the wallet's change."""
    moved: dict[tuple, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    shown: dict[tuple, str] = {}
    for rows, sign in ((spent, -1), (produced, 1)):
        for u in rows:
            if u["address"] in (pool.address, pool.quarantine_address):
                continue
            wallet = _payment_credential(u["address"]) or (None, u["address"])
            shown.setdefault(wallet, u["address"])
            for a in u["amount"]:
                moved[wallet][a["unit"]] += sign * int(a["quantity"])
    strangers, beyond, diverted = {}, [], {}
    for wallet, units in moved.items():
        received = units.get(pool.cmatra_unit, 0)
        given = {u: -q for u, q in units.items() if q < 0 and _redemption_of(u, pool.redemptions)}
        taken = {u: q for u, q in units.items() if q > 0 and _redemption_of(u, pool.redemptions)}
        entitled = _entitlement(given, pool.redemptions)[0] if wallet[0] is False else 0
        if taken:
            diverted[shown[wallet]] = taken
        if received > 0 and entitled == 0:
            strangers[shown[wallet]] = received
        elif received > entitled:
            beyond.append(f"cMATRA paid to {shown[wallet][:24]}…: {names.quantity(pool.cmatra_unit, received)} "
                          f"against the {names.quantity(pool.cmatra_unit, entitled)} its own deposit is entitled to")
    lines = []
    if strangers:
        recipients = ", ".join(f"{a[:24]}… {names.quantity(pool.cmatra_unit, q)}" for a, q in strangers.items())
        lines.append(f"cMATRA paid to a non-claimant: {recipients}")
    lines.extend(beyond)
    if diverted:
        lines.append("legacy units left this surrender for a wallet that did not give them up: " +
                     ", ".join(f"{a[:24]}… {names.value(units)}" for a, units in diverted.items()))
    return lines


def _classify_pool(network: CardanoNetwork, spent: list[dict], produced: list[dict], redeemers: list[dict],
                   minted: dict[str, int], names: _Names) -> tuple[Severity, list[str], int]:
    pool = network.pool
    lines: list[tuple[Severity, str]] = []
    by_policy: dict[str, dict[str, int]] = defaultdict(dict)
    for unit, quantity in minted.items():
        by_policy[unit[:56]][unit] = quantity
    for policy_id, value in sorted(by_policy.items()):
        lines.append((CRITICAL, f"{pool.label} spend mints or burns under policy {policy_id}: {names.value(value)}"))
    pool_in = [u for u in spent if u["address"] == pool.address]
    pool_out = [u for u in produced if u["address"] == pool.address]

    script = _script_hash(pool.address)
    actions = [_redeemer_name(pool, r.get("json_value")) for r in redeemers
               if r.get("purpose") == "spend" and r.get("script_hash") == script]
    for action in sorted(set(actions)) or ["no redeemer"]:
        if action != pool.surrender_redeemer:
            lines.append((CRITICAL, f"{pool.label} spent with {action}"))

    leaving = _delta(_value(pool_in), _value(pool_out))
    paid = leaving.pop(pool.cmatra_unit, 0)
    leaving.pop("lovelace", None)
    if leaving:
        lines.append((CRITICAL, f"non-cMATRA value moved in {pool.label}: {names.value(leaving)}"))
    for out in pool_out:
        if not out.get("inline_datum"):
            lines.append((CRITICAL, f"{pool.label} output left without an inline datum (unspendable)"))

    funders = {_payment_credential(u["address"]) for u in spent if u["address"] != pool.address}
    custody = {_payment_credential(w.address) for w in network.addresses if w.role == "custody"}
    if funders & custody - {None}:
        lines.append((CRITICAL, "a custody wallet funded this pool spend as the claimant"))
    lines.extend((CRITICAL, line) for line in _misdirected(pool, spent, produced, names))

    deposited: dict[str, int] = {}
    if pool.quarantine_address:
        deposited = _delta(_value([u for u in produced if u["address"] == pool.quarantine_address]),
                           _value([u for u in spent if u["address"] == pool.quarantine_address]))
    locked = deposited.pop(pool.cmatra_unit, 0)
    if locked > 0:
        lines.append((CRITICAL, f"cMATRA paid into the quarantine address, where nothing can spend it: "
                                f"{names.quantity(pool.cmatra_unit, locked)}"))
    entitlement, counts, unknown, unpinned = _entitlement(deposited, pool.redemptions)
    if unknown:
        lines.append((ALERT, f"unrecognized asset surrendered: {names.value(unknown)}"))
    if unpinned:
        lines.append((CRITICAL, f"surrendered units outside the pinned redeemable names: {names.value(unpinned)}"))
    if pool.max_payout is not None and paid > pool.max_payout:
        lines.append((ALERT, f"payout {names.quantity(pool.cmatra_unit, paid)} is above the per-surrender ceiling "
                             f"of {names.quantity(pool.cmatra_unit, pool.max_payout)}"))
    if paid <= 0 and not counts:
        lines.append((CRITICAL, f"{pool.label} spent without a surrender (cMATRA out {names.quantity(pool.cmatra_unit, paid)})"))
    elif paid > entitlement:
        lines.append((CRITICAL, f"overpaid: {names.quantity(pool.cmatra_unit, paid)} left the pool against an "
                                f"entitlement of {names.quantity(pool.cmatra_unit, entitlement)} for {dict(counts)}"))
    elif paid < entitlement:
        lines.append((ALERT, f"underpaid: {names.quantity(pool.cmatra_unit, paid)} against an entitlement of "
                             f"{names.quantity(pool.cmatra_unit, entitlement)} for {dict(counts)}"))
    if not lines:
        lines.append((INFO, f"surrender: {names.quantity(pool.cmatra_unit, paid)} for {dict(counts)}"))
    return max(s for s, _ in lines), [t for _, t in lines], paid


def _moved(rows: list[dict], valid: bool) -> list[dict]:
    """The input or output rows of a transaction that the ledger consumed or produced.

    A valid transaction's collateral is flagged ``collateral`` and stays unspent. A
    transaction whose script failed moves only its collateral and collateral return;
    db-sync stores those as its inputs and outputs, so Blockfrost lists them unflagged,
    where its documentation has them flagged. Every row of such a transaction counts,
    once per UTxO, as the row that lists the most units, since the flagged listing
    carries lovelace alone."""
    if valid:
        return [u for u in rows if not u.get("reference") and not u.get("collateral")]
    kept: dict[tuple, dict] = {}
    for u in rows:
        if u.get("reference"):
            continue
        at = (u.get("tx_hash"), u["output_index"])
        if at not in kept or len(u["amount"]) > len(kept[at]["amount"]):
            kept[at] = u
    return list(kept.values())


def classify_cardano_tx(network: CardanoNetwork, tx: dict, utxos: dict, redeemers: list[dict]) -> Finding | None:
    valid = tx.get("valid_contract", True)
    spent = _moved(utxos["inputs"], valid)
    referenced = [u for u in utxos["inputs"] if u.get("reference")]
    produced = _moved(utxos["outputs"], valid)
    names = _Names(network)
    lines: list[tuple[Severity, str]] = []
    kind, amount = "event", 0
    # Anyone may pay into an address, so a transaction that only pays in, or only spends
    # from a contract, is grouped with the others at that address; one that moves custody
    # or pool value or mints under a watched policy always pages alone.
    alone, label = False, None

    for watched in network.addresses:
        out = _value([u for u in spent if u["address"] == watched.address])
        into = _value([u for u in produced if u["address"] == watched.address])
        custody = watched.role == "custody"
        if out:
            lines.append((CRITICAL if custody else watched.severity,
                          f"{'outflow from' if custody else 'spent from'} {watched.label}: "
                          f"net {names.value(_delta(into, out))}"))
            alone = alone or custody
            label = label or watched.label
        elif into:
            lines.append((ALERT, f"{'inflow to' if custody else 'paid to'} {watched.label}: {names.value(into)}"))
            label = label or watched.label
        elif any(u["address"] == watched.address for u in referenced):
            lines.append((INFO, f"{watched.label} read as a reference input"))

    minted = _delta(_value(produced), _value(spent))
    minted.pop("lovelace", None)
    for policy in network.policies:
        for unit, quantity in sorted(minted.items()):
            if unit.startswith(policy.policy_id):
                verb = "minted" if quantity > 0 else "burned"
                lines.append((policy.severity, f"{verb} {names.quantity(unit, abs(quantity))} under {policy.label}"))
                alone = True

    pool = network.pool
    if pool and any(u["address"] == pool.address for u in spent):
        severity, pool_lines, paid = _classify_pool(network, spent, produced, redeemers, minted, names)
        lines.extend((severity, t) for t in pool_lines)
        alone = True
        if severity == INFO and valid:
            kind, amount = "surrender", paid
    elif pool and any(u["address"] == pool.address for u in produced):
        received = _value([u for u in produced if u["address"] == pool.address])
        lines.append((ALERT, f"{pool.label} received value outside a pool spend: {names.value(received)}"))
        label = label or pool.label

    if not valid:
        lines.append((INFO, "phase-2 script failure: the ledger consumed the collateral in place of the inputs"))
    if not lines:
        return None
    when = datetime.fromtimestamp(tx["block_time"], timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    return Finding(
        severity=max(s for s, _ in lines),
        key=f"{network.name}:{tx['hash']}",
        headline=f"{network.name} tx {tx['hash']} (block {tx['block_height']}, {when})",
        details=tuple(t for _, t in lines),
        kind=kind,
        amount=amount,
        group=None if alone or label is None else f"{network.name} {label}",
    )
