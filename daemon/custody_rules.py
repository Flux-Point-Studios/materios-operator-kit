"""Classify custody and authority moves on Materios and Cardano.

Pure functions over decoded chain data; ``custody_watch`` does the fetching.

Materios: every extrinsic of a finalized block is decoded against runtime
metadata and its call tree walked through Sudo, Multisig, Utility and Recovery
wrappers. Root calls, runtime-code changes, forced balance moves, recovery,
treasury spends, finality overrides and committee-gate levers are CRITICAL.

Cardano: a transaction is classified by what it spends and produces at watched
addresses and what it mints under watched policies. A surrender-pool spend is
INFO only when it has every mark of a surrender (the surrender redeemer, the
pool's continuing output keeping its inline datum, cMATRA leaving the pool only
to the wallets that funded the transaction, and a payout equal to the rate-table
entitlement for the legacy assets deposited at the quarantine address). An
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


ADDRESS_ROLES = ("custody", "contract")


def parse_config(doc: dict, base_dir: Path | None = None) -> WatchConfig:
    """``base_dir`` anchors relative key paths, so a config handed over by systemd
    ``LoadCredential`` can name its sibling key files."""
    materios = doc.get("materios")
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
    )


def _parse_network(doc: dict, base_dir: Path | None) -> CardanoNetwork:
    addresses = []
    for a in doc.get("addresses", []):
        if a["role"] not in ADDRESS_ROLES:
            raise ValueError(f"{doc['name']}: address {a['label']} has unknown role {a['role']!r}")
        addresses.append(WatchedAddress(a["label"], a["address"], a["role"],
                                        Severity[a.get("severity", "ALERT")]))
    pool = doc.get("surrender_pool")
    return CardanoNetwork(
        name=doc["name"],
        blockfrost_url=doc["blockfrost_url"].rstrip("/"),
        project_id_file=str(base_dir / doc["project_id_file"]) if base_dir else doc["project_id_file"],
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
            redemptions=tuple(_parse_redemption(doc["name"], r) for r in pool.get("redemptions", [])),
            max_payout=pool.get("max_payout"),
        ) if pool else None,
    )


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


def _signed(extrinsic_hex: str) -> bool:
    """Whether an extrinsic carries a signature: the top bit of the version byte that
    follows its compact length prefix."""
    first = int(extrinsic_hex[2:4], 16)
    prefix = 1 << (first & 3) if first & 3 < 3 else (first >> 2) + 5
    version = extrinsic_hex[2 + 2 * prefix:4 + 2 * prefix]
    return not version or int(version, 16) & 0x80 != 0


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

    def extrinsics(self, extrinsics_hex: list[str]) -> list[dict]:
        """Each extrinsic decoded, or ``{"undecodable": hex, "error": ...}`` for one this
        runtime's metadata cannot read or the block's DECODE_BUDGET does not reach, so a
        call the watcher has not read is reported rather than silently skipped.

        The block's unsigned extrinsics, its inherents among them, are decoded first and
        the signed ones smallest first, so a large extrinsic cannot spend the budget
        that the inherents and a privileged call, which is small, need."""
        decoded: list[dict] = [{} for _ in extrinsics_hex]
        budget = DECODE_BUDGET
        for index in sorted(range(len(extrinsics_hex)),
                            key=lambda i: (_signed(extrinsics_hex[i]), len(extrinsics_hex[i]))):
            x = extrinsics_hex[index]
            before = self.values
            try:
                decoded[index] = self._decode("Extrinsic", x, budget)
            except Exception as e:  # scalecodec raises any type on bytes it cannot place
                decoded[index] = {"undecodable": x, "error": f"{type(e).__name__}: {e}"[:200]}
            budget -= self.values - before
        return decoded

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


def _hex_digest(hex_text: str) -> str:
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
        return f"<{_hex_digest(value)}>"
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


@dataclass(frozen=True)
class _Site:
    """A privileged call in an extrinsic's call tree, with its own arguments rendered;
    ``inert`` when its origin cannot dispatch it, ``unlisted`` when it is in neither
    call table."""
    path: str
    args: str
    depth: int
    severity: Severity
    inert: bool
    unlisted: bool

    def line(self, counted: bool) -> str:
        notes = [" (cannot take effect from this origin)"] if self.inert and not counted else []
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


def _walk(call: dict, origin, tree: _Tree, depth: int, parents: tuple[str, ...], inert: bool,
          proven: bool) -> None:
    """Record ``call`` and every call it wraps. A subtree is inert when its origin is an
    account that cannot dispatch it: a Sudo call from any account but Sudo.Key, or a
    root-gated call from any account at all. ``proven`` holds while the origin follows
    from the signer alone; only such an origin can make an authority's attempt."""
    module, function = call["call_module"], call["call_function"]
    args = _args(call)
    path = (*parents, f"{module}.{function}")
    rendered = ", ".join(f"{k}={_render_value(v)}" for k, v in args.items() if not _holds_calls(v))
    tree.line(f"{_indent(depth)}{module}.{function}({rendered})")
    if isinstance(origin, bytes):
        inert = inert or (module == "Sudo" and origin != tree.sudo_key) or (module, function) in _ROOT_GATED
    inner = _inner_origin(module, function, args, origin, tree, depth)
    inner_proven = proven and (module, function) in _DERIVED
    if any(p and isinstance(o, bytes) and o in tree.authority for o, p in ((origin, proven), (inner, inner_proven))):
        tree.involved = True
    severity = _severity(module, function, args, origin, inner, tree)
    unlisted = severity is None and (module, function) not in _ROUTINE
    if unlisted:
        severity = ALERT
    if severity is not None:
        tree.sites.append(_Site(_abridged(path), rendered, depth, severity, inert, unlisted))
    for value in args.values():
        for child in ([value] if _is_call(value) else value if isinstance(value, list) else []):
            if _is_call(child):
                _walk(child, inner, tree, depth + 1, path, inert, inner_proven)


RUNTIME_ENVIRONMENT_UPDATED = "0x08"


def runtime_upgraded(header: dict) -> bool:
    """Whether this block changed the runtime, so the blocks after it need new metadata."""
    return RUNTIME_ENVIRONMENT_UPDATED in header["digest"]["logs"]


def plural(n: int, word: str) -> str:
    return f"{n:,} {word}" + ("" if n == 1 else "s")


def unclassifiable(key: str, what: str, error: Exception, *details: str, group: str | None = None) -> Finding:
    """The CRITICAL raised in place of a classification that failed, so the item is
    still paged and its cursor still moves."""
    return Finding(CRITICAL, key, f"{what} could not be classified",
                   details=(*details, f"{type(error).__name__}: {error}"[:200]), group=group)


def classify_materios_block(chain: str, number: int, extrinsics: list[dict],
                            read_events: Callable[[], dict[int, list[str]] | None], sudo_key: bytes | None,
                            authorities: frozenset[bytes] = frozenset()) -> list[Finding]:
    """Findings for a block's extrinsics, each call tree walked once. ``read_events``
    returns the block's events by extrinsic, or None when they could not be read, and is
    called once, only when an extrinsic has something to report. Without events an
    attempt is paged as though it took effect, since nothing shows it failed."""
    findings = []
    # What cannot be read may hide any call, so it pages CRITICAL; grouped, because
    # anyone can send it.
    unreadable = f"{chain} unclassifiable"
    undecoded = [(index, ext) for index, ext in enumerate(extrinsics) if "undecodable" in ext]
    if undecoded:
        # One finding a block, however many extrinsics it could not read.
        listed = [f"extrinsic {index}: {_hex_digest(ext['undecodable'])}: {ext['error']}"
                  for index, ext in undecoded[:MAX_UNDECODED_LINES]]
        if len(undecoded) > MAX_UNDECODED_LINES:
            listed.append(f"... {len(undecoded) - MAX_UNDECODED_LINES:,} more")
        findings.append(Finding(CRITICAL, f"{chain}:{number}:undecoded",
                                f"{chain} #{number}: {plural(len(undecoded), 'extrinsic')} could not be decoded",
                                details=tuple(listed), group=unreadable))
    reported = []
    for index, ext in enumerate(extrinsics):
        if "undecodable" in ext:
            continue
        key = f"{chain}:{number}:{index}"
        signer = _account(ext.get("address"))
        tree = _Tree(sudo_key, authorities | ({sudo_key} if sudo_key else frozenset()))
        try:
            _walk(ext["call"], signer, tree, 0, (), False, True)
        except Exception as e:  # argument values are the signer's choice; none may stall the block
            findings.append(unclassifiable(key, f"{chain} #{number} extrinsic {index}", e,
                                           f"extrinsic hash {ext.get('extrinsic_hash')}", group=unreadable))
            continue
        if tree.sites or (signer is not None and signer == sudo_key):
            reported.append((index, ext, signer, tree))
    events = read_events() if reported else None
    findings.extend(_report(chain, number, index, ext, signer, tree, events) for index, ext, signer, tree in reported)
    return findings


def _report(chain: str, number: int, index: int, ext: dict, signer: bytes | None, tree: _Tree,
            events: dict[int, list[str]] | None) -> Finding:
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
    counted = tree.sites
    if own is None:
        notes.append("events unavailable (state pruned or undecodable): dispatch result not verified")
    else:
        shown = [e for e in own if not e.startswith(EVENT_NOISE)]
        notes.append("result: " + (", ".join(shown) if shown else "no events"))
        # An authority's attempts page whatever their outcome; anyone else's page only
        # when they could have taken effect.
        if not tree.involved and any(e.startswith("System.ExtrinsicFailed") for e in own):
            counted = []
            notes.append("dispatch failed: nothing took effect")
        elif not tree.involved:
            counted = [s for s in tree.sites if not s.inert]
    severity = CRITICAL if by_sudo else max((s.severity for s in counted), default=INFO)
    # Calls that count lead, most severe and then innermost first, so the headline and
    # the top of a truncated page name the call that matters.
    live = {id(s) for s in counted}
    ranked = sorted(tree.sites, key=lambda s: (id(s) not in live, -s.severity, -s.depth))
    call = ext["call"]
    top = ranked[0].path if ranked else f"{call['call_module']}.{call['call_function']}"
    sites = [s.line(id(s) in live) for s in ranked[:MAX_SITE_LINES]]
    if len(ranked) > MAX_SITE_LINES:
        sites.append(f"... {len(ranked) - MAX_SITE_LINES:,} more privileged calls")
    lines = tree.lines + ([f"... {tree.hidden:,} more lines of the call tree"] if tree.hidden else [])
    return Finding(
        severity=severity,
        key=f"{chain}:{number}:{index}",
        headline=f"{chain} #{number} extrinsic {index}: {top}",
        details=tuple(notes + sites + ["call tree:"] + lines),
        group=None if tree.involved or signer is None else f"{chain} signer {render_account(signer)}",
    )


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


def _redeemer_name(pool: SurrenderPool, json_value) -> str:
    constructor = json_value.get("constructor") if isinstance(json_value, dict) else None
    if isinstance(constructor, int) and constructor in pool.redeemers:
        return pool.redeemers[constructor]
    return f"unrecognized redeemer {json.dumps(json_value, default=str)[:80]}"


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

    claimants = set()
    for u in spent:
        credential = _payment_credential(u["address"]) if u["address"] != pool.address else None
        if credential and not credential[0]:
            claimants.add(credential[1])
    custody = {c[1] for c in (_payment_credential(w.address) for w in network.addresses if w.role == "custody") if c}
    if claimants & custody:
        lines.append((CRITICAL, "a custody wallet funded this pool spend as the claimant"))

    to_others: dict[str, int] = defaultdict(int)
    for out in produced:
        if out["address"] == pool.address:
            continue
        quantity = _value([out]).get(pool.cmatra_unit, 0)
        credential = _payment_credential(out["address"])
        if quantity and not (credential and not credential[0] and credential[1] in claimants):
            to_others[out["address"]] += quantity
    if to_others:
        recipients = ", ".join(f"{a[:24]}\u2026 {names.quantity(pool.cmatra_unit, q)}" for a, q in to_others.items())
        lines.append((CRITICAL, f"cMATRA paid to a non-claimant: {recipients}"))

    deposited: dict[str, int] = {}
    if pool.quarantine_address:
        deposited = _delta(_value([u for u in produced if u["address"] == pool.quarantine_address]),
                           _value([u for u in spent if u["address"] == pool.quarantine_address]))
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
