"""The custody watcher's mainnet profile (``materios.network: "mainnet"``).

On mainnet only FPS role accounts hold MATRA, signed transfers are refused, and
TotalIssuance must stay at the genesis declaration. So every Balances and Recovery
call, a vested transfer, a committee change, a change of the RootTimelock guardian
or of the attestation committee, any movement in the Balances events and any change
of TotalIssuance page CRITICAL. The config must say what issuance it pins, name its
own webhook channel and a dead-man ping by file, and watch the partner chain's
governance on Cardano; the watcher pings the dead-man after every cycle in which no
source is stale and no page or due digest has waited DELIVERY_GRACE for Discord. Preprod is untouched: none of this is read or paged there.
"""

import http.server
import json
import threading
from pathlib import Path

import pytest
from substrateinterface.utils.hasher import xxh128

from daemon import custody_rules as rules
from daemon import custody_watch as cw
from daemon import discord
from tests.test_custody_rules import _call, _signed, _signed_hex, decoder_242  # noqa: F401 (fixture)
from tests.test_custody_watch import ALICE, SUDO_KEY, FakeChain, Posts, StubSource, _read, _recovering

FIX = Path(__file__).parent / "fixtures" / "custody"
TEMPLATE = Path(__file__).resolve().parents[1] / "ops" / "custody-watch" / "mainnet.example.json"
BOB = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
GUARDIAN = "5FLSigC9HGRKVhB9FiEo4Y3koPsNmBmLJbpXg2mp1hXcS59Y"
SUCCEEDED = {0: ["System.ExtrinsicSuccess"]}
DECLARED = 990_000

# Cardano mainnet stand-ins, taken from the fixture config's cardano-mainnet watches.
_FIXTURE = json.loads((FIX / "config.json").read_text())
_MAINNET_ADDRESSES = [a["address"] for a in _FIXTURE["cardano"][0]["addresses"]]
_POLICIES = [p["policy_id"] for p in _FIXTURE["cardano"][0]["policies"]]


def filled_template() -> dict:
    """The mainnet template with every placeholder filled in."""
    doc = json.loads(TEMPLATE.read_text())
    doc["materios"]["rpc_url"] = "ws://materios.invalid:9944"
    doc["materios"]["total_issuance"] = str(DECLARED)
    doc["materios"]["authority_accounts"] = [ALICE, BOB]
    [network] = doc["cardano"]
    for watched, address in zip(network["addresses"], _MAINNET_ADDRESSES * 2):
        watched["address"] = address
    for policy, policy_id in zip(network["policies"], _POLICIES * 2):
        policy["policy_id"] = policy_id
    return doc


def _mainnet(doc=None) -> rules.WatchConfig:
    return rules.parse_config(doc or filled_template())


# --- configuration ----------------------------------------------------------


def test_a_config_names_preprod_unless_it_says_otherwise():
    config = rules.parse_config(json.loads((FIX / "config.json").read_text()))
    assert config.materios.network == "preprod" and not config.materios.mainnet
    assert config.deadman_ping_file is None


def test_an_unknown_network_is_refused():
    doc = filled_template()
    doc["materios"]["network"] = "mainet"
    with pytest.raises(ValueError, match="mainet"):
        rules.parse_config(doc)


def test_the_filled_template_is_a_mainnet_config_pinning_issuance():
    config = _mainnet()
    assert config.materios.mainnet and config.materios.total_issuance == DECLARED
    assert config.discord_webhook_file == "discord-webhook" and config.deadman_ping_file == "deadman-ping"


def test_the_template_refuses_to_load_until_every_placeholder_is_filled():
    with pytest.raises(ValueError) as refused:
        rules.parse_config(json.loads(TEMPLATE.read_text()))
    text = str(refused.value)
    for unfilled in ("total_issuance", "authority_accounts", "DParameterValidator", "DParameter policy_id"):
        assert unfilled in text


@pytest.mark.parametrize("field", ["total_issuance", "discord_webhook_file", "deadman_ping_file"])
def test_a_mainnet_config_without_what_it_pins_or_pages_through_is_refused(field):
    doc = filled_template()
    (doc["materios"] if field == "total_issuance" else doc).pop(field)
    with pytest.raises(ValueError, match=field):
        rules.parse_config(doc)


@pytest.mark.parametrize("label", ["VersionOracleValidator", "DParameterValidator", "PermissionedCandidatesValidator"])
def test_a_mainnet_config_must_watch_each_partner_chain_governance_address_as_custody(label):
    doc = filled_template()
    for watched in doc["cardano"][0]["addresses"]:
        if watched["label"] == label:
            watched["role"] = "contract"
    with pytest.raises(ValueError, match=label):
        rules.parse_config(doc)


@pytest.mark.parametrize("label", ["VersionOracle", "DParameter", "PermissionedCandidates"])
def test_a_mainnet_config_must_page_a_mint_under_each_partner_chain_policy_critical(label):
    doc = filled_template()
    for policy in doc["cardano"][0]["policies"]:
        if policy["label"] == label:
            policy["severity"] = "ALERT"
    with pytest.raises(ValueError, match=label):
        rules.parse_config(doc)


def test_a_mainnet_config_refuses_a_testnet_address():
    doc = filled_template()
    doc["cardano"][0]["addresses"][0]["address"] = _FIXTURE["cardano"][1]["addresses"][0]["address"]
    with pytest.raises(ValueError, match="mainnet"):
        rules.parse_config(doc)


def test_the_template_embeds_no_webhook_or_ping_url():
    text = TEMPLATE.read_text()
    assert "http" not in text.replace("https://mainnet.insight.iagon.com", "")


def test_a_partner_chain_governance_update_pages_critical_alone_under_the_templates_roles():
    """The template's watch kinds over the preprod partner chain's real D-parameter upsert."""
    preprod = {a["label"]: a["address"] for a in _FIXTURE["cardano"][1]["addresses"]}
    policies = {p["label"]: p["policy_id"] for p in _FIXTURE["cardano"][1]["policies"]}
    doc = filled_template()
    [network] = doc["cardano"]
    for watched in network["addresses"]:
        watched["address"] = preprod[watched["label"]]
    for policy in network["policies"]:
        policy["policy_id"] = policies[policy["label"]]
    doc["materios"]["network"] = "preprod"
    [watched] = rules.parse_config(doc).cardano
    tx = json.loads((FIX / "cardano" / "d_parameter_upsert.json").read_text())
    finding = rules.classify_cardano_tx(watched, tx["tx"], tx["utxos"], tx["redeemers"])
    assert finding.severity == rules.CRITICAL and finding.authority and finding.group is None
    assert "DParameterValidator" in finding.render()


# --- calls ------------------------------------------------------------------


def _classify(ext, mainnet: bool, events=SUCCEEDED, authorities=frozenset()):
    return rules.classify_materios_block("materios", 9, [ext], read_events=lambda: events,
                                         sudo_key=rules.account_bytes(SUDO_KEY), authorities=authorities,
                                         mainnet=mainnet)


MOVES = [
    _call("Balances", "transfer_allow_death", dest=BOB, value=1),
    _call("Balances", "transfer_keep_alive", dest=BOB, value=1),
    _call("Balances", "transfer_all", dest=BOB, keep_alive=False),
    _call("Balances", "burn", value=1, keep_alive=False),
    _call("Vesting", "vested_transfer", target=BOB, schedule={"locked": 1, "per_block": 1, "starting_block": 0}),
    _call("Recovery", "initiate_recovery", account=BOB),
    _call("Recovery", "vouch_recovery", lost=BOB, rescuer=ALICE),
    _call("Recovery", "create_recovery", friends=[BOB], threshold=1, delay_period=10),
]


@pytest.mark.parametrize("call", MOVES, ids=lambda c: f"{c['call_module']}.{c['call_function']}")
def test_on_mainnet_every_balances_and_recovery_call_and_a_vested_transfer_is_critical(call):
    [finding] = _classify({"address": ALICE, "call": call}, mainnet=True)
    assert finding.severity == rules.CRITICAL


@pytest.mark.parametrize("call", MOVES, ids=lambda c: f"{c['call_module']}.{c['call_function']}")
def test_on_preprod_the_same_calls_page_as_before(call):
    findings = _classify({"address": ALICE, "call": call}, mainnet=False)
    assert all(f.severity < rules.CRITICAL for f in findings)


@pytest.mark.parametrize("function, args", [("schedule", {"call": _call("System", "remark", remark="0x00")}),
                                            ("enact", {"id": 3, "call": _call("System", "remark", remark="0x00")}),
                                            ("cancel", {"id": 3})])
def test_a_root_timelock_schedule_enact_or_cancel_pages_critical(decoder_242, function, args):
    encoded = _signed_hex(decoder_242, ALICE, "RootTimelock", function, **{
        k: ({"call_module": v["call_module"], "call_function": v["call_function"],
             "call_args": {a["name"]: a["value"] for a in v["call_args"]}} if isinstance(v, dict) else v)
        for k, v in args.items()})
    [ext] = decoder_242.extrinsics([encoded])
    [finding] = _classify(ext, mainnet=True)
    assert finding.severity == rules.CRITICAL and f"RootTimelock.{function}" in finding.headline


def test_on_mainnet_a_committee_membership_change_is_critical_and_pages_alone():
    previous, current = (("a",), ("b",)), (("a",),)
    preprod = rules.committee_change("materios", 9, previous, current)
    mainnet = rules.committee_change("materios", 9, previous, current, mainnet=True)
    assert preprod.severity == rules.ALERT and not preprod.authority
    assert mainnet.severity == rules.CRITICAL and mainnet.authority


# --- issuance and Balances events ------------------------------------------


def test_issuance_at_its_declaration_raises_nothing():
    assert rules.issuance_change("materios", 9, DECLARED, None, DECLARED) is None
    assert rules.issuance_change("materios", 9, DECLARED, DECLARED, DECLARED) is None


def test_issuance_first_read_off_its_declaration_is_critical():
    finding = rules.issuance_change("materios", 9, DECLARED + 1, None, DECLARED)
    assert finding.severity == rules.CRITICAL and finding.authority
    assert "990,001" in finding.render() and "990,000" in finding.render()


def test_any_change_of_issuance_is_critical_even_back_to_its_declaration():
    assert rules.issuance_change("materios", 9, DECLARED + 5, DECLARED, DECLARED).severity == rules.CRITICAL
    assert rules.issuance_change("materios", 9, DECLARED, DECLARED + 5, DECLARED).severity == rules.CRITICAL


def _records(decoder, *events) -> list[dict]:
    """Decoded ``System.Events``, each ``(phase, pallet, event, attributes)``."""
    return decoder.event_records(_events(decoder, *events))


def _events(decoder, *events) -> str:
    records = [{"phase": phase, "event": {pallet: {name: attributes}}, "topics": []}
               for phase, pallet, name, attributes in events]
    obj = decoder._config.create_scale_object(decoder._events_type, metadata=decoder._metadata)
    return obj.encode(records).to_hex()


TRANSFER = ({"ApplyExtrinsic": 1}, "Balances", "Transfer", {"from": ALICE, "to": BOB, "amount": 7})
MINTED = ("Initialization", "Balances", "Minted", {"who": BOB, "amount": 3})
RESERVED = ({"ApplyExtrinsic": 1}, "Balances", "Reserved", {"who": ALICE, "amount": 2})
SUCCESS = ({"ApplyExtrinsic": 0}, "System", "ExtrinsicSuccess",
           {"dispatch_info": {"weight": {"ref_time": 1, "proof_size": 1}, "class": "Mandatory", "pays_fee": "Yes"}})


def test_a_balances_transfer_or_mint_in_any_phase_is_critical(decoder_242):
    finding = rules.balance_moves("materios", 9, _records(decoder_242, SUCCESS, RESERVED, TRANSFER, MINTED))
    assert finding.severity == rules.CRITICAL and finding.authority
    text = finding.render()
    assert "Balances.Transfer" in text and "extrinsic 1" in text
    assert "Balances.Minted" in text and "Initialization" in text
    assert "Reserved" not in text


def test_reserves_locks_and_other_pallets_events_move_no_balance(decoder_242):
    assert rules.balance_moves("materios", 9, _records(decoder_242, SUCCESS, RESERVED)) is None


# --- the watcher on a mainnet chain ------------------------------------------


def _key(pallet: bytes, item: bytes) -> str:
    return "0x" + (xxh128(pallet) + xxh128(item)).hex()


GUARDIAN_KEY = _key(b"RootTimelock", b"Guardian")
COMMITTEE_KEY = _key(b"OrinqReceipts", b"CommitteeMembers")
ISSUANCE_KEY = _key(b"Balances", b"TotalIssuance")
MAINNET_KEYS = (GUARDIAN_KEY, COMMITTEE_KEY, ISSUANCE_KEY)


def _account(ss58: str) -> str:
    return "0x" + rules.account_bytes(ss58).hex()


def _members(*accounts: str) -> str:
    return "0x" + (rules._compact(len(accounts)) + b"".join(rules.account_bytes(a) for a in accounts)).hex()


def _issuance(amount: int) -> str:
    return "0x" + amount.to_bytes(16, "little").hex()


class MainnetChain(FakeChain):
    """A FakeChain on spec 242 that serves the guardian, the attestation committee
    and TotalIssuance from ``storage``; its blocks hold only ``extra``."""

    def __init__(self, head, events_hex, **kwargs):
        super().__init__(head, {}, events_hex=events_hex, **kwargs)
        self.metadata = _read("materios/metadata_spec242.hex.gz").decode()
        self.code_hash = "0x" + "42" * 32
        self.storage.update({GUARDIAN_KEY: _account(GUARDIAN), COMMITTEE_KEY: _members(ALICE),
                             ISSUANCE_KEY: _issuance(DECLARED)})

    def _block(self, number):
        header = {"number": hex(number), "parentHash": self.hash_of(number - 1), "digest": {"logs": []}}
        return {"block": {"header": header, "extrinsics": self.extra.get(number, [])}}

    def rpc(self, method, params):
        if method == "state_getStorage" and params[0] in MAINNET_KEYS:
            self.calls.append((method, params))
            self._state(params[1])
            return self.value(*params)
        return super().rpc(method, params)


@pytest.fixture
def quiet(decoder_242):
    return _events(decoder_242, SUCCESS)


def _source(store, chain, network="mainnet"):
    doc = filled_template()
    doc["materios"]["network"] = network
    config = rules.parse_config(doc)
    return cw.MateriosSource(config.materios, chain, store, config.source_stale_seconds)


def _poll_to(source, chain, head, now=1.0):
    chain.head = head
    chain.state_at.update(range(head - 5, head + 1))
    while not source.poll(now):
        pass


def _paged(store) -> dict:
    return {f.key: f for f in store.findings() if f.severity > rules.INFO}


def test_a_quiet_mainnet_chain_pages_nothing(tmp_path, quiet):
    store = cw.Store(str(tmp_path / "state.db"))
    chain = MainnetChain(100, quiet, state_at={100})
    source = _source(store, chain)
    source.poll(1.0)
    _poll_to(source, chain, 103)
    assert _paged(store) == {}


@pytest.mark.parametrize("key, value, headline", [
    (GUARDIAN_KEY, _account(BOB), "RootTimelock.Guardian changed"),
    (COMMITTEE_KEY, _members(ALICE, BOB), "attestation committee changed"),
])
def test_a_guardian_or_attestation_committee_change_pages_critical_on_mainnet(tmp_path, quiet, key, value, headline):
    store = cw.Store(str(tmp_path / "state.db"))
    chain = MainnetChain(100, quiet, state_at={100})
    source = _source(store, chain)
    source.poll(1.0)
    chain.storage[key] = value
    _poll_to(source, chain, 101)
    [finding] = _paged(store).values()
    assert finding.severity == rules.CRITICAL and finding.authority and headline in finding.headline
    assert BOB in finding.render()


def test_preprod_reads_neither_the_guardian_nor_the_committee_nor_issuance(tmp_path, quiet):
    store = cw.Store(str(tmp_path / "state.db"))
    chain = MainnetChain(100, quiet, state_at={100})
    source = _source(store, chain, network="preprod")
    source.poll(1.0)
    chain.storage.update({GUARDIAN_KEY: _account(BOB), ISSUANCE_KEY: _issuance(1)})
    _poll_to(source, chain, 102)
    assert _paged(store) == {}
    assert not [p for m, p in chain.calls if m == "state_getStorage" and p[0] in MAINNET_KEYS]


def test_what_the_guardian_attempts_pages_whatever_its_outcome_on_mainnet(tmp_path, decoder_242):
    failed = _events(decoder_242, ({"ApplyExtrinsic": 0}, "System", "ExtrinsicFailed", {
        "dispatch_error": "BadOrigin",
        "dispatch_info": {"weight": {"ref_time": 1, "proof_size": 1}, "class": "Normal", "pays_fee": "Yes"}}))
    attempt = _signed_hex(decoder_242, GUARDIAN, "Sudo", "sudo",
                          call={"call_module": "System", "call_function": "remark", "call_args": {"remark": "0x00"}})
    paged = {}
    for network in ("preprod", "mainnet"):
        store = cw.Store(str(tmp_path / f"{network}.db"))
        chain = MainnetChain(100, failed, state_at={100}, extra={101: [attempt]})
        source = _source(store, chain, network)
        source.poll(1.0)
        _poll_to(source, chain, 101)
        paged[network] = _paged(store)
    assert paged["preprod"] == {}
    [finding] = paged["mainnet"].values()
    assert finding.severity == rules.CRITICAL and finding.authority


def test_issuance_off_its_declaration_pages_once_and_each_change_after_it(tmp_path, quiet):
    store = cw.Store(str(tmp_path / "state.db"))
    chain = MainnetChain(100, quiet, state_at={100})
    source = _source(store, chain)
    source.poll(1.0)
    chain.storage_at[101] = {ISSUANCE_KEY: _issuance(DECLARED + 1)}
    chain.storage_at[102] = {ISSUANCE_KEY: _issuance(DECLARED + 1)}
    chain.storage_at[103] = {ISSUANCE_KEY: _issuance(DECLARED)}
    _poll_to(source, chain, 103)
    assert sorted(k for k in _paged(store)) == ["materios-mainnet:101:issuance", "materios-mainnet:103:issuance"]


def test_a_balances_transfer_in_a_mainnet_block_pages_critical(tmp_path, decoder_242):
    store = cw.Store(str(tmp_path / "state.db"))
    chain = MainnetChain(100, _events(decoder_242, SUCCESS, TRANSFER), state_at={100})
    source = _source(store, chain)
    source.poll(1.0)
    _poll_to(source, chain, 101)
    finding = _paged(store)["materios-mainnet:101:balances"]
    assert finding.severity == rules.CRITICAL and "Balances.Transfer" in finding.render()


def test_a_mainnet_block_whose_state_is_pruned_pages_its_balances_unverified_grouped(tmp_path, quiet):
    store = cw.Store(str(tmp_path / "state.db"))
    chain = MainnetChain(100, quiet, state_at={100})
    source = _source(store, chain)
    source.poll(1.0)
    chain.head = 102
    chain.state_at = {102}
    while not source.poll(1.0):
        pass
    finding = _paged(store)["materios-mainnet:101:unverified"]
    assert finding.severity == rules.ALERT and finding.group == "materios-mainnet unverified"
    assert "TotalIssuance" in finding.render() and "Balances events" in finding.render()


def _recovery_started(chain):
    _recovering(chain, rules.account_bytes(SUDO_KEY), rules.account_bytes(BOB), [])


def test_a_recovery_of_sudo_key_started_pages_critical_alone_on_mainnet(tmp_path, quiet):
    severity = {}
    for network in ("preprod", "mainnet"):
        store = cw.Store(str(tmp_path / f"{network}.db"))
        chain = MainnetChain(100, quiet, state_at={100})
        if network == "preprod":
            chain.metadata = _read("materios/metadata_spec238.hex.gz").decode()
        source = _source(store, chain, network)
        source.poll(1.0)
        _recovery_started(chain)
        chain.head = 101
        chain.state_at.add(101)
        source.poll(2.0)
        [finding] = [f for f in store.findings() if ":recovery:" in f.key]
        severity[network] = (finding.severity, finding.group)
    assert severity["preprod"] == (rules.ALERT, "materios-mainnet recovery started")
    assert severity["mainnet"] == (rules.CRITICAL, None)


# --- the dead-man --------------------------------------------------------------


class Clock:
    def __init__(self):
        self.now = 1_000_000.0

    def __call__(self):
        return self.now


def _watch(tmp_path, sources, pings):
    config = _mainnet()
    store = cw.Store(str(tmp_path / "state.db"))
    clock = Clock()
    watch = cw.Watch(config, store, sources, Posts(), clock=clock, ping=lambda: pings.append(clock.now))
    return watch, clock


def test_the_dead_man_is_pinged_after_a_cycle_at_most_once_a_minute(tmp_path):
    pings = []
    watch, clock = _watch(tmp_path, [StubSource("a")], pings)
    for _ in range(4):
        watch.cycle()
        clock.now += 20
    assert pings == [1_000_000.0, 1_000_060.0]


def test_the_dead_man_is_not_pinged_while_a_source_is_stale(tmp_path):
    pings = []
    watch, clock = _watch(tmp_path, [StubSource("a", behind=True)], pings)
    watch.cycle()
    clock.now += 2 * 900
    watch.cycle()
    clock.now += 120
    watch.cycle()
    assert pings == [1_000_000.0]


def test_the_dead_man_is_not_pinged_while_a_source_is_failing(tmp_path):
    pings = []
    source = StubSource("a")
    watch, clock = _watch(tmp_path, [source], pings)
    watch.cycle()
    source.fail = True
    for _ in range(10):
        clock.now += 60
        watch.cycle()
    assert pings == [1_000_000.0]

    source.fail = False
    clock.now += 60
    watch.cycle()
    assert pings == [1_000_000.0, clock.now]


def test_a_failed_ping_is_retried_on_the_next_cycle(tmp_path):
    pings = []
    calls = []

    def ping():
        calls.append(1)
        if len(calls) == 1:
            raise OSError("unreachable")
        pings.append(1)

    config = _mainnet()
    store = cw.Store(str(tmp_path / "state.db"))
    watch = cw.Watch(config, store, [StubSource("a")], Posts(), clock=Clock(), ping=ping)
    watch.cycle()
    watch.cycle()
    assert len(calls) == 2 and pings == [1]


class Webhook:
    """Answers every post with ``status`` (None: unreachable) until healed."""

    def __init__(self, status):
        self.status = status
        self.healed = False
        self.sent = []

    def __call__(self, payload):
        if not self.healed:
            raise discord.DiscordError("webhook refused", self.status)
        self.sent.append(payload)


def _mute(tmp_path, status):
    pings = []
    webhook = Webhook(status)
    clock = Clock()
    store = cw.Store(str(tmp_path / "state.db"))
    watch = cw.Watch(_mainnet(), store, [StubSource("a")], webhook, clock=clock,
                     ping=lambda: pings.append(clock.now))
    return watch, store, webhook, clock, pings


def _minutes(watch, clock, n):
    for _ in range(n):
        watch.cycle()
        clock.now += 60


@pytest.mark.parametrize("status", [404, 401, 500, 400, None])
def test_a_page_the_webhook_cannot_deliver_stops_the_pings_until_it_goes_out(tmp_path, status):
    watch, store, webhook, clock, pings = _mute(tmp_path, status)
    store.put("digest:last-day", "1970-01-12")
    found = clock.now
    store.add(rules.Finding(rules.CRITICAL, "materios:9:balances", "Balances moved", authority=True), found)
    _minutes(watch, clock, 30)
    assert pings and max(pings) < found + cw.DELIVERY_GRACE
    assert [f.key for f in store.unsent_pages()] == ["materios:9:balances"]
    webhook.healed = True
    healed = clock.now
    _minutes(watch, clock, 3)
    assert store.unsent_pages() == [] and "Balances moved" in webhook.sent[0]["content"]
    assert [p for p in pings if p >= healed]


def test_a_digest_the_webhook_cannot_deliver_stops_the_pings_until_it_goes_out(tmp_path):
    watch, store, webhook, clock, pings = _mute(tmp_path, 404)
    started = clock.now
    _minutes(watch, clock, 30)
    assert pings and max(pings) < started + cw.DELIVERY_GRACE
    webhook.healed = True
    healed = clock.now
    _minutes(watch, clock, 3)
    assert store.get("digest:last-day") == "1970-01-12" and [p for p in pings if p >= healed]


def test_a_page_held_back_by_its_group_does_not_stop_the_pings(tmp_path):
    watch, store, webhook, clock, pings = _mute(tmp_path, None)
    webhook.healed = True
    store.put("digest:last-day", "1970-01-12")
    store.add(rules.Finding(rules.ALERT, "materios:1:transfer", "first", group="transfers"), clock.now)
    watch.cycle()
    clock.now += 60
    store.add(rules.Finding(rules.ALERT, "materios:2:transfer", "second", group="transfers"), clock.now)
    _minutes(watch, clock, 8)
    assert len(webhook.sent) == 1 and len(pings) == 9


def test_the_dead_man_ping_url_is_read_from_its_file_and_must_be_http(tmp_path):
    doc = filled_template()
    (tmp_path / "deadman-ping").write_text("https://deadman.invalid/ping/abc\n")
    config = rules.parse_config(doc, base_dir=tmp_path)
    assert cw.deadman_url(config) == "https://deadman.invalid/ping/abc"
    (tmp_path / "deadman-ping").write_text("file:///etc/passwd\n")
    with pytest.raises(ValueError, match="deadman_ping_file"):
        cw.deadman_url(config)


def test_a_preprod_config_without_a_dead_man_pings_nothing():
    config = rules.parse_config(json.loads((FIX / "config.json").read_text()))
    assert cw.deadman_url(config) is None


def test_a_ping_is_a_get_on_the_url_and_a_refusal_raises(tmp_path):
    hits = []

    class Check(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            hits.append(self.path)
            self.send_response(200 if self.path == "/ping/abc" else 404)
            self.end_headers()
            self.wfile.write(b"OK")

        def log_message(self, *args):
            pass

    server = http.server.HTTPServer(("127.0.0.1", 0), Check)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        base = f"http://127.0.0.1:{server.server_port}"
        cw._ping(f"{base}/ping/abc")
        with pytest.raises(OSError):
            cw._ping(f"{base}/ping/unknown")
    finally:
        server.shutdown()
    assert hits == ["/ping/abc", "/ping/unknown"]
