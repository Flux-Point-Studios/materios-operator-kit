"""What differs between the networks a daemon serves, chosen by MATERIOS_NETWORK.

Preprod is the default and behaves as it always has: its attestors bond toward
``OrinqReceipts.BondRequirement`` and sign immortal extrinsics. Mainnet admits
attestors through Root with no bond, and its accounts hold no MATRA, so the
daemon never bonds; it signs mortal extrinsics, so a signed extrinsic cannot be
replayed once it has expired. On every network ``join_committee`` is Root's call
(spec 228 on): the daemon never submits it and says once that it waits for Root.
"""
import asyncio
import gzip
import json
import queue
from pathlib import Path

import pytest
from scalecodec.base import ScaleBytes
from substrateinterface import SubstrateInterface

from daemon.cert_daemon import CertDaemon
from daemon.config import DaemonConfig, NETWORKS
from tests.fake_chain import dialling_client, including_client

SPEC_242_METADATA = Path(__file__).parent / "fixtures" / "custody" / "materios" / "metadata_spec242.hex.gz"


def test_preprod_is_the_default_network(monkeypatch):
    monkeypatch.delenv("MATERIOS_NETWORK", raising=False)
    config = DaemonConfig.from_env()
    assert config.network == "preprod"
    assert config.profile.bonds and config.profile.mortal_period == 0


def test_mainnet_neither_bonds_nor_signs_immortal(monkeypatch):
    monkeypatch.setenv("MATERIOS_NETWORK", "mainnet")
    config = DaemonConfig.from_env()
    assert config.network == "mainnet"
    assert not config.profile.bonds and config.profile.mortal_period == 64


def test_an_unknown_network_is_refused(monkeypatch):
    monkeypatch.setenv("MATERIOS_NETWORK", "mainet")
    with pytest.raises(ValueError, match="MATERIOS_NETWORK"):
        DaemonConfig.from_env()


def test_every_network_has_a_profile():
    assert set(NETWORKS) == {"preprod", "mainnet"}


# --- signing ----------------------------------------------------------------


def _hash(number: int) -> str:
    return f"0x{number:064x}"


class Node:
    """A websocket onto a node running spec 242 whose best block is ``best`` and
    finalized block ``finalized``, under the RPC names substrate-interface 1.x
    signs with. ``asked`` records the block numbers whose hashes were read."""

    connected = True

    def __init__(self, best: int, finalized: int):
        self.frames = queue.Queue()
        self.best, self.finalized = best, finalized
        self.asked: list[int] = []
        self.version = {"specName": "materios", "specVersion": 242, "transactionVersion": 6}
        self.metadata = gzip.decompress(SPEC_242_METADATA.read_bytes()).decode()

    def answer(self, method: str, params: list):
        if method == "chain_getBlockHash":
            number = params[0] if params else self.best
            self.asked.append(number)
            return _hash(number)
        if method == "chain_getFinalizedHead":
            return _hash(self.finalized)
        if method == "chain_getHeader":
            number = int(params[0], 16) if params and params[0] else self.best
            return {"parentHash": _hash(number - 1), "number": hex(number), "stateRoot": _hash(0),
                    "extrinsicsRoot": _hash(0), "digest": {"logs": []}}
        if method == "chain_getHead":
            return _hash(self.best)
        if method in ("chain_getRuntimeVersion", "state_getRuntimeVersion"):
            return self.version
        if method == "state_getMetadata":
            return self.metadata
        if method == "rpc_methods":
            return {"methods": ["state_getMetadata"]}
        if method == "system_accountNextIndex":
            return 7
        if method == "system_chain":
            return "Materios Test"
        if method in ("state_getStorage", "state_getStorageAt"):
            return None
        raise AssertionError(f"unexpected RPC {method}")

    def send(self, text):
        request = json.loads(text)
        result = self.answer(request["method"], request.get("params", []))
        self.frames.put(json.dumps({"jsonrpc": "2.0", "id": request["id"], "result": result}))

    def recv(self):
        return self.frames.get(timeout=5)

    def close(self):
        self.connected = False


def _signed(network: str, best: int, finalized: int):
    """A remark signed by a client of ``network`` against ``Node(best,
    finalized)``, decoded, and the block numbers whose hashes it read."""
    node = Node(best, finalized)
    client = dialling_client(lambda: SubstrateInterface(websocket=node, auto_discover=False),
                             DaemonConfig(rpc_url="ws://test-rpc:9944", network=network))
    call = client.compose_call(call_module="System", call_function="remark", call_params={"remark": "0x00"})
    extrinsic = client.create_signed_extrinsic(call=call)
    decoder = client._write.si.runtime_config.create_scale_object(
        "Extrinsic", data=ScaleBytes(extrinsic.data.to_hex()), metadata=client._write.si.metadata)
    return decoder.decode(), node.asked


def test_a_preprod_extrinsic_is_immortal():
    decoded, asked = _signed("preprod", best=1000, finalized=998)
    assert decoded["era"] == "00"
    assert decoded["nonce"] == 7


def test_a_mainnet_extrinsic_is_mortal_from_the_finalized_head_while_finality_keeps_up():
    decoded, asked = _signed("mainnet", best=1000, finalized=995)
    assert decoded["era"] == (64, 995 % 64)
    assert 995 in asked, "the signature binds the hash of the era's first block"


def test_a_mainnet_extrinsic_is_mortal_from_the_best_blocks_parent_once_finality_lags():
    decoded, asked = _signed("mainnet", best=1000, finalized=990)
    assert decoded["era"] == (64, 999 % 64)
    assert 999 in asked


def test_signing_reads_the_era_on_the_node_the_submit_goes_to():
    client, write, read = including_client()
    client.config = DaemonConfig(network="mainnet")
    write.best, write.finalized = 1000, 997
    client.create_signed_extrinsic(call="remark")
    assert read.names() == []
    assert write.names() == ["get_block_header", "get_chain_finalised_head", "get_block_header",
                             "create_signed_extrinsic"]
    assert write.eras == [{"period": 64, "current": 997}]


def test_a_preprod_signature_reads_nothing_more_and_passes_no_era():
    client, write, _ = including_client()
    client.create_signed_extrinsic(call="remark")
    assert write.names() == ["create_signed_extrinsic"]
    assert write.eras == [None]


# --- committee admission ----------------------------------------------------


def _outsider(network: str, members=()):
    """A daemon of ``network`` whose signer is outside the committee, with 0 of a
    10-unit bond posted and MATRA to bond; its Discord messages land in ``sent``."""
    client, write, _ = including_client()
    client.get_committee_members = lambda: list(members)
    client.get_bond_requirement = lambda: 10
    client.get_attestor_bond = lambda address: 0
    client.get_free_balance = lambda address: 100
    client.refresh_backup_delay = lambda: None
    daemon = CertDaemon.__new__(CertDaemon)
    daemon.config = DaemonConfig(network=network)
    daemon.client = client
    daemon._chain_write_lock = asyncio.Lock()
    daemon._pending_lock = None
    daemon._concurrency_sem = None
    daemon.sent = []

    async def send_discord(message, level="info"):
        daemon.sent.append((level, message))

    daemon.send_discord = send_discord
    return daemon, write


def _calls(write) -> list:
    return [(call.module, call.function) for call in write.composed]


def test_a_preprod_outsider_bonds_for_roots_admission_and_never_joins():
    daemon, write = _outsider("preprod")
    asyncio.run(daemon._ensure_committee_membership())
    assert _calls(write) == [("OrinqReceipts", "bond")]


def test_a_mainnet_outsider_neither_bonds_nor_joins():
    daemon, write = _outsider("mainnet")
    asyncio.run(daemon._ensure_committee_membership())
    assert write.names() == []


@pytest.mark.parametrize("network", ["preprod", "mainnet"])
def test_an_outsider_says_once_that_it_waits_for_root_and_once_when_admitted(network):
    daemon, write = _outsider(network)
    address = daemon.client.keypair.ss58_address
    for _ in range(3):
        asyncio.run(daemon._ensure_committee_membership())
    waiting = [message for _, message in daemon.sent if "join_committee" in message]
    assert len(waiting) == 1 and address in waiting[0] and "Root" in waiting[0]

    daemon.client.get_committee_members = lambda: [address]
    asyncio.run(daemon._ensure_committee_membership())
    asyncio.run(daemon._ensure_committee_membership())
    admitted = [message for _, message in daemon.sent if "admitted" in message]
    assert len(admitted) == 1


def test_a_member_from_the_start_announces_nothing():
    daemon, write = _outsider("mainnet")
    daemon.client.get_committee_members = lambda: [daemon.client.keypair.ss58_address]
    asyncio.run(daemon._ensure_committee_membership())
    assert daemon.sent == [] and write.names() == []
