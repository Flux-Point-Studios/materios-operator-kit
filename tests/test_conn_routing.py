"""Every chain call names its connection, and the write conn carries only the
calls listed here (#69).

A submit waiting for inclusion holds the write conn's websocket, so a read
there waits behind it. Reads run on the read conn. A listed function runs
every one of its calls on the write conn: signing and submitting, the
keepalive that keeps that socket alive, and two reads that must see the node
a submit goes to. Both of those run under the chain-write lock, so no submit
holds the socket while they read.
"""
import ast
from pathlib import Path

import pytest

from tests.fake_chain import connected_client

DAEMON = Path(__file__).resolve().parents[1] / "daemon"
WRITE_CONN_FUNCTIONS = {
    "compose_call": "composes the call a submit signs, under the runtime it is submitted to",
    "create_signed_extrinsic": "reads the signer's nonce at the best block of the node the submit goes to",
    "submit_extrinsic": "submits",
    "submit_join_committee": "composes, signs and submits",
    "submit_bond": "composes, signs and submits",
    "_submit_settlement_call": "composes, signs and submits",
    "submit_availability_cert": "composes, signs and submits",
    "keepalive": "keeps the write conn's own socket alive",
    "attestation_on_chain": "judges a refused attest at one best block of the node that refused it (#66 F2)",
    "get_settlement_request": "rereads a request under the chain-write lock right before its supersede or slash",
}


def chain_calls(source: str) -> list[tuple[str, str | None, int]]:
    """(innermost enclosing function, ``conn=`` source or None, line) of every
    ``_call`` in ``source``."""
    found = []

    def visit(node, function):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            function = node.name
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "_call":
            conn = next((ast.unparse(k.value) for k in node.keywords if k.arg == "conn"), None)
            found.append((function, conn, node.lineno))
        for child in ast.iter_child_nodes(node):
            visit(child, function)

    visit(ast.parse(source), None)
    return found


def misrouted(source: str) -> list[tuple[str, str | None, int]]:
    """The calls in ``source`` not on their function's conn: the write conn
    for a function in WRITE_CONN_FUNCTIONS, the read conn for any other."""
    return [
        (function, conn, line) for function, conn, line in chain_calls(source)
        if conn != ("self._write" if function in WRITE_CONN_FUNCTIONS else "self._read")
    ]


def raw_interface_uses(source: str) -> list[int]:
    """Lines of ``source`` that reach ``client.substrate``, the write conn's
    SubstrateInterface: a call on it runs on the write conn outside ``_call``."""
    return [
        node.lineno for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Attribute) and node.attr == "substrate"
    ]


def daemon_sources():
    return {path.name: path.read_text() for path in sorted(DAEMON.glob("*.py"))}


def test_every_chain_call_runs_on_its_functions_conn():
    assert {name: misrouted(source) for name, source in daemon_sources().items() if misrouted(source)} == {}


def test_every_write_conn_function_still_makes_a_chain_call():
    functions = {function for source in daemon_sources().values() for function, _, _ in chain_calls(source)}
    assert sorted(WRITE_CONN_FUNCTIONS.keys() - functions) == []


def test_no_module_reaches_the_raw_write_interface():
    assert {name: raw_interface_uses(source) for name, source in daemon_sources().items()
            if raw_interface_uses(source)} == {}


def test_the_guard_flags_each_way_a_call_lands_off_its_conn():
    source = '''
class Client:
    def get_unnamed(self):
        return self._call("query", "M", "A")

    def get_on_write(self):
        return self._call("query", "M", "B", conn=self._write)

    def get_elsewhere(self):
        return self._call("query", "M", "C", conn=self.client._read)

    def keepalive(self):
        self._call("rpc_request", "system_health", [], conn=self._read)

    def submit_extrinsic(self, extrinsic):
        def nested():
            return self._call("query", "M", "D", conn=self._write)
        return self._call("submit_extrinsic", extrinsic, conn=self._write)

    def get_on_read(self):
        return self._call("query", "M", "E", conn=self._read)
'''
    assert [(function, conn) for function, conn, _ in misrouted(source)] == [
        ("get_unnamed", None),
        ("get_on_write", "self._write"),
        ("get_elsewhere", "self.client._read"),
        ("keepalive", "self._read"),
        ("nested", "self._write"),
    ]
    assert raw_interface_uses("payload = encode(self.client.substrate, rows)\n") == [1]


def test_a_call_that_names_no_conn_is_refused():
    client, endpoint = connected_client()

    with pytest.raises(TypeError):
        client._call("rpc_request", "system_health", [])

    assert [si.calls for si in endpoint.opened] == [[], []]
