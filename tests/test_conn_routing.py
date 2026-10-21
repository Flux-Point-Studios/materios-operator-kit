"""Every chain call names its connection, and the write conn carries only the
calls listed here (#69).

A submit waiting for inclusion holds the write conn's websocket, so a read
there waits behind it. Reads run on the read conn. A listed function runs
every one of its calls on the write conn: signing and submitting, the
keepalive that keeps that socket alive, and two reads that must see the node
a submit goes to. Both of those run under the chain-write lock, so no submit
holds the socket while they read. No daemon code reaches a SubstrateInterface
but through ``_call``: not ``client.substrate``, and not a conn's ``si`` or
``bound`` outside the functions that open, hold or replace it.

Every other daemon module reaches the write conn only under the chain-write
lock (#71), which keeps the signer's nonce monotonic and each write-conn call
off a socket another path's submit holds. The check knows such a call by the
function's name on any object, so an alias or a parameter cannot hide the
client, or in a string, as getattr and methodcaller take it. A getattr whose
name is computed counts when its object is spelled ``...client``, and no
module binds a client to a name spelled otherwise.
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
# The functions that open, hold or replace a conn's SubstrateInterface, and
# the conn's own accessor.
SI_KEEPERS = {"si", "connect", "connected", "substrate", "_call", "_live_conn", "_reconnect", "_call_once",
              "_replace_dead_conn", "_on_rpc_timeout", "_force_replace"}
CHAIN_WRITE_LOCK = "_chain_write_lock"


def by_function(source: str):
    """Every node of ``source``, with its innermost enclosing function's name."""
    def visit(node, function):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            function = node.name
        yield function, node
        for child in ast.iter_child_nodes(node):
            yield from visit(child, function)

    return visit(ast.parse(source), None)


def chain_calls(source: str) -> list[tuple[str, str | None, int]]:
    """(innermost enclosing function, ``conn=`` source or None, line) of every
    ``_call`` in ``source``."""
    return [
        (function, next((ast.unparse(k.value) for k in node.keywords if k.arg == "conn"), None), node.lineno)
        for function, node in by_function(source)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "_call"
    ]


def misrouted(source: str) -> list[tuple[str, str | None, int]]:
    """The calls in ``source`` not on their function's conn: the write conn
    for a function in WRITE_CONN_FUNCTIONS, the read conn for any other."""
    return [
        (function, conn, line) for function, conn, line in chain_calls(source)
        if conn != ("self._write" if function in WRITE_CONN_FUNCTIONS else "self._read")
    ]


def raw_interface_uses(source: str) -> list[int]:
    """Lines of ``source`` that reach a SubstrateInterface outside ``_call``:
    ``client.substrate``, the write conn's, or a read of a conn's ``si`` or
    ``bound`` outside SI_KEEPERS. A call on either runs outside ``_call``'s
    budget."""
    return [
        node.lineno for function, node in by_function(source)
        if isinstance(node, ast.Attribute)
        and (node.attr == "substrate"
             or (node.attr in ("si", "bound") and isinstance(node.ctx, ast.Load) and function not in SI_KEEPERS))
    ]


def write_conn_methods(client_source: str) -> set[str]:
    """The client's functions that reach the write conn: each one in
    WRITE_CONN_FUNCTIONS, and each one that calls such a function."""
    used = {}
    for function, node in by_function(client_source):
        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == "self":
            used.setdefault(function, set()).add(node.attr)
    reach = set(WRITE_CONN_FUNCTIONS)
    while more := {function for function, names in used.items() if names & reach} - reach:
        reach |= more
    return reach


def spelled_client(node: ast.AST) -> bool:
    return ast.unparse(node).endswith("client")


def reached_write(node: ast.AST, writes: set[str]) -> str | None:
    """The client function in ``writes`` that ``node`` reaches, if any: an
    attribute named for one, on any object, so no alias or parameter hides
    the client; a string naming one, as getattr and methodcaller take; or,
    as its source, the computed name of a getattr on a client, which may name
    any of them."""
    if isinstance(node, ast.Attribute) and node.attr in writes:
        return node.attr
    if isinstance(node, ast.Constant) and isinstance(node.value, str) and node.value in writes:
        return node.value
    if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "getattr"
            and len(node.args) > 1 and not isinstance(node.args[1], ast.Constant)
            and spelled_client(node.args[0])):
        return ast.unparse(node.args[1])
    return None


def client_aliases(source: str) -> list[int]:
    """Lines of ``source`` that bind a client, an expression spelled
    ``...client``, to a name spelled otherwise. A getattr with a computed name
    is known to reach a client by that spelling alone."""
    bindings = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Assign):
            bindings += [(target, node.value) for target in node.targets]
        elif isinstance(node, (ast.AnnAssign, ast.NamedExpr)) and node.value is not None:
            bindings.append((node.target, node.value))
    return [value.lineno for target, value in bindings if spelled_client(value) and not spelled_client(target)]


def unlocked_writes(source: str, writes: set[str]) -> list[tuple[str | None, str, int]]:
    """(function, name, line) of each use in ``source`` of a client function
    in ``writes`` outside the chain-write lock. A use is under the lock inside
    an ``async with`` on it, or in a function every use of which is. A nested
    function or lambda runs whenever it is called, so it starts unlocked."""
    uses = []

    def visit(node, function, locked):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            function, locked = node.name, False
        elif isinstance(node, ast.Lambda):
            locked = False
        uses.append((function, node, locked))
        takes_lock = isinstance(node, ast.AsyncWith) and any(
            ast.unparse(item.context_expr).endswith(CHAIN_WRITE_LOCK) for item in node.items)
        for child in ast.iter_child_nodes(node):
            visit(child, function, locked or (takes_lock and child in node.body))

    tree = ast.parse(source)
    visit(tree, None, False)
    defined = {node.name for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}
    held = set()

    def under_lock(function, locked):
        return locked or function in held

    def own_uses(name):
        return [(function, locked) for function, node, locked in uses
                if isinstance(node, ast.Attribute) and node.attr == name
                and isinstance(node.value, ast.Name) and node.value.id == "self"]

    while more := {name for name in defined - held
                   if own_uses(name) and all(under_lock(*use) for use in own_uses(name))}:
        held |= more
    return [(function, name, node.lineno) for function, node, locked in uses
            if (name := reached_write(node, writes)) and not under_lock(function, locked)]


def daemon_sources():
    return {path.name: path.read_text() for path in sorted(DAEMON.glob("*.py"))}


def test_every_chain_call_runs_on_its_functions_conn():
    assert {name: misrouted(source) for name, source in daemon_sources().items() if misrouted(source)} == {}


def test_every_write_conn_function_still_makes_a_chain_call():
    functions = {function for source in daemon_sources().values() for function, _, _ in chain_calls(source)}
    assert sorted(WRITE_CONN_FUNCTIONS.keys() - functions) == []


def test_every_si_keeper_still_reads_an_interface():
    readers = {function for source in daemon_sources().values() for function, node in by_function(source)
               if isinstance(node, ast.Attribute) and node.attr in ("si", "bound")
               and isinstance(node.ctx, ast.Load)}
    assert sorted(SI_KEEPERS - readers) == []


def test_every_write_conn_call_runs_under_the_chain_write_lock():
    sources = daemon_sources()
    writes = write_conn_methods(sources.pop("substrate_client.py"))
    assert {name: unlocked_writes(source, writes) for name, source in sources.items()
            if unlocked_writes(source, writes)} == {}


def test_no_module_binds_a_client_to_a_name_spelled_otherwise():
    assert {name: client_aliases(source) for name, source in daemon_sources().items()
            if client_aliases(source)} == {}


def test_the_write_conn_methods_include_every_submit_wrapper():
    writes = write_conn_methods(daemon_sources()["substrate_client.py"])
    assert {"submit_attest_settle", "submit_attest_settle_envelope", "submit_attest_expire_policy",
            "submit_attest_expire_policy_envelope", "submit_slash_bad_settlement_evidence",
            "submit_supersede_settlement_request", "submit_join_committee", "submit_bond"} <= writes
    assert {"get_committee_members", "get_receipt", "rpc"}.isdisjoint(writes)


def test_no_module_reaches_an_interface_outside_the_bounded_call():
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
    assert raw_interface_uses('''
payload = encode(self.client.substrate, rows)

def __init__(self):
    self.si = None

def build(self, rows):
    return encode(self.client._write.si, rows)

def get_min_signer_threshold(self):
    return self._write.si.query("IntentSettlement", "MinSignerThreshold")

def _force_replace(self, conn):
    old_si = conn.si

def get_receipt(self):
    si, lock = self._read.bound
''') == [2, 8, 11, 17]


def test_the_guard_flags_each_write_conn_call_off_the_chain_write_lock():
    source = '''
class Daemon:
    async def unlocked(self):
        self.client.submit_bond(1)

    async def locked(self):
        async with self._chain_write_lock:
            await asyncio.to_thread(self.client.submit_join_committee)

    async def reads(self):
        return self.client.get_committee_members()

    async def caller(self):
        async with self._chain_write_lock:
            await self.helper()

    async def helper(self):
        return await asyncio.to_thread(self.client.get_settlement_request, b"")

    async def both(self):
        await self.mixed()
        async with self._chain_write_lock:
            await self.mixed()

    async def mixed(self):
        return self.client.keepalive()

    async def nested(self):
        async with self._chain_write_lock:
            def later():
                return client.attestation_on_chain("0x00", None)
            return later

    async def deferred(self):
        async with self._chain_write_lock:
            return lambda: self.client.submit_extrinsic(None)

    async def other_lock(self):
        async with self._pending_lock:
            self._client.create_signed_extrinsic(call=None)

    async def aliased(self):
        c = self.client
        c.submit_bond(1)

    async def handed(self, chain):
        chain.keepalive()

    async def by_name(self):
        getattr(self.client, "submit_join_committee")()

    async def by_method_caller(self):
        operator.methodcaller("submit_extrinsic", None)(self.client)

    async def by_computed_name(self, kind):
        return getattr(self.client, f"submit_{kind}")

    async def locked_by_name(self, kind):
        async with self._chain_write_lock:
            getattr(self.client, "submit_bond")(1)
            getattr(self.client, f"submit_{kind}")()

    async def other_getattrs(self, request, field):
        return getattr(request, field.name), getattr(self.client, "get_receipt")
'''
    writes = {"submit_bond", "submit_join_committee", "get_settlement_request", "keepalive",
              "attestation_on_chain", "submit_extrinsic", "create_signed_extrinsic"}
    assert [(function, name) for function, name, _ in unlocked_writes(source, writes)] == [
        ("unlocked", "submit_bond"),
        ("mixed", "keepalive"),
        ("later", "attestation_on_chain"),
        ("deferred", "submit_extrinsic"),
        ("other_lock", "create_signed_extrinsic"),
        ("aliased", "submit_bond"),
        ("handed", "keepalive"),
        ("by_name", "submit_join_committee"),
        ("by_method_caller", "submit_extrinsic"),
        ("by_computed_name", "f'submit_{kind}'"),
    ]


def test_the_guard_flags_each_client_bound_to_a_name_spelled_otherwise():
    assert client_aliases('''
class Watcher:
    def __init__(self, substrate_client, client):
        self.client = substrate_client
        self._client = client
        c = self.client
        chain: SubstrateClient = client
        if (ws := self._client) is not None:
            pass
        client = self.client
        rows = self.client.rpc("state_getKeys", [])
''') == [6, 7, 8]


def test_a_call_that_names_no_conn_is_refused():
    client, endpoint = connected_client()

    with pytest.raises(TypeError):
        client._call("rpc_request", "system_health", [])

    assert [si.calls for si in endpoint.opened] == [[], []]
