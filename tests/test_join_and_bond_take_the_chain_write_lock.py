"""Join and bond submit under the chain-write lock, as every other submit
does (#71).

The lock keeps the signer's nonce monotonic: substrate-interface reads it at
the best block, blind to the pool, so two submits signed before either is
included share a nonce and one is refused. It also keeps every write-conn
call off a socket another path's submit is holding. The submits run on a
worker thread, so the event loop keeps serving while they wait for inclusion.
"""
import asyncio

from daemon.cert_daemon import CertDaemon
from daemon.config import DaemonConfig
from tests.fake_chain import including_client


def joining_daemon(bonded: int):
    """A daemon outside the committee, ``bonded`` of a 10-unit bond
    requirement posted, whose write conn's interface records whether the
    chain-write lock was held for each call."""
    client, write, _ = including_client()
    client.get_committee_members = lambda: []
    client.get_bond_requirement = lambda: 10
    client.get_attestor_bond = lambda address: bonded
    client.get_free_balance = lambda address: 100
    daemon = CertDaemon.__new__(CertDaemon)
    daemon.config = DaemonConfig()
    daemon.client = client
    daemon._chain_write_lock = asyncio.Lock()
    daemon._pending_lock = None
    daemon._concurrency_sem = None

    async def send_discord(message, level="info"):
        return None

    daemon.send_discord = send_discord
    write.lock = daemon._chain_write_lock
    return daemon, write


SUBMIT = ["compose_call", "create_signed_extrinsic", "submit_extrinsic", "get_block", "get_events"]


def test_bond_and_join_submit_under_the_chain_write_lock():
    daemon, write = joining_daemon(bonded=0)

    asyncio.run(daemon._ensure_committee_membership())

    assert write.names() == SUBMIT * 2, "a bond, then a join"
    assert [(name, held) for name, _, held in write.calls if not held] == []


def test_a_join_waits_for_another_paths_submit():
    daemon, write = joining_daemon(bonded=10)

    async def scenario():
        async with daemon._chain_write_lock:
            join = asyncio.create_task(daemon._ensure_committee_membership())
            await asyncio.sleep(0.2)
            assert write.calls == [], "the join reached the write conn while another submit held the lock"
        await join

    asyncio.run(scenario())
    assert write.names() == SUBMIT
