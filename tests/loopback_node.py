"""A JSON-RPC websocket node on a loopback port, for what only the real
websocket-client and substrate-interface on a real socket show: a node that goes
silent while its TCP connections stay open, one that accepts a connection and never
answers the websocket upgrade, and a port whose connects are never answered.

``Caller`` is one thread standing for a daemon's only thread: a call that never
returns fails the test with where it is blocked instead of hanging the run.
"""
import base64
import hashlib
import json
import queue
import socket
import struct
import sys
import threading
import time
import traceback

import pytest

WEBSOCKET_GUID = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
CLOSE, TEXT = 0x8, 0x1


class LoopbackNode:
    """Answers ``system_chain`` itself and every other method with ``answer(method,
    params)``. ``silence()`` stops it answering on the sockets open now, which stay
    open and are still read, close frames included, while sockets opened later are
    served. With ``answers_upgrades`` False it accepts a connection and never answers
    its websocket upgrade; with ``answers_calls`` False it upgrades and then answers
    nothing."""

    def __init__(self, answer):
        self.answer = answer
        self.answers_upgrades = True
        self.answers_calls = True
        self.sockets: list[socket.socket] = []
        self._silent: set[int] = set()
        self._listener = socket.socket()
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen(16)
        self.url = f"ws://127.0.0.1:{self._listener.getsockname()[1]}"
        threading.Thread(target=self._accept, daemon=True).start()

    def silence(self) -> None:
        self._silent = set(range(len(self.sockets)))

    def drop_sockets(self) -> None:
        """Closes every open socket, as a node process that exits does."""
        for conn in self.sockets:
            try:
                conn.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            conn.close()

    def close(self) -> None:
        self._listener.close()
        self.drop_sockets()

    def _accept(self) -> None:
        while True:
            try:
                conn, _ = self._listener.accept()
            except OSError:
                return
            self.sockets.append(conn)
            threading.Thread(target=self._serve, args=(conn, len(self.sockets) - 1), daemon=True).start()

    @staticmethod
    def _read(conn: socket.socket, length: int) -> bytes:
        data = b""
        while len(data) < length:
            chunk = conn.recv(length - len(data))
            if not chunk:
                raise ConnectionError("peer closed")
            data += chunk
        return data

    def _frame(self, conn: socket.socket) -> tuple[int, bytes]:
        head = self._read(conn, 2)
        length = head[1] & 0x7F
        if length == 126:
            length = struct.unpack("!H", self._read(conn, 2))[0]
        elif length == 127:
            length = struct.unpack("!Q", self._read(conn, 8))[0]
        mask = self._read(conn, 4)
        return head[0] & 0x0F, bytes(b ^ mask[i % 4] for i, b in enumerate(self._read(conn, length)))

    def _serve(self, conn: socket.socket, index: int) -> None:
        try:
            request = b""
            while b"\r\n\r\n" not in request:
                chunk = conn.recv(4096)
                if not chunk:
                    return
                request += chunk
            if not self.answers_upgrades:
                return
            key = next(line.split(b":", 1)[1].strip() for line in request.split(b"\r\n")
                       if line.lower().startswith(b"sec-websocket-key"))
            accept = base64.b64encode(hashlib.sha1(key + WEBSOCKET_GUID).digest())
            conn.sendall(b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                         b"Sec-WebSocket-Accept: " + accept + b"\r\n\r\n")
            while True:
                opcode, payload = self._frame(conn)
                if index in self._silent or not self.answers_calls:
                    continue
                if opcode == CLOSE:
                    conn.sendall(bytes([0x80 | CLOSE, 0]))
                    return
                call = json.loads(payload)
                result = ("Materios Loopback" if call["method"] == "system_chain"
                          else self.answer(call["method"], call.get("params", [])))
                body = json.dumps({"jsonrpc": "2.0", "id": call["id"], "result": result}).encode()
                if len(body) < 126:
                    head = bytes([0x80 | TEXT, len(body)])
                elif len(body) < 1 << 16:
                    head = bytes([0x80 | TEXT, 126]) + struct.pack("!H", len(body))
                else:
                    head = bytes([0x80 | TEXT, 127]) + struct.pack("!Q", len(body))
                conn.sendall(head + body)
        except (ConnectionError, OSError):
            return


def unanswered_port():
    """A loopback port whose connects are never answered, as a host that drops packets:
    a listener that accepts nothing, its backlog full. Returns the port and what must
    stay referenced while it is used."""
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(0)
    port = listener.getsockname()[1]
    held = [listener]
    for _ in range(16):
        filler = socket.socket()
        filler.settimeout(0.3)
        try:
            filler.connect(("127.0.0.1", port))
        except TimeoutError:
            filler.close()
            return port, held
        held.append(filler)
    raise AssertionError("the loopback listener kept answering connects with its backlog full")


class Caller:
    """Runs each call on one daemon thread and waits ``cap`` seconds for it."""

    def __init__(self, cap: float = 20.0):
        self.cap = cap
        self._jobs: queue.Queue = queue.Queue()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        while True:
            fn, done = self._jobs.get()
            started = time.monotonic()
            try:
                outcome = fn()
            except Exception as error:  # noqa: BLE001
                outcome = error
            done.put((outcome, time.monotonic() - started))

    def __call__(self, fn):
        """What ``fn`` returned or raised, and the seconds it took."""
        done: queue.Queue = queue.Queue()
        self._jobs.put((fn, done))
        try:
            return done.get(timeout=self.cap)
        except queue.Empty:
            stack = traceback.extract_stack(sys._current_frames()[self._thread.ident])
            where = " <- ".join(f"{frame.name}@{frame.filename.rsplit('/', 1)[-1]}:{frame.lineno}"
                                for frame in reversed(stack[-7:]))
            pytest.fail(f"still blocked after {self.cap:.0f} s in {where}")
