# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import asyncio
import socket
import typing

from asyncpg import cluster


class StopServer(Exception):
    pass


class TCPFuzzingProxy:
    def __init__(self, *, listening_addr: str='127.0.0.1',
                 listening_port: typing.Optional[int]=None,
                 backend_host: str, backend_port: int,
                 settings: typing.Optional[dict]=None,
                 loop: typing.Optional[asyncio.AbstractEventLoop]) -> None:
        self.listening_addr = listening_addr
        self.listening_port = listening_port
        self.backend_host = backend_host
        self.backend_port = backend_port
        self.settings = settings or {}
        self.loop = loop or asyncio.get_event_loop()
        self.connections = {}
        self.connectivity = asyncio.Event(loop=self.loop)
        self.connectivity.set()
        self.connectivity_loss = asyncio.Event(loop=self.loop)
        self.stop_event = asyncio.Event(loop=self.loop)
        self.sock = None

    async def _wait(self, work):
        work_task = asyncio.ensure_future(work, loop=self.loop)
        stop_event_task = asyncio.ensure_future(self.stop_event.wait(),
                                                loop=self.loop)

        try:
            await asyncio.wait(
                [work_task, stop_event_task],
                return_when=asyncio.FIRST_COMPLETED,
                loop=self.loop)

            if self.stop_event.is_set():
                raise StopServer()
            else:
                return work_task.result()
        finally:
            if not work_task.done():
                work_task.cancel()
            if not stop_event_task.done():
                stop_event_task.cancel()

    async def start(self):
        if self.listening_port is None:
            self.listening_port = cluster.find_available_port()

        self.sock = socket.socket()
        self.sock.bind((self.listening_addr, self.listening_port))
        self.sock.listen(50)
        self.sock.setblocking(False)
        self.loop.create_task(self.listen())

    async def stop(self):
        self.stop_event.set()
        for conn, conn_task in self.connections.items():
            conn_task.cancel()
            conn.close()
        self.sock.close()
        await asyncio.sleep(0.2, loop=self.loop)

    async def listen(self):
        while True:
            try:
                client_sock, _ = await self._wait(
                    self.loop.sock_accept(self.sock))

                backend_sock = socket.socket()
                backend_sock.setblocking(False)

                await self._wait(self.loop.sock_connect(
                    backend_sock, (self.backend_host, self.backend_port)))
            except StopServer:
                break

            conn = Connection(
                client_sock, backend_sock,
                connectivity=self.connectivity,
                connectivity_loss=self.connectivity_loss, loop=self.loop)
            conn_task = self.loop.create_task(conn.handle())
            self.connections[conn] = conn_task

    def trigger_connectivity_loss(self):
        self.connectivity.clear()
        self.connectivity_loss.set()

    def restore_connectivity(self):
        self.connectivity.set()
        self.connectivity_loss.clear()

    def reset(self):
        self.restore_connectivity()


class Connection:
    def __init__(self, client_sock, backend_sock,
                 connectivity, connectivity_loss, loop):
        self.client_sock = client_sock
        self.backend_sock = backend_sock
        self.loop = loop
        self.connectivity = connectivity
        self.connectivity_loss = connectivity_loss
        self.proxy_to_backend_task = None
        self.proxy_from_backend_task = None

    def close(self):
        if self.proxy_to_backend_task is not None:
            self.proxy_to_backend_task.cancel()

        if self.proxy_from_backend_task is not None:
            self.proxy_from_backend_task.cancel()

    async def handle(self):
        self.proxy_to_backend_task = self.loop.create_task(
            self.proxy_to_backend())

        self.proxy_from_backend_task = self.loop.create_task(
            self.proxy_from_backend())

        try:
            await asyncio.gather(
                self.proxy_to_backend_task, self.proxy_from_backend_task,
                loop=self.loop)
        finally:
            self.client_sock.close()
            self.backend_sock.close()

    async def _read(self, sock, n):
        read_task = asyncio.ensure_future(
            self.loop.sock_recv(sock, n),
            loop=self.loop)
        conn_event_task = asyncio.ensure_future(
            self.connectivity_loss.wait(),
            loop=self.loop)

        try:
            await asyncio.wait(
                [read_task, conn_event_task],
                return_when=asyncio.FIRST_COMPLETED,
                loop=self.loop)

            if self.connectivity_loss.is_set():
                return None
            else:
                return read_task.result()
        finally:
            if not read_task.done():
                read_task.cancel()
            if not conn_event_task.done():
                conn_event_task.cancel()

    async def _write(self, sock, data):
        write_task = asyncio.ensure_future(
            self.loop.sock_sendall(sock, data), loop=self.loop)
        conn_event_task = asyncio.ensure_future(
            self.connectivity_loss.wait(), loop=self.loop)

        try:
            await asyncio.wait(
                [write_task, conn_event_task],
                return_when=asyncio.FIRST_COMPLETED,
                loop=self.loop)

            if self.connectivity_loss.is_set():
                return None
            else:
                return write_task.result()
        finally:
            if not write_task.done():
                write_task.cancel()
            if not conn_event_task.done():
                conn_event_task.cancel()

    async def proxy_to_backend(self):
        buf = None

        while True:
            await self.connectivity.wait()
            if buf is not None:
                data = buf
                buf = None
            else:
                data = await self._read(self.client_sock, 4096)
            if data == b'':
                self.close()
                break
            if self.connectivity_loss.is_set():
                if data:
                    buf = data
                continue
            await self._write(self.backend_sock, data)

    async def proxy_from_backend(self):
        buf = None

        while True:
            await self.connectivity.wait()
            if buf is not None:
                data = buf
                buf = None
            else:
                data = await self._read(self.backend_sock, 4096)
            if data == b'':
                self.close()
                break
            if self.connectivity_loss.is_set():
                if data:
                    buf = data
                continue
            await self._write(self.client_sock, data)
