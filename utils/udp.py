import pickle
import socket
import asyncio
from db.tables import DMKSource


class UdpServer:
    def __init__(self, host: str = "127.0.0.1", port: int = 40000):
        self.host = host
        self.port = port

        self.transport = None
        self.protocol = None
        self._data_receive_q = asyncio.Queue()

    async def start_listen(self):
        if self.transport is not None:
            return

        class ServerProtocol(asyncio.Protocol):
            def __init__(self, q: asyncio.Queue):
                self.transport = None
                self.data_receive_q = q

            def connection_made(self, transport):
                self.transport = transport

            def datagram_received(self, data, addr):
                self.data_receive_q.put_nowait((data, addr))

        event_loop = asyncio.get_event_loop()
        self.transport, self.protocol = await event_loop.create_datagram_endpoint(
            lambda: ServerProtocol(self._data_receive_q),
            local_addr=(self.host, self.port)
        )

    def qzise(self) -> int:
        return self._data_receive_q.qsize()

    def get_nowait(self) -> DMKSource:
        return self._data_receive_q.get_nowait()

    async def get(self) -> DMKSource:
        return await self._data_receive_q.get()


class UdpClient:
    def __init__(self, host: str = "127.0.0.1", port: int = 40000):
        self.host = host
        self.port = port

        self.transport = None
        self.protocol = None
        self.sync_udp_client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    async def _create_transport(self):
        class MyProtocol(asyncio.Protocol):
            def datagram_received(self, d, addr):
                pass

            def error_received(self, exc):
                pass

        event_loop = asyncio.get_event_loop()
        self.transport, self.protocol = await event_loop.create_datagram_endpoint(
            lambda: MyProtocol(),
            remote_addr=(self.host, self.port)
        )

    async def _sendto(self, data):
        if self.transport is None:
            await self._create_transport()

        if isinstance(data, str):
            data = data.encode()
        return self.transport.sendto(data)

    def put_nowait(self, message: DMKSource):
        py_obj_bytes = pickle.dumps(message)
        return self.sync_udp_client.sendto(
            data=py_obj_bytes,
            address=(self.host, self.port)
        )

    async def put(self, message: DMKSource):
        py_obj_bytes = pickle.dumps(message)
        await self._sendto(py_obj_bytes)


mq_client = UdpClient()
mq_server = UdpServer()
