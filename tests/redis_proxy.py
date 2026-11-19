"""TCP proxy in front of Redis that can force two clients' commands to overlap.

Bytes from clients are always forwarded to Redis immediately. When armed with
a marker, a connection becomes "marked" once it sends bytes containing the
marker, and replies to marked connections are held back until `parties`
distinct connections are marked (or a safety timeout passes). This makes
"both requests reach Redis before either sees a reply" happen on every run,
so the outcome does not depend on timing.
"""
import asyncio
import threading


class Proxy:
    def __init__(self, listen_port, upstream_port):
        self.listen_port = listen_port
        self.upstream_port = upstream_port
        self.loop = asyncio.new_event_loop()
        self._gate = None
        started = threading.Event()
        threading.Thread(target=self._run, args=(started,), daemon=True).start()
        if not started.wait(10):
            raise RuntimeError("proxy did not start")

    def _run(self, started):
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(
            asyncio.start_server(self._handle, "127.0.0.1", self.listen_port)
        )
        started.set()
        self.loop.run_forever()

    def _call(self, fn):
        async def wrapper():
            fn()
        asyncio.run_coroutine_threadsafe(wrapper(), self.loop).result(5)

    def arm(self, marker, parties=2, timeout=5.0):
        def do():
            self._gate = {
                "marker": marker,
                "parties": parties,
                "marked": set(),
                "event": asyncio.Event(),
                "timeout": timeout,
            }
        self._call(do)

    def disarm(self):
        def do():
            if self._gate is not None:
                self._gate["event"].set()
            self._gate = None
        self._call(do)

    async def _handle(self, creader, cwriter):
        try:
            ureader, uwriter = await asyncio.open_connection("127.0.0.1", self.upstream_port)
        except OSError:
            cwriter.close()
            return
        conn = object()

        async def client_to_upstream():
            tail = b""
            try:
                while True:
                    data = await creader.read(65536)
                    if not data:
                        break
                    gate = self._gate
                    if gate is not None and conn not in gate["marked"]:
                        marker = gate["marker"]
                        if marker in tail + data:
                            gate["marked"].add(conn)
                            if len(gate["marked"]) >= gate["parties"]:
                                gate["event"].set()
                        tail = (tail + data)[-(len(marker) - 1):] if len(marker) > 1 else b""
                    uwriter.write(data)
                    await uwriter.drain()
            finally:
                uwriter.close()

        async def upstream_to_client():
            try:
                while True:
                    data = await ureader.read(65536)
                    if not data:
                        break
                    gate = self._gate
                    if gate is not None and conn in gate["marked"] and not gate["event"].is_set():
                        try:
                            await asyncio.wait_for(gate["event"].wait(), gate["timeout"])
                        except asyncio.TimeoutError:
                            gate["event"].set()
                    cwriter.write(data)
                    await cwriter.drain()
            finally:
                cwriter.close()

        await asyncio.gather(client_to_upstream(), upstream_to_client(), return_exceptions=True)
