import asyncio


class Listener:
    """The class for listen postgres notifications"""
    MAX_BUFFER_SIZE = 1000000

    def __init__(self, pool, channel):
        self.pool = pool
        self.channel = channel
        self.connection = None
        self.results = []

    async def start(self):
        if self.connection is not None:
            raise UserWarning(f"already listening on channel: {self.channel}")
        self.connection = await self.pool.acquire()
        self.results = [asyncio.Future()]
        await self.connection.add_listener(self.channel, self.handle_notifications)

    async def stop(self):
        try:
            for f in self.results:
                f.cancel()
            await self.connection.remove_listener(self.channel, self.handle_notifications)
        finally:
            await self.pool.release(self.connection)
            self.connection = None
            self.results = []

    def handle_notifications(self, conn, pid, channel, payload):
        if(len(self.results) >= self.MAX_BUFFER_SIZE):
            raise UserWarning(
                "too many unprocessed notifications: {}".format(self.channel))
        f = self.results[-1]
        self.results.append(asyncio.Future())
        f.set_result({
            'listener_pid': conn.get_server_pid(),
            'notifier_pid': pid,
            'channel': channel,
            'payload': payload
        })

    async def next(self):
        if self.connection is None:
            await self.start()
        if(len(self.results) == 0):
            return None
        result = await self.results[0]
        self.results.pop(0)
        return result

    def __await__(self):
        return self.next().__await__()

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.stop()
