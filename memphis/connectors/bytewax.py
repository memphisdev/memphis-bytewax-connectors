import asyncio

from bytewax.inputs import DynamicInput
from bytewax.inputs import StatelessSource

from .._internal import Memphis

__all__ = ["MemphisConsumerInput"]

class _MemphisConsumerSource(StatelessSource):
    def __init__(self, host, username, password, station, consumer_name, consumer_group):
        self.loop = asyncio.get_event_loop()
        self.messages = deque()

        self.memphis = Memphis()
        self.loop.run_until_complete(self.memphis.connect(host=host, username=username, password=password))

        self.consumer = self.loop.run_until_complete(self.memphis.consumer(station_name=station, consumer_name=consumer_name, consumer_group=consumer_group, pull_interval_ms=100))

    def next(self):
        if len(self.messages) == 0:
            batch = self.loop.run_until_complete(self.consumer.fetch())
            if batch is not None:
                self.messages.extend(batch)

        if len(self.messages) == 0:
            return None

        msg = self.messages.popleft()

        self.loop.run_until_complete(msg.ack())

        return msg.get_data()

    def close(self):
        self.loop.run_until_complete(self.consumer.destroy())
        self.loop.run_until_complete(self.memphis.close())

class MemphisConsumerInput(DynamicInput):
    def __init__(self, host, username, password, station, consumer_group):
        self.host = host
        self.username = username
        self.password = password
        self.station = station
        self.consumer_group = consumer_group

    def build(self, worker_index, worker_count):
        consumer_name = self.consumer_group + "_" + str(worker_index)
        return _MemphisConsumerSource(self.host, self.username, self.password, self.station, consumer_name, self.consumer_group)
