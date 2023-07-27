import asyncio
from collections import deque

from bytewax.inputs import DynamicInput
from bytewax.inputs import StatelessSource

from .._internal import Memphis

__all__ = ["MemphisConsumerInput"]

class _MemphisConsumerSource(StatelessSource):
    def __init__(self, host, username, password, station, consumer_name, consumer_group):
        self.loop_ = asyncio.get_event_loop()
        self.messages_ = deque()
        self.current_msg_ = None

        self.memphis_ = Memphis()
        self.loop_.run_until_complete(self.memphis_.connect(host=host, username=username, password=password))

        self.consumer_ = self.loop_.run_until_complete(self.memphis_.consumer(station_name=station,
                                                                              consumer_name=consumer_name,
                                                                              consumer_group=consumer_group,
                                                                              pull_interval_ms=100))

    def next(self):
        if len(self.messages_) == 0:
            batch = self.loop_.run_until_complete(self.consumer_.fetch())
            if batch is not None:
                self.messages_.extend(batch)

        if self.current_msg_ != None:
            self.loop_.run_until_complete(self.current_msg_.ack())

        if len(self.messages_) == 0:
            return None

        self.current_msg_ = self.messages_.popleft()

        return self.current_msg_.get_data()

    def close(self):
        self.loop_.run_until_complete(self.consumer_.destroy())
        self.loop_.run_until_complete(self.memphis_.close())

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
