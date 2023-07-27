import asyncio
from collections import deque

from bytewax.inputs import DynamicInput
from bytewax.inputs import StatelessSource
from bytewax.outputs import DynamicOutput
from bytewax.outputs import StatelessSink

from .._internal import Memphis

__all__ = ["MemphisConsumerInput", "MemphisProducerOutput"]

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
        if self.current_msg_ != None:
            self.loop_.run_until_complete(self.current_msg_.ack())
            self.current_msg_ = None

        if len(self.messages_) == 0:
            batch = self.loop_.run_until_complete(self.consumer_.fetch())
            if batch is None or len(batch) == 0:
                return None
            else:
                self.messages_.extend(batch)

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


class _MemphisProducerSink(StatelessSink):
    def __init__(self, host, username, password, station, producer_name):
        self.loop_ = asyncio.get_event_loop()

        self.memphis_ = Memphis()
        self.loop_.run_until_complete(self.memphis_.connect(host=host, username=username, password=password))

        self.producer_ = self.loop_.run_until_complete(self.memphis_.producer(station_name=station,
                                                                              producer_name=producer_name))

    def write(self, item):
        self.loop_.run_until_complete(self.producer_.produce(bytearray(item, "utf-8")))

    def close(self):
        self.loop_.run_until_complete(self.producer_.destroy())
        self.loop_.run_until_complete(self.memphis_.close())

class MemphisProducerOutput(DynamicOutput):
    def __init__(self, host, username, password, station, producer_prefix):
        self.host = host
        self.username = username
        self.password = password
        self.station = station
        self.producer_prefix = producer_prefix

    def build(self, worker_index, worker_count):
        producer_name = self.producer_prefix + "_" + str(worker_index)
        return _MemphisProducerSink(self.host, self.username, self.password, self.station, producer_name)
