import asyncio
from collections import deque
import time

from bytewax.inputs import PartitionedInput
from bytewax.inputs import StatefulSource
from bytewax.outputs import DynamicOutput
from bytewax.outputs import StatelessSink

from .._internal import Memphis

__all__ = ["MemphisConsumerInput", "MemphisProducerOutput"]

class _MemphisConsumerSource(StatefulSource):
    def _run(self, awaitable):
        """
        Uses the event loop's run_until_complete() method to
        run an async function as if it were synchronous.
        """
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(awaitable)

    def __init__(self, host, username, password, station, consumer_name, resume_state):
        self._messages = deque()
        self._current_seq_num = None

        self._memphis = Memphis()
        self._run(self._memphis.connect(host=host, username=username, password=password))

        # we are going to use 1 consumer per consumer group so we can destroy the
        # group and recreate it with the desired starting sequence_id
        consumer_group = consumer_name

        # destroy the consumer on the server side if it exists
        print("Destroying {}".format(consumer_name))
        consumer = self._run(self._memphis.consumer(station_name=station,
                                                    consumer_name=consumer_name,
                                                    consumer_group=consumer_name,
                                                    start_consume_from_sequence=1,
                                                    pull_interval_ms=100))
        self._run(consumer.destroy())

        # to give me time to check that the consumer was
        # remove from the UI
        time.sleep(15)

        # default initial seq num
        initial_seq_num = 1

        # we are resuming from a previous run
        print("Resume state: {}".format(resume_state))
        if resume_state is not None:
            initial_seq_num = resume_state
        
        self._consumer = self._run(self._memphis.consumer(station_name=station,
                                                          consumer_name=consumer_name,
                                                          consumer_group=consumer_name,
                                                          start_consume_from_sequence=initial_seq_num,
                                                          pull_interval_ms=100))

    def next(self):
        if len(self._messages) == 0:
            batch = self._run(self._consumer.fetch())
            if batch is None or len(batch) == 0:
                return None
            else:
                self._messages.extend(batch)

        msg = self._messages.popleft()
        self._current_seq_num = msg.get_sequence_number()
        self._run(msg.ack())

        return msg.get_data()

    def snapshot(self):
        print("Snapshotting: {}".format(self._current_seq_num))
        return self._current_seq_num

    def close(self):
        self._run(self._consumer.destroy())
        self._run(self._memphis.close())

class MemphisConsumerInput(PartitionedInput):    
    def __init__(self, host, username, password, station, consumer_name_prefix):
        self.host = host
        self.username = username
        self.password = password
        self.station = station
        self.consumer_name_prefix = consumer_name_prefix

    def list_parts(self):
        """
        Only allowing one consumer to exist right now.
        When Memphis supports partitions, we will create
        a consumer group per partition.
        """

        return { "0" }

    def build_part(self, for_part, resume_state):
        return _MemphisConsumerSource(self.host,
                                      self.username,
                                      self.password,
                                      self.station,
                                      self.consumer_name_prefix + "_" + for_part,
                                      resume_state)


class _MemphisProducerSink(StatelessSink):
    def _run(self, awaitable):
        """
        Uses the event loop's run_until_complete() method to
        run an async function as if it were synchronous.
        """
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(awaitable)

    def __init__(self, host, username, password, station, producer_name):
        self._memphis = Memphis()
        self._run(self._memphis.connect(host=host, username=username, password=password))

        self._producer = self._run(self._memphis.producer(station_name=station,
                                                          producer_name=producer_name))

    def write(self, item):
        self._run(self._producer.produce(bytearray(item, "utf-8")))

    def close(self):
        self._run(self._producer.destroy())
        self._run(self._memphis.close())

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
