import asyncio
from collections import deque

from bytewax.inputs import PartitionedInput
from bytewax.inputs import StatefulSource
from bytewax.outputs import DynamicOutput
from bytewax.outputs import StatelessSink

from .._internal import Memphis

__all__ = ["MemphisInput", "MemphisOutput"]

class _MemphisConsumerSource(StatefulSource):
    def _run(self, awaitable):
        """
        Uses the event loop's run_until_complete() method to
        run an async function as if it were synchronous.
        """
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(awaitable)

    def __init__(self, host, username, password, station, consumer_name, start_consume_from_sequence, pull_interval_ms=100):
        self._messages = deque()
        self._current_seq_num = None

        self._memphis = Memphis()
        self._run(self._memphis.connect(host=host, username=username, password=password))

        # create an entirely new consumer every time so that we can control the starting
        # offset
        consumer_name = f"{consumer_name}-{self._memphis.connection_id}"

        # we are going to use 1 consumer per consumer group so we can
        # more easily manage the lifecycle to support replaying events
        consumer_group = consumer_name

        self._consumer = self._run(self._memphis.consumer(station_name=station,
                                                          consumer_name=consumer_name,
                                                          consumer_group=consumer_group,
                                                          start_consume_from_sequence=start_consume_from_sequence,
                                                          pull_interval_ms=pull_interval_ms))
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
        return self._current_seq_num

    def close(self):
        self._run(self._consumer.destroy())

class MemphisInput(PartitionedInput):
    """
    Use a Memphis.dev station as an input.

    Currently, this input connector supports:
    * 1 consumer per station: Adding partitions to Memphis is ongoing work.
      When available, we will update the connector to support 1 consumer
      per station partition.
    * At-least once semantics: If the Bytewax flow is killed and restarted,
      the connector will restart from the last messaged processed before the
      resume state was saved. All messages processed since the resume state
      will be reprocessed.
    * Replaying messages: If replay_messages is set to True, consumption
      will start at the first message in the station.
    
    Args:

        host: The hostname of the Memphis broker.

        username: The username of the Memphis account.

        password: The password of the Memphis account.

        station: The name of the Memphis station

        consumer_prefix: The prefix for the consumer name that will show up
                 in the Memphis UI.

        replay_messages: Start consuming from first message in the station

    """

    def __init__(self, host, username, password, station, consumer_prefix, replay_messages=False):
        self.host = host
        self.username = username
        self.password = password
        self.station = station
        self.consumer_prefix = consumer_prefix
        self.replay_messages = replay_messages

    def list_parts(self):
        """
        Only allowing one consumer to exist right now.
        When Memphis supports partitions, we will create
        a consumer group per partition.
        """

        return { "0" }

    def build_part(self, for_part, resume_state):
        start_consume_from_sequence = 1
        if resume_state is not None and not self.replay_messages:
            start_consume_from_sequence = resume_state

        return _MemphisConsumerSource(self.host,
                                      self.username,
                                      self.password,
                                      self.station,
                                      self.consumer_prefix + "_part" + for_part,
                                      start_consume_from_sequence)


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
        self._run(self._producer.produce(item))

    def close(self):
        self._run(self._producer.destroy())
        self._run(self._memphis.close())

class MemphisOutput(DynamicOutput):
    """
    Output to a Memphis.dev station.

    The following output formats are supported:

    * Bytearray
    * Dictionary (for modeling a JSON-like object)

    Currently, this output connector supports:
    * 1 producer per worker: Adding partitions to Memphis is ongoing work.
      When available, we will update the connector to support 1 consumer
      per station partition.
    * At-least once semantics: If the Bytewax flow is killed and restarted,
      it may replay some messages.  If so, those messages will be delivered
      to the station multiple times.  See the Memphis station settings 
      that catch the delivery of multiple messages to filter out duplicates.

    Args:

        host: The hostname of the Memphis broker.

        username: The username of the Memphis account.

        password: The password of the Memphis account.

        station: The name of the Memphis station

        producer_prefix: The prefix for the producer name that will show up
                 in the Memphis UI.
    """

    def __init__(self, host, username, password, station, producer_prefix):
        self.host = host
        self.username = username
        self.password = password
        self.station = station
        self.producer_prefix = producer_prefix

    def build(self, worker_index, worker_count):
        producer_name = self.producer_prefix + "-" + str(worker_index)
        return _MemphisProducerSink(self.host, self.username, self.password, self.station, producer_name)
