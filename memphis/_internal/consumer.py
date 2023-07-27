from __future__ import annotations

import asyncio
import json

from .exceptions import MemphisError
from .utils import default_error_handler, get_internal_name
from .message import Message


class Consumer:
    MAX_BATCH_SIZE = 5000

    def __init__(
        self,
        connection,
        station_name: str,
        consumer_name,
        consumer_group,
        pull_interval_ms: int,
        batch_size: int,
        batch_max_time_to_wait_ms: int,
        max_ack_time_ms: int,
        max_msg_deliveries: int = 10,
        error_callback=None,
        start_consume_from_sequence: int = 1,
        last_messages: int = -1,
    ):
        self.connection = connection
        self.station_name = station_name.lower()
        self.consumer_name = consumer_name.lower()
        self.consumer_group = consumer_group.lower()
        self.pull_interval_ms = pull_interval_ms
        self.batch_size = batch_size
        self.batch_max_time_to_wait_ms = batch_max_time_to_wait_ms
        self.max_ack_time_ms = max_ack_time_ms
        self.max_msg_deliveries = max_msg_deliveries
        self.ping_consumer_interval_ms = 30000
        if error_callback is None:
            error_callback = default_error_handler
        self.start_consume_from_sequence = start_consume_from_sequence
        self.last_messages = last_messages
        self.context = {}
        self.dls_messages = []
        self.dls_current_index = 0
        self.dls_callback_func = None
        self.t_consume = None

    def set_context(self, context):
        """Set a context (dict) that will be passed to each message handler call."""
        self.context = context

    async def fetch(self, batch_size: int = 10):
        """
        Fetch a batch of messages.

        Returns a list of Message objects. If the connection is
        not active or no messages are recieved before timing out,
        an empty list is returned.

        Example:

            import asyncio
            
            from memphis import Memphis

            async def main(host, username, password, station):
                memphis = Memphis()
                await memphis.connect(host=host,
                                      username=username,
                                      password=password)
            
                consumer = await memphis.consumer(station_name=station,
                                                  consumer_name="test-consumer",
                                                  consumer_group="test-consumer-group")
            
                while True:
                    batch = await consumer.fetch()
                    print("Recieved {} messages".format(len(batch)))
                    for msg in batch:
                        serialized_record = msg.get_data()
                        print("Message:", serialized_record)
            
                await memphis.close()

            if __name__ == '__main__':
                asyncio.run(main(host,
                                 username,
                                 password,
                                 station))
        
        """
        messages = []
        if self.connection.is_connection_active:
            try:
                if batch_size > self.MAX_BATCH_SIZE:
                    raise MemphisError(
                        f"Batch size can not be greater than {self.MAX_BATCH_SIZE}")
                self.batch_size = batch_size
                if len(self.dls_messages) > 0:
                    if len(self.dls_messages) <= batch_size:
                        messages = self.dls_messages
                        self.dls_messages = []
                        self.dls_current_index = 0
                    else:
                        messages = self.dls_messages[0:batch_size]
                        del self.dls_messages[0:batch_size]
                        self.dls_current_index -= len(messages)
                    return messages

                durable_name = ""
                if self.consumer_group != "":
                    durable_name = get_internal_name(self.consumer_group)
                else:
                    durable_name = get_internal_name(self.consumer_name)
                subject = get_internal_name(self.station_name)
                self.psub = await self.connection.broker_connection.pull_subscribe(
                    subject + ".final", durable=durable_name
                )
                msgs = await self.psub.fetch(batch_size)
                for msg in msgs:
                    messages.append(
                        Message(msg, self.connection, self.consumer_group))
                return messages
            except Exception as e:
                if "timeout" not in str(e).lower():
                    raise MemphisError(str(e)) from e

        return messages


    async def destroy(self):
        """Destroy the consumer."""
        self.pull_interval_ms = None
        try:
            destroy_consumer_req = {
                "name": self.consumer_name,
                "station_name": self.station_name,
                "username": self.connection.username,
                "connection_id": self.connection.connection_id,
                "req_version": 1,
            }
            consumer_name = json.dumps(
                destroy_consumer_req, indent=2).encode("utf-8")
            res = await self.connection.broker_manager.request(
                "$memphis_consumer_destructions", consumer_name, timeout=5
            )
            error = res.data.decode("utf-8")
            if error != "" and not "not exist" in error:
                raise MemphisError(error)
            internal_station_name = get_internal_name(self.station_name)
            map_key = internal_station_name + "_" + self.consumer_name.lower()
            del self.connection.consumers_map[map_key]
        except Exception as e:
            raise MemphisError(str(e)) from e
