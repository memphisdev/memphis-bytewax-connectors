# Credit for The NATS.IO Authors
# Copyright 2021-2022 The Memphis Authors
# Licensed under the Apache License, Version 2.0 (the “License”);
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an “AS IS” BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
import copy
import json
import ssl
import uuid

import nats as broker

from .consumer import Consumer
from .exceptions import MemphisConnectError, MemphisError
from .producer import Producer
from .utils import get_internal_name, random_bytes


class Memphis:
    MAX_BATCH_SIZE = 5000
    MEMPHIS_GLOBAL_ACCOUNT_NAME = "$memphis"

    def __init__(self):
        self.is_connection_active = False
        self.schema_updates_data = {}
        self.schema_updates_subs = {}
        self.producers_per_station = {}
        self.schema_tasks = {}
        self.proto_msgs = {}
        self.graphql_schemas = {}
        self.json_schemas = {}
        self.cluster_configurations = {}
        self.station_schemaverse_to_dls = {}
        self.update_configurations_sub = {}
        self.configuration_tasks = {}
        self.producers_map = {}
        self.consumers_map = {}

    async def get_broker_manager_connection(self, connection_opts):
        if "user" in connection_opts:
            async def ping_error_cb(e):
                if "authorization violation" not in (str(e)).lower():
                    print(MemphisError(str(e)))

            async def error_cb(e):
                return

            ping_connection_opts = copy.deepcopy(connection_opts)
            ping_connection_opts["allow_reconnect"] = False
            ping_connection_opts["error_cb"] = ping_error_cb

            try:
                conn = await broker.connect(**ping_connection_opts)
                await conn.close()
            except Exception as e:
                if "authorization violation" in str(e).lower():
                    try:
                        if "localhost" in connection_opts['servers']: # for handling bad quality networks like port fwd
                            await asyncio.sleep(1)
                        ping_connection_opts["user"] = self.username
                        ping_connection_opts["error_cb"] = error_cb
                        conn = await broker.connect(**ping_connection_opts)
                        await conn.close()
                        connection_opts["user"] = self.username
                    except Exception as e_1:
                        raise e_1
                else:
                    raise e

        if "localhost" in connection_opts['servers']:
            await asyncio.sleep(1) # for handling bad quality networks like port fwd

        return await broker.connect(**connection_opts)

    async def connect(
        self,
        host: str,
        username: str,
        account_id: int = 1,
        connection_token: str = "",
        password: str = "",
        port: int = 6666,
        reconnect: bool = True,
        max_reconnect: int = 10,
        reconnect_interval_ms: int = 1500,
        timeout_ms: int = 2000,
        cert_file: str = "",
        key_file: str = "",
        ca_file: str = "",
    ):
        """Creates connection with Memphis.
        Args:
            host (str): memphis host.
            username (str): user of type root/application.
            account_id (int): You can find it on the profile page in the Memphis UI. This field should be sent only on the cloud version of Memphis, otherwise it will be ignored
            connection_token (str): connection token.
            password (str): depends on how Memphis deployed - default is connection token-based authentication.
            port (int, optional): port. Defaults to 6666.
            reconnect (bool, optional): whether to do reconnect while connection is lost. Defaults to True.
            max_reconnect (int, optional): The reconnect attempt. Defaults to 3.
            reconnect_interval_ms (int, optional): Interval in milliseconds between reconnect attempts. Defaults to 200.
            timeout_ms (int, optional): connection timeout in milliseconds. Defaults to 15000.
            key_file (string): path to tls key file.
            cert_file (string): path to tls cert file.
            ca_file (string): path to tls ca file.
        """
        self.host = self.__normalize_host(host)
        self.username = username
        self.account_id = account_id
        self.connection_token = connection_token
        self.password = password
        self.port = port
        self.reconnect = reconnect
        self.max_reconnect = 9 if max_reconnect > 9 else max_reconnect
        self.reconnect_interval_ms = reconnect_interval_ms
        self.timeout_ms = timeout_ms
        self.connection_id = str(uuid.uuid4())
        try:
            if self.connection_token != "" and self.password != "":
                raise MemphisConnectError(
                    "You have to connect with one of the following methods: connection token / password")
            if self.connection_token == "" and self.password == "":
                raise MemphisConnectError(
                    "You have to connect with one of the following methods: connection token / password")

            connection_opts = {
                "servers": self.host + ":" + str(self.port),
                "allow_reconnect": self.reconnect,
                "reconnect_time_wait": self.reconnect_interval_ms / 1000,
                "connect_timeout": self.timeout_ms / 1000,
                "max_reconnect_attempts": self.max_reconnect,
                "name": self.connection_id + "::" + self.username,
            }
            if cert_file != "" or key_file != "" or ca_file != "":
                if cert_file == "":
                    raise MemphisConnectError("Must provide a TLS cert file")
                if key_file == "":
                    raise MemphisConnectError("Must provide a TLS key file")
                if ca_file == "":
                    raise MemphisConnectError("Must provide a TLS ca file")
                ssl_ctx = ssl.create_default_context(
                    purpose=ssl.Purpose.SERVER_AUTH)
                ssl_ctx.load_verify_locations(ca_file)
                ssl_ctx.load_cert_chain(certfile=cert_file, keyfile=key_file)
                connection_opts["tls"] = ssl_ctx
                connection_opts["tls_hostname"] = self.host
            if self.connection_token != "":
                connection_opts["token"] = self.connection_token
            else:
                connection_opts["user"] = self.username + "$" + str(self.account_id)
                connection_opts["password"] = self.password

            self.broker_manager = await self.get_broker_manager_connection(connection_opts)
            self.broker_connection = self.broker_manager.jetstream()
            self.is_connection_active = True
        except Exception as e:
            raise MemphisError(str(e))

    async def close(self):
        """Close Memphis connection."""
        try:
            if self.is_connection_active:
                await self.broker_manager.close()
                self.connection_id = None
                self.is_connection_active = False
                keys_schema_updates_subs = self.schema_updates_subs.keys()
                self.configuration_tasks.cancel()
                for key in keys_schema_updates_subs:
                    sub = self.schema_updates_subs.get(key)
                    task = self.schema_tasks.get(key)
                    if key in self.schema_updates_data:
                        del self.schema_updates_data[key]
                    if key in self.schema_updates_subs:
                        del self.schema_updates_subs[key]
                    if key in self.producers_per_station:
                        del self.producers_per_station[key]
                    if key in self.schema_tasks:
                        del self.schema_tasks[key]
                    if task is not None:
                        task.cancel()
                    if sub is not None:
                        await sub.unsubscribe()
                if self.update_configurations_sub is not None:
                    await self.update_configurations_sub.unsubscribe()
                self.producers_map.clear()
                for consumer in self.consumers_map:
                    consumer.dls_messages.clear()
                self.consumers_map.clear()
        except Exception:
            return

    def __generate_random_suffix(self, name: str) -> str:
        return name + "_" + random_bytes(8)

    def __normalize_host(self, host):
        if host.startswith("http://"):
            return host.split("http://")[1]
        if host.startswith("https://"):
            return host.split("https://")[1]
        return host

    async def producer(
        self,
        station_name: str,
        producer_name: str,
        generate_random_suffix: bool = False,
    ):
        """Creates a producer.
        Args:
            station_name (str): station name to produce messages into.
            producer_name (str): name for the producer.
            generate_random_suffix (bool): false by default, if true concatenate a random suffix to producer's name
        Raises:
            Exception: _description_
        Returns:
            _type_: _description_
        """
        try:
            if not self.is_connection_active:
                raise MemphisError("Connection is dead")
            real_name = producer_name.lower()
            if generate_random_suffix:
                producer_name = self.__generate_random_suffix(producer_name)
            create_producer_req = {
                "name": producer_name,
                "station_name": station_name,
                "connection_id": self.connection_id,
                "producer_type": "application",
                "req_version": 1,
                "username": self.username
            }
            create_producer_req_bytes = json.dumps(create_producer_req, indent=2).encode(
                "utf-8"
            )
            create_res = await self.broker_manager.request(
                "$memphis_producer_creations", create_producer_req_bytes, timeout=5
            )
            create_res = create_res.data.decode("utf-8")
            create_res = json.loads(create_res)
            if create_res["error"] != "":
                raise MemphisError(create_res["error"])

            internal_station_name = get_internal_name(station_name)
            producer = Producer(self, producer_name, station_name, real_name)
            map_key = internal_station_name + "_" + real_name
            self.producers_map[map_key] = producer
            return producer

        except Exception as e:
            raise MemphisError(str(e)) from e

    async def consumer(
        self,
        station_name: str,
        consumer_name: str,
        consumer_group: str = "",
        pull_interval_ms: int = 1000,
        batch_size: int = 10,
        batch_max_time_to_wait_ms: int = 5000,
        max_ack_time_ms: int = 30000,
        max_msg_deliveries: int = 10,
        generate_random_suffix: bool = False,
        start_consume_from_sequence: int = 1,
        last_messages: int = -1,
    ):
        """Creates a consumer.
        Args:.
            station_name (str): station name to consume messages from.
            consumer_name (str): name for the consumer.
            consumer_group (str, optional): consumer group name. Defaults to the consumer name.
            pull_interval_ms (int, optional): interval in milliseconds between pulls. Defaults to 1000.
            batch_size (int, optional): pull batch size. Defaults to 10.
            batch_max_time_to_wait_ms (int, optional): max time in milliseconds to wait between pulls. Defaults to 5000.
            max_ack_time_ms (int, optional): max time for ack a message in milliseconds, in case a message not acked in this time period the Memphis broker will resend it. Defaults to 30000.
            max_msg_deliveries (int, optional): max number of message deliveries, by default is 10.
            generate_random_suffix (bool): false by default, if true concatenate a random suffix to consumer's name
            start_consume_from_sequence(int, optional): start consuming from a specific sequence. defaults to 1.
            last_messages: consume the last N messages, defaults to -1 (all messages in the station).
        Returns:
            object: consumer
        """
        try:
            if not self.is_connection_active:
                raise MemphisError("Connection is dead")
            if batch_size > self.MAX_BATCH_SIZE:
                raise MemphisError(
                    f"Batch size can not be greater than {self.MAX_BATCH_SIZE}")
            real_name = consumer_name.lower()
            if generate_random_suffix:
                consumer_name = self.__generate_random_suffix(consumer_name)
            cg = consumer_name if not consumer_group else consumer_group

            if start_consume_from_sequence <= 0:
                raise MemphisError(
                    "start_consume_from_sequence has to be a positive number"
                )

            if last_messages < -1:
                raise MemphisError("min value for last_messages is -1")

            if start_consume_from_sequence > 1 and last_messages > -1:
                raise MemphisError(
                    "Consumer creation options can't contain both start_consume_from_sequence and last_messages"
                )
            create_consumer_req = {
                "name": consumer_name,
                "station_name": station_name,
                "connection_id": self.connection_id,
                "consumer_type": "application",
                "consumers_group": consumer_group,
                "max_ack_time_ms": max_ack_time_ms,
                "max_msg_deliveries": max_msg_deliveries,
                "start_consume_from_sequence": start_consume_from_sequence,
                "last_messages": last_messages,
                "req_version": 1,
                "username": self.username
            }

            create_consumer_req_bytes = json.dumps(create_consumer_req, indent=2).encode(
                "utf-8"
            )
            err_msg = await self.broker_manager.request(
                "$memphis_consumer_creations", create_consumer_req_bytes, timeout=5
            )
            err_msg = err_msg.data.decode("utf-8")

            if err_msg != "":
                raise MemphisError(err_msg)

            internal_station_name = get_internal_name(station_name)
            map_key = internal_station_name + "_" + real_name
            consumer = Consumer(
                self,
                station_name,
                consumer_name,
                cg,
                pull_interval_ms,
                batch_size,
                batch_max_time_to_wait_ms,
                max_ack_time_ms,
                max_msg_deliveries,
                start_consume_from_sequence=start_consume_from_sequence,
                last_messages=last_messages,
            )
            self.consumers_map[map_key] = consumer
            return consumer
        except Exception as e:
            raise MemphisError(str(e)) from e
