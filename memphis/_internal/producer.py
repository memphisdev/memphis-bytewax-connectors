from __future__ import annotations

import asyncio
import json
import time
from typing import Union

from .exceptions import MemphisError
from .headers import Headers
from .utils import get_internal_name

schemaverse_fail_alert_type = "schema_validation_fail_alert"


class Producer:
    def __init__(
        self, connection, producer_name: str, station_name: str, real_name: str
    ):
        self.connection = connection
        self.producer_name = producer_name.lower()
        self.station_name = station_name
        self.internal_station_name = get_internal_name(self.station_name)
        self.loop = asyncio.get_running_loop()
        self.real_name = real_name

    async def produce(
        self,
        message,
        ack_wait_sec: int = 15,
        headers: Union[Headers, None] = None,
        async_produce: bool = False,
        msg_id: Union[str, None] = None,
    ):
        """Produces a message into a station.
        Args:
            message (bytearray/dict): message to send into the station - bytearray/protobuf class (schema validated station - protobuf) or bytearray/dict (schema validated station - json schema) or string/bytearray/graphql.language.ast.DocumentNode (schema validated station - graphql schema)
            ack_wait_sec (int, optional): max time in seconds to wait for an ack from memphis. Defaults to 15.
            headers (dict, optional): Message headers, defaults to {}.
            async_produce (boolean, optional): produce operation won't wait for broker acknowledgement
            msg_id (string, optional): Attach msg-id header to the message in order to achieve idempotency
        Raises:
            Exception: _description_
        """
        try:
            memphis_headers = {
                "$memphis_producedBy": self.producer_name,
                "$memphis_connectionId": self.connection.connection_id,
            }

            if msg_id is not None and msg_id != "":
                memphis_headers["msg-id"] = msg_id

            if headers is not None:
                headers = headers.headers
                headers.update(memphis_headers)
            else:
                headers = memphis_headers

            await self.connection.broker_connection.publish(
                self.internal_station_name + ".final",
                message,
                timeout=ack_wait_sec,
                headers=headers,
            )
        except Exception as e: # pylint: disable-next=no-member
            if hasattr(e, "status_code") and e.status_code == "503":
                raise MemphisError(
                    "Produce operation has failed, please check whether Station/Producer still exist"
                )
            raise MemphisError(str(e)) from e

    async def destroy(self):
        """Destroy the producer."""
        try:
            destroy_producer_req = {
                "name": self.producer_name,
                "station_name": self.station_name,
                "username": self.connection.username,
                "connection_id": self.connection.connection_id,
                "req_version": 1,
            }

            producer_name = json.dumps(destroy_producer_req).encode("utf-8")
            res = await self.connection.broker_manager.request(
                "$memphis_producer_destructions", producer_name, timeout=5
            )
            error = res.data.decode("utf-8")
            if error != "" and not "not exist" in error:
                raise Exception(error)

            internal_station_name = get_internal_name(self.station_name)

            map_key = internal_station_name + "_" + self.real_name
            del self.connection.producers_map[map_key]

        except Exception as e:
            raise Exception(e)
