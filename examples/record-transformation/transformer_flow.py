import datetime as dt
import json
import os

from bytewax.dataflow import Dataflow
from bytewax.connectors.stdio import StdOutput

from memphis.connectors.bytewax import MemphisInput
from memphis.connectors.bytewax import MemphisOutput

memphis_src = MemphisInput("localhost",
                           "testuser",
                           "%o3sH$Qfae",
                           "input-messages",
                           "bytewax",
                           replay_messages = "REPLAY_MESSAGES" in os.environ)

memphis_sink = MemphisOutput("localhost",
                             "testuser",
                             "%o3sH$Qfae",
                             "transformed-messages",
                             "bytewax")

def deserialize_inner_record(obj):
    """
    For MongoDB, Debezium returns the payload before and after fields as
    serialized JSON objects rather than subdocuments.  This function
    deserializes the JSON object, replaces the strings with JSON subdocuments,
    and then reserializes the entire message back to a bytearray.
    """

    if "payload" in obj:
        payload = obj["payload"]

        if "before" in payload:
            before_payload = payload["before"]
            if before_payload is not None:
                payload["before"] = json.loads(before_payload)

        if "after" in payload:
            after_payload = payload["after"]
            if after_payload is not None:
                payload["after"] = json.loads(after_payload)

    return obj

def is_valid_todo_item(obj):
    if obj is not None:
        if "creation_timestamp" not in obj:
            return False

        try:
            dt.datetime.fromisoformat(obj["creation_timestamp"])
        except Exception:
            return False

        if "due_date" not in obj:
            return False

        try:
            dt.date.fromisoformat(obj["due_date"])
        except Exception:
            if obj["due_date"] is not None:
                return False

        if "description" not in obj:
            return False

        if not isinstance(obj["description"], str):
            return False

        if "completed" not in obj:
            return False

        if not isinstance(obj["completed"], bool):
            return False

    return True


def is_valid_record(obj):
    if "payload" not in obj:
        return False

    payload = obj["payload"]

    if "before" not in payload:
        return False

    before_payload = payload["before"]

    if not is_valid_todo_item(before_payload):
        return False

    if "after" not in payload:
        return False

    after_payload = payload["after"]

    if not is_valid_todo_item(after_payload):
        return False

    return True

flow = Dataflow()
flow.input("memphis-consumer", memphis_src)

# bytearray to UTF-8 string
flow.map(lambda m: m.decode("utf-8"))

# deserialize JSON document
flow.map(json.loads)


# transform record
flow.map(deserialize_inner_record)

# filter out invalid records
flow.filter(is_valid_record)

# serialize JSON document
flow.map(json.dumps)

# convert to bytearray
flow.map(lambda s: bytearray(s, "utf-8"))

flow.output("out", StdOutput())
flow.output("memphis-producer", memphis_sink)
