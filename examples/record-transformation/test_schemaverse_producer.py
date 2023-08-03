import asyncio
import datetime as dt
import json
import pprint
import random
import time

from memphis._internal import Memphis

STATION = "input-messages"
USERNAME = "testuser"
PASSWORD = "%o3sH$Qfae"
HOST = "localhost"
DELAY_SEC = 0.1

DESCRIPTION_LENGTH = 20
ASCII_START = 65 # uppercase A
ASCII_END = 90 # uppercase Z

def simulated_cdc_events():
    while True:
        # generate a todo item
        todo_item = {}
        creation_timestamp = dt.datetime.now()
        todo_item["creation_timestamp"] = creation_timestamp.isoformat()
        todo_item["due_date"] = None if random.random() >= 0.5 else creation_timestamp.date().isoformat()
        chars = [chr(random.randint(ASCII_START, ASCII_END))
                    for i in range(DESCRIPTION_LENGTH)]
        todo_item["description"] = "".join(chars)
        todo_item["completed"] = random.random() < 0.1

        # break the schema by deleting a key or changing to an unexpected type
        if random.random() < 0.25:
            key = random.choice(list(todo_item.keys()))
            if random.random() < 0.5:
                del todo_item[key]
            else:
                todo_item[key] = -5000

        cdc_event = {
            "schema" : None,
            "payload" : {
                "before" : json.dumps(todo_item), # simulated MongoDB-style CDC event
                "after" : None
            }
        }

        yield cdc_event

def main():
    try:
        loop = asyncio.get_event_loop()

        memphis = Memphis()
        loop.run_until_complete(memphis.connect(host=HOST, username=USERNAME, password=PASSWORD, account_id=1))

        producer = loop.run_until_complete(memphis.producer(station_name=STATION, producer_name="test-producer"))

        for msg in simulated_cdc_events():
            pprint.pprint(msg)
            output_str = json.dumps(msg)
            output_bytes = bytearray(output_str, "utf-8")

            loop.run_until_complete(producer.produce(message=output_bytes))

            time.sleep(DELAY_SEC)
            print()

    except Exception as e:
        print(e)

    finally:
        loop.run_until_complete(memphis.close())

if __name__ == "__main__":
    main()
