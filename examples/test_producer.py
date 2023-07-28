from __future__ import annotations
import asyncio
import time

from memphis._internal import Memphis

STATION = "todo-cdc-events"
USERNAME = "todocdcservice"
PASSWORD = "%o3sH$Qfae"
HOST = "localhost"

def main():
    loop = asyncio.get_event_loop()
    try:
        memphis = Memphis()
        loop.run_until_complete(memphis.connect(host=HOST, username=USERNAME, password=PASSWORD, account_id=1))

        producer = loop.run_until_complete(memphis.producer(station_name=STATION, producer_name="test-producer"))
        msg_id = 0
        while True:
            msg = "This is test message {}.".format(msg_id)
            print("Sending message: {}".format(msg))
            loop.run_until_complete(producer.produce(bytearray(msg, "utf-8")))
            time.sleep(0.5)
            msg_id += 1

    except Exception as e:
        print(e)

    finally:
        loop.run_until_complete(memphis.close())

if __name__ == "__main__":
    main()