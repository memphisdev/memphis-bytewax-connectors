from __future__ import annotations
import asyncio
from memphis._internal import Memphis

STATION = "test-messages"
USERNAME = "testuser"
PASSWORD = "%o3sH$Qfae"
HOST = "localhost"

def main():
    loop = asyncio.get_event_loop()
    try:
        print("Waiting on messages...")
        memphis = Memphis()
        loop.run_until_complete(memphis.connect(host=HOST, username=USERNAME, password=PASSWORD))

        partitions = [1, 2, 3, 4]
        consumer = loop.run_until_complete(memphis.consumer(station_name=STATION,
                                                            consumer_name="test-consumer",
                                                            partitions=partitions,
                                                            consumer_group=""))
        while True:
            msgs = loop.run_until_complete(consumer.fetch())
            if msgs is not None:
                for msg in msgs:
                    print("message: ", msg.get_data())
                    loop.run_until_complete(msg.ack())

    except Exception as e:
        print(e)

    finally:
        loop.run_until_complete(memphis.close())

if __name__ == '__main__':
    main()
