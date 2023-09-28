import datetime as dt
import os

from bytewax.dataflow import Dataflow
from bytewax.connectors.stdio import StdOutput

from memphis.connectors.bytewax import MemphisInput
from memphis.connectors.bytewax import MemphisOutput

memphis_src = MemphisInput("localhost",
                           "testuser",
                           "%o3sH$Qfae",
                           "test-messages",
                           "bytewax",
                           replay_messages = "REPLAY_MESSAGES" in os.environ)

memphis_sink = MemphisOutput("localhost",
                             "testuser",
                             "%o3sH$Qfae",
                             "copied-test-messages",
                             "bytewax")

flow = Dataflow()
flow.input("memphis-consumer", memphis_src)
flow.map(lambda m: m.decode("utf-8") + " " + dt.datetime.now().isoformat())
flow.map(lambda s: bytearray(s, "utf-8"))
flow.output("out", StdOutput())
flow.output("memphis-producer", memphis_sink)
