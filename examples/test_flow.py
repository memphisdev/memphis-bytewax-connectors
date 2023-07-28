import datetime as dt

from bytewax.dataflow import Dataflow
from bytewax.connectors.stdio import StdOutput

from memphis.connectors.bytewax import MemphisInput
from memphis.connectors.bytewax import MemphisOutput

memphis_src = MemphisInput("localhost",
                           "todocdcservice",
                           "%o3sH$Qfae",
                           "todo-cdc-events",
                           "bytewax")

memphis_sink = MemphisOutput("localhost",
                             "todocdcservice",
                             "%o3sH$Qfae",
                             "copied-events",
                             "bytewax")

flow = Dataflow()
flow.input("memphis-consumer", memphis_src)
flow.map(lambda m: m.decode("utf-8") + " " + dt.datetime.now().isoformat())
flow.output("out", StdOutput())
flow.output("memphis-producer", memphis_sink)
