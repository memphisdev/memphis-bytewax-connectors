<div align="center">
  
  ![Banner- Memphis dev streaming  (2)](https://github.com/memphisdev/memphis.py/assets/107035359/6787500c-d806-4f22-96aa-a182d4c24dfa)
  
</div>

<div align="center">

  <h4>

**[Memphis](https://memphis.dev)** is an intelligent, frictionless message broker.<br>Made to enable developers to build real-time and streaming apps fast.

  </h4>
  
  <a href="https://landscape.cncf.io/?selected=memphis"><img width="200" alt="CNCF Silver Member" src="https://github.com/cncf/artwork/raw/master/other/cncf-member/silver/white/cncf-member-silver-white.svg#gh-dark-mode-only"></a>
  
</div>

<div align="center">
  
  <img width="200" alt="CNCF Silver Member" src="https://github.com/cncf/artwork/raw/master/other/cncf-member/silver/color/cncf-member-silver-color.svg#gh-light-mode-only">
  
</div>
 
 <p align="center">
  <a href="https://memphis.dev/pricing/">Cloud</a> - <a href="https://memphis.dev/docs/">Docs</a> - <a href="https://twitter.com/Memphis_Dev">Twitter</a> - <a href="https://www.youtube.com/channel/UCVdMDLCSxXOqtgrBaRUHKKg">YouTube</a>
</p>

<p align="center">
<a href="https://discord.gg/WZpysvAeTf"><img src="https://img.shields.io/discord/963333392844328961?color=6557ff&label=discord" alt="Discord"></a>
<a href="https://github.com/memphisdev/memphis/issues?q=is%3Aissue+is%3Aclosed"><img src="https://img.shields.io/github/issues-closed/memphisdev/memphis?color=6557ff"></a> 
  <img src="https://img.shields.io/npm/dw/memphis-dev?color=ffc633&label=installations">
<a href="https://github.com/memphisdev/memphis/blob/master/CODE_OF_CONDUCT.md"><img src="https://img.shields.io/badge/Code%20of%20Conduct-v1.0-ff69b4.svg?color=ffc633" alt="Code Of Conduct"></a> 
<a href="https://docs.memphis.dev/memphis/release-notes/releases/v0.4.2-beta"><img alt="GitHub release (latest by date)" src="https://img.shields.io/github/v/release/memphisdev/memphis?color=61dfc6"></a>
<img src="https://img.shields.io/github/last-commit/memphisdev/memphis?color=61dfc6&label=last%20commit">
</p>

# memphis-bytewax-connectors
This library provides connectors for using the [Memphis.dev](https://memphis.dev) event
streaming platform as an input source and output sink with the [Bytewax](https://bytewax.io)
streaming processing engine.

## Features

Currently, the input connector supports:
* 1 consumer per station: Adding partitions to Memphis is ongoing work.
  When available, we will update the connector to support 1 consumer
  per station partition.
* At-most once semantics: Memphis messages are acknwoledged as they are
  recieved.  If the Bytewax flow is killed and restarted, the connector
  will restart from the next unacknowledged message.  This can cause cases
  where messages are not processed. We expect at-least once semantics
  to be available soon.
* Replaying messages: If replay_events is set to True, any previous
  consumer will be destroyed and a new consumer that starts at the beginning
  of the stream will be created.

Currently, the output connector supports:
* 1 producer per worker: Adding partitions to Memphis is ongoing work.
  When available, we will update the connector to support 1 consumer
  per station partition.
* At-least once semantics: If the Bytewax flow is killed and restarted,
  it may replay some messages.  If so, those messages will be delivered
  to the station multiple times.  See the Memphis station settings
  that catch the delivery of multiple messages to filter out duplicates.

## Usage


### Installing

```bash
$ python3 -m venv venv
$ source venv/bin/activate
$ pip install -U pip wheel
$ python setup.py install
```

### Using within a Bytewax Flow
```python
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
```

### Running
The resulting flow can be run with:

```bash
$ python3 -m bytewax.run module:flow
```

For more details on options, run:

```bash
$ python3 -m bytewax.run -h
```
 
