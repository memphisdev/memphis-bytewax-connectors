<div align="center">
  
![Github (3)](https://github.com/memphisdev/memphis-bytewax-connectors/assets/107035359/0e100b97-a745-4615-99fa-c68ed044dc0f)  
</div>

<div align="center">

   <h4>

**[Memphis.dev](https://memphis.dev)** is a highly scalable, painless, and effortless data streaming platform.<br>
Made to enable developers and data teams to collaborate and build<br>
real-time and streaming apps fast.

  </h4>
  
<img width="177" alt="cloud_native 2 (5)" src="https://github.com/memphisdev/memphis-bytewax-connectors/assets/107035359/7b1e52d6-8bb5-4262-b1a1-c37930acb732">  
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
* At-least once semantics: If the Bytewax flow is killed and restarted,
  the connector will restart from the last messaged processed before the
  resume state was saved. All messages processed since the resume state
  will be reprocessed.
* Replaying messages: If replay_messages is set to True, consumption
  will start at the first message in the station.

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
$ pip install .
```

### Using within a Bytewax Flow
```python
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

### Testing At-Least Once Semantics (AOS) and Message Replay

1. Start an instance of Memphis or use Memphis Cloud
2. Create a virtual environment and install this package
3. Navigate to the examples directory
4. Run the producer script until there are several thousand messages in the `test-messages` station
   ```bash
   $ python test_producer.py
   ```
5. Create a recovery directory:
   ```bash
   $ mkdir recovery
   ```
6. Run the test flow:
   ```bash
   $ python -m bytewax.run --sqlite-directory recovery --epoch-interval 1 test_flow:flow
   ```
7. Kill the flow after a couple hundred messages by pressing Ctrl-C
8. Restart the flow.  You should see the message count be lower than when you killed the previous job
   (rolled back to the last snapshot).
   ```bash
   This is test message 6351. 2023-07-31T23:10:18.331608
   This is test message 6352. 2023-07-31T23:10:18.331733
   This is test message 6353. 2023-07-31T23:10:18.331857
   This is test message 6354. 2023-07-31T23:10:18.331978
   This is test message 6355. 2023-07-31T23:10:18.332104
   This is test message 6356. 2023-07-31T23:10:18.332226
   ^Cthread '<unnamed>' panicked at 'Box<dyn Any>', src/inputs.rs:227:35
   note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
   
   Traceback (most recent call last):
     File "<frozen runpy>", line 198, in _run_module_as_main
     File "<frozen runpy>", line 88, in _run_code
     File "/home/rjmemphis/projects/memphis-bytewax-connectors/venv/lib/python3.11/site-packages/bytewax-0.16.2-py3.11-linux-x86_64.egg/bytewax/run.py", line 431, in <module>
       cli_main(**kwargs)
   KeyboardInterrupt: (src/run.rs:145:17) interrupt signal received, all processes have been shut down
   
   $ python -m bytewax.run --sqlite-directory recovery --epoch-interval 1 test_flow:flow
   This is test message 4956. 2023-07-31T23:10:42.023572
   This is test message 4957. 2023-07-31T23:10:42.045039
   This is test message 4958. 2023-07-31T23:10:42.046310
   This is test message 4959. 2023-07-31T23:10:42.047550
   This is test message 4960. 2023-07-31T23:10:42.048545
   This is test message 4961. 2023-07-31T23:10:42.049397
   This is test message 4962. 2023-07-31T23:10:42.050126
   ```
9. To replay the messages from the beginning, set the `REPLAY_MESSAGES` environment variable:
   ```bash
   $ REPLAY_MESSAGES=1 python -m bytewax.run --sqlite-directory recovery --epoch-interval 1 test_flow:flow
   This is test message 0. 2023-07-31T23:13:27.491024
   This is test message 1. 2023-07-31T23:13:27.512143
   This is test message 2. 2023-07-31T23:13:27.513292
   This is test message 3. 2023-07-31T23:13:27.514117
   This is test message 4. 2023-07-31T23:13:27.515174
   This is test message 5. 2023-07-31T23:13:27.516098
   This is test message 6. 2023-07-31T23:13:27.517095
   This is test message 7. 2023-07-31T23:13:27.517969
   This is test message 8. 2023-07-31T23:13:27.518790
   This is test message 9. 2023-07-31T23:13:27.519489
   ```
