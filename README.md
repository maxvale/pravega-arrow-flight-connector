# Pravega-arrow-flight-connector

## Requirements
- Python v3.9.x
- Pravega v0.9.0
- Pravega client v0.2.0
- Arrow v4.0.1
- Ubuntu v18

## Installation
1. Install Python3.9
```
 sudo apt install python3.9
 python3.9 -m pip --version
```

If there is any issue with pip in python3.9, then install pip
```
 curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
 python3.9 get-pip.py
```

2. Install PyArrow
```
 python3.9 -m pip install pyarrow==4.0.1
```

Check pyarrow via
```
 python3.9 -m pip list
```

3. Download Pravega and pravega_client

Pravega_client for python
```
 python3.9 -m pip install pravega==0.2.0
```

Install pravega using *[quick start guide](https://github.com/pravega/pravega/blob/master/documentation/src/docs/getting-started/quick-start.md)* & check it:

## Run
1. Start pravega-standalone
```
 ./pravega-0.9.0/bin/pravega-standalone
```
2. Start server that connects to pravega and create arrow stream
```
python3.9 ./pravega-arrow-flight-connector/src/flight_server.py
```
3. Start client
```
python3.9 ./pravega-arrow-flight-connector/src/flight_client.py
```

## Useful links
- [Pravega docs](https://pravega.io/docs/nightly/pravega-concepts/)
- [Arrow introduce article](https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/)
- [Tools powered by apache arrow](https://arrow.apache.org/powered_by/)
- [PyArrow lib docs](https://arrow.apache.org/docs/python/api/memory.html)
- [Simple Flight client example](https://github.com/apache/arrow/blob/master/python/examples/flight/client.py)

