# Pravega-arrow-flight-connector

## Requirements
- Python v3.9.x
- Pravega v0.9.0
- Pravega client v0.2.0
- Arrow v4.0.1

## Run
1. Start pravega-standalone
```
 ./pravega-0.9.0/bin/pravega-standalone
```
2. Start server that connects to pravega and create arrow stream
```
python ./pravega-arrow-flight-connector/src/flight_server.py
```
3. Start client
```
python ./pravega-arrow-flight-connector/src/flight_client.py
```
