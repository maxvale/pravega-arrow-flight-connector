import asyncio
import json

import pyarrow as pa
import pyarrow.flight as fl
import pravega_client as pc

import data_processor
import pravega_reader


JSON_FILE = '..\\data\\test'

JSON_SCHEMA = pa.schema([('timestamp', pa.string()),
                         ('id', pa.string()),
                         ('data', pa.int32())])

HOST = '127.0.0.1:9090'


class FlightStream:

    def __init__(self, scope, stream, name, schema, host):
        self.stream = data_processor.ArrowStream(schema)
        self.reader = pravega_reader.PravegaReader(scope, stream, name, host)
        self.descriptor = fl.FlightDescriptor.for_path(str(scope + '/' + stream))

    def get_data(self) -> pa.RecordBatchStreamReader:
        return self.stream.read_stream()

    def put_data(self, segment, event_count):
        self.stream.write_segment(segment, event_count)

    def update_data(self, reader_name, timeout=None):
        if timeout is None:
            asyncio.run(self.reader.read_segment_async(reader_name))
        else:
            asyncio.wait_for(self.reader.read_segment_async(reader_name),
                             timeout=timeout)
        event_count = self.reader.get_count()
        if event_count == 0:
            print("There is no new data in pravega!")
            return
        buffer = self.reader.get_buffer()
        self.stream.write_segment(buffer, event_count)

    def get_descriptor(self):
        return self.descriptor

