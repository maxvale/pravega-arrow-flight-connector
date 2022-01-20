import asyncio

import pyarrow as pa
import pyarrow.flight as fl

import data_processor
import pravegaIO


class FlightStream:

    def __init__(self, scope, stream, name, schema, host):
        self.stream = data_processor.ArrowStream(schema)
        self.IO = pravegaIO.PravegaIO(scope, stream, name, host)
        self.descriptor = fl.FlightDescriptor.for_path(str(scope + '/' + stream))

    def get_data(self) -> pa.RecordBatchStreamReader:
        return self.stream.read_stream()

    def put_data(self, segment, event_count):
        self.stream.write_segment(segment, event_count)

    def update_data(self, reader_name, timeout=None):
        if timeout is None:
            asyncio.run(self.IO.read_segment_async(reader_name))
        else:
            asyncio.wait_for(self.IO.read_segment_async(reader_name),
                             timeout=timeout)
        event_count = self.IO.get_count()
        if event_count == 0:
            print("There is no new data in pravega!")
            return
        buffer = self.IO.get_buffer()
        self.stream.write_segment(buffer, event_count)

    def get_descriptor(self):
        return self.descriptor

    def read_write(self, json_obj):
        self.IO.read_write(json_obj)

    def write(self, path):
        self.IO.write_event(path)

