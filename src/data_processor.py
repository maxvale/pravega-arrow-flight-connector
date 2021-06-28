import asyncio
import pyarrow as pa
import pravega_client as pc
import json

import pravega_reader as pr


class ArrowStream:

    def __init__(self, schema):
        self.event_count = 0
        self.schema = schema
        self.sink = pa.BufferOutputStream()
        self.writer = pa.ipc.new_stream(self.sink, schema)

    def write_segment(self, segment, event_count):
        if event_count == 0:
            return
        reader = pa.BufferReader(segment)
        step = reader.size() // event_count
        start = reader.tell()
        buffer = dict()
        while start < reader.size():
            data_fragment = reader.read(step)
            start = reader.tell()
            print('Position: ' + str(start))
            print('Data fragment: ' + str(data_fragment))
            self._process_fragment(data_fragment, buffer)
        batch = self._create_batch(buffer)
        self.writer.write_batch(batch)

    def read_stream(self):
        self.writer.close()
        buffer = self.sink.getvalue()
        self._flush_stream()
        reader = pa.ipc.open_stream(buffer)
        return reader

    def _flush_stream(self):
        self.sink = pa.BufferOutputStream()
        self.writer = pa.ipc.new_stream(self.sink, self.schema)

    def _process_fragment(self, fragment, buffer):
        fragment_data = json.loads(fragment)
        for key in fragment_data:
            if key in self.schema.names:
                if key not in buffer.keys():
                    buffer[key] = list()
                buffer[key].append(fragment_data[key])

    def _create_batch(self, buffer):
        data = list()
        for key in self.schema.names:
            data.append(pa.array(buffer[key]))
        batch = pa.record_batch(data, names=self.schema)
        return batch

