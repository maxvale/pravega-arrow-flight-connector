import asyncio

import pyarrow as pa
import pravega_client as pc
import json

import pravega_reader as pr

JSON_FILE = '..\\data\\test'

JSON_SCHEMA = pa.schema([('timestamp', pa.string()),
                         ('id',        pa.string()),
                         ('data',      pa.int32())])

HOST = '127.0.0.1:9090'


class ArrowData:

    def __init__(self, schema):
        self.schema = schema
        self.buffer = dict()
        self.recover_buffer()

    def add_data(self, name, data):
        for key in self.schema.names:
            if key == name:
                self.buffer[key].append(data[key])
                return

    def recover_buffer(self):
        self.buffer = dict()
        for key in self.schema.names:
            self.buffer[key] = list()

    def get_buffer(self):
        return self.buffer


class ArrowStream:

    def __init__(self, schema):
        self.event_count = 0
        self.arrow_data = ArrowData(schema)
        self.sink = pa.BufferOutputStream()
        self.writer = pa.ipc.new_stream(self.sink, schema)

    def write_segment(self, segment, event_count):
        reader = pa.BufferReader(segment)
        step = reader.size() // event_count
        start = reader.tell()
        while start < reader.size():
            data_fragment = reader.read(step)
            start = reader.tell()
            print('Position: ' + str(start))
            print('Data fragment: ' + str(data_fragment))
            self._process_fragment(data_fragment)

    def write_stream(self):
        batch = self._create_batch()
        self.writer.write_batch(batch)

    def read_stream(self):
        buffer = self.sink.getvalue()
        self.flush_stream()
        reader = pa.ipc.open_stream(buffer)
        return reader

    def flush_stream(self):
        self.arrow_data.recover_buffer()
        self.sink = pa.BufferOutputStream()
        self.writer = pa.ipc.new_stream(self.sink, self.arrow_data.schema)

    def _create_batch(self):
        data = list()
        for key in self.arrow_data.schema.names:
            data.append(pa.array(self.arrow_data.get_buffer()[key]))
        batch = pa.record_batch(data, names=self.arrow_data.schema)
        self.arrow_data.recover_buffer()
        return batch

    def _process_fragment(self, fragment):
        fragment_data = json.loads(fragment)
        for key in fragment_data:
            self.arrow_data.add_data(key, fragment_data)

    def get_stream(self):
        return self.sink

    def get_data(self):
        return self.arrow_data


def main():
    manager = pc.StreamManager(HOST, False, False)
    manager.create_scope('scope')
    manager.create_stream('scope', 'stream', 1)
    writer = manager.create_writer('scope', 'stream')

    for i in range(3):
        with open(str(JSON_FILE + str(i + 1) + '.json')) as json_file:
            json_obj = json.load(json_file)
        print(json_obj)
        str_ = json.dumps(json_obj)

        print(str_)
        str_ = bytes(str_, 'utf-8')
        print(str_)
        writer.write_event(str_)

    reader = pr.PravegaReader('scope', 'stream', 'rg1', HOST)
    reader.add_reader('rd1')
    asyncio.run(reader.read_segment_async('rd1'))
    num = reader.get_count()
    buf = reader.get_buffer()

    ar = ArrowStream(JSON_SCHEMA)
    ar.write_segment(buf, num)

    # sink = pa.BufferOutputStream()
    # sink.write(str_)
    # sink.write(str_)
    # buf = sink.getvalue()
    # ar = ArrowStream(JSON_SCHEMA)
    # ar.write_segment(buf, 2)
    # ar.write_stream()
    # reader = ar.read_stream()
    # print("First read stream: ")
    # print(reader.read_pandas())
    ar.write_stream()
    print("First test output: ")
    reader = ar.read_stream()
    batch_1 = reader.read_next_batch()
    print(batch_1)
    print(batch_1.to_pandas())

    # ar.write_segment(buf, 2)
    # ar.write_stream()
    # print("Third try: ")
    # reader = ar.read_stream()
    # print(reader.read_pandas())


if __name__ == '__main__':
    main()
