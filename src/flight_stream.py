import asyncio
import json

import pyarrow as pa
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
        #self.reader = pravega_reader.PravegaReader(scope, stream, name, host)

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

    def test_update_data(self):
        stream = pa.BufferOutputStream()
        for i in range(3):
            with open(str(JSON_FILE + str(i + 1) + '.json')) as json_file:
                json_obj = json.load(json_file)
            str_ = json.dumps(json_obj)
            str_ = bytes(str_, 'utf-8')
            print(str_)
            stream.write(str_)
        buf = stream.getvalue()
        self.stream.write_segment(buf, 3)


def main():
    manager = pc.StreamManager(HOST, False, False)
    manager.create_scope('scope')
    manager.create_stream('scope', 'stream', 1)
    writer = manager.create_writer('scope', 'stream')

    for i in range(3):
        with open(str(JSON_FILE + str(i + 1) + '.json')) as json_file:
            json_obj = json.load(json_file)
        str_ = json.dumps(json_obj)

        print(str_)
        str_ = bytes(str_, 'utf-8')
        print(str_)
        writer.write_event_bytes(str_)

    flight = FlightStream('scope', 'stream', 'rg1', JSON_SCHEMA, HOST)
    flight.update_data('rd1')


if __name__ == '__main__':
    main()
