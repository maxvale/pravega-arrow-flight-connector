import json

import pravega_client as pc
import pyarrow as pa


JSON_FILE = '..\\data\\test.json'


class PravegaReader:
    def __init__(self, scope, stream, name, host):
        self.scope = scope
        self.stream = stream
        self.name = name
        self.readers = dict()
        self.buffer = pa.BufferOutputStream()
        self.event_count = 0

        manager = pc.StreamManager(host)
        self.reader_group = manager.create_reader_group(self.name,
                                                        self.scope,
                                                        self.stream)

    def add_reader(self, name):
        self.readers[name] = self.reader_group.create_reader(name)
        
    async def read_segment_async(self, name):
        reader = self.readers[name]
        segment_slice = await reader.get_segment_slice_async()
        print(segment_slice)
        for item in segment_slice:
            print(item.data())
            self.buffer.write(item.data())
            self.event_count += 1
        reader.release_segment(segment_slice)
        reader.reader_offline()
        del self.readers[name]

    def get_buffer(self):
        return self.buffer.getvalue()

    def flush_buffer(self):
        self.buffer.flush()
        self.event_count = 0

    def test_write_data(self):
        with open(JSON_FILE) as json_file:
            json_obj = json.load(json_file)
        print(json_obj)
        data_string = json.dumps(json_obj)
        print(data_string)
        data_string = bytes(data_string, 'utf-8')
        data = data_string + data_string
        self.buffer.write(data)
        self.event_count = 2
