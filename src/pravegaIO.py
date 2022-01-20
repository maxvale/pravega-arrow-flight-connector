import json
from datetime import datetime
import random

import pravega_client as pc
import pyarrow as pa


JSON_FILE = '../data/test1.json'


class PravegaIO:

    def __init__(self, scope, stream, name, host, segment_count=1):
        self.scope = scope
        self.stream = stream
        self.name = name
        self.readers = dict()
        self.segment_count = segment_count
        self.buffer = pa.BufferOutputStream()
        self.event_count = 0

        manager = pc.StreamManager(host)
        self.reader_group = manager.create_reader_group(self.name,
                                                        self.scope,
                                                        self.stream)

        self.writer = manager.create_writer(self.scope, self.stream)

    def add_reader(self, name):
        self.readers[name] = self.reader_group.create_reader(name)
        
    async def read_segment_async(self, name):
        self.add_reader('rd1')
        reader = self.readers[name]
        segment_slice = await reader.get_segment_slice_async()
        for item in segment_slice:
            length = len(item.data())
            print('Reading event' + str(self.event_count) + ': '
                  + str(item.data()) + ' len: ' + str(length))
            length = length.to_bytes(1, byteorder='big')
            self.buffer.write(length + item.data())
            self.event_count += 1
        reader.release_segment(segment_slice)
        reader.reader_offline()
        del self.readers[name]
        self.buffer.close()

    def get_buffer(self):
        value = self.buffer.getvalue()
        self._flush_buffer()
        return value

    def get_count(self):
        return self.event_count

    def _flush_buffer(self):
        self.buffer = pa.BufferOutputStream()
        self.event_count = 0

    def read_write(self, json_obj):
        for p in json_obj:
                str_ = json.dumps(p)
                str_ = bytes(str_, 'utf-8')
                self.writer.write_event_bytes(str_)


    def write_event(self, path):
        with open(path, 'r') as json_file:
            json_obj = json.load(json_file)
        for p in json_obj:
            str_ = json.dumps(p)
            str_ = bytes(str_, 'utf-8')
            self.writer.write_event_bytes(str_)
