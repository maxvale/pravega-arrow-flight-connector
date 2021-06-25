import pravega_client as pc
import pyarrow as pa


JSON_FILE = '../data/test1.json'


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
        for item in segment_slice:
            print('Reading data: ' + str(item.data()))
            self.buffer.write(item.data())
            self.event_count += 1
        reader.release_segment(segment_slice)
        reader.reader_offline()
        del self.readers[name]
        self.buffer.close()

    def is_checked(self):
        return self.buffer.closed

    def get_buffer(self):
        value = self.buffer.getvalue()
        self._flush_buffer()
        return value

    def get_count(self):
        return self.event_count

    def _flush_buffer(self):
        self.buffer = pa.BufferOutputStream()
        self.event_count = 0
