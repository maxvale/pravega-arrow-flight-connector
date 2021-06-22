import pravega_client as pc
import pyarrow as pa


class PravegaReader:
    def __init__(self, scope, stream, name, host):
        self.scope = scope
        self.stream = stream
        self.name = name
        self.readers = dict()
        self.buffer = list()

        manager = pc.StreamManager(host)
        self.reader_group = manager.create_reader_group(self.name, self.scope, self.stream)

    def add_reader(self, name):
        self.readers[name] = self.reader_group.create_reader(name)
        
    async def read_event_async(self, name):
        reader = self.readers[name]
        segment_slice = await reader.get_segment_slice_async()
        print(segment_slice)
        for item in segment_slice:
            print(item.data())
            self.buffer.append(item.data())
        reader.release_segment(segment_slice)
        reader.reader_offline()
        del self.readers[name]

    def get_buffer(self):
        result = self.buffer
        self.buffer = list()
        return result
