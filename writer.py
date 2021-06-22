import pravega_client
import asyncio

class PravegaWriter:
    
    def __init__(self, scope, stream) -> None:
        self.manager = pravega_client.StreamManager("127.0.0.1:9090", False, False)
        self.scope = scope
        self.stream = stream

        self.manager.create_scope(self.scope)
        self.manager.create_stream(self.scope, self.stream, 1)

        self.writer = self.manager.create_writer(self.scope, self.stream)
        print("Writer created successfully")

    def writeEvent(self, event_data):
        self.writer.write_event(event_data)
        print("Data was writed successfully")


class PravegaReader:

    def __init__(self, scope, stream) -> None:
        self.manager = pravega_client.StreamManager("127.0.0.1:9090", False, False)
        self.scope = scope
        self.stream = stream

        self.reader_group = self.manager.create_reader_group("rgTest", self.scope, self.stream)
        self.reader = self.reader_group.create_reader("rg1r")

    async def readEvent(self) -> None:
        segment_slice = await self.reader.get_segment_slice_async()
        print(segment_slice)
        count = 0
        for event in segment_slice:
            count += 1
            print(event.data())
        self.reader.release_segment(segment_slice)
        self.reader.reader_offlane()

async def testFunc():
    manager = pravega_client.StreamManager("127.0.0.1:9090")
    manager.create_scope("TestScope")
    manager.create_stream("TestScope", "TestStream", 1)
    print("Scope and stream created!")

    writer = manager.create_writer("TestScope", "TestStream")
    writer.write_event("TestEvent1")
    writer.write_event("TestEvent2")
    print("Data was writed successfully")

    reader_group = manager.create_reader_group("rg1", "TestScope", "TestStream");
    reader1 = reader_group.create_reader("rdr1")
    slice = await reader1.get_segment_slice_async()
    print(slice)
    for event in slice:
        print(event.data())


if __name__ == '__main__':
    asyncio.run(testFunc())