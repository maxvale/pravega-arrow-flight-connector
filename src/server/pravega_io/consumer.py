import asyncio
import logging
import uuid
from typing import Callable

import pravega_client as pc
import pyarrow as pa

from src.server.location.location import Location
from src.server.processor.processor import Processor


class Consumer:
    def __init__(self, manager: pc.StreamManager, location: Location):
        self.reader_groups = dict()
        self.manager = manager
        self.location = location

    def consume(self, client_id: str, serialize_func: Callable) -> pa.Buffer:
        buffer = pa.BufferOutputStream()
        reader_group = self.reader_groups.get(client_id)
        if reader_group is None:
            reader_group = self._add_consumer_group(client_id)

        reader = self._create_reader(reader_group)
        asyncio.run(self._read_new_segment(reader, buffer, serialize_func))
        buffer.close()
        reader.reader_offline()
        return buffer.getvalue()

    def _create_reader(self, reader_group: pc.StreamReaderGroup) -> pc.StreamReader:
        reader_id = uuid.uuid1()
        reader = reader_group.create_reader("reader" + str(reader_id))
        return reader

    async def _read_new_segment(self, reader: pc.StreamReader, buffer: pa.BufferOutputStream, serialize: Callable):
        segment_slice = await reader.get_segment_slice_async()
        for item in segment_slice:
            logging.info("Reading event: " + str(item))
            data = serialize(item)
            buffer.write(data)  # TODO:!
        reader.release_segment()

    def _add_consumer_group(self, client_id) -> pc.StreamReaderGroup:
        new_reader_group = self.manager.create_reader_group(
            client_id,
            self.location.get_scope(),
            self.location.get_stream()
        )
        self.reader_groups[client_id] = new_reader_group
        return self.reader_groups[client_id]

    def _ping_stream(self):
        pass
