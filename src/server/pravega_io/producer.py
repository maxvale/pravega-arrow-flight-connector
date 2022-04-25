import logging
from typing import Callable

import pravega_client as pc
import pyarrow as pa

from src.server.location.location import Location


class Producer:
    def __init__(self, manager: pc.StreamManager, location: Location):
        self.manager = manager
        self.location = location
        self.writer = manager.create_writer(location.get_scope(), location.get_stream())

    def produce(self, buffer: pa.Buffer, message_len: int, deserialize_func: Callable):
        stream = pa.BufferReader(buffer)
        for i in range(0, stream.size(), message_len):
            data = stream.read(message_len)
            event = deserialize_func(data)
            logging.info("Writing event: " + str(event))
            self.writer.write_event_bytes(event)
        stream.close()
