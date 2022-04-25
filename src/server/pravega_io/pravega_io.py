import pravega_client as client
import pyarrow as pa

from src.server.location.location import Location
from src.server.pravega_io.consumer import Consumer
from src.server.pravega_io.producer import Producer


class PravegaIO:
    def __init__(self, location: Location, manager: client.StreamManager):
        self.location = location
        self.manager = manager
        self.consumer = Consumer(manager, location)
        self.producer = Producer(manager, location)

    def consume(self, client_id: str, buffer: pa.BufferOutputStream):
        self.consumer.consume(client_id, buffer)

    def produce(self, buffer: pa.BufferOutputStream, message_len: int):
        self.producer.produce(buffer, message_len)
