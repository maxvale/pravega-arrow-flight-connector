import pyarrow as pa

from src.server.pravega_io.pravega_io import PravegaIO
from src.server.processor.processor import Processor


class Stream:
    def __init__(self, manager, location, schema):
        self.pravega = PravegaIO(location, manager)
        self.processor = Processor()
        # self.stream =
        self.location = location

    def put_data_bytes(self, data):
        pass

    def get_data_bytes(self, client_id: str):
        buffer = self.pravega.consumer.consume(client_id)
        self.processor.serialize(buffer)
