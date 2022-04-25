import pyarrow as pa


class Processor:
    def serialize(self, data: bytes):
        pass

    def deserialize(self, buffer: pa.Buffer) -> pa.BufferOutputStream:
        pass


class ArrowProcessor(Processor):
    def __init__(self, schema: pa.Schema):
        self._schema = schema

    def serialize(self, data: bytes):
        pass

    def deserialize(self, buffer: pa.Buffer) -> pa.BufferOutputStream:
        pass


class JSONProcessor(Processor):
    def __init__(self):
        pass

    def serialize(self, data: bytes):
        pass

    def deserialize(self, buffer: pa.Buffer) -> pa.BufferOutputStream:
        pass
