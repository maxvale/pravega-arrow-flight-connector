import asyncio

import pravega_reader as pr
import data_processor as dp
import pyarrow as pa
import pravega_client as pc

JSON_FILE = '..\\data\\test'

JSON_SCHEMA = pa.schema([('timestamp', pa.string()),
                         ('id', pa.string()),
                         ('data', pa.int32())])

HOST = '127.0.0.1:9090'


class Connector:
    def __init__(self):
        self.stream_dict = dict()
        self.reader_group_dict = dict()

    def add_stream(self, name, schema):
        if name not in self.stream_dict.keys():
            self.stream_dict[name] = dp.ArrowStream(schema)

    def add_reader(self, name, scope, stream, host):
        if name not in self.reader_group_dict.keys():
            self.reader_group_dict[name] = pr.PravegaReader(scope,
                                                            stream,
                                                            name,
                                                            host)

    def get_reader(self, name):
        return self.reader_group_dict[name]

    def get_stream(self, name):
        return self.stream_dict[name]


async def pravega_reading(buf):
    await asyncio.sleep(2)
    buf.write(b'Hello')
    await asyncio.sleep(4)
    buf.write(b'World!')


async def main():
    buf = pa.BufferOutputStream()
    try:
        await asyncio.wait_for(pravega_reading(buf),
                               timeout=5.0)
    except asyncio.TimeoutError:
        buf.close()

    if buf.closed:
        reader = buf.getvalue()
        print(reader.to_pybytes())


if __name__ == '__main__':
    asyncio.run(main())
