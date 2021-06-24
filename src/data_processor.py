import pyarrow as pa
import json

JSON_FILE = '..\\data\\test.json'

JSON_SCHEMA = pa.schema([('timestamp', pa.string()),
                         ('id',        pa.string()),
                         ('data',      pa.string())])


class ArrowData:
    def __init__(self, schema):
        self.schema = schema
        self.buffer = dict()
        self.flush_buffer()

    def add_data(self, name, data):
        for key in self.schema.names:
            print(key)
            if key == name:
                self.buffer[key].append(data[key])

    def flush_buffer(self):
        self.buffer = dict()
        for key in self.schema.names:
            self.buffer[key] = list()

    def get_buffer(self):
        return self.buffer


class ArrowStream:

    def __init__(self, schema):
        self.event_count = 0
        self.arrow_data = ArrowData(schema)
        self.sink = pa.BufferOutputStream()
        self.writer = pa.ipc.new_stream(self.sink, JSON_SCHEMA)

    def process_segment(self, segment, event_count):
        reader = pa.BufferReader(segment)
        step = reader.size() // event_count
        start = reader.tell()
        while start < reader.size():
            data_fragment = reader.read(step)
            start = reader.tell()
            print('Position: ' + str(start))
            print('Data fragment: ' + str(data_fragment))
            self._process_fragment(data_fragment)

    def create_batch(self):
        data = list()
        for key in self.arrow_data.schema.names:
            data.append(pa.array(self.arrow_data.get_buffer()[key]))
        batch = pa.record_batch(data, names=self.arrow_data.schema)
        return batch

    def write_stream(self):
        batch = self.create_batch()
        self.writer.write_batch(batch)

    def _process_fragment(self, fragment):
        fragment_data = json.loads(fragment)
        for key in fragment_data:
            self.arrow_data.add_data(key, fragment_data)

    def get_stream(self):
        return self.sink

    def get_data(self):
        return self.arrow_data


def main():
    with open(JSON_FILE) as json_file:
        json_obj = json.load(json_file)
    print(json_obj)
    str_ = json.dumps(json_obj)

    print(str_)
    str_ = bytes(str_, 'utf-8')
    print(str_)
    sink = pa.BufferOutputStream()
    sink.write(str_)
    sink.write(str_)

    buf = sink.getvalue()
    ar = ArrowStream(JSON_SCHEMA)
    ar.process_segment(buf, 2)

    print('/////')
    ar.write_stream()
    stream = ar.get_stream()
    buf = stream.getvalue()
    reader = pa.ipc.open_stream(buf)
    print(reader.read_pandas())



if __name__ == '__main__':
    main()
