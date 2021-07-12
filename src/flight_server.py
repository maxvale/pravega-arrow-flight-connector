import ast
import threading
import time
import json

import pyarrow as pa
import pyarrow.flight as fl
import pravega_client as pc

import flight_stream


JSON_FILE = '../data/test'

JSON_SCHEMA = pa.schema([('timestamp', pa.string()),
                         ('id', pa.string()),
                         ('data', pa.int32())])

HOST = '127.0.0.1:9090'


class FlightServer(fl.FlightServerBase):
    
    def __init__(self, host, location=None, tls_cert=None,
                 verify_cli=False, root_cert=None, auth_hand=None):
        super(FlightServer, self).__init__(
            location, auth_hand, tls_cert, verify_cli, root_cert)
        self.stream_list = list()
        self.flights = dict()
        self.host = host

    def add_new_data_stream(self, stream):
        self.stream_list.append(stream)

    def update_flight_table(self, descriptor, reader):
        key = self._descriptor_to_key(descriptor)
        table = reader.read_all()
        if key not in self.flights.keys():
            self.flights[key] = table
            return
        tables = [self.flights[key], table]
        self.flights[key] = pa.concat_tables(tables)

    def update_all_tables(self):
        for stream in self.stream_list:
            stream.update_data('rd1')
            reader = stream.get_data()
            descriptor = stream.get_descriptor()
            self.update_flight_table(descriptor, reader)

    def get_stream_list(self):
        return self.stream_list

    def list_flights(self, context, criteria):
        for key, stream in self.flights.items():
            if key[1] is not None:
                descriptor = fl.FlightDescriptor.for_command(key[1])
            else:
                descriptor = fl.FlightDescriptor.for_path(key[2])
            table = self.flights[key]
            yield self._make_flight_info(key, descriptor, table)

    def get_flight_info(self, context, descriptor):
        key = self._descriptor_to_key(descriptor)
        table = self.flights[key]
        return self._make_flight_info(key, descriptor, table)

    def do_get(self, context, ticket):
        key = ast.literal_eval(ticket.ticket.decode())
        return fl.RecordBatchStream(self.flights[key])

    def do_put(self, context, descriptor, reader, writer):
        key = self._descriptor_to_key(descriptor)
        self.flights[key] = reader.read_all()

    def list_actions(self, context):
        return [
            ("shutdown", "Shutdown"),
            ("healthcheck", "Check availability"),
            ("update", "Update data of all flights")
        ]

    # TODO: THINK ABOUT LOGIC AND EXTRA ACTIONS

    def do_action(self, context, action):
        if action.type == "shutdown":
            yield fl.Result(pa.py_buffer(b'Shutdown!'))
            threading.Thread(target=self._shutdown).start()
        elif action.type == "healthcheck":
            yield fl.Result(pa.py_buffer(b'I am good! What about you?'))
        elif action.type == "update":
            yield fl.Result(pa.py_buffer(b'Every stream was updated'))
            self.update_all_tables()
        else:
            raise KeyError("Unknown action {!r}".format(action.type))

    @classmethod
    def _descriptor_to_key(self, descriptor):
        return(descriptor.descriptor_type.value,
               descriptor.command,
               *tuple(descriptor.path or tuple()))

    def _make_flight_info(self, key, descriptor, table):
        location = fl.Location.for_grpc_tcp(self.host, self.port)
        endpoints = [fl.FlightEndpoint(repr(key), [location]), ]
        mock_sink = pa.MockOutputStream()
        stream_writer = pa.RecordBatchStreamWriter(mock_sink, table.schema)
        stream_writer.write_table(table)
        stream_writer.close()
        data_size = mock_sink.size()
        return fl.FlightInfo(table.schema, descriptor, endpoints,
                             table.num_rows, data_size)

    def _shutdown(self):
        print("Shutting down!")
        time.sleep(2)
        self.shutdown()


def write_test_data():
    manager = pc.StreamManager(HOST, False, False)
    manager.create_scope('scope')
    manager.create_stream('scope', 'stream', 1)
    writer = manager.create_writer('scope', 'stream')

    for i in range(3):
        with open(str(JSON_FILE + str(i + 1) + '.json')) as json_file:
            json_obj = json.load(json_file)
        str_ = json.dumps(json_obj)

        print(str_)
        str_ = bytes(str_, 'utf-8')
        print(str_)
        writer.write_event_bytes(str_)


def main():
#    write_test_data()

    location = "{}://{}:{}".format('grpc+tcp', 'localhost', '8080')
    server = FlightServer('localhost', location=location)
    stream = flight_stream.FlightStream('scope', 'stream', 'rg1',
                                        JSON_SCHEMA, HOST)
    server.add_new_data_stream(stream)
    server.update_all_tables()
    print("Starts on ", location)
    server.serve()


if __name__ == '__main__':
    main()
