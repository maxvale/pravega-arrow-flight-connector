import ast
import threading
import time

import pyarrow as pa
import pyarrow.flight as fl

import flight_stream


JSON_FILE = '..\\data\\test'

JSON_SCHEMA = pa.schema([('timestamp', pa.string()),
                         ('id', pa.string()),
                         ('data', pa.int32())])

HOST = '127.0.0.1:9090'


class FlightServer(fl.FlightServerBase):
    
    def __init__(self, host, location=None, tls_cert=None,
                 verify_cli=False, root_cert=None, auth_hand=None):
        super(FlightServer, self).__init__(
            location, auth_hand, tls_cert, verify_cli, root_cert)
        self.flights = dict()
        self.host = host

    # TODO: NEED TO RELEASE A LOGIC FOR TIMEOUT UPDATE STREAM DATA AND
    # TODO: AND AFTER CALL UPDATE FUNC

    def update_flight_table(self, descriptor, reader):
        key = self._descriptor_to_key(descriptor)
        table = reader.read_all()
        if key not in self.flights.keys():
            self.flights[key] = table
            return
        tables = [self.flights[key], table]
        self.flights[key] = pa.concat_tables(tables)

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
        pass

    def list_actions(self, context):
        return [
            ("shutdown", "Shutdown"),
            ("healthcheck", "Check availability")
        ]

    # TODO: THINK ABOUT LOGIC AND EXTRA ACTIONS

    def do_action(self, context, action):
        if action.type == "shutdown":
            yield fl.Result(pa.py_buffer(b'Shutdown!'))
            threading.Thread(target=self._shutdown).start()
        elif action.type == "healthcheck":
            yield fl.Result(pa.py_buffer(b'I am good! What about you?'))
        elif action.type == "update_data":
            pass
        # TODO: IMPL BIG FUNC FOR UPDATING ALL LIST FLIGHTS
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


def main():
    # TODO: NOT SO TRASH SERVE LOGIC
    location = "{}://{}:{}".format('grpc+tcp', 'localhost', '8080')
    server = FlightServer('localhost', location=location)
    desc_path = fl.FlightDescriptor.for_path('stream/scope')
    print(desc_path)
    stream = flight_stream.FlightStream('scope', 'stream', 'rg1',
                                        JSON_SCHEMA, HOST)
    stream.test_update_data()
    reader = stream.get_data()
    server.update_flight_table(desc_path, reader)
    print("Starts on ", location)
    server.serve()


if __name__ == '__main__':
    main()
