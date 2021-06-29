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

    def add_flight_stream(self, descriptor, stream):
        key = self._descriptor_to_key(descriptor)
        if key in self.flights.keys():
            raise Exception("This stream is already exist")
        self.flights[key] = stream
        self.flights[key].test_update_data()

    def list_flights(self, context, criteria):
        for key, stream in self.flights.items():
            if key[1] is not None:
                descriptor = fl.FlightDescriptor.for_command(key[1])
            else:
                descriptor = fl.FlightDescriptor.for_path(key[2])
            table = stream.get_data().read_all()
            yield self._make_flight_info(key, descriptor, table)

    def get_flight_info(self, context, descriptor):
        key = self._descriptor_to_key(descriptor)
        table = self.flights[key].get_data().read_all()
        return self._make_flight_info(key, descriptor, table)

    def do_get(self, context, ticket):
        key = ast.literal_eval(ticket.ticket.decode())
        return fl.RecordBatchStream(self.flights[key].get_data().read_all())

    def do_put(self, context, descriptor, reader, writer):
        pass

    def list_actions(self, context):
        pass

    def do_action(self, context, action):
        if action.type == "shutdown":
            yield fl.Result(pa.py_buffer(b'Shutdown!'))
            threading.Thread(target=self._shutdown).start()
        elif action.type == "info":
            yield fl.Result(pa.py_buffer(b'SERVER WORK!'))

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
    location = "{}://{}:{}".format('grpc+tcp', 'localhost', '8080')
    server = FlightServer('localhost', location=location)
    desc_path = fl.FlightDescriptor.for_path('stream/scope')
    print(desc_path)
    stream = flight_stream.FlightStream('scope', 'stream', 'rg1',
                                        JSON_SCHEMA, HOST)
    server.add_flight_stream(desc_path, stream)
    print("Starts on ", location)

    ticket = fl.Ticket('ticket')
    print(ticket)
    t = ticket.ticket.decode()
    print(t)
    server.serve()





if __name__ == '__main__':
    main()
