import argparse
import cmd
import sys

import pyarrow as pa
import pyarrow.flight as fl


class FlightClient(cmd.Cmd):

    def __init__(self, location):
        super().__init__()
        self.location = location
        self.client = fl.FlightClient(location)

        while True:
            try:
                action = fl.Action("healthcheck", b"")
                answer = list(self.client.do_action(action))
                break
            except pa.ArrowIOError as e:
                if "Deadline" in str(e):
                    print("Server upping...")
        print(answer)
        print("Server is up and ready!")

    def do_list(self):
        for flight in self.client.list_flights():
            descriptor = flight.descriptor
            print('PATH: ', descriptor.path)
            print('TOTAL RECORDS: ', flight.total_records)
            print('Total bytes: ', flight.total_bytes)
            print('Number of endpoints: ', len(flight.endpoints))
            print('Schema: ', flight.schema)
            print('---')

        for action in self.client.list_actions():
            print('Type: ', action.type)
            print('Description: ', action.description)

    def do_healthcheck(self):
        buffer = self._server_action('healthcheck')
        print(buffer)

    def do_shutdown(self):
        buffer = self._server_action('shutdown')

    def do_quit(self):
        sys.exit(0)

    def _server_action(self, action_type):
        try:
            buffer = pa.allocate_buffer(0)
            action = fl.Action(action_type, buffer)
            print('Running ', action_type)
            for result in self.client.do_action(action):
                print('Result: ', result.body.to_pybytes())
            return buffer
        except pa.lib.ArrowIOError as e:
            print("Error with action: ", e)


def main():
    location = "{}://{}:{}".format('grpc+tcp', 'localhost', '8080')
    client = fl.FlightClient(location)

    des = fl.FlightDescriptor.for_path('scope/stream')
    info = client.get_flight_info(des)
    for endpoint in info.endpoints:
        print('Ticket : ', endpoint.ticket)
        for location in endpoint.locations:
            print(location)
            get_client = fl.FlightClient(location)
            reader = get_client.do_get(endpoint.ticket)
            df = reader.read_pandas()
            print(df)

    action = fl.Action("shutdown", b"")
    try:
        list(client.do_action(action))
    except Exception as e:
        print("Smth go wrong!")


if __name__ == '__main__':
    main()
