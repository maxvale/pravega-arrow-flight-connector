import argparse
import cmd
import sys
import pandas
import matplotlib.pyplot as plt

import pyarrow as pa
import pyarrow.flight as fl


class FlightClient(cmd.Cmd):

    def __init__(self, location):
        super().__init__()
        self.location = location
        self.prompt = '> '
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

    def do_list(self, arg):
        print('--- Flights list ---')
        for flight in self.client.list_flights():
            descriptor = flight.descriptor
            print('Path: ', descriptor.path)
            print('Total records: ', flight.total_records)
            print('Total bytes: ', flight.total_bytes)
            print('Number of endpoints: ', len(flight.endpoints))
            print('Schema: \n', flight.schema)
        print('---------')

        print('--- Flights actions ---')
        for action in self.client.list_actions():
            print('Type: ', action.type)
            print('Description: ', action.description)
        print('---------')

    def do_get_flight(self, arg):
        descriptor = fl.FlightDescriptor.for_path(arg)
        try:
            info = self.client.get_flight_info(descriptor)
            for endpoint in info.endpoints:
                print('Ticket: ', endpoint.ticket)
                reader = self.client.do_get(endpoint.ticket)
                dataframe = reader.read_pandas()
                print('Dataframe: \n', dataframe)
                print('---------')
                dataframe.plot(x='id', y='data')
                plt.savefig('demo.png')
        except fl.FlightError:
            print('Unknown stream')

    def do_update(self, arg):
        try:
            self._server_action('update')
        except ConnectionError as e:
            print("Cannot reach server on", self.location)

    def do_healthcheck(self, arg):
        try:
            self._server_action('healthcheck')
        except ConnectionError as e:
            print("Cannot reach server on", self.location)

    def do_shutdown(self, arg):
        try:
            self._server_action('shutdown')
        except ConnectionError as e:
            print("Cannot reach server on", self.location)

    def do_quit(self, arg):
        sys.exit(0)

    def _server_action(self, action_type):
        try:
            buffer = pa.allocate_buffer(0)
            action = fl.Action(action_type, buffer)
            print('Running ', action_type)
            for result in self.client.do_action(action):
                print('Result: ', result.body.to_pybytes().decode('utf-8'))
        except pa.lib.ArrowIOError as e:
            print("Error with action: ", e)


def main():
    location = "{}://{}:{}".format('grpc+tcp', 'localhost', '8080')
    client = FlightClient(location)
    client.cmdloop()


if __name__ == '__main__':
    main()
