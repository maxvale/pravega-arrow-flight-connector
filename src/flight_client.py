import pyarrow as pa
import pyarrow.flight as fl


def main():
    location = "{}://{}:{}".format('grpc+tcp', 'localhost', '8080')
    client = fl.FlightClient(location)

# TODO: NOT SO TRASH IMPL

    while True:
        try:
            action = fl.Action("info", b"")
            list(client.do_action(action))
            break
        except pa.ArrowIOError as e:
            if "Deadline" in str(e):
                print("Server upping")

    des = fl.FlightDescriptor.for_path('stream/scope')
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
