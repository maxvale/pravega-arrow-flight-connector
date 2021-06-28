import pyarrow as pa
import pyarrow.flight as fl


class FlightServer(fl.FlightServerBase):
    
    def __init__(self, host, location=None, tls_cert=None,
                 verify_cli=False, root_cert=None, auth_hand=None):
        super(FlightServer, self).__init__(
            location, auth_hand, tls_cert, verify_cli, root_cert)
        self.flights = dict()
        self.host = host


def main():
    pass


if __name__ == '__main__':
    main()