import pravega_client as pc

class Writer:

    def __init__(self, scope, stream, name):
        self.scope = scope
        self.stream = stream
        self.name = name

        