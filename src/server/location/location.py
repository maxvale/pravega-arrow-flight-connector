class Location:
    def __init__(self, scope: str, stream: str):
        self._scope = scope
        self._stream = stream

    def get_scope(self) -> str:
        return self._scope

    def get_stream(self) -> str:
        return self._stream
