import pyarrow as pa
import json

JSON_FILE = '..\\data\\test.json'


class ArrowStream:

    def __init__(self):
        self.buffer = list()

    def json_converter(self, data):
        pass


def main():
    with open(JSON_FILE) as json_file:
        json_obj = json.load(json_file)
    print(json_obj)
    str_ = json.dumps(json_obj)
    str_ = ' '.join(format(ord(letter), 'b') for letter in str_)

    json_obj = ''.join(chr(int(x, 2)) for x in str_.split())
    data = json.loads(json_obj)
    print(data)


if __name__ == '__main__':
    main()
