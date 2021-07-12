import json
import random
import time
from datetime import datetime

import yaml

import pravega_client as pc

CONFIG_FILE = 'config.yaml'

JSON_FILE = '../data/test1.json'

def write_event(writer, idx):
    with open(JSON_FILE) as json_file:
        json_obj = json.load(json_file)
    json_obj['timestamp'] = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
    json_obj['id'] = idx
    json_obj['data'] = random.randint(10, 90)
    str_ = json.dumps(json_obj)
    str_ = bytes(str_, 'utf-8')
    print(str_)
    writer.write_event_bytes(str_)


def main():

    with open(CONFIG_FILE) as yaml_file:
        config = yaml.load(yaml_file)
    manager = pc.StreamManager(config['pravega'], False, False)
    manager.create_scope('scope')
    manager.create_stream('scope', 'stream', 1)
    writer = manager.create_writer('scope', 'stream')

    idx = 1

    while True:
        write_event(writer, idx)
        idx += 1
        time.sleep(5)


if __name__ == '__main__':
    main()
