import pravega_client as pc
import pravega_reader
import asyncio
import yaml

CONFIG_FILE = 'config.yaml'

if __name__ == '__main__':
    with open(CONFIG_FILE) as file:
        config_data = yaml.load(file)
    host = config_data['pravega']

    manager = pc.StreamManager("192.168.56.1:9090", False, False)
    manager.create_scope("scope")
    manager.create_stream("scope", "stream", 3)

    writer_one = manager.create_writer("scope", "stream")
    writer_two = manager.create_writer("scope", "stream")

    writer_one.write_event("event11")
    writer_two.write_event("event21")
    writer_two.write_event("event22", "key2")
    writer_one.write_event("event12", "key1")

    reader_group = pravega_reader.Reader("scope", "stream", "rg1", host)
    reader_group.add_reader("rd1")

    buffer = asyncio.run(reader_group.read_event_async("rd1"))
    for item in buffer:
        print(item)    
