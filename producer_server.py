import pathlib
import json
import logging
import pykafka
import time
from pykafka import KafkaClient

INPUT_FILE = 'police-department-calls-for-service.json'

logger = logging.getLogger(__name__)


def read_file() -> json:
    with open(INPUT_FILE, 'r') as f:
        data = json.load(f)
    return data


def generate_data() -> None:
    data = read_file()
    for i in data:
        message = dict_to_binary(i)
        producer.produce(message)
        time.sleep(2)


# TODO complete this function
def dict_to_binary(json_dict: dict) -> bytes:
    """
    Encode your json to utf-8
    :param json_dict:
    :return:
    """
    data_final = json.dumps(json_dict).encode('utf-8')
    return data_final 

# TODO set up kafka client
if __name__ == "__main__":
    client = KafkaClient(hosts="127.0.0.1:9092")
    print("topics", client.topics)
    producer = client.topics[b'service-calls'].get_producer() #they have given the name of the topic

    generate_data()
