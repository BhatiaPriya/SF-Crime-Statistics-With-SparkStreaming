import logging
from pykafka import KafkaClient
from pykafka.simpleconsumer import OffsetType

logger = logging.getLogger("Testing_Producer") 
logger.setLevel('WARNING')  # various levels can be warning, error, critical, info, debug

client = KafkaClient(hosts="127.0.0.1:9092")
topic = client.topics[b'service-calls']

consumer = topic.get_balanced_consumer(
    consumer_group='producer_test',
    auto_commit_enable=False,
    auto_offset_reset=OffsetType.EARLIEST,
    zookeeper_connect='127.0.0.1:2181'
)
for data in consumer:
    if data is not None:
        print(data.offset, data.value)