import unittest
import json

from confluent_kafka import Producer

from src.utils.config import get_config

_CONFIG = get_config()


class TestIntegration(unittest.TestCase):

    def test_basic(self):
        producer = Producer({'bootstrap.servers': _CONFIG['kafka_server']})
        print("Producing test message.")
        producer.produce(
            _CONFIG['elasticsearch_save_topic'],
            json.dumps({
                'doc': {'name': 'Bob Ross'},
                'id': '123',
                'index': 'myindex'
            }),
            callback=delivery_report
        )
        producer.poll(60)
        print("Done producing test message.")


# -- Utils

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed:', err)
    else:
        print('Message delivered to', msg.topic(), msg.partition())
