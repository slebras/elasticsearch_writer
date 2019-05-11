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
            'index',
            callback=delivery_report
        )
        producer.poll(60)
        print("Done producing test message.")

    def test_init_index(self):
        producer = Producer({'bootstrap.servers': _CONFIG['kafka_server']})
        print("Producing init_index message.")
        producer.produce(
            _CONFIG['elasticsearch_save_topic'],
            json.dumps({
                'props': {'timestamp': {'type': 'date'}},
                'name': 'test_index:1',
                'alias': 'test_index'
            }),
            'init_index',
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
