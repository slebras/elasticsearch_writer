"""
Consume elasticsearch save events from kafka.
"""
import sys

from .utils.kafka_consumer import kafka_consumer
from .utils.config import get_config


def main(queue):
    """
    Main event loop for consuming messages from Kafka and saving to elasticsearch.
    """
    config = get_config()
    topics = [config['elasticsearch_save_topic']]
    for msg_data in kafka_consumer(topics):
        try:
            _validate_message(msg_data)
        except RuntimeError as err:
            sys.stderr.write(str(err) + '\n')
        print(f"Received message for id {msg_data['id']} in index {msg_data['index']}")
        # Push the data to save in the queue
        queue.put(msg_data)


def _validate_message(msg_data):
    """
    Validate the input message.
    """
    required_keys = ['doc', 'id', 'index']
    for key in required_keys:
        if not msg_data.get(key):
            raise RuntimeError(f"Message to Elasticsearch malformed, does not contain required field: {key}")
