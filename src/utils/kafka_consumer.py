"""
Convenience wrapper / generator function arounda kafka consumer for a given
topic/client group.
"""
import sys
import traceback
import json
from confluent_kafka import Consumer, KafkaError

from .config import get_config


def kafka_consumer(topics, handlers):
    """
    Generic handler of kafka messages for a given set of topics.
    Args:
        topics - list of topic names to consume
        handlers - dictionary where each key is a message key (byte string) and
            each value is a function that handles a message with that key.
    """
    config = get_config()
    consumer = Consumer({
        'bootstrap.servers': config['kafka_server'],
        'group.id': config['kafka_clientgroup'],
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe(topics)
    print(f"Listening to {topics} in group {config['kafka_clientgroup']}")
    while True:
        msg = consumer.poll(0.5)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print("Reached end of the stream.")
            else:
                sys.stderr.write("Kafka message error: {msg.error()}\n")
            continue
        print(f'New message in {topics}: {msg.value()}')
        try:
            data = json.loads(msg.value().decode('utf-8'))
        except ValueError as err:
            # JSON parsing error
            sys.stderr.write(f'JSON parsing error: {err}\n')
            continue
        key = msg.key()
        if key not in handlers:
            sys.stderr.write("No handlers for message '{key}'\n")
            continue
        try:
            handlers[key](data)
        except Exception as err:
            sys.stderr.write(f"Error handling message '{key}':\n")
            sys.stderr.write(str(err) + '\n')
            traceback.print_exc()
    consumer.close()
