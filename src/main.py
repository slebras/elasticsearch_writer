"""
Main daemon runner, launches two child threads.
Will try to automatically restart them if either crashes.
"""
import time
import queue

from . import elasticsearch_consumer
from . import elasticsearch_writer
from .utils.threadify import threadify
from .utils.set_up_indexes import set_up_indexes

if __name__ == '__main__':
    # Create the indexes for elasticsearch
    set_up_indexes()
    print('Starting threads...')
    update_queue = queue.Queue()  # type: queue.Queue
    consumer_thread = threadify(elasticsearch_consumer.main, [update_queue])
    writer_thread = threadify(elasticsearch_writer.main, [update_queue])
    # Parent process event loop that checks our threads.
    # If a thread dies, we restart it.
    while True:
        if not consumer_thread.is_alive():
            print('Kafka consumer thread died, restarting...')
            consumer_thread = threadify(elasticsearch_consumer.main, [update_queue])
        if not writer_thread.is_alive():
            print('ES writer thread died, restarting...')
            writer_thread = threadify(elasticsearch_writer.main, [update_queue])
        time.sleep(10)
