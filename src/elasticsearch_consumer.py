"""
Consume elasticsearch save events from kafka.
"""
import jsonschema

from .utils.kafka_consumer import kafka_consumer
from .utils.init_index import init_index
from .utils.config import get_config


def main(queue):
    """
    Main event loop for consuming messages from Kafka and saving to elasticsearch.
    """
    config = get_config()
    topics = [config['elasticsearch_save_topic']]

    # Message handlers based on message key
    handlers = {
        b'index': _handle_index(queue),
        b'init_index': _handle_init_index,
        b'delete': _handle_delete(queue, 'delete'),
        b'delete_workspace': _handle_delete(queue, 'delete_workspace'),
        b'': _handle_permissions(queue)
    }
    kafka_consumer(topics, handlers)


def _handle_permissions(queue):
    """
    This is the handler for permissions events
    """
    def handler(msg_data):
        jsonschema.validate(instance=msg_data, schem=_PERMISSIONS_SCHEMA)
        msg_data['perm'] = True
        queue.put(msg_data)

    return handler


def _handle_delete(queue, del_str):
    """
    This is  the hander for the 'delete' and 'delete_workspace' events.
    Delete documents from elasticsearch based on id and index name.
    """
    def handler(msg_data):
        jsonschema.validate(instance=msg_data, schema=_DELETE_SCHEMA)
        msg_data[del_str] = True
        queue.put(msg_data)
    return handler


def _handle_index(queue):
    """
    Handle an event to save a new index document.
    Note that this function is curried to accept the thread queue.
    """
    def handler(msg_data):
        # Save a document to an existing index
        jsonschema.validate(instance=msg_data, schema=_INDEX_SCHEMA)
        # Push the data to save into the thread queue.
        # This will be consumed by the writer thread (see ./elasticsearch_writer.py)
        queue.put(msg_data)
    return handler


def _handle_init_index(msg_data):
    """
    Handle an event to initialize a new index with a type mapping.
    """
    print(f"Initializing index '{msg_data['name']}'")
    # Initialize a new index with a type mapping
    jsonschema.validate(instance=msg_data, schema=_INIT_INDEX_SCHEMA)
    init_index(msg_data)


_INDEX_SCHEMA = {
    'type': 'object',
    'required': ['doc', 'id', 'index'],
    'additionalProperties': False,
    'properties': {
        'id': {'type': 'string'},
        'index': {'type': 'string'},
        'doc': {'type': 'object'}
    }
}


_DELETE_SCHEMA = {
    'type': 'object',
    'required': ['id', 'index'],
    'additionalProperties': False,
    'properties': {
        'id': {'type': 'string'},
        'index': {'type': 'string'}
    }
}


_INIT_INDEX_SCHEMA = {
    'type': 'object',
    'required': ['name', 'props', 'alias'],
    'additionalProperties': False,
    'properties': {
        'name': {'type': 'string'},
        'alias': {'type': 'string'},
        'props': {'type': 'object'}
    }
}
