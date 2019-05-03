import os


def get_config():
    """
    Initialize configuration data.
    """
    return {
        'kafka_server': os.environ.get('KAFKA_SERVER', 'kafka'),
        'kafka_clientgroup': os.environ.get('KAFKA_CLIENTGROUP', 'search_indexer'),
        'elasticsearch_host': os.environ.get("ELASTICSEARCH_HOST", 'elasticsearch'),
        'elasticsearch_port': os.environ.get("ELASTICSEARCH_PORT", 9200),
        'elasticsearch_index_prefix': os.environ.get("ELASTICSEARCH_INDEX_PREFIX", "search2"),
        'elasticsearch_data_type': os.environ.get("ELASTICSEARCH_DATA_TYPE", "data"),
        'elasticsearch_save_topic': os.environ.get('KAFKA_ES_UPDATE_TOPIC', 'elasticsearch_updates')
    }
