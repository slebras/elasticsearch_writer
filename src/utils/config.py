import os


def get_config():
    """
    Initialize configuration data.
    """
    es_host = os.environ.get("ELASTICSEARCH_HOST", 'elasticsearch')
    es_port = os.environ.get("ELASTICSEARCH_PORT", 9200)
    return {
        'kafka_server': os.environ.get('KAFKA_SERVER', 'kafka'),
        'kafka_clientgroup': os.environ.get('KAFKA_CLIENTGROUP', 'search_indexer'),
        'elasticsearch_host': es_host,
        'elasticsearch_port': es_port,
        'elasticsearch_index_prefix': os.environ.get("ELASTICSEARCH_INDEX_PREFIX", "search2"),
        'elasticsearch_data_type': os.environ.get("ELASTICSEARCH_DATA_TYPE", "data"),
        'elasticsearch_url': f"http://{es_host}:{es_port}",
        'elasticsearch_save_topic': os.environ.get('KAFKA_ES_UPDATE_TOPIC', 'elasticsearch_updates')
    }
