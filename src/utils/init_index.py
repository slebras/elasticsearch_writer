import json
import requests

from .config import get_config

_HEADERS = {"Content-Type": "application/json"}


def init_index(data):
    """
    Initialize a new index on elasticsearch
    `data` should have:
        name - index name
        alias - alias name
        props - elasticsearch type mapping properties
    """
    config = get_config()
    # Fetch all index and alias names
    aliases_resp = requests.get(config['elasticsearch_url'] + "/_aliases?format=json")
    if not aliases_resp.ok:
        raise RuntimeError(f"Error fetching aliases:\n{aliases_resp.text}")
    # `indices` will be a dict where the keys are index names. Each value is a
    # dict with a key for "aliases".
    indices = aliases_resp.json()
    prefix = config['elasticsearch_index_prefix']
    index_name = f"{prefix}.{data['name']}"
    alias_name = f"{prefix}.{data['alias']}"
    aliases = indices[index_name].get('aliases', {})
    if index_name not in indices:
        # Neither index nor alias has been created
        print(f"Creating new index '{index_name}' with alias '{alias_name}'.")
        _create_index(index_name, data['props'], config)
        _create_alias(alias_name, index_name, config)
    elif alias_name not in aliases:
        # Index is created, but let's check for aliases
        print(f"Creating missing alias '{alias_name}'.")
        _create_alias(alias_name, index_name, config)
    print("Finished loading index.")


def _create_index(index_name, mapping, config):
    """
    Create an index on Elasticsearch using a type mapping.
    """
    es_type = config['elasticsearch_data_type']
    request_body = {
        "mappings": {es_type: {"properties": mapping}},
        "settings": {
            "index": {
                "number_of_shards": 10,
                "number_of_replicas": 2
            }
        }
    }
    url = config['elasticsearch_url'] + '/' + index_name
    resp = requests.put(url, data=json.dumps(request_body), headers=_HEADERS)
    if not resp.ok:
        raise RuntimeError(f"Error while creating new index {index_name}:\n{resp.text}")


def _create_alias(alias_name, index_name, config):
    """
    Create an alias from `alias_name` to the  `index_name`.
    """
    body = {
        'actions': [{'add': {'index': index_name, 'alias': alias_name}}]
    }
    url = config['elasticsearch_url'] + '/_aliases'
    resp = requests.post(url, data=json.dumps(body), headers=_HEADERS)
    if not resp.ok:
        raise RuntimeError(f"Error creating alias '{alias_name}':\n{resp.text}")
