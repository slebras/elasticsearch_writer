import sys
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
    prefix = config['elasticsearch_index_prefix']
    index_name = f"{prefix}.{data['name']}"
    alias_name = f"{prefix}.{data['alias']}"
    # Try to create index and alias, if not already present
    try:
        _create_index(index_name, config)
        _create_alias(alias_name, index_name, config)
    except RuntimeError as err:
        sys.stderr.write(str(err) + '\n')
    # Update the type mapping
    _put_mapping(index_name, data['props'], config)
    print("Finished loading index.")


def _create_index(index_name, config):
    """
    Create an index on Elasticsearch with a given name.
    """
    request_body = {
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


def _put_mapping(index_name, mapping, config):
    """
    Create or update the type mapping for a given index.
    """
    type_name = config['elasticsearch_data_type']
    url = f"{config['elasticsearch_url']}/{index_name}/_mapping/{type_name}"
    resp = requests.put(url, data=json.dumps({'properties': mapping}), headers=_HEADERS)
    if not resp.ok:
        raise RuntimeError(f"Error updating mapping for index {index_name}:\n{resp.text}")
    print('Updated mapping', resp.text)
