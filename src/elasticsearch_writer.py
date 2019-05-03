"""
Make bulk writes to elasticsearch.
"""
import requests
import json
import time

from .utils.config import get_config


def main(queue):
    batch_writes = []
    while True:
        while queue.qsize():
            batch_writes.append(queue.get())
        if batch_writes:
            _save_to_elastic(batch_writes)
            batch_writes = []
        time.sleep(3)


def _save_to_elastic(data):
    """
    Bulk save a list of indexed
    Each entry in the list has {doc, id, index}
        doc - document data
        id - document id
        index - index name
    """
    config = get_config()
    es_type = config['elasticsearch_data_type']
    prefix = config['elasticsearch_index_prefix']
    # Construct the post body for the bulk index
    json_body = ''
    while data:
        datum = data.pop()
        index_name = f"{prefix}.{datum['index']}"
        json_body += json.dumps({
            'index': {
                '_index': index_name,
                '_type': es_type,
                '_id': datum['id']
            }
        })
        json_body += '\n'
        json_body += json.dumps(datum['doc'])
        json_body += '\n'
    es_url = "http://" + config['elasticsearch_host'] + ":" + str(config['elasticsearch_port'])
    # Save the document to the elasticsearch index
    resp = requests.post(
        f"{es_url}/_bulk",
        data=json_body,
        headers={"Content-Type": "application/json"}
    )
    if not resp.ok:
        # Unsuccesful save to elasticsearch.
        raise RuntimeError(f"Error saving to elasticsearch:\n{resp.text}")
    print("Elasticsearch bulk save successful.")
