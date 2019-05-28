"""
Make bulk writes to elasticsearch.
"""
import requests
import json
import time
import collections

from .utils.config import get_config
from .utils.elastic_utils import get_indexes_with_prefix


def main(queue):
    batch_writes = []  # type: list
    batch_deletes = []  # type: list
    while True:
        # I don't really think this is the best way of doing this,
        # but I can't come up with a better solution.
        while queue.qsize() and len(batch_writes) < 1000 and len(batch_deletes) < 1000:
            msg_data = queue.get()
            if msg_data.get('delete') or msg_data.get('delete_workspace'):
                batch_deletes.append(msg_data)
            else:
                batch_writes.append(msg_data)
        if not batch_writes and not batch_deletes:
            time.sleep(3)
        if batch_writes:
            _save_to_elastic(_aggregate_batch(batch_writes))
            batch_writes = []
            time.sleep(3)
        if batch_deletes:
            _delete_from_elastic(batch_deletes)
            batch_deletes = []
            time.sleep(3)


def _get_id(data, _id):
    for msg_data in data:
        if _id == msg_data['id']:
            return msg_data


def _aggregate_batch(data):
    """
    aggregate a bulk of messages to avoid conflicts
    All conflicts are for 2+ Messages for same index and id:
    Conflicts it handles and how it handles them:
        - multiple reindexing:
            if has field 'obj_type'
    """
    id_index_strs = [str(d['index']) + str(d['id']) for d in data]
    if len(id_index_strs) == len(set(id_index_strs)):
        return data

    data_dict = collections.defaultdict(lambda:[])
    for msg_data in data:
        data_dict[str(msg_data['index']) + '.' + str(msg_data['id'])].append(msg_data)

    new_data = []
    for index_id, msg_datas in data_dict.items():
        if len(msg_datas) > 1:
            # we're going to prioritize by version, most recent version gets precedent
            # if there is no 'version' field, simply takes last read.
            curr_ver = -1
            curr_msg = None
            for msg_data in msg_datas:
                if msg_data['doc'].get('version'):
                    if msg_data['doc']['version'] > curr_ver:
                        curr_ver = msg_data['doc']['version']
                        curr_msg = msg_data
                else:
                    curr_msg = msg_data
            if curr_msg != None:
                new_data.append(curr_msg)
        else:
            new_data.append(msg_datas[0])

    return new_data


def _delete_from_elastic(data):
    """
    Given a list of messages with field 'id' (in form 'workspace_id' or 'workspace_id:object_id'),
    constructs a bulk delete_by_query request for elasticsearch.
    """
    config = get_config()
    es_type = config['elasticsearch_data_type']
    prefix = config['elasticsearch_index_prefix']
    # Construct the post body for the bulk index
    should_body = []
    # make sure we don't use same id more than once.
    id_list = []
    while data:
        datum = data.pop()
        id_ = datum['id']
        if id_ in id_list:
            continue
        prefix_body = {
            'prefix': {'guid': datum['id']}
        }
        id_list.append(id_)
        should_body.append(prefix_body)
    json_body = json.dumps({
        'query': {
            'bool': {
                'should': should_body
            }
        }
    })
    index_name = f"{prefix}.*"
    es_url = "http://" + config['elasticsearch_host'] + ":" + str(config['elasticsearch_port'])
    # Save the document to the elasticsearch index
    resp = requests.post(
        f"{es_url}/{index_name}/_delete_by_query",
        data=json_body,
        headers={"Content-Type": "application/json"}
    )
    if not resp.ok:
        # Unsuccesful save to elasticsearch.
        raise RuntimeError(f"Error saving to elasticsearch:\n{resp.text}")
    print(f"Elasticsearch delete by query successful.")


def _save_to_elastic(data):
    """
    Bulk save a list of indexed
    Each entry in the list has {doc, id, index}
        doc - document data (for indexing events)
        id - document id
        index - index name
        delete - bool (for delete events)

    EDITS:
        this function should be renamed to something like _write_to_elastic

    """
    config = get_config()
    es_type = config['elasticsearch_data_type']
    prefix = config['elasticsearch_index_prefix']
    # Construct the post body for the bulk index
    json_body = ''
    length = len(data)
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
    print(f"Bulk saving {length} documents..")
    # Save the document to the elasticsearch index
    resp = requests.post(
        f"{es_url}/_bulk",
        data=json_body,
        headers={"Content-Type": "application/json"}
    )
    if not resp.ok:
        # Unsuccesful save to elasticsearch.
        raise RuntimeError(f"Error saving to elasticsearch:\n{resp.text}")
    print(f"Elasticsearch bulk save successful.")
