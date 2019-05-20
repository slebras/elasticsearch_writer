"""
Make bulk writes to elasticsearch.
"""
import requests
import json
import time
import collections

from .utils.config import get_config


def main(queue):
    batch_writes = []  # type: list
    while True:
        while queue.qsize() and len(batch_writes) < 1000:
            batch_writes.append(queue.get())
        if batch_writes:
            _save_to_elastic(_aggregate_batch(batch_writes))
            batch_writes = []
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
        - delete message + any other message: keeps the delete
        - multiple reindexing, keeps the one it reads last.
    """
    id_index_strs = [str(d['index']) + str(d['id']) for d in data]
    # check if there are messages for the same index and id
    # if not, return data
    if len(id_index_strs) == len(set(id_index_strs)):
        return data
    conflict_dict = collections.defaultdict(lambda: 0)

    for d in data:
        conflict_dict[d['index'] + '.' + d['id']] += 1
    conflict_ids = [index_id for index_id, val in conflict_dict.items() if val > 1]

    new_data = []
    for index_id, val in conflict_dict.items()
        if val == 1:
            index, _id = index_id.split('.')
            new_data.append(_get_id(data, _id))

    # get all the ids that have conflicts
    for index_id in conflict_ids:
        index, _id = index_id.split('.')
        msg_datas = [d for d in data if d['id'] == _id and d['index'] == index]
        # if there is a 'delete' field in any of the messages use that,
        curr_msg = None
        curr_ver = -1
        for msg_data in msg_datas:
            if msg_data.get('delete'):
                curr_msg = msg_data
                break
            elif msg_data.get('doc'):
                if curr_ver < msg_data['doc']['obj_type_version']:
                    curr_msg = msg_data
                    curr_ver = msg_data['doc']['obj_type_version']
                elif curr_ver == msg_data['doc']['obj_type_version']:
                    # this shouldn't happen, but if it does
                    # we should choose the second msg_data.
                    curr_msg = msg_data
        if curr_msg != None:
            new_data.append(curr_msg)
        else:
            # should throw error here perhaps
            pass

    return new_data

'''
'''
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
