# For Elastic related utilities.ad
# from .utils.config import get_config
import requests
import json


def get_indexes_with_prefix(config, prefix):
    """
    """
    es_url = "http://" + config['elasticsearch_host'] + ":" + str(config['elasticsearch_port'])

    resp = requests.get(
        f"{es_url}/_aliases",
        headers={"Content-Type": "application/json"}
    )
    if not resp.ok:
        raise RuntimeError(f"Error retrieving aliases from Elastic:\n{resp.text}")
    print("Elasticsearch aliases get request successful")
    json_resp = resp.json()
    return [idx for idx in json_resp if idx.startswith(prefix)]

# def delete_by_query(data):
#     """
#     We need this specific case for when genomes, pangenomes and other objects that have
#     subsections indexed separately are deleted.
#     """
#     config = get_config()
#     es_type = config['elasticsearch_data_type']
#     prefix = config['elasticsearch_index_prefix']
#     # Construct the post body for the bulk index

#     guid = data['id']
#     index_name = f"{prefix}.{data['index']}"
#     body = {
#         'query': {
#             'prefix': {
#                 'guid': guid
#             }
#         }
#     }
#     json_body = json.dumps(body)

#     es_url = "http://" + config['elasticsearch_host'] + ":" + str(config['elasticsearch_port'])
#     # Save the document to the elasticsearch index
#     resp = requests.post(
#         f"{es_url}/{index_name}/_delete_by_query",
#         data=json_body,
#         headers={"Content-Type": "application/json"}
#     )
#     if not resp.ok:
#         # Unsuccesful save to elasticsearch.
#         raise RuntimeError(f"Error saving to elasticsearch:\n{resp.text}")
#     print(f"Elasticsearch delete by query successful.")
