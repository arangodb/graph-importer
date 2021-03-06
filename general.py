import json
import os
import random
import threading

import requests
from psutil import process_iter, NoSuchProcess, AccessDenied, ZombieProcess
from requests import Response

from helper_classes import DatabaseInfo


def graph_exists(db_info: DatabaseInfo) -> bool:
    url = os.path.join(db_info.endpoint, f'_api/gharial/{db_info.graph_name}')
    try:
        response = requests.get(url, auth=(db_info.username, db_info.password))
        return response.status_code == 200
    except RuntimeError:
        print("Error: database connection failed.")
    return False

def collection_exists(db_info: DatabaseInfo, collection_name: str) -> bool:
    url = os.path.join(db_info.endpoint, f'_api/collection/{collection_name}')
    response = requests.get(url, auth=(db_info.username, db_info.password))
    return response.status_code != 404


def get_all_graphs(db_info: DatabaseInfo):
    url = os.path.join(db_info.endpoint, '_api/gharial')
    response = requests.get(url, auth=(db_info.username, db_info.password))
    if response.status_code != 200:
        raise RuntimeError(f'get_all_graphs error: Error Code: {response.status_code}. Message: {response.text}')
    s = response.content.decode('utf-8')

    d = json.loads(s)
    graphs = d['graphs']
    return [g['_key'] for g in graphs]


def get_all_collections(db_info: DatabaseInfo):
    url = os.path.join(db_info.endpoint, '_api/collection')
    response = requests.get(url, auth=(db_info.username, db_info.password))
    if response.status_code != 200:
        raise RuntimeError(f'get_all_graphs error: Error Code: {response.status_code}. Message: {response.text}')
    s = response.content.decode('utf-8')
    d = json.loads(s)
    collections = d['result']
    return [c['name'] for c in collections]


def create_graph(db_info: DatabaseInfo):
    """
    Create a new smart graph with vertices v_coll and edges edge_coll_name with given parameters.
    If the graph and/or the vertex/edge collection exist, they are dropped first.
    :param db_info:
    :return: None
    """
    # drop the graph (if it exists)
    url = os.path.join(db_info.endpoint, '_api/gharial', db_info.graph_name)
    url = url + '?dropCollections=true'
    requests.delete(url, auth=(db_info.username, db_info.password))
    # drop edges
    url = os.path.join(db_info.endpoint, '_api/collection', db_info.edge_coll_name)
    requests.delete(url, auth=(db_info.username, db_info.password))
    # drop vertices
    url = os.path.join(db_info.endpoint, '_api/collection', db_info.vertices_coll_name)
    requests.delete(url, auth=(db_info.username, db_info.password))

    # create graph
    url = os.path.join(db_info.endpoint, '_api/gharial')
    if db_info.isSmart:
        response = requests.post(url, auth=(db_info.username, db_info.password), json={
            "name": db_info.graph_name,
            "edgeDefinitions": [
                {
                    "collection": db_info.edge_coll_name,
                    "from": [db_info.vertices_coll_name],
                    "to": [db_info.vertices_coll_name]
                }
            ],
            "orphanCollections": [db_info.vertices_coll_name],
            "isSmart": "true",
            "options": {
                "replicationFactor": db_info.replication_factor,
                "numberOfShards": db_info.number_of_shards,
                "smartGraphAttribute": db_info.smart_attribute
            }
        })
    else:
        response = requests.post(url, auth=(db_info.username, db_info.password), json={
            "name": db_info.graph_name,
            "edgeDefinitions": [
                {
                    "collection": db_info.edge_coll_name,
                    "from": [db_info.vertices_coll_name],
                    "to": [db_info.vertices_coll_name]
                }
            ],
            "orphanCollections": [db_info.vertices_coll_name],
            "options": {
                "replicationFactor": db_info.replication_factor,
                "numberOfShards": db_info.number_of_shards
            }
        })
    if response.status_code == 409:
        raise RuntimeError(f'The graph or the edge collection already exist. Server response: {response.text}')
    if response.status_code not in [201, 202]:
        raise RuntimeError(f'Invalid response from bulk insert{response.text}')


class ResponseWrapper:
    def __init__(self):
        self.response = Response()


def _call_request_post(response_wrapper: ResponseWrapper, url: str, documents: dict, username: str, password: str):
    response_wrapper.response = requests.post(url, json=documents, auth=(username, password))
    if response_wrapper.response.status_code != 202:
        raise RuntimeError(f"Invalid response from bulk insert: {response_wrapper.response.text}")


def insert_documents(db_info: DatabaseInfo, documents, collection_name: str):
    """
    Insert an edge or (typically) a list of edges into the edge collection.
    :param db_info:
    :param documents:
    :param collection_name:
    :return: None
    """
    url = os.path.join(db_info.endpoint, "_api/document/", collection_name)
    response_wrapper = ResponseWrapper()
    thr = threading.Thread(target=_call_request_post,
                           args=(response_wrapper, url, documents, db_info.username, db_info.password))
    thr.start()
    thr.join()
    # response = requests.post(url, json=documents, auth=(db_info.username, db_info.password))


def file_reader(filename, bulk_size):
    """
    Yield bulk_size characters from the file with filename filename or the whole content of the file if it has less
    characters.
    :param filename: the filename
    :param bulk_size: the number of characters to return at most
    :return: None
    """
    with open(filename, "r") as f:
        res = list()
        for line in f:
            res.append(line.strip())
            if len(res) == bulk_size:
                yield res
                res = list()
        if len(res) != 0:
            yield res


def yes_with_prob(prob: float):
    return random.randint(1, 1000) < prob * 1000


def arangodIsRunning():
    """
    Check if arangod is running.
    """
    for proc in process_iter():
        try:
            # Check if process name contains the given name string.
            if 'arangod' in proc.name():
                return True
        except (NoSuchProcess, AccessDenied, ZombieProcess):
            pass
    return False


def get_time_difference_string(t_diff: float) -> str:
    t_diff = int(t_diff * 100) / 100
    hours = str(t_diff // 3600) + " h " if t_diff > 3600 else ""
    minutes = f'{int(t_diff % 3600 // 60)}' + " min " if hours or t_diff % 3600 > 60 else ""
    secs = f'{t_diff % 60:2.2f}' + " sec"
    return hours + minutes + secs
