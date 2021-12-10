import os
from typing import Union

import requests

from databaseinfo import DatabaseInfo


def create_graph(database_info: DatabaseInfo):
    '''
    Create a new smart graph with vertices v_coll and edges edge_coll_name with given parameters.
    If overwrite is True and the graph and/or the vertex/edge collection exist, they are dropped first.
    :param database_info:
    :return: None
    '''
    if database_info.overwrite:
        # drop the graph (if it exists)
        url = os.path.join(database_info.endpoint, '_api/gharial', database_info.graph_name)
        url = url + '?dropCollections=true'
        requests.delete(url, auth=(database_info.username, database_info.password))
        # drop edges
        url = os.path.join(database_info.endpoint, '_api/collection', database_info.edge_coll_name)
        requests.delete(url, auth=(database_info.username, database_info.password))
        # drop vertices
        url = os.path.join(database_info.endpoint, '_api/collection', database_info.vertices_coll_name)
        requests.delete(url, auth=(database_info.username, database_info.password))

    url = os.path.join(database_info.endpoint, '_api/gharial')
    response = requests.post(url, auth=(database_info.username, database_info.password), json={
        "name": database_info.graph_name,
        "edgeDefinitions": [
            {
                "collection": database_info.edge_coll_name,
                "from": [database_info.vertices_coll_name],
                "to": [database_info.vertices_coll_name]
            }
        ],
        "orphanCollections": [database_info.vertices_coll_name],
        "isSmart": "true",
        "options": {
            "replicationFactor": database_info.replication_factor,
            "numberOfShards": database_info.number_of_shards,
            "smartGraphAttribute": database_info.smart_attribute
        }
    })
    if response.status_code == 409:
        raise RuntimeError(f'The graph or the edge collection already exist. Server response: {response.text}')
    if response.status_code not in [201, 202]:
        raise RuntimeError(f'Invalid response from bulk insert{response.text}')


def insert_vertices_unique(db_info: DatabaseInfo, vertices):
    doc = dict()
    vertices = list(vertices)
    q = f'''
    let vertex_ids = (
            FOR vertex IN @@vertex_coll
                RETURN vertex.smartProp
                )
    FOR v in @vertices
        FILTER TO_STRING(v) NOT IN vertex_ids
        INSERT {{ {db_info.smart_attribute} : v }} INTO @@vertex_coll
    '''
    doc['query'] = q
    doc['bindVars'] = {'vertices': vertices, '@vertex_coll': db_info.vertices_coll_name}
    url = os.path.join(db_info.endpoint, f"_api/cursor/")
    response = requests.post(url, json=doc, auth=(db_info.username, db_info.password))
    if response.status_code != 201:
        raise RuntimeError(f'Invalid response from server during insert_vertices_unique: {response.text}')


def insert_documents(db_info: DatabaseInfo, documents, collection_name: str):
    '''
    Insert an edge or (typically) a list of edges into the edge collection.
    :param db_info:
    :param documents:
    :param collection_name:
    :return: None
    '''
    url = os.path.join(db_info.endpoint, "_api/document/", collection_name)
    response = requests.post(url, json=documents, auth=(db_info.username, db_info.password))
    if response.status_code != 202:
        raise RuntimeError(f"Invalid response from bulk insert{response.text}")


def file_reader(filename, bulk_size):
    '''
    Yield bulk_size characters from the file with filename filename or the whole content of the file if it has less
    characters.
    :param filename: the filename
    :param bulk_size: the number of characters to return at most
    :return: None
    '''
    with open(filename, "r") as f:
        res = list()
        for line in f:
            res.append(line.strip())
            if len(res) == bulk_size:
                yield res
                res = list()
        if len(res) != 0:
            yield res


class ConverterToVertex:
    def __init__(self, vertex_coll_name: str):
        self.vertex_coll_name = vertex_coll_name

    def idx_to_vertex(self, idx: Union[int, str]) -> str:
        return str(f"{self.vertex_coll_name}/{idx}:{idx}")
