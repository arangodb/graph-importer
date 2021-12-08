import os

import requests


def create_graph(endpoint, graphname, vertices_coll_name, edge_coll_name, replication_factor: int,
                 number_of_shards: int, overwrite: bool, smart_attribute: str, username: str, password: str):
    '''
    Create a new smart graph with vertices vertices_coll_name and edges edge_coll_name with given parameters.
    If overwrite is True and the graph and/or the vertex/edge collection exist, they are dropped first.
    :param endpoint: the server address
    :param graphname: the name of the grph to create
    :param vertices_coll_name: the name of the vertex collection to create
    :param edge_coll_name: the name of the edge collection to create
    :param replication_factor: the replication factor
    :param number_of_shards: the number of shards
    :param overwrite: whether to overwrite the  graph and the vertex and edge collections
    :param smart_attribute: the name of the field to shard vertices after
    :param username: the username
    :param password: the password
    :return: None
    '''
    if overwrite:
        # drop the graph (if it exists)
        url = os.path.join(endpoint, '_api/gharial', graphname)
        url = url + '?dropCollections=true'
        requests.delete(url, auth=(username, password))
        # drop edges
        url = os.path.join(endpoint, '_api/collection', edge_coll_name)
        requests.delete(url, auth=(username, password))
        # drop vertices
        url = os.path.join(endpoint, '_api/collection', vertices_coll_name)
        requests.delete(url, auth=(username, password))

    url = os.path.join(endpoint, '_api/gharial')
    response = requests.post(url, auth=(username, password), json={
        "name": graphname,
        "edgeDefinitions": [
            {
                "collection": edge_coll_name,
                "from": [vertices_coll_name],
                "to": [vertices_coll_name]
            }
        ],
        "orphanCollections": [vertices_coll_name],
        "isSmart": "true",
        "options": {
            "replicationFactor": replication_factor,
            "numberOfShards": number_of_shards,
            "smartGraphAttribute": smart_attribute
        }
    })
    if response.status_code == 409:
        raise RuntimeError(f'The graph or the edge collection already exist. Server response: {response.text}')
    if response.status_code not in [201, 202]:
        raise RuntimeError(f'Invalid response from bulk insert{response.text}')


def insert_vertices_unique(endpoint, vertices_coll_name, vertices, smart_attribute, username, password):
    doc = dict()
    vertices = list(vertices) # convert to list
    q = f'''
    let vertex_ids = (
            FOR vertex IN @@vertex_coll
                RETURN vertex.smartProp
                )
    FOR v in @vertices
        FILTER TO_STRING(v) NOT IN vertex_ids
        INSERT {{ {smart_attribute} : v }} INTO @@vertex_coll
    '''
    doc['query'] = q
    doc['bindVars'] = {'vertices': vertices, '@vertex_coll': vertices_coll_name}
    url = os.path.join(endpoint, f"_api/cursor/")
    response = requests.post(url, json=doc, auth=(username, password))
    if response.status_code != 201:
        raise RuntimeError(f'Invalid response from server during insert_vertices_unique: {response.text}')


def insert_vertices(endpoint, vertex_coll_name, vertices, username, password):
    '''
    Insert a vertex or a list of vertices into the vertex collection.
    :param endpoint: the server address
    :param vertex_coll_name: the vertex collection name
    :param vertices: the vertex or (typically) the list of vertices
    :param username: the username
    :param password: the password
    :return: None
    '''
    url = os.path.join(endpoint, "_api/document/", vertex_coll_name)
    response = requests.post(url, json=vertices, auth=(username, password))
    if response.status_code != 202:
        raise RuntimeError(f"Invalid response from server during insert_vertices: {response.text}")


def insert_edges(endpoint, edges_coll_name, vertices_coll_name, edges, smart_attribute, username, password):
    '''
    Insert an edge or (typically) a list of edges into the edge collection.
    :param endpoint: the srever address
    :param edges_coll_name: the name of the edge collection
    :param vertices_coll_name: the name of the vertex collection
    :param edges: the edge or (typically) the list of edges to insert
    :param username: the username
    :param password: the password
    :return: None
    '''
    doc = dict()

    q = f'''FOR p in @edges
    LET _from = ( 
        FOR vertex IN @@vertex_coll
        FILTER vertex.{smart_attribute} == p._from
        RETURN vertex._id
    )[0]
    LET _to = ( 
        FOR vertex IN @@vertex_coll
        FILTER vertex.{smart_attribute} == p._to
        RETURN vertex._id
    )[0]
    INSERT {{_from, _to, weight: p.weight}} INTO @@edge_coll'''
    doc['query'] = q
    doc['bindVars'] = {'edges': edges, '@vertex_coll': vertices_coll_name, '@edge_coll': edges_coll_name}
    url = os.path.join(endpoint, f"_api/cursor/")
    response = requests.post(url, json=doc, auth=(username, password))
    if response.status_code != 201:
        raise RuntimeError(f"Invalid response from server during insert_edges: {response.text}")


def file_reader(filename, bulk_size):
    '''
    Yield bulk_size characters from the file with filename filename or the whole content of the file if it has less characters.
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