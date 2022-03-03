#!/usr/bin/env python3
import argparse
import json
import os
from time import sleep
from typing import Optional, Union

import requests

from arguments import make_global_parameters, make_database_input_parameters, make_pregel_parameters, \
    database_parameters, database_mult_collections, query_parameters
from general import arangodIsRunning
from helper_classes import DatabaseInfo


def get_arguments():
    parser = argparse.ArgumentParser(description='Call a Pregel3 algorithm on the given graph in the given database.')

    # general
    make_global_parameters(parser)

    database_parameters(parser)
    database_mult_collections(parser)
    query_parameters(parser)

    arguments = parser.parse_args()
    return arguments


def make_query(db_info: DatabaseInfo, graph_spec: Union[dict, str], algorithm_spec: dict,
               queryId: Optional[str] = None) -> bool:
    """
    Create a Pregel3 query.

    If queryId is given, Pregel3 tries to create a query with this id. If another query with this id already exists, a
    warning is returned and no query is created. If no query id is passed, the new query obtains an automatically
    generated id.

    If no graph_spec is given or the graph_spec is not an object, a warning is returned and no query is created.
    :param db_info:
    :param algorithm_spec:
    :param params: {graph_spec: [graphName | (vertexCollectionName01, ..., edgeCollectionName01)]}
    :return:
    """
    url = os.path.join(db_info.endpoint, "_api/pregel3/queries/")
    if (queryId):
        json_ = {"queryId": queryId, "algorithmSpec": algorithm_spec, "graphSpec": graph_spec}
    else:
        json_ = {"algorithmName": algorithm_spec, "graph_spec": graph_spec}

    response = requests.post(url, json=json_, auth=(db_info.username, db_info.password))
    if response.status_code != 200:
        print('A query with this id exists already.')
        return False
    return True


def call_pregel_algorithm(db_info: DatabaseInfo, algorithm_name: str, params: Optional[dict] = None):
    """
    Call a Pregel algorithm. If graph_name is not None, vertexCollections and edgeCollections are ignored.
    """
    url = os.path.join(db_info.endpoint, "_api/control_pregel/")

    json_ = {"algorithm": algorithm_name}
    if db_info.graph_name:
        json_['graphName'] = db_info.graph_name

    if params:
        json_['params'] = params

    response = requests.post(url, json=json_, auth=(db_info.username, db_info.password))
    if response.status_code == 400:
        reason = 'the set of collections for the Pregel job includes a system collection, ' \
                 'or the collections do not conform to the sharding requirements for Pregel jobs.'
    elif response.status_code == 403:
        reason = 'there are not sufficient privileges to access the collections.'
    elif response.status_code == 403:
        reason = 'the specified \"algorithm\" is not found, or the graph specified in \"graphName\" ' \
                 'is not found, or at least one the collections specified in \"vertexCollections\" ' \
                 'or \"edgeCollections\" is not found.'
    else:
        reason = 'Unexpected error.'

    if response.status_code != 200:
        raise RuntimeError(f'Pregel returned an error. Error code: {response.status_code}. '
                           f'Message: {response.text}. Reason: {reason}')
    return response.text


if __name__ == "__main__":
    print("Starting")
    args = get_arguments()

    db_info = DatabaseInfo(args.endpoint, args.graphname,
                           isSmart=True, username=args.user, password=args.pwd)

    if not arangodIsRunning():
        raise RuntimeError('The process "arangod" is not running, please, run it first.')

    print(args)

    query_id = args.query_id

    graph_spec = dict()
    if args.graphname:
        graph_spec = args.graphname
    else:  # vertex_collections
        graph_spec["vertexCollNames"] = args.vertex_collections
        graph_spec["edgeCollNames"] = args.edge_collections

    alg_spec = dict()
    alg_spec["algorithmName"] = args.algorithm
    if alg_spec["algorithmName"] == "MinCut":
        alg_spec["capacityProp"] = args.capacity_property
        alg_spec["defaultCapacity"] = args.default_capacity
        alg_spec["sourceVertexId"] = args.source_vertex_id
        alg_spec["targetVertexId"] = args.target_vertex_id

    if make_query(db_info, graph_spec, alg_spec, query_id):
        print("SUCCESS")
