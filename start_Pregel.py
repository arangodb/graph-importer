#!/usr/bin/env python3
import argparse
import json
import os
from time import sleep
from typing import List, Optional

import requests

from arguments import make_global_parameters, make_database_input_parameters, make_pregel_parameters
from helper_classes import DatabaseInfo


def get_arguments():
    parser = argparse.ArgumentParser(description='Call a Pregel algorithm on the given graph in the given database.')

    # general
    make_global_parameters(parser)

    make_database_input_parameters(parser)
    make_pregel_parameters(parser)

    arguments = parser.parse_args()
    return arguments


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


def get_pregel_exec_status(db_info: DatabaseInfo, algorithm_id: str):
    """
    Returns the status (result) of the algorithm execution by its id.
    :param db_info:
    :param algorithm_id:
    :return:
    """
    url = os.path.join(db_info.endpoint, f'_api/control_pregel/{algorithm_id}')
    response = requests.get(url, auth=(db_info.username, db_info.password))

    if response.status_code != 200:
        raise RuntimeError(f'Error retrieving the execution status of the algorithm with id: {algorithm_id}. Error '
                           f'code: {response.status_code}. Error message: {response.text}. Reason: no Pregel job with '
                           f'the specified execution number is found or the execution number is invalid.')

    return response


def get_width(key: str) -> int:
    if key == 'state':
        return 25
    elif key == 'aggregators':
        return 35
    elif key == 'computationTime':
        return 20
    else:
        return 15


def print_pregel_status_variable(d: json) -> None:
    information = ''
    for key, value in d.items():
        if key in [
                # these values are always the same:
                'id', 'database', 'algorithm', 'created', 'ttl',
                # these values seem to appear only when 'state' == 'done'
                'expires', 'storageTime', 'vertexCount', 'edgeCount']:
            continue
        if type(value) == float:
            val = f'{value:.5f}'
        elif type(value) == dict and value:
            for vkey, vval in value.items():
                if type(vval) == float:
                    value[vkey] = f'{vval:.8f}'
            val = str(value)
        else:
            val = str(value)

        information += f'{val:<{get_width(key)}}'

    print(f'{information:15}')


def print_pregel_status(db_info: DatabaseInfo, algorithm_id: str, sleep_time: float):
    status = get_pregel_exec_status(db_info, algorithm_id)
    if status.status_code == 200:
        d = json.loads(status.text.strip('"'))
        print(f'id: {d["id"]}')
        print(f'database: {d["database"]}')
        print(f'algorithm: {d["algorithm"]}')
        print(f'created: {d["created"]}')
        print(f'ttl: {d["ttl"]}')
        print()
        # print column names
        first_line = ''
        for key, value in d.items():
            if key in ['id', 'database', 'algorithm', 'created', 'ttl']:
                continue
            first_line += f'{key:<{get_width(key)}}'
        print(first_line + '\n')
    else:
        raise RuntimeError(f'Pregel returned error. Error code: {status.status_code}. Message: {status.text}')

    while d['state'] == 'running' or d['state'] == 'storing' or d['state'] == 'recovering':
        sleep(sleep_time)
        print_pregel_status_variable(d)
        status = get_pregel_exec_status(db_info, algorithm_id)
        d = json.loads(status.text.strip('"'))
    print_pregel_status_variable(d)
    print()
    for key, value in d.items():
        if key in ['expires', 'storageTime', 'vertexCount', 'edgeCount']:
            print(f'{key}: {value}')

    exit(0)


if __name__ == "__main__":
    args = get_arguments()

    db_info = DatabaseInfo(args.endpoint, args.graphname,
                           isSmart=True, username=args.user, password=args.pwd)

    params = dict()

    # general algorithm parameters
    if args.store:
        params['store'] = 'true'
    if args.maxGSS:
        params['maxGSS'] = args.maxGSS
    if args.parallelism:
        params['parallelism'] = args.parallelism
    if args.asynchronous:
        params['async'] = 'true'
    if args.resultField:
        params['resultField'] = args.resultField
    if args.useMemoryMaps:
        params['useMemoryMaps'] = 'true'
    if args.shardKeyAttribute:
        params['shardKeyAttribute'] = args.shardKeyAttribute

    # pagerank
    if args.algorithm == 'pagerank':
        if args.pr_threshold:
            params['threshold'] = args.pr_threshold
        if args.pr_sourceField:
            params['sourceField'] = args.pr_sourceField

    # sssp
    if args.algorithm == 'sssp':
        if args.sssp_source:
            params['source'] = args.sssp_source
        if args.sssp_resultField:
            params['_resultField'] = args.sssp_resultField

    algorithm_id = call_pregel_algorithm(db_info, args.algorithm, params).strip('"')
    print_pregel_status(db_info, algorithm_id, args.sleep_time)
 
