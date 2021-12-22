#!/usr/bin/env python3
import argparse
import json
import os
from time import sleep
from typing import List, Optional

import requests

from helper_classes import DatabaseInfo


def get_arguments():
    parser = argparse.ArgumentParser(description='Call a Pregel algorithm on the given graph in the given database.')

    # general
    parser.add_argument('--endpoint', type=str, required=False, help='Endpoint, e.g. http://localhost:8529/_db/_system')
    parser.add_argument('--bulk_size', type=int, nargs='?', default=10000,
                        help='The number of vertices/edges written in one go.')
    parser.add_argument('--silent', action='store_true',  # default: False
                        help='Print progress and statistics.')
    parser.add_argument('--sleep_time', type=int, default=1000, help='Time in milliseconds to wait before requesting '
                                                                     'the status of the Pregel program again.')

    # database
    parser.add_argument('--user', nargs='?', default='root', help='User name for the server.')
    parser.add_argument('--pwd', nargs='?', default='', help='Password for the server.')
    parser.add_argument('--graphname', default='importedGraph', help='Name of the new graph in the database.')
    parser.add_argument('--edgeCollections', default='e', help='Name of the new edge relation in the database.')
    parser.add_argument('--vertexCollections', default='v', help='Name of the new vertex relation in the database.')

    # pregel specific
    parser.add_argument('--store', action='store_true',  # default: False
                        help='Whether the results computed by the Pregel algorithm '
                             'are written back into the source collections.')
    parser.add_argument('--maxGSS', type=int,
                        help='Execute a fixed number of iterations (or until the threshold is met).')
    parser.add_argument('--parallelism', type=int,
                        help='The maximum number of parallel threads that will execute the Pregel algorithm.')
    parser.add_argument('--asynchronous', action='store_true',
                        help='Algorithms which support asynchronous mode will run without synchronized global iterations.')
    parser.add_argument('--resultField', type=str,
                        help='The attribute of vertices to write the result into.')
    parser.add_argument('--useMemoryMaps', action='store_true',  # default: False
                        help='Whether to use disk based files to store temporary results.')
    parser.add_argument('--shardKeyAttribute', help='The shard key that edge collections are sharded after.')

    # subparsers
    subparsers = parser.add_subparsers(dest='cmd', help='''The name of the Gregel algorithm, one of:
                                            pagerank - Page Rank; 
                                            sssp - Single-Source Shortest Path; 
                                            connectedcomponents - Connected Components;
                                            wcc - Weakly Connected Components;
                                            scc - Strongly Connected Components;
                                            hits - Hyperlink-Induced Topic Search;
                                            effectivecloseness - Effective Closeness;
                                            linerank - LineRank;
                                            labelpropagation - Label Propagation;
                                            slpa - Speaker-Listener Label Propagation''')

    # pagerank
    parser_pr = subparsers.add_parser('pagerank', help='Page Rank')

    parser_pr.add_argument('--threshold', type=float,
                           help='Execute until the value changes in the vertices are at most the threshold.')
    parser_pr.add_argument('--sourceField', type=str,
                           help='The attribute of vertices to read the initial rank value from.')

    # sssp
    parser_sssp = subparsers.add_parser('sssp', help='Single-Source Shortest Path')
    parser_sssp.add_argument('--source', help='The vertex ID to calculate distances ')
    parser_sssp.add_argument('--sssp_resultField', help='The vertex ID to calculate distances ')

    arguments = parser.parse_args()

    return arguments


def call_pregel_algorithm(db_info: DatabaseInfo, algorithm_name: str,
                          vertexCollections: Optional[List[str]] = None,
                          edgeCollections: Optional[List[str]] = None,
                          params: Optional[dict] = None):
    """
    Call a Pregel algorithm. If graph_name is not None, vertexCollections and edgeCollections are ignored.
    """
    url = os.path.join(db_info.endpoint, "_api/control_pregel/")

    json = {"algorithm": algorithm_name}
    if db_info.graph_name:
        json['graphName'] = db_info.graph_name
    else:
        json['vertexCollections'] = vertexCollections
        json['edgeCollections'] = edgeCollections

    response = requests.post(url, json=json, params=params, auth=(db_info.username, db_info.password))
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
    if args.cmd == 'pagerank':
        if args.threshold:
            params['threshold'] = args.threshold
        if args.sourceField:
            params['sourceField'] = args.sourceField

        algorithm_id = call_pregel_algorithm(db_info, 'pagerank', args.edgeCollections, args.vertexCollections,
                                             params).strip('"')
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
            sleep(args.sleep_time)
            print_pregel_status_variable(d)
            status = get_pregel_exec_status(db_info, algorithm_id)
            d = json.loads(status.text.strip('"'))
        print_pregel_status_variable(d)
        print()
        for key, value in d.items():
            if key in ['expires', 'storageTime', 'vertexCount', 'edgeCount']:
                print(f'{key}: {value}')

        exit(0)
    # sssp
    if args.cmd == 'sssp':
        if args.source:
            params['source'] = args.source
        if args.sssp_resultField:
            params['_resultField'] = args.sssp_resultField
