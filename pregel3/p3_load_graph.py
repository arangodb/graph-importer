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
    parser.add_argument('--query_id', help='The query id.')

    arguments = parser.parse_args()
    return arguments


def load_graph(db_info: DatabaseInfo, query_id: str) -> bool:
    """
    Load a graph with the given query_id. If no query with query_id exists, an error is returned.
    """
    url = os.path.join(db_info.endpoint, "_api/pregel3/queries/" + query_id + "/loadGraph");

    response = requests.get(url, auth=(db_info.username, db_info.password))
    if response.status_code != 200:
        print(json.loads(response.content)['errorMessage'])
        return False
    return True


if __name__ == "__main__":
    print("Start loading graph")
    args = get_arguments()

    db_info = DatabaseInfo(args.endpoint, args.graphname,
                           isSmart=True, username=args.user, password=args.pwd)

    if not arangodIsRunning():
        raise RuntimeError('The process "arangod" is not running, please, run it first.')

    query_id = args.query_id

    if load_graph(db_info, query_id):
        print("Graph loaded")
