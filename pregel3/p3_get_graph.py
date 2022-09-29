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


def get_graph(endpoint: str, user: str, passw: str, query_id: str) -> None:
    """
    Get the graph from the query with the given query_id. If no query with query_id exists, an error is returned.
    """
    url = os.path.join(endpoint, "_api/pregel3/queries/" + query_id + "/getGraph")

    response = requests.get(url, auth=(username, password))
    if response.status_code != 200:
        print(json.loads(response.content)['errorMessage'])
    else:
        print(json.loads(response.content)['result'])


if __name__ == "__main__":
    print("Getting the graph")
    args = get_arguments()

    endpoint = args.endpoint
    username = args.user
    password = args.pwd
    query_id = args.query_id

    if not arangodIsRunning():
        raise RuntimeError('The process "arangod" is not running, please, run it first.')

    get_graph(endpoint, username, password, query_id)
