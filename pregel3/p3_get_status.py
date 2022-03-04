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

    parser.add_argument('--endpoint', required=True, help='Endpoint, e.g. http://localhost:8529/_db/_system')
    parser.add_argument('--user', nargs='?', default='root', help='User name for the server.')
    parser.add_argument('--pwd', nargs='?', default='', help='Password for the server.')
    parser.add_argument('--query_id', help='The query id.')

    arguments = parser.parse_args()
    return arguments


def get_status(endpoint: str, user: str, passw: str, query_id: str) -> bool:
    """
    Return the status of the wuery with the given query_id. If no query with query_id exists, an error is returned.
    """
    url = os.path.join(endpoint, "_api/pregel3/queries/" + query_id);

    response = requests.get(url, auth=(user, passw))
    if response.status_code != 200:
        print('No query with this query id was found.')
        return False
    return True


if __name__ == "__main__":
    print("Start loading graph")
    args = get_arguments()

    endpoint = args.endpoint
    username = args.user
    password = args.pwd
    query_id = args.query_id

    if not arangodIsRunning():
        raise RuntimeError('The process "arangod" is not running, please, run it first.')

    print(args)

    if get_status(endpoint, username, password, query_id):
        print("Graph loaded")
