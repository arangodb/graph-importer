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

    arguments = parser.parse_args()
    return arguments


def get_all_queries(endpoint: str, username: str, password: str):
    """
    Gets all query ids.
    """
    url = os.path.join(endpoint, "_api/pregel3/queries")

    response = requests.get(url, auth=(username, password))
    if response.status_code != 200:
        print(json.loads(response.content)['errorMessage'])
        return False
    print(json.loads(response.content)['result'])
    return True


if __name__ == "__main__":
    print("Getting all query ids.")
    args = get_arguments()

    if not arangodIsRunning():
        raise RuntimeError('The process "arangod" is not running, please, run it first.')

    get_all_queries(args.endpoint, args.user, args.pwd)