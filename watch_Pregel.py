#!/usr/bin/env python3
import argparse

from arguments import make_global_parameters, make_pregel_watch_parameters, make_database_input_parameters
from general import arangodIsRunning
from helper_classes import DatabaseInfo
from start_Pregel import print_pregel_status


def get_arguments():
    parser = argparse.ArgumentParser(description='Call a Pregel algorithm on the given graph in the given database.')

    # general
    make_global_parameters(parser)
    make_database_input_parameters(parser)
    make_pregel_watch_parameters(parser)

    arguments = parser.parse_args()

    if not arguments.algorithm_id:
        raise RuntimeError(f'Argument missing: algorithm id is needed: --algorithm_id <int>.')
    return arguments

if __name__ == "__main__":
    args = get_arguments()

    db_info = DatabaseInfo(args.endpoint, graph_name='dummy',
                           isSmart=True, username=args.user, password=args.pwd)

    if not arangodIsRunning():
        raise RuntimeError('The process "arangod" is not running, please, run it first.')

    print_pregel_status(db_info, args.algorithm_id, args.sleep_time, args.extended_info)
