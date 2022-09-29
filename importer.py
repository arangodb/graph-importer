#!/usr/bin/env python3
import argparse
import time

from arguments import make_global_parameters, make_database_parameters, make_importer_files_parameters
from edge_list import import_edge_list
from general import get_time_difference_string
from graphalytics_importer import import_graphalytics_get_files_from_directory, import_graphalytics
from helper_classes import DatabaseInfo, GraphInfo, VertexOrEdgeProperty


def get_arguments():
    parser = argparse.ArgumentParser(description='Import a graph from a file/files to ArangoDB.')

    make_global_parameters(parser)
    make_database_parameters(parser)
    make_importer_files_parameters(parser)

    parser.add_argument('sourcetype', type=str, nargs='?', default='edge-list',
                        choices=['edge-list', 'graphalytics'],
                        help='Source kind')

    arguments = parser.parse_args()

    # check arguments
    if arguments.sourcetype == 'graphalytics' and not arguments.dir_graphalytics and not (
            arguments.vertices_file_graphalytics and arguments.edges_file_graphalytics and
            arguments.properties_file_graphalytics):
        raise Exception(
            'With sourcetype graphalytics, either --dir_graphalytics, or all of --vertices_file_graphalytics, '
            '--edges_file_graphalytics and --properties_file_graphalytics must be given.')
    if arguments.sourcetype == 'edge-list' and not arguments.edges_file_edge_list:
        raise Exception(
            'With sourcetype edge-list, edges_file_edge_list must be given.')

    return arguments


if __name__ == "__main__":

    args = get_arguments()

    db_info = DatabaseInfo(args.endpoint, args.graphname, args.vertex_collection_name,
                           args.edge_collection_name, args.make_smart,
                           args.repl_factor, args.num_shards, args.overwrite, args.smart_attribute,
                           '', 'weight', args.user, args.pwd)

    vertex_property = VertexOrEdgeProperty('none')
    edge_property = VertexOrEdgeProperty('none')
    graph_info = GraphInfo(vertex_property=vertex_property, edge_property=edge_property)

    if args.sourcetype == 'graphalytics':
        if args.dir_graphalytics:
            vertices_filename, edges_filename, properties_filename = import_graphalytics_get_files_from_directory(
                args.dir_graphalytics)
        else:
            vertices_filename = args.vertices_file_graphalytics
            edges_filename = args.edges_file_graphalytics
            properties_filename = args.properties_file_graphalytics

        start = time.monotonic()
        import_graphalytics(db_info, vertices_filename, edges_filename, properties_filename, args.bulk_size,
                            not args.silent)
        if not args.silent:
            print('Total time: ' + get_time_difference_string(time.monotonic() - start))
        exit(0)
    if args.sourcetype == 'edge-list':
        start = time.monotonic()
        import_edge_list(db_info, args.edges_file_edge_list, args.bulk_size, not args.silent)
        if not args.silent:
            print('Total time: ' + get_time_difference_string(time.monotonic() - start))
        exit(0)
