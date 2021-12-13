#!/usr/bin/env python
import argparse

from databaseinfo import DatabaseInfo, GraphInfo, VertexOrEdgeProperty
from edge_list import import_edge_list
from graphalytics_importer import import_graphalytics_get_files, import_graphalytics

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Import a graph from a file/files to ArangoDB.')

    parser.add_argument('endpoint', type=str, help='Endpoint, e.g. http://localhost:8529/_db/_system')
    parser.add_argument('sourcetype', type=str, nargs='?', default='edge-list',
                        choices=['edge-list', 'graphalytics'],
                        help='Source kind')
    parser.add_argument('--dir_graphalytics', '-d', type=str, nargs='?',
                        help='For Graphalytics graphs, the directory containing the files. '
                             'If given, overwrites possible arguments --vgf, --egf and --pgf.')
    parser.add_argument('--vertices_file_graphalytics', '-v', type=str, nargs='?',
                        help='For Graphalytics graphs, the file containing the vertices.')
    parser.add_argument('--edges_file_graphalytics', type=str, nargs='?',
                        help='For Graphalytics graphs, the file containing the edges.')
    parser.add_argument('--edges_file_edge_list', type=str, nargs='?',
                        help='For graphs given by an edge list, the file containing the edges.')
    parser.add_argument('--properties_file_graphalytics', '-p', type=str, nargs='?',
                        help='For Graphalytics graphs, the file containing the graph properties.')
    parser.add_argument('--bulk_size', type=int, nargs='?', default=10000,
                        help='The number of vertices/edges written in one go.')
    parser.add_argument('--enforce_undirected', '-u', action='store_true',  # default: false
                        help='Make the edge relation symmetric. For Graphalytics graphs, '
                             'overwrites the specification from the file.')
    parser.add_argument('--user', nargs='?', default='root', help='User name for the server.')
    parser.add_argument('--pwd', nargs='?', default='', help='Password for the server.')
    parser.add_argument('--graphname', default='importedGraph', help='Name of the new graph in the database.')
    parser.add_argument('--edges', default='e', help='Name of the new edge relation in the database.')
    parser.add_argument('--vertices', default='v', help='Name of the new vertex relation in the database.')
    parser.add_argument('--num_shards', default=5, type=int, help='Number of shards.')
    parser.add_argument('--repl_factor', default=2, type=int, help='Replication factor.')
    parser.add_argument('--smart_attribute', default='smartProp',
                        help='The name of the field to shard the vertices after.')
    parser.add_argument('--overwrite', action='store_true',  # default: false
                        help='Overwrite the graph and the collection if they already exist.')

    args = parser.parse_args()

    # check arguments
    if args.sourcetype == 'graphalytics':
        if not args.dir_graphalytics and not (
                args.vertices_file_graphalytics and args.edges_file_graphalytics and args.properties_file_graphalytics):
            raise Exception(
                'With sourcetype graphalytics, either --dir_graphalytics, or all of --vertices_file_graphalytics, '
                '--edges_file_graphalytics and --properties_file_graphalytics must be given.')
    if args.sourcetype == 'edge-list':
        if not args.edges_file_edge_list:
            raise Exception(
                'With sourcetype edge-list, edges_file_edge_list must be given.')

    db_info = DatabaseInfo(args.endpoint, args.graphname, args.vertices, args.edges,
                           args.repl_factor, args.num_shards, args.overwrite, args.smart_attribute,
                           '', 'weight', args.user, args.pwd)

    vertex_property = VertexOrEdgeProperty('none')
    edge_property = VertexOrEdgeProperty('none')
    graph_info = GraphInfo(hasSelfLoops=False, isDirected=not args.enforce_undirected, vertex_property=vertex_property,
                           edge_property=edge_property)
    # for graphalytics, get file names from parameters
    if args.sourcetype == 'graphalytics':
        if args.dir_graphalytics:
            vertices_filename, edges_filename, properties_filename = import_graphalytics_get_files(
                args.dir_graphalytics)
        else:
            vertices_filename = args.vertices_file_graphalytics
            edges_filename = args.edges_file_graphalytics
            properties_filename = args.properties_file_graphalytics

        import_graphalytics(db_info, graph_info, vertices_filename, edges_filename, properties_filename, args.bulk_size)
        exit(0)
    if args.sourcetype == 'edge-list':
        import_edge_list(db_info, graph_info, args.edges_file_edge_list, args.bulk_size)
        exit(0)
