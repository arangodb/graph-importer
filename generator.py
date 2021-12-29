#!/usr/bin/env python3

import argparse
import cProfile
import time

from arguments import make_database_parameters, make_general_graph_parameters_generator, make_cliques_graph_parameters
from arguments import make_k_partite_parameters, make_global_parameters, make_attribute_parameters
from clique_generator import create_one_clique_graph, create_cliques_graph
from edges_generator import get_edge_property
from general import arangodIsRunning, get_time_difference_string
from helper_classes import DatabaseInfo, GraphInfo, CliquesGraphInfo
from k_partite_generator import create_k_partite_graph
from vertices_generator import get_vertex_property


def get_arguments():
    parser = argparse.ArgumentParser(description='Import a graph from a file/files to ArangoDB.')

    make_global_parameters(parser)
    make_general_graph_parameters_generator(parser)
    make_cliques_graph_parameters(parser)
    make_k_partite_parameters(parser)
    make_database_parameters(parser)
    make_attribute_parameters(parser)

    arguments = parser.parse_args()

    # check parameters
    if arguments.additional_vertex_attribute == 'part':
        raise RuntimeError('--additional_vertex_attribute cannot be \'part\', choose another name.')
    if arguments.additional_vertex_attribute == 'id':
        raise RuntimeError('--additional_vertex_attribute cannot be \'id\', choose another name.')
    if arguments.vertex_property_type == 'none' and arguments.smart_attribute == arguments.additional_vertex_attribute:
        raise RuntimeError('If --smart_attribute is --arguments.additional_vertex_attribute, --vertex_property_type '
                           'cannot be \'none\'.')
    if arguments.make_smart and not arguments.smart_attribute:
        raise RuntimeError('If --make_smart is given, then also --smart_attribute must be given.')

    return arguments


if __name__ == "__main__":
    if not arangodIsRunning():
        raise RuntimeError('The process \'arangod\' is not running, please, run it first.')


    args = get_arguments()

    v_property = get_vertex_property(args)
    edge_property = get_edge_property(args)

    database_info = DatabaseInfo(args.endpoint, args.graphname, args.vertex_collection_name,
                                 args.edge_collection_name,
                                 args.make_smart,
                                 args.repl_factor,
                                 args.num_shards, args.overwrite, args.smart_attribute,
                                 args.additional_vertex_attribute,
                                 args.edge_attribute,
                                 args.user, args.pwd)

    g_info = GraphInfo(v_property, edge_property)

    start = time.monotonic()
    if args.graphtype == 'cliques-graph':
        clique_graph_info = CliquesGraphInfo(args.num_cliques, args.min_size_clique, args.max_size_clique,
                                             args.prob_missing_one, args.prob_missing_all,
                                             args.prob_missing_one_between
                                             )
        create_cliques_graph(database_info, g_info, clique_graph_info, args.bulk_size,
                             be_verbose=not args.silent)
    elif args.graphtype == 'clique':
        create_one_clique_graph(database_info, args.bulk_size, args.num_vertices, g_info,
                                be_verbose=not args.silent)
    elif args.graphtype == 'k-partite':
        parts_graph_info = CliquesGraphInfo(args.num_parts, args.min_size_clique, args.max_size_clique, 0.0, 0.0,
                                            0.0)
        create_k_partite_graph(database_info, g_info, parts_graph_info, args.bulk_size,
                               be_verbose=not args.silent)
    else:
        pass

    if not args.silent:
        print('Global time: ' + get_time_difference_string(time.monotonic() - start))
