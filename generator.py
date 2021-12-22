import argparse
import time
import cProfile

from clique_generator import create_one_clique_graph, create_cliques_graph
from time_tracking import TimeTracking
from helper_classes import DatabaseInfo, GraphInfo, CliquesGraphInfo
from edges_generator import get_edge_property
from general import arangodIsRunning, get_time_difference_string
from k_partite_generator import create_k_partite_graph
from vertices_generator import get_vertex_property


def get_arguments():
    parser = argparse.ArgumentParser(description='Import a graph from a file/files to ArangoDB.')

    # global
    parser.add_argument('endpoint', type=str, help='Endpoint, e.g. http://localhost:8529/_db/_system')
    parser.add_argument('--bulk_size', type=int, nargs='?', default=10000,
                        help='The number of vertices/edges written in one go.')
    parser.add_argument('--silent', action='store_true',  # default: False
                        help='Print progress and statistics.')

    # general graph parameters
    parser.add_argument('graphtype', type=str, default='clique', choices=['clique', 'cliques-graph', 'k-partite'],
                        help='Source kind')
    parser.add_argument('--hasSelfLoops', action='store_true',  # default: false
                        help='Whether the graphs should have selfloops.')
    parser.add_argument('--num_vertices', '-s', type=int, nargs='?', default=10000,
                        help='The number of vertices.')

    # cliques-graph parameters
    parser.add_argument('--num_cliques', type=int,
                        help='Number of cliques in a cliques-graph. Ignored for other graphs.')
    parser.add_argument('--min_size_clique', type=int,
                        help='Minimum clique num_vertices in a cliques-graph. Ignored for other graphs.')
    parser.add_argument('--max_size_clique', type=int,
                        help='Maximum clique num_vertices in a cliques-graph. Ignored for other graphs.')
    parser.add_argument('--prob_missing_one', type=float,
                        help='The probability for an edge in a clique to be missing  in a cliques-graph. '
                             'Ignored for other graphs.')
    parser.add_argument('--prob_missing_all', type=float,
                        help='The probability that there are edges between two cliques in a cliques-graph. '
                             'Ignored for other graphs.')
    parser.add_argument('--prob_missing_one_between', type=float, default=0.5,
                        help='The probability for an edge between two parts to be missing in a cliques-graph or in '
                             'a k-partite graph.')

    # k-partite parameters
    parser.add_argument('--num_parts', '-k', type=int,
                        help='Number of parts in a k-partite graph. Ignored for other graphs.')
    parser.add_argument('--min_size_part', type=int,
                        help='Minimum part num_vertices in a k-partite graph. Ignored for other graphs.')
    parser.add_argument('--max_size_part', type=int,
                        help='Maximum part num_vertices in a k-partite graph. Ignored for other graphs.')

    # database parameters
    parser.add_argument('--user', nargs='?', default='root', help='User name for the server.')
    parser.add_argument('--pwd', nargs='?', default='', help='Password for the server.')
    parser.add_argument('--graphname', default='generatedGraph', help='Name of the new graph in the database.')
    parser.add_argument('--edge_collection_name', default='e', help='Name of the new edge collection in the database.')
    parser.add_argument('--vertex_collection_name', default='v', help='Name of the new vertex collection'
                                                                      ' in the database.')
    parser.add_argument('--num_shards', default=5, type=int, help='Number of shards.')
    parser.add_argument('--repl_factor', default=2, type=int, help='Replication factor.')
    parser.add_argument('--smart_attribute', default='smartProp',
                        help='The name of the attribute to shard the vertices after.')
    parser.add_argument('--overwrite', action='store_true',  # default: false
                        help='Overwrite the graph and the collection if they already exist.')
    parser.add_argument('--make_smart', action='store_true',  # default: false
                        help='Create a smart graph.')

    # attributes
    # todo: check allow input file
    parser.add_argument('--vertex_property_type', nargs='?', choices=['none', 'random'], default='none',
                        help="""Vertex property_ kind. Default is \'none\', then --vertex_property is ignored and 
                         no properties are saved. If \'random\', --vertex_property should contain two numbers a and b; 
                         the property_ value is chosen randomly in [a, b).""")
    parser.add_argument('--vertex_property', nargs='+', help="""Vertex property_. This parameter must be given 
                            if and only if --vertex_property_type is not skipped and not \'none\', 
                            otherwise an exception is thrown. If --vertex_property_type is \'random\',
                            two numbers a,b must be given with a <= b, the value for the property_ will be chosen
                            randomly with equal probability from the interval [a,b).""")
    parser.add_argument('--edge_property_type', nargs='?', choices=['none', 'random'], default='none',
                        help="""Edge property_ kind. Default is \'none\', then --edge_property is ignored and 
                             the default property_ 'weight' with values Null is saved. 
                             If \'random\', --edge_property should contain two numbers a and b; 
                             the property_ value is chosen randomly in [a, b).""")
    parser.add_argument('--edge_property', nargs='+', help="""Edge property_. This parameter must be given 
                          if and only if --edge_property_type is not skipped and not \'none\', 
                          otherwise an exception is thrown. If skipped, the default property_ 'weight' 
                          with values Null is saved. 
                          If --edge_property_type is \'random\', two numbers a,b must be given with a <= b, 
                          the value for the property_ will be chosen randomly with equal probability from 
                          the interval [a,b).""")
    parser.add_argument('--additional_vertex_attribute', default='color',
                        help="""Additional vertex attribute name used for --vertex_property. Default is \'color\'.
                        Cannot be \'part\'.""")
    parser.add_argument('--edge_attribute', default='weight',
                        help="""Edge attribute name used for --edge_property. Default is \'weight\'.""")

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

    with cProfile.Profile() as profile:

        args = get_arguments()

        v_property = get_vertex_property(args)
        edge_property = get_edge_property(args)

        database_info = DatabaseInfo(args.endpoint, args.graphname, args.certex_collection_name,
                                     args.edge_collection_name,
                                     args.make_smart,
                                     args.repl_factor,
                                     args.num_shards, args.overwrite, args.smart_attribute,
                                     args.additional_vertex_attribute,
                                     args.edge_attribute,
                                     args.user, args.pwd)

        g_info = GraphInfo(args.hasSelfLoops, v_property, edge_property)

        time_tracking = TimeTracking()
        start = time.monotonic()
        if args.graphtype == 'cliques-graph':
            clique_graph_info = CliquesGraphInfo(args.num_cliques, args.min_size_clique, args.max_size_clique,
                                                 args.prob_missing_one, args.prob_missing_all,
                                                 args.prob_missing_one_between
                                                 )
            create_cliques_graph(database_info, g_info, clique_graph_info, args.bulk_size, time_tracking, not args.silent)
        elif args.graphtype == 'clique':
            create_one_clique_graph(database_info, args.bulk_size, args.num_vertices, g_info, time_tracking)
        elif args.graphtype == 'k-partite':
            parts_graph_info = CliquesGraphInfo(args.num_parts, args.min_size_clique, args.max_size_clique, 0.0, 0.0)
            create_k_partite_graph(database_info, g_info, parts_graph_info, args.bulk_size, time_tracking)
        else:
            pass

        if not args.silent:
            print('Global time: ' + get_time_difference_string(time.monotonic() - start))
            print('Time inserting vertices: ' + get_time_difference_string(time_tracking.insert_vertices_time))
            print('Time preparing vertices: ' + get_time_difference_string(time_tracking.prepare_vertices_time))
            if args.graphtype == 'clique' or args.graphtype == 'cliques-graph':
                print('Time making and inserting edges in cliques: ' + get_time_difference_string(
                    time_tracking.make_and_insert_clique_edges_time))
            print('Time making edges: ' + get_time_difference_string(time_tracking.add_edge_time))
            if args.graphtype != 'clique':
                print('Time connecting parts: ' + get_time_difference_string(time_tracking.connect_parts_time))
            print('Time inserting edges: ' + get_time_difference_string(time_tracking.insert_edges_time))

