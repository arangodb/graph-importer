import argparse


def make_global_parameters(parser: argparse.ArgumentParser) -> None:
    parser.add_argument('--endpoint', required=True, help='Endpoint, e.g. http://localhost:8529/_db/_system')
    parser.add_argument('--bulk_size', type=int, nargs='?', default=10000,
                        help='The number of vertices/edges written in one go.')
    parser.add_argument('--silent', action='store_true',  # default: False
                        help='Print progress and statistics.')


def database_parameters(parser: argparse.ArgumentParser) -> None:
    parser.add_argument('--user', nargs='?', default='root', help='User name for the server.')
    parser.add_argument('--pwd', nargs='?', default='', help='Password for the server.')
    parser.add_argument('--graphname', default='generatedGraph', help='Name of the new graph in the database.')


def database_mult_collections(parser: argparse.ArgumentParser) -> None:
    parser.add_argument('--vertex_collections', type=str, nargs='+',
                        help='Names of the vertex collections in the database (separator: space).')
    parser.add_argument('--edge_collections', type=str, nargs='+',
                        help='Names of the edge collections in the database (separator: space).')


def make_database_input_parameters(parser: argparse.ArgumentParser) -> None:
    """
    Make database parameters necessary to use an existing graph.
    :param parser: the parser to add parameters
    :return: None
    """
    database_parameters(parser)
    parser.add_argument('--edge_collection_name', default='e', help='Name of the new edge collection in the database.')
    parser.add_argument('--vertex_collection_name', default='v', help='Name of the new vertex collection'
                                                                      ' in the database.')


def make_database_parameters(parser: argparse.ArgumentParser) -> None:
    make_database_input_parameters(parser)

    parser.add_argument('--num_shards', default=5, type=int, help='Number of shards.')
    parser.add_argument('--repl_factor', default=2, type=int, help='Replication factor.')
    parser.add_argument('--smart_attribute', default='smartProp',
                        help='The name of the attribute to shard the vertices after.')
    parser.add_argument('--overwrite', action='store_true',  # default: false
                        help='Overwrite the graph and the collection if they already exist.')
    parser.add_argument('--make_smart', action='store_true',  # default: false
                        help='Create a smart graph (ignored for benchmarking with Pregel).')


def make_general_graph_parameters_generator(parser: argparse.ArgumentParser) -> None:
    parser.add_argument('graphtype', type=str, default='clique', choices=['clique', 'cliques-graph', 'k-partite'],
                        help='Source kind')
    parser.add_argument('--num_vertices', '-s', type=int, nargs='?', default=10000,
                        help='The number of vertices.')


def make_cliques_graph_parameters(parser: argparse.ArgumentParser) -> None:
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


def make_k_partite_parameters(parser: argparse.ArgumentParser) -> None:
    parser.add_argument('--num_parts', '-k', type=int,
                        help='Number of parts in a k-partite graph. Ignored for other graphs.')
    parser.add_argument('--min_size_part', type=int,
                        help='Minimum part num_vertices in a k-partite graph. Ignored for other graphs.')
    parser.add_argument('--max_size_part', type=int,
                        help='Maximum part num_vertices in a k-partite graph. Ignored for other graphs.')


def make_attribute_parameters(parser: argparse.ArgumentParser) -> None:
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


def make_importer_files_parameters(parser: argparse.ArgumentParser) -> None:
    parser.add_argument('--dir_graphalytics', default='.', type=str, nargs='?',
                        help='For Graphalytics graphs, the directory containing the files. '
                             'If given, overwrites possible arguments --vgf, --egf and --pgf.')
    parser.add_argument('--vertices_file_graphalytics', type=str, nargs='?',
                        help='For Graphalytics graphs, the file containing the vertices.')
    parser.add_argument('--edges_file_graphalytics', type=str, nargs='?',
                        help='For Graphalytics graphs, the file containing the edges.')
    parser.add_argument('--properties_file_graphalytics', type=str, nargs='?',
                        help='For Graphalytics graphs, the file containing the properties of the graph.')
    parser.add_argument('--edges_file_edge_list', default='graph.txt', type=str, nargs='?',
                        help='For graphs given by an edge list, the file containing the edges.')


def make_pregel_watch_parameters(parser: argparse.ArgumentParser) -> None:
    parser.add_argument('--sleep_time', type=int, default=1, help='Time in seconds to wait before requesting '
                                                                  'the status of the Pregel program again.')
    parser.add_argument('--extended_info', action='store_true',  # default: False
                        help='Show extended information (in addition to the official API.)')
    parser.add_argument('--no_watch', action='store_true',  # default: False
                        help='Regularly print the status of the Pregel run.')
    parser.add_argument('--max_num_states', type=int, default=10000000,  # there is no inf for int in pyhton3
                        help='Maximum number of states to be qeueried and printed.')
    parser.add_argument('--algorithm_id', type=int,
                        help='For watching a Pregel run without starting it, the algorithm id.')


def query_parameters(parser: argparse.ArgumentParser) -> None:
    parser.add_argument('--query_id', help='The query id.')
    parser.add_argument('--vertex_properties', nargs='*',
                        help='Vertex properties that should be loaded from vertex documents.')
    parser.add_argument('--edge_properties', nargs='*',
                        help='Edges properties that should be loaded from edge documents.')
    parser.add_argument('--algorithm', help='The name of the algorithm.')
    parser.add_argument('--capacity_property',
                        help='The name of the field of edges containing the capacity (for MinCut).')
    parser.add_argument('--default_capacity', type=float,
                        help='Default capacity for edges (for MinCut).')
    parser.add_argument('--source_vertex_id', help='The id of the source vertex (for MinCut).')
    parser.add_argument('--target_vertex_id', help='The id of the target vertex (for MinCut).')



def make_pregel_parameters(parser: argparse.ArgumentParser) -> None:
    parser.add_argument('--store', action='store_true',  # default: False
                        help='Whether the results computed by the Pregel algorithm '
                             'are written back into the source collections.')
    parser.add_argument('--maxGSS', type=int,
                        help='Execute a fixed number of iterations (or until the threshold is met).')
    parser.add_argument('--parallelism', type=int,
                        help='The maximum number of parallel threads that will execute the Pregel algorithm.')
    parser.add_argument('--asynchronous', action='store_true', help='Algorithms which support asynchronous mode '
                                                                    'will run without synchronized global iterations.')
    parser.add_argument('--resultField', type=str,
                        help='The attribute of vertices to write the result into.')
    parser.add_argument('--useMemoryMaps', action='store_true',  # default: False
                        help='Whether to use disk based files to store temporary results.')
    parser.add_argument('--shardKeyAttribute', help='The shard key that edge collections are sharded after.')

    # Pregel watching
    make_pregel_watch_parameters(parser)

    # the algorithm
    parser.add_argument('algorithm', help='''The name of the Pregel algorithm, one of:
                                             pagerank - Page Rank; 
                                             sssp - Single-Source Shortest Path; 
                                             connectedcomponents - Connected Components;
                                             wcc - Weakly Connected Components;
                                             scc - Strongly Connected Components;
                                             hits - Hyperlink-Induced Topic Search;
                                             effectivecloseness - Effective Closeness;
                                             linerank - LineRank;
                                             labelpropagation - Label Propagation;
                                             slpa - Speaker-Listener Label Propagation''',
                        choices=['pagerank', 'sssp', 'connectedcomponents', 'wcc', 'scc', 'hits', 'effectivecloseness',
                                 'linerank', 'labelpropagation', 'slpa'])
    # pagerank
    parser.add_argument('--pr_threshold', type=float,
                        help='If \'algorithm\' is \'pagerank\', execute until the value changes in the vertices '
                             'are at most pr_threshold. Otherwise ignored.')
    parser.add_argument('--pr_sourceField', type=str,
                        help='If \'algorithm\' is \'pagerank\', the attribute of vertices to read the initial '
                             'rank value from. Otherwise ignored.')

    # sssp
    parser.add_argument('--sssp_source', help='If \'algorithm\' is \'sssp\', the vertex ID to calculate distances.'
                                              ' Otherwise ignored.')
    parser.add_argument('--sssp_resultField', help='If \'algorithm\' is \'sssp\', the vertex ID to calculate '
                                                   'distance. Otherwise ignored.')
