import argparse
import random
import time
from typing import Union, List, Dict, Callable

from clique_generator import CliquesHelper, create_clique_graph, create_cliques_graph
from databaseinfo import DatabaseInfo, GraphInfo, CliquesGraphInfo, VertexOrEdgeProperty
from general import insert_documents, yes_with_prob, arangodIsRunning, \
    get_time_difference_string


def make_vertices(graph_info: GraphInfo,
                  db_info: DatabaseInfo, size: int,
                  bulk_size: int):
    c_begin = graph_info.next_id
    c_end = graph_info.next_id + size
    if graph_info.vertex_property.type == "none":
        while graph_info.next_id + bulk_size <= c_end:
            yield [{f'{db_info.smart_attribute}': vid, 'clique': c_begin} for vid in
                   range(graph_info.next_id, graph_info.next_id + bulk_size)]
            graph_info.next_id += bulk_size
        yield [{f'{db_info.smart_attribute}': vid, 'clique': c_begin} for vid in
               range(graph_info.next_id, c_end)]
        graph_info.next_id += c_end
    elif graph_info.vertex_property.type == "list":
        if len(graph_info.vertex_property.list) != size:
            raise RuntimeError(
                f'make_vertices: the length of vertex_property ({len(graph_info.vertex_property.list)}) '
                f'must be equal to size ({size}).')
        while graph_info.next_id + bulk_size <= c_end:
            yield [{f'{db_info.smart_attribute}': vid,
                    f'{db_info.additional_vertex_attribute}': graph_info.vertex_property.list[vid],
                    'clique': c_begin} for vid in
                   range(graph_info.next_id, graph_info.next_id + bulk_size)]
            graph_info.next_id += bulk_size
        yield [{f'{db_info.smart_attribute}': vid,
                f'{db_info.additional_vertex_attribute}': graph_info.vertex_property.list[vid],
                'clique': c_begin} for vid in
               range(graph_info.next_id, c_end)]
        graph_info.next_id += c_end
    # random values, kind(vertex_property) == tuple
    elif graph_info.vertex_property.type == "random":
        while graph_info.next_id + bulk_size <= c_end:
            yield [
                {f'{db_info.smart_attribute}': str(vid),
                 f'{db_info.additional_vertex_attribute}': str(
                     random.uniform(float(graph_info.vertex_property.min), float(graph_info.vertex_property.max))),
                 'clique': c_begin}
                for vid in
                range(graph_info.next_id, graph_info.next_id + bulk_size)]
            graph_info.next_id += bulk_size
        yield [
            {f'{db_info.smart_attribute}': str(vid),
             f'{db_info.additional_vertex_attribute}': str(
                 random.uniform(float(graph_info.vertex_property.min), float(graph_info.vertex_property.max)))
                , 'clique': c_begin}
            for vid in
            range(graph_info.next_id, c_end)]
        graph_info.next_id += c_end
    else:
        raise RuntimeError(
            f"Wrong vertex property kind: {graph_info.vertex_property.type}. "
            f"Allowed values are \'none\', \'list\' and \'random\'.")


# todo make a recordable seed

def append_edges(edges: List[Dict], isDirected: bool, f: int, t: int, to_v: Callable[[Union[int, str]], str],
                 attr_name: str = None, attr_value: str = None):
    doc = {"_from": to_v(f), "_to": to_v(t)}
    if attr_name:
        doc[attr_name] = attr_value
    edges.append(doc)
    if not isDirected:
        doc = {"_from": to_v(t), "_to": to_v(f)}
        if attr_name:
            doc[attr_name] = attr_value
        edges.append(doc)

def add_edge(i: int, j: int, edges: List, prob_missing: float, db_info: DatabaseInfo,  graph_info: GraphInfo,
             pos_in_prop: int,
             to_v: Callable[[Union[int, str]], str]):
    if yes_with_prob(prob_missing):
        return False
    if graph_info.edge_property.type == 'none':
        append_edges(edges, graph_info.isDirected, i, j, to_v)
    elif graph_info.edge_property.type == 'list':
        append_edges(edges, graph_info.isDirected, i, j, to_v, db_info.edge_coll_name,
                     graph_info.edge_property.list[pos_in_prop])
    elif graph_info.edge_property.type == 'random':
        append_edges(edges, graph_info.isDirected, i, j, to_v, db_info.edge_coll_name,
                     str(random.uniform(graph_info.edge_property.min, graph_info.edge_property.max)))
    else:
        raise RuntimeError(
            f"Wrong vertex property kind: {graph_info.vertex_property.type}. "
            f"Allowed values are \'none\', \'list\' and \'random\'.")  # todo refactor


def make_and_insert_vertices(db_info: DatabaseInfo,
                             graph_info: GraphInfo, c_helper: CliquesHelper, size: int, bulk_size: int):
    for vertices in make_vertices(graph_info, db_info, size, bulk_size):
        insert_documents(db_info, vertices, db_info.vertices_coll_name)
    c_helper.update(size)


if __name__ == "__main__":
    if not arangodIsRunning():
        raise RuntimeError('The process \'arangod\' is not running, please, run it first.')

    parser = argparse.ArgumentParser(description='Import a graph from a file/files to ArangoDB.')

    # global
    parser.add_argument('endpoint', type=str, help='Endpoint, e.g. http://localhost:8529/_db/_system')
    parser.add_argument('--bulk_size', type=int, nargs='?', default=10000,
                        help='The number of vertices/edges written in one go.')

    # general graph parameters
    parser.add_argument('graphtype', type=str, default='clique', choices=['clique', 'cliques-graph'],
                        help='Source kind')
    parser.add_argument('--hasSelfLoops', action='store_true',  # default: false
                        help='Whether the graphs should have selfloops.')
    parser.add_argument('--size', '-s', type=int, nargs='?', default=10000,
                        help='The number of vertices.')

    # cliques-graph parameters
    parser.add_argument('--num_cliques', type=int,
                        help='Number of cliques in a cliques-graph. Ignored for other graphs.')
    parser.add_argument('--min_size_clique', type=int,
                        help='Minimum clique size in a cliques-graph. Ignored for other graphs.')
    parser.add_argument('--max_size_clique', type=int,
                        help='Maximum clique size in a cliques-graph. Ignored for other graphs.')
    parser.add_argument('--prob_missing', type=float,
                        help='The probability for an edge in a clique to be missing  in a cliques-graph. '
                             'Ignored for other graphs.')
    parser.add_argument('--inter_cliques_density', type=float,
                        help='The probability that there are edges between two cliques in a cliques-graph. '
                             'Ignored for other graphs.')
    parser.add_argument('--density_between_two_cliques', type=float, default=[0.5],
                        help='The density of edges between two cliques, i.e., if the cliques have sizes s1 and s2, '
                             'and there are m edges between the two cliques, the density is m/(s1*s2).')

    # database parameters
    parser.add_argument('--user', nargs='?', default='root', help='User name for the server.')
    parser.add_argument('--pwd', nargs='?', default='', help='Password for the server.')
    parser.add_argument('--graphname', default='generatedGraph', help='Name of the new graph in the database.')
    parser.add_argument('--edges', default='e', help='Name of the new edge relation in the database.')
    parser.add_argument('--vertices', default='v', help='Name of the new vertex relation in the database.')
    parser.add_argument('--num_shards', default=5, type=int, help='Number of shards.')
    parser.add_argument('--repl_factor', default=2, type=int, help='Replication factor.')
    parser.add_argument('--smart_attribute', default='smartProp',
                        help='The name of the attribute to shard the vertices after.')
    parser.add_argument('--overwrite', action='store_true',  # default: false
                        help='Overwrite the graph and the collection if they already exist.')

    # attributes
    # todo: check allow input file
    parser.add_argument('--vertex_property_type', nargs='?', choices=['none', 'random', 'list'], default='none',
                        help="""Vertex property kind. Default is \'none\', then --vertex_property is ignored and 
                         no properties are saved. If \'random\', --vertex_property should contain two numbers a and b; 
                         the property value is chosen randomly in [a, b). If \'list\', --vertex_property should
                         contain a list of values of length exactly the number of vertices 
                         in the graph.""")
    parser.add_argument('--vertex_property', nargs='+', help="""Vertex property. This parameter must be given 
                            if and only if --vertex_property_type is not skipped and not \'none\', 
                            otherwise an exception is thrown. If --vertex_property_type is \'random\',
                            two numbers a,b must be given with a <= b, the value for the property will be chosen
                            randomly with equal probability from the interval [a,b). 
                            If --vertex_property_type is \'list\', a list of values of length exactly the number
                            of vertices in the graph must be given.""")
    # todo make as for vertices
    parser.add_argument('--edge_property_type', nargs='?', choices=['none', 'random', 'list'], default='none',
                        help="""Edge property kind. Default is \'none\', then --edge_prop is ignored and 
                             the default property 'weight' with values Null is saved. 
                             If \'random\', --edge_prop should contain two numbers a and b; 
                             the property value is chosen randomly in [a, b). If \'list\', --edge_prop should
                             contain a list of values of length exactly the number of
                            vertices in the graph squared must be given (regardless of the actual number of edges).""")
    parser.add_argument('--edge_prop', nargs='+', help="""Edge property. This parameter must be given 
                          if and only if --edge_property_type is not skipped and not \'none\', 
                          otherwise an exception is thrown. If skipped, the default property 'weight' 
                          with values Null is saved. 
                          If --edge_property_type is \'random\', two numbers a,b must be given with a <= b, 
                          the value for the property will be chosen randomly with equal probability from 
                          the interval [a,b). 
                          If --edge_property_type is \'list\', a list of values of length exactly the number 
                          of vertices in the graph squared must be given (regardless of the actual number of edges).""")
    parser.add_argument('--additional_vertex_attribute', default='color',
                        help="""Additional vertex attribute name used for --vertex_property. Default is \'color\'.""")
    parser.add_argument('--edge_attribute', default='weight',
                        help="""Edge attribute name used for --edge_prop. Default is \'weight\'.""")

    args = parser.parse_args()

    if not args.vertex_property_type or args.vertex_property_type == 'none':
        v_property = None
    elif args.vertex_property_type == 'random':
        if len(args.vertex_property) != 2:
            raise RuntimeError(
                'If --vertex_property_type is \'random\', --vertex_property must have exactly two arguments.')
        v_property = VertexOrEdgeProperty('random', float(args.vertex_property[0]), float(args.vertex_property[1]))
    else:  # remains list values
        if args.graphtype == 'clique':
            if len(args.vertex_property) < args.size:
                raise RuntimeError(
                    'If --vertex_property_type is \'list\' and --graphtype == \'clique\', --vertex_property must have '
                    'exactly --size many arguments.')
        if args.graphtype == 'cliques-graph':
                raise RuntimeError(
                    'If --graphtype == \'cliques-graph\', --vertex_property_type == \'list\' is not allowed.')
        v_property = VertexOrEdgeProperty('list', val_list=list(args.vertex_property))

    if not args.edge_property_type or args.edge_property_type == 'none':
        edge_property = None
    elif args.edge_property_type == 'random':
        if len(args.edge_prop) != 2:
            raise RuntimeError(
                'If --edge_property_type is \'random\', --edge_prop must have exactly two arguments.')
        edge_property = VertexOrEdgeProperty('random', float(args.edge_prop[0]), float(args.edge_prop[1]))
    else:  # remains list values
        if args.graphtype == 'clique':
            if len(args.edge_prop) < args.size * args.size:
                raise RuntimeError(
                    'If --edge_property_type is \'list\' and --graphtype == \'clqiue\', --edge_prop must have '
                    'at least (--size)^2 many arguments.')
        if args.graphtype == 'cliques-graph':
            if len(args.edge_prop) < (args.num_cliques * args.max_size_clique) ** 2:
                raise RuntimeError(
                    'If --edge_property_type is \'list\' and --graphtype == \'clqiues-graph\', --edge_prop must have '
                    'at least (args.num_cliques * args.max_size_clique)^2 many arguments.')

        edge_property = VertexOrEdgeProperty('list', val_list=list(args.edge_prop))

    database_info = DatabaseInfo(args.endpoint, args.graphname, args.vertices, args.edges, args.repl_factor,
                                 args.num_shards, args.overwrite, args.smart_attribute,
                                 args.additional_vertex_attribute,
                                 args.edge_attribute,
                                 args.user, args.pwd)

    g_info = GraphInfo(args.hasSelfLoops, False, v_property, edge_property)

    if args.graphtype == 'cliques-graph':
        clique_graph_info = CliquesGraphInfo(args.num_cliques, args.min_size_clique, args.max_size_clique,
                                             args.prob_missing, args.inter_cliques_density,
                                             args.density_between_two_cliques)
        start = time.time()
        create_cliques_graph(database_info, g_info, clique_graph_info, args.bulk_size)
        print('Time: ' + get_time_difference_string(time.time() - start))
    else:  # must be clique as args.graphtype has choices == ['clique', 'cliques-graph']
        start = time.time()
        create_clique_graph(database_info, args.bulk_size, args.size, g_info)
        print('Time: ' + get_time_difference_string(time.time() - start))