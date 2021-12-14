import argparse
import random
import time
from typing import Union, List, Dict, Callable

from clique_generator import CliquesHelper, create_clique_graph, create_cliques_graph
from databaseinfo import DatabaseInfo, GraphInfo, CliquesGraphInfo, VertexOrEdgeProperty
from general import insert_documents, yes_with_prob, arangodIsRunning, \
    get_time_difference_string
from k_partite_generator import create_k_partite_graph, connect_parts_time, insert_edges_time

insert_vertices_time = 0
prepare_vertices_time = 0
add_edge_time = 0

def prepare_vertices(db_info: DatabaseInfo, graph_info: GraphInfo, part_label: str, start: int, end: int):

    # todo change readme
    s = time.time()
    for vid in range(start, end):
        if db_info.isSmart: # smart_attribute exists and makes  sense
            if db_info.smart_attribute != db_info.additional_vertex_attribute and db_info.smart_attribute != 'part':
                doc = {f'{db_info.smart_attribute}': vid}
                if graph_info.vertex_property.type == 'random':
                    doc[db_info.additional_vertex_attribute] = str(random.uniform(float(graph_info.vertex_property.min), float(graph_info.vertex_property.max)))
                if part_label != "":
                    doc['part'] = part_label
            elif db_info.smart_attribute == db_info.additional_vertex_attribute: ## type == 'random'
                doc = {'id': vid}
                doc[db_info.smart_attribute] = str(random.uniform(float(graph_info.vertex_property.min), float(graph_info.vertex_property.max)))
                if part_label != "":
                    doc['part'] = part_label
            else: # db_info.smart_attribute == 'part'
                doc = {'id': vid}
                doc['part'] = part_label
                if graph_info.vertex_property.type == 'random':
                    doc[db_info.additional_vertex_attribute] = str(random.uniform(float(graph_info.vertex_property.min), float(graph_info.vertex_property.max)))
        else:
            doc = {'id': vid}
            if part_label != "":
                doc['part'] = part_label
            if graph_info.vertex_property.type == 'random':
                doc[db_info.additional_vertex_attribute] = str(
                    random.uniform(float(graph_info.vertex_property.min), float(graph_info.vertex_property.max)))
    global prepare_vertices_time
    prepare_vertices_time += time.time() - s

def make_vertices(graph_info: GraphInfo,
                  db_info: DatabaseInfo, size: int,
                  bulk_size: int):
    c_begin = graph_info.next_id
    c_end = graph_info.next_id + size
    while graph_info.next_id + bulk_size <= c_end:
        yield prepare_vertices(db_info, graph_info, str(c_begin), graph_info.next_id, graph_info.next_id + bulk_size)
    yield prepare_vertices(db_info, graph_info, str(c_begin), graph_info.next_id, c_end)


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


def add_edge(i: int, j: int, edges: List, prob_missing: float, db_info: DatabaseInfo, graph_info: GraphInfo,
             pos_in_prop: int,
             to_v: Callable[[Union[int, str]], str]):
    s = time.time()
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
    global add_edge_time
    add_edge_time += time.time() - s

def make_and_insert_vertices(db_info: DatabaseInfo,
                             graph_info: GraphInfo, c_helper: CliquesHelper, size: int, bulk_size: int):
    for vertices in make_vertices(graph_info, db_info, size, bulk_size):
        s_insert_vertices = time.time()
        insert_documents(db_info, vertices, db_info.vertices_coll_name)
        global insert_vertices_time
        insert_vertices_time += time.time() - s_insert_vertices
    c_helper.update(size)


def get_vertex_property(args) -> VertexOrEdgeProperty:
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
    return v_property


def get_edge_property(a) -> Union[None, VertexOrEdgeProperty]:
    if not a.edge_property_type or a.edge_property_type == 'none':
        return None
    elif a.edge_property_type == 'random':
        if len(a.edge_prop) != 2:
            raise RuntimeError(
                'If --edge_property_type is \'random\', --edge_prop must have exactly two arguments.')
        return VertexOrEdgeProperty('random', float(a.edge_prop[0]), float(a.edge_prop[1]))
    else:  # remains list values
        if a.graphtype == 'clique':
            if len(a.edge_prop) < a.size * a.size:
                raise RuntimeError(
                    'If --edge_property_type is \'list\' and --graphtype == \'clqiue\', --edge_prop must have '
                    'at least (--size)^2 many arguments.')
        if a.graphtype == 'cliques-graph':
            if len(a.edge_prop) < (a.num_cliques * a.max_size_clique) ** 2:
                raise RuntimeError(
                    'If --edge_property_type is \'list\' and --graphtype == \'clqiues-graph\', --edge_prop must have '
                    'at least (a.num_cliques * a.max_size_clique)^2 many arguments.')

        return VertexOrEdgeProperty('list', val_list=list(a.edge_prop))


if __name__ == "__main__":
    if not arangodIsRunning():
        raise RuntimeError('The process \'arangod\' is not running, please, run it first.')

    parser = argparse.ArgumentParser(description='Import a graph from a file/files to ArangoDB.')

    # global
    parser.add_argument('endpoint', type=str, help='Endpoint, e.g. http://localhost:8529/_db/_system')
    parser.add_argument('--bulk_size', type=int, nargs='?', default=10000,
                        help='The number of vertices/edges written in one go.')

    # general graph parameters
    parser.add_argument('graphtype', type=str, default='clique', choices=['clique', 'cliques-graph', 'k-partite'],
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

    # k-partite parameters
    parser.add_argument('--num_parts', '-k', type=int,
                        help='Number of parts in a k-partite graph. Ignored for other graphs.')
    parser.add_argument('--min_size_part', type=int,
                        help='Minimum part size in a k-partite graph. Ignored for other graphs.')
    parser.add_argument('--max_size_part', type=int,
                        help='Maximum part size in a k-partite graph. Ignored for other graphs.')

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
    parser.add_argument('--make_smart', action='store_false',  # default: true
                        help='Create a smart graph.')

    # attributes
    # todo: check allow input file
    parser.add_argument('--vertex_property_type', nargs='?', choices=['none', 'random'], default='none',
                        help="""Vertex property kind. Default is \'none\', then --vertex_property is ignored and 
                         no properties are saved. If \'random\', --vertex_property should contain two numbers a and b; 
                         the property value is chosen randomly in [a, b).""")
    parser.add_argument('--vertex_property', nargs='+', help="""Vertex property. This parameter must be given 
                            if and only if --vertex_property_type is not skipped and not \'none\', 
                            otherwise an exception is thrown. If --vertex_property_type is \'random\',
                            two numbers a,b must be given with a <= b, the value for the property will be chosen
                            randomly with equal probability from the interval [a,b).""")
    parser.add_argument('--edge_property_type', nargs='?', choices=['none', 'random'], default='none',
                        help="""Edge property kind. Default is \'none\', then --edge_prop is ignored and 
                             the default property 'weight' with values Null is saved. 
                             If \'random\', --edge_prop should contain two numbers a and b; 
                             the property value is chosen randomly in [a, b).""")
    parser.add_argument('--edge_prop', nargs='+', help="""Edge property. This parameter must be given 
                          if and only if --edge_property_type is not skipped and not \'none\', 
                          otherwise an exception is thrown. If skipped, the default property 'weight' 
                          with values Null is saved. 
                          If --edge_property_type is \'random\', two numbers a,b must be given with a <= b, 
                          the value for the property will be chosen randomly with equal probability from 
                          the interval [a,b).""")
    parser.add_argument('--additional_vertex_attribute', default='color',
                        help="""Additional vertex attribute name used for --vertex_property. Default is \'color\'.
                        Cannot be \'part\'.""")
    parser.add_argument('--edge_attribute', default='weight',
                        help="""Edge attribute name used for --edge_prop. Default is \'weight\'.""")

    args = parser.parse_args()

    if args.additional_vertex_attribute == 'part':
        raise RuntimeError('--additional_vertex_attribute cannot be \'part\', choose another name.')
    if args.vertex_property_type == 'none' and args.smart_attribute == args.additional_vertex_attribute:
        raise RuntimeError('If --smart_attribute is --args.additional_vertex_attribute, --vertex_property_type '
                           'cannot be \'none\'.')
    if args.make_smart and not args.smart_attribute:
        raise RuntimeError('If --make_smart is given, then also --smart_attribute must be given.')

    v_property = get_vertex_property(args)
    edge_property = get_edge_property(args)

    database_info = DatabaseInfo(args.endpoint, args.graphname, args.vertices, args.edges, args.make_smart,
                                 args.repl_factor,
                                 args.num_shards, args.overwrite, args.smart_attribute,
                                 args.additional_vertex_attribute,
                                 args.edge_attribute,
                                 args.user, args.pwd)

    g_info = GraphInfo(args.hasSelfLoops, False, v_property, edge_property)

    start = time.time()
    if args.graphtype == 'cliques-graph':
        clique_graph_info = CliquesGraphInfo(args.num_cliques, args.min_size_clique, args.max_size_clique,
                                             args.prob_missing, args.inter_cliques_density,
                                             args.density_between_two_cliques)
        create_cliques_graph(database_info, g_info, clique_graph_info, args.bulk_size)
    elif args.graphtype == 'clique':
        create_clique_graph(database_info, args.bulk_size, args.size, g_info)
    elif args.graphtype == 'k-partite':
        parts_graph_info = CliquesGraphInfo(args.num_parts, args.min_size_clique, args.max_size_clique, 0.0, 0.0, 0.0)
        create_k_partite_graph(database_info, g_info, parts_graph_info, args.bulk_size)
    else:
        pass
    print('Global time: ' + get_time_difference_string(time.time() - start))
    print('Time inserting vertices: ' + get_time_difference_string(insert_vertices_time))
    print('Time preparing vertices: ' + get_time_difference_string(prepare_vertices_time))
    print('Time adding edges: ' + get_time_difference_string(add_edge_time))
    print('Time connecting parts: ' + get_time_difference_string(connect_parts_time))
    print('Time inserting edges: ' + get_time_difference_string(insert_edges_time))



