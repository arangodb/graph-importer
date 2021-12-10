import argparse
import random
from typing import Union, Tuple, List, Callable

from databaseinfo import DatabaseInfo, GraphInfo, CliquesGraphInfo, VertexOrEdgeProperty
from general import create_graph, insert_documents, ConverterToVertex


class CliquesHelper:
    '''
    Keep track of vertex ids in the cliques.
    The ids of the vertices of the graph are enumerated 0..n-1. Every clique has ids k..k+m.
    Clique[i] starts with index starts_of_cliques[i].
    '''

    def __init__(self):
        self.starts_of_cliques: List[int] = [0]
        self.num_vertices: int = 0

    def size_of_clique(self, i: int):
        if i == len(self.starts_of_cliques) - 1:
            return self.num_vertices - self.starts_of_cliques[i]
        else:
            return self.starts_of_cliques[i + 1] - self.starts_of_cliques[i]

    def update(self, size: int):
        self.starts_of_cliques = self.starts_of_cliques.append(self.starts_of_cliques[-1] + size)
        self.num_vertices += size

    def num_cliques(self):
        return len(self.starts_of_cliques)


def num_edges_between_cliques(size1: int, size2: int):
    return random.randint(size1, size2)


def make_vertices(graph_info: GraphInfo,
                  db_info: DatabaseInfo, size: int,
                  bulk_size: int):
    bulk_number = 0
    if graph_info.vertex_property.type == "none":
        while (bulk_number + 1) * bulk_size < size:
            yield [{f'{db_info.smart_attribute}': vid} for vid in
                   range(bulk_number * bulk_size, (bulk_number + 1) * bulk_size)]
            bulk_number += 1
    elif graph_info.vertex_property.type == "list":
        if len(graph_info.vertex_property.list) != size:
            raise RuntimeError(
                f'make_vertices: the length of vertex_property ({len(graph_info.vertex_property.list)}) '
                f'must be equal to size ({size}).')
        while (bulk_number + 1) * bulk_size < size:
            yield [{f'{db_info.smart_attribute}': vid,
                    f'{db_info.additional_vertex_attribute}': graph_info.vertex_property.list[vid]} for vid in
                   range(graph_info.next_id, bulk_number * bulk_size, (bulk_number + 1) * bulk_size)]
            graph_info.next_id += bulk_number * bulk_size, (bulk_number + 1) * bulk_size
            bulk_number += 1
    # random values, kind(vertex_property) == tuple
    elif graph_info.vertex_property.type == "random":
        while (bulk_number + 1) * bulk_size < size:
            yield [
                {f'{db_info.smart_attribute}': str(vid),
                 f'{db_info.additional_vertex_attribute}': str(
                     random.uniform(float(graph_info.vertex_property.min), float(graph_info.vertex_property.max)))}
                for vid in
                range(graph_info.next_id, bulk_number * bulk_size, (bulk_number + 1) * bulk_size)]
            graph_info.next_id = bulk_size
            bulk_number += 1
        yield [
            {f'{db_info.smart_attribute}': str(vid),
             f'{db_info.additional_vertex_attribute}': str(
                 random.uniform(float(graph_info.vertex_property.min), float(graph_info.vertex_property.max)))}
            for vid in
            range(graph_info.next_id, bulk_number * bulk_size, size)]
    else:
        raise RuntimeError(
            f"Wrong vertex property kind: {graph_info.vertex_property.type}. "
            f"Allowed values are \'none\', \'list\' and \'random\'.")


def yes_with_prob(prob: float):
    return random.randint(1, 1000) < prob * 1000


# todo make a recordable seed


def connect_cliques(clique_helper: CliquesHelper, density: float, num_e_between_cliques: Callable[[int, int], int],
                    v_coll: str,
                    bulk_size: int,
                    isDirected: bool):
    '''
    Given a list cliques of disjoint vertex sets (disjointness is not verified), decides for every pair of such sets
    whether there should be edges between them randomly with probability density. If yes, function num_e_between_cliques
    returns for the sizes of the two sets how many edges there should be between them. The endpoints of the edges
    are chosen randomly with equal distribution.
    :param clique_helper:
    :param density:
    :param num_e_between_cliques:
    :param v_coll:
    :param bulk_size:
    :param isDirected:
    :return:
    '''
    edges = []
    to_v = ConverterToVertex(v_coll).idx_to_vertex

    for i in range(clique_helper.num_cliques()):
        for j in range(i + 1, clique_helper.num_cliques()):
            if not yes_with_prob(density):
                continue
            size_i = clique_helper.size_of_clique(i)
            size_j = clique_helper.size_of_clique(j)
            num_edges = num_e_between_cliques(size_i, size_j)
            indexes_endpoints_i = random.sample(range(size_i), num_edges)
            indexes_endpoints_j = random.sample(range(size_j), num_edges)
            for ell in range(num_edges):
                f = str(size_i + indexes_endpoints_i[ell])
                t = str(size_j + indexes_endpoints_j[ell])
                if isDirected:
                    if yes_with_prob(0.5):  # randomly choose direction
                        t, f = f, t
                    edges.append({'_from': to_v(f), '_to': to_v(t)})
                else:
                    edges.append({'_from': to_v(f), '_to': to_v(t)})
                    edges.append({'_from': to_v(t), '_to': to_v(f)})
                if len(edges) >= bulk_size:
                    yield edges
                    edges.clear()


def make_edges_generalized_clique(db_info: DatabaseInfo,
                                  graph_info: GraphInfo,
                                  size: int, bulk_size: int,
                                  prob_missing: float):
    edges = []
    to_v = ConverterToVertex(db_info.vertices_coll_name).idx_to_vertex

    next_id = graph_info.next_id
    graph_info.next_id += (size - 1) * size
    for i in range(next_id, size):
        for j in range(i + 1, size):
            if yes_with_prob(prob_missing):
                return []
            if graph_info.edge_property.type == 'none':
                edges.append({"_from": to_v(i), "_to": to_v(j)})
                edges.append({"_from": to_v(j), "_to": to_v(i)})
            elif graph_info.edge_property.type == 'list':
                edges.append({"_from": to_v(i), "_to": to_v(j),
                              db_info.edge_coll_name: graph_info.edge_property.list[i * size + j]})
                edges.append({"_from": to_v(j), "_to": to_v(i),
                              db_info.edge_coll_name: graph_info.edge_property.list[j * size + i]})
            elif graph_info.edge_property.type == 'random':
                edges.append({"_from": to_v(i), "_to": to_v(j),
                              db_info.edge_coll_name: str(
                                  random.uniform(graph_info.edge_property.min, graph_info.edge_property.max))})
                edges.append({"_from": to_v(j), "_to": to_v(i),
                              db_info.edge_coll_name: str(
                                  random.uniform(graph_info.edge_property.min, graph_info.edge_property.max))})
            else:
                raise RuntimeError(
                    f"Wrong vertex property kind: {graph_info.vertex_property.type}. "
                    f"Allowed values are \'none\', \'list\' and \'random\'.")  # todo refactor
            if len(edges) >= bulk_size:
                yield edges
                edges.clear()
        if edges:
            yield edges
            edges.clear()
    if edges:
        yield edges


def make_tournament_edges(edge_prop: Union[str, None, Tuple[str, List[str]]],
                          vertices_coll_name: str,
                          size: int, bulk_size: int,
                          prob_missing: float):
    edges = []
    to_v = ConverterToVertex(vertices_coll_name).idx_to_vertex
    for i in range(size):
        for j in range(i + 1, size):
            if yes_with_prob(prob_missing):
                return []
            if type(edge_prop) is None:
                if random.randint(0, 1):
                    edges.append({"_from": to_v(i), "_to": to_v(j)})
                else:
                    edges.append({"_from": to_v(j), "_to": to_v(i)})
            elif len(edge_prop) == 2:
                if random.randint(0, 1):
                    edges.append({"_from": to_v(i), "_to": to_v(j),
                                  edge_prop[0]: edge_prop[1][i * size + j]})
                else:
                    edges.append({"_from": to_v(j), "_to": to_v(i),
                                  edge_prop[0]: edge_prop[1][j * size + i]})
            else:  # str
                if random.randint(0, 1):
                    edges.append({"_from": to_v(i), "_to": to_v(j),
                                  edge_prop: str(random.uniform(0, 1))})
                else:
                    edges.append({"_from": to_v(j), "_to": to_v(i),
                                  edge_prop: str(random.uniform(0, 1))})
            if len(edges) >= bulk_size:
                yield edges
                edges.clear()
        if edges:
            yield edges
            edges.clear()
    if edges:
        yield edges


def create_clique(db_info: DatabaseInfo,
                  bulk_size,
                  size: int,
                  graph_info: GraphInfo):
    create_graph(db_info)

    make_and_insert_vertices(db_info, graph_info, size, bulk_size)
    make_and_insert_edges(db_info, graph_info, size, bulk_size, 0.0)


def make_and_insert_vertices(db_info: DatabaseInfo,
                             graph_info: GraphInfo, size: int, bulk_size: int):
    for vertices in make_vertices(graph_info, db_info, size, bulk_size):
        insert_documents(db_info, vertices, db_info.vertices_coll_name)


def make_and_insert_edges(db_info: DatabaseInfo, graph_info: GraphInfo, size: int, bulk_size: int,
                          prob_missing: float):
    for edges in make_edges_generalized_clique(db_info, graph_info, size, bulk_size, prob_missing):
        insert_documents(db_info, edges, db_info.edge_coll_name)


def create_cliques_graph(db_info: DatabaseInfo,
                         graph_info: GraphInfo,
                         c_graph_info: CliquesGraphInfo,
                         bulk_size: int
                         ):
    create_graph(db_info)

    # create the cliques

    c_help = CliquesHelper()

    for i in range(c_graph_info.num_cliques):
        size = random.randint(c_graph_info.min_size_clique, c_graph_info.max_size_clique)
        c_help.update(size)
        make_and_insert_vertices(db_info, graph_info, size, bulk_size)
        make_and_insert_edges(db_info, graph_info, size, bulk_size, c_graph_info.prob_missing)

    # create edges between cliques
    connect_cliques(c_help, c_graph_info.inter_cliques_density, c_graph_info.num_edges_between_cliques,
                    db_info.vertices_coll_name, bulk_size, graph_info.isDirected)


if __name__ == "__main__":
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
    parser.add_argument('--num_cliques', help='Number of cliques in a cliques-graph. Ignored for other graphs.')
    parser.add_argument('--min_size_clique', help='Minimum clique size in a cliques-graph. Ignored for other graphs.')
    parser.add_argument('--max_size_clique', help='Maximum clique size in a cliques-graph. Ignored for other graphs.')
    parser.add_argument('--prob_missing',
                        help='The probability for an edge in a clique to be missing  in a cliques-graph. '
                             'Ignored for other graphs.')
    parser.add_argument('--inter_cliques_density',
                        help='The probability that there are edges between two cliques in a cliques-graph. '
                             'Ignored for other graphs.')
    parser.add_argument('--num_edges_between_cliques', type=str, choices=['random'], default='random',
                        help='The name of a function that given the sizes of two cliques that must have edges'
                             'between them returns this number in a cliques-graph. Currently only \'random\' is'
                             'implemented. Ignored for other graphs.')

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
        v_property = VertexOrEdgeProperty('random', args.vertex_property[0], args.vertex_property[1])
    else:  # remains list values
        if len(args.vertex_property) != args.size * args.size:
            raise RuntimeError(
                'If --vertex_property_type is \'list\', --vertex_property must have exactly --size many arguments.')
        v_property = VertexOrEdgeProperty('list', val_list=list(args.vertex_property))

    if not args.edge_property_type or args.edge_property_type == 'none':
        edge_property = None
    elif args.edge_property_type == 'random':
        if len(args.edge_property) != 2:
            raise RuntimeError(
                'If --edge_property_type is \'random\', --edge_prop must have exactly two arguments.')
        edge_property = VertexOrEdgeProperty('random', args.edge_property[0], args.edge_property[1])
    else:  # remains list values
        if len(args.edge_property) != args.size * args.size:
            raise RuntimeError(
                'If --edge_property_type is \'list\', --edge_prop must have exactly --size^2 many arguments.')
        edge_property = VertexOrEdgeProperty('list', val_list=list(args.edge_property))

    database_info = DatabaseInfo(args.endpoint, args.graphname, args.vertices, args.edges, args.repl_factor,
                                 args.num_shards, args.overwrite, args.smart_attribute,
                                 args.additional_vertex_attribute,
                                 args.edge_attribute,
                                 args.user, args.pwd)

    g_info = GraphInfo(args.hasSelfLoops, False, v_property, edge_property)

    if args.graphtype == 'cliques-graph':
        clique_graph_info = CliquesGraphInfo(args.num_cliques, args.min_size_clique, args.max_size_clique,
                                             args.prob_missing, args.inter_cliques_density, num_edges_between_cliques)
        create_cliques_graph(database_info, g_info, clique_graph_info, args.bulk_size)
    else:  # must be clique as args.graphtype has choices == ['clique', 'cliques-graph']
        create_clique(database_info, args.bulk_size, args.size, g_info)
