import argparse
import random
from typing import Union, Tuple, List

from general import create_graph, insert_vertices, insert_edges


# todo fill_tournament

def make_vertices(vertex_property: Union[Tuple[float, float], None, List[str]], smart_attribute: str,
                  additional_attribute: str, size: int,
                  bulk_size: int):
    bulk_number = 0
    if type(vertex_property) is None:
        while (bulk_number + 1) * bulk_size < size:
            yield [{f'{smart_attribute}': vid} for vid in range(bulk_number * bulk_size, (bulk_number + 1) * bulk_size)]
            bulk_number += 1
    elif type(vertex_property) == list:
        if len(vertex_property) != size:
            raise RuntimeError(
                f'make_vertices: the length of vertex_property ({len(vertex_property)}) must be equal to size ({size}).')
        while (bulk_number + 1) * bulk_size < size:
            yield [{f'{smart_attribute}': vid, f'{additional_attribute}': vertex_property[vid]} for vid in
                   range(bulk_number * bulk_size, (bulk_number + 1) * bulk_size)]
            bulk_number += 1
    # random values, type(vertex_property) == tuple
    else:
        while (bulk_number + 1) * bulk_size < size:
            yield [
                {f'{smart_attribute}': str(vid),
                 f'{additional_attribute}': str(random.uniform(float(vertex_property[0]), float(vertex_property[1])))}
                for vid in
                range(bulk_number * bulk_size, (bulk_number + 1) * bulk_size)]
            bulk_number += 1
        yield [
                {f'{smart_attribute}': str(vid),
                 f'{additional_attribute}': str(random.uniform(float(vertex_property[0]), float(vertex_property[1])))}
                for vid in
                range(bulk_number * bulk_size, size)]


def make_clique_edges(edge_property: Union[Tuple[str, float, float], None, Tuple[str, List[str]]], hasSelfLoops: bool,
                      size: int, bulk_size: int):
    '''
    Yield at most bulk_size + 1 edges at one time.
    :param edge_property:
    :param hasSelfLoops:
    :param size:
    :param bulk_size:
    :return: None
    '''
    edges = []
    if type(edge_property) is None:
        for i in range(size):
            for j in range(i+1, size):
                edges.append({"_from": f"{i}", "_to": f"{j}"})
                edges.append({"_from": f"{j}", "_to": f"{i}"})
                if len(edges) >= bulk_size:
                    yield edges
                    edges.clear()
            if edges:
                yield edges
                edges.clear()
        if hasSelfLoops:
            edges.extend([{"_from": f"{i}", "_to": f"{i}"} for i in range(size)])
        if edges:
            yield edges
    else:
        edge_property_name = edge_property[0]
        if len(edge_property) == 3:  # Tuple, random
            minimum = float(edge_property[1])
            maximum = float(edge_property[2])
            for i in range(size):
                for j in range(i+1, size):
                    edges.append({"_from": f"{i}", "_to": f"{j}", edge_property_name: f'{random.uniform(minimum, maximum)}'})
                    edges.append({"_from": f"{j}", "_to": f"{i}", edge_property_name: f'{random.uniform(minimum, maximum)}'})
                    if len(edges) >= bulk_size:
                        yield edges
                        edges.clear()
                if edges:
                    yield edges
                    edges.clear()
            if hasSelfLoops:
                edges.extend(
                    {"_from": f"{i}", "_to": f"{i}", edge_property_name: f'{random.uniform(minimum, maximum)}'})
            if edges:
                yield edges # for the last i
        else:  # len = 2: list of values
            # check that the number of edge properties is correct
            if len(edge_property[1]) != size * size:  # even if no self loops; we just don't use values at (i,i)
                raise RuntimeError(
                    'make_clique_edges: with selfloops, the length of edge_property[1] must be equal to size**2.')

            for i in range(size):
                for j in range(i+1, size):
                    edges.append({"_from": f"{i}", "_to": f"{j}", edge_property_name: f'{edge_property[1][i * size + j]}'})
                    edges.append({"_from": f"{j}", "_to": f"{i}", edge_property_name: f'{edge_property[1][j * size + i]}'})
                    if len(edges) >= bulk_size:
                        yield edges
                        edges.clear()
                if edges:
                    yield edges
                    edges.clear()
            if hasSelfLoops:
                edges.append({"_from": f"{i}", "_to": f"{i}", edge_property_name: f'{edge_property[1][i * size + i]}'})
            if edges:
                yield edges

def fill_clique(endpoint: str, vertices_coll_name: str, edge_coll_name: str, smart_attribute: str,
                additional_attribute: str, bulk_size: int,
                username: str, password: str, size: int, hasSelfLoops: bool,
                vertex_property: Union[Tuple[float, float], None, List[str]],
                edge_property: Union[Tuple[str, float, float], None, Tuple[str, List[str]]]):
    '''
    If vertex_property is not None, it is used as the shard smart_attribute.
    :param endpoint:
    :param vertices_coll_name:
    :param edge_coll_name:
    :param smart_attribute:
    :param username:
    :param password:
    :param size:
    :param hasSelfLoops:
    :param vertex_property:
    :param edge_property:
    :return:
    '''

    for vertices in make_vertices(vertex_property, smart_attribute, additional_attribute, size, bulk_size):
        insert_vertices(endpoint, vertices_coll_name, vertices, username, password)
    for edges in make_clique_edges(edge_property, hasSelfLoops, size, bulk_size):
        insert_edges(endpoint, edge_coll_name, vertices_coll_name, edges, smart_attribute, username, password)


def create_clique(endpoint, graph_name, vertices_coll_name, edge_coll_name, replication_factor: int,
                  number_of_shards: int, overwrite: bool, smart_attribute: str, additional_attribute: str, bulk_size,
                  username: str, password: str, size: int, hasSelfLoops: bool,
                  vertex_property: Union[Tuple[float, float], None, List[str]],
                  edge_property: Union[Tuple[str, float, float], None, Tuple[str, List[str]]]):
    create_graph(endpoint, graph_name, vertices_coll_name, edge_coll_name, replication_factor,
                 number_of_shards, overwrite, smart_attribute, username, password)
    fill_clique(endpoint, vertices_coll_name, edge_coll_name, smart_attribute, additional_attribute, bulk_size,
                username, password, size,
                hasSelfLoops, vertex_property, edge_property)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Import a graph from a file/files to ArangoDB.')

    parser.add_argument('endpoint', type=str, help='Endpoint, e.g. http://localhost:8529/_db/_system')
    parser.add_argument('graphtype', type=str, nargs='?', default='clique', choices=['clique'],
                        help='Source type')
    parser.add_argument('--hasSelfLoops', action='store_true',  # default: false
                        help='Whether the graphs should have selfloops.')
    parser.add_argument('--size', '-s', type=int, nargs='?', default=10000,
                        help='The number of vertices.')
    parser.add_argument('--user', nargs='?', default='root', help='User name for the server.')
    parser.add_argument('--pwd', nargs='?', default='', help='Password for the server.')
    parser.add_argument('--graphname', default='generatedGraph', help='Name of the new graph in the database.')
    parser.add_argument('--edges', default='e', help='Name of the new edge relation in the database.')
    parser.add_argument('--vertices', default='v', help='Name of the new vertex relation in the database.')
    parser.add_argument('--num_shards', default=5, type=int, help='Number of shards.')
    parser.add_argument('--repl_factor', default=2, type=int, help='Replication factor.')
    parser.add_argument('--smart_attribute', default='smartProp',
                        help='The name of the attribute to shard the vertices after.')
    parser.add_argument('--additional_attribute', default='prop',
                        help='The name of the additional attribute.')
    parser.add_argument('--overwrite', action='store_true',  # default: false
                        help='Overwrite the graph and the collection if they already exist.')
    parser.add_argument('--vertex_property', nargs='+', help="""Vertex properties. If skipped, no properties are saved. 
                            Otherwise either '<property name:str> <lower bound:float> <upper bound:float>'
                            for random float values between <lower bound> and <upper bound>, or
                            '<property name:str> <value 1:str> [,...[,<value n:str>]]' for explicit values.
                            The length of the list must be exactly <size>.""")
    parser.add_argument('--edge_property', nargs='+', help="""Edge properties. If skipped, the default property 'weight' 
                                with values Null is saved.no properties are saved. 
                                Otherwise either '<property name:str> <lower bound:float> <upper bound:float>'
                                for random float values between <lower bound> and <upper bound>, or
                                '<property name:str> <value 1:str> [,...[,<value n:str>]]' for explicit values.
                                The length of the list must be exactly <size>**2, regardless of <hasSelfLoops>.""")
    parser.add_argument('--bulk_size', type=int, nargs='?', default=10000,
                        help='The number of vertices/edges written in one go.')

    args = parser.parse_args()

    if not args.vertex_property:  # not given
        vertex_property = None
    elif len(args.vertex_property) == 2:  # convert to tuple, will be random floats
        vertex_property = args.vertex_property[0], args.vertex_property[1]
    else:  # remains list, explicit values
        vertex_property = args.vertex_property

    if not args.edge_property:  # not given
        edge_property = None
    elif len(args.edge_property) == 3:  # convert to tuple, will be random floats
        edge_property = args.edge_property[0], args.edge_property[1], args.edge_property[2]
    else:  # convert to Tuple[str, List[str]], explicit values
        edge_property = args.edge_property[0],

    create_clique(args.endpoint, args.graphname, args.vertices, args.edges, args.repl_factor,
                  args.num_shards, args.overwrite, args.smart_attribute, args.additional_attribute, args.bulk_size,
                  args.user, args.pwd, args.size,
                  args.hasSelfLoops, vertex_property, edge_property)
