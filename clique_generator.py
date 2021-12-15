import random
from typing import List, Union, Tuple, Iterable

from tqdm import tqdm

from databaseinfo import DatabaseInfo, GraphInfo, CliquesGraphInfo
from general import ConverterToVertex, yes_with_prob, insert_documents, create_graph
from generator import add_edge, make_and_insert_vertices


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
        self.starts_of_cliques.append(self.starts_of_cliques[-1] + size)
        self.num_vertices += size

    def num_cliques(self):
        return len(self.starts_of_cliques) - 1


def get_num_edges_between_cliques(size1: int, size2: int):
    return random.randint(1, size1 * size2 + 1)


def make_edges_generalized_clique(db_info: DatabaseInfo,
                                  graph_info: GraphInfo,
                                  c_helper: CliquesHelper,
                                  clique_idx: int,
                                  size: int, bulk_size: int,
                                  prob_missing: float):


    edges = []
    to_v = ConverterToVertex(db_info.vertices_coll_name).idx_to_vertex

    # last entry in c_helper.starts_of_cliques is for the next clique
    c_begin = c_helper.starts_of_cliques[clique_idx]
    c_end = c_helper.starts_of_cliques[clique_idx + 1] if clique_idx < len(c_helper.starts_of_cliques) else size

    # have a clique now
    for i in range(c_begin, c_end):
        for j in range(i + 1, c_end):  # todo randomly find num_edges, iterate over edges, find endpoints randomly
            add_edge(i, j, edges, prob_missing, db_info, graph_info, i * size + j, to_v)

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


def make_and_insert_clique_edges(db_info: DatabaseInfo, graph_info: GraphInfo, c_helper: CliquesHelper,
                                 clique_idx: int, size: int, bulk_size: int, prob_missing: float):
    for edges in make_edges_generalized_clique(db_info, graph_info, c_helper, clique_idx, size, bulk_size,
                                               prob_missing):
        insert_documents(db_info, edges, db_info.edge_coll_name)


def create_clique_graph(db_info: DatabaseInfo,
                        bulk_size,
                        size: int,
                        graph_info: GraphInfo):
    create_graph(db_info)
    c_helper = CliquesHelper()
    make_and_insert_vertices(db_info, graph_info, c_helper, size, bulk_size, add_part=False)
    make_and_insert_clique_edges(db_info, graph_info, c_helper,
                                 clique_idx=0, size=size, bulk_size=bulk_size, prob_missing=0.0)


def create_cliques_graph(db_info: DatabaseInfo,
                         graph_info: GraphInfo,
                         c_graph_info: CliquesGraphInfo,
                         bulk_size: int
                         ):
    def connect_cliques(clique_helper: CliquesHelper, density: float,
                        # num_e_between_cliques: Callable[[int, int], int],
                        v_coll: str,
                        bulk_size: int,
                        isDirected: bool) -> Iterable:
        '''
        Given a list cliques of disjoint vertex sets (disjointness is not verified), decides for every pair of such sets
        whether there should be edges between them randomly with probability density. If yes, function num_e_between_cliques
        returns for the sizes of the two sets how many edges there should be between them. The endpoints of the edges
        are chosen randomly with equal distribution.
        :rtype: None
        :param clique_helper:
        :param density:
        :param num_e_between_cliques:
        :param v_coll:
        :param bulk_size:
        :param isDirected:
        :return:
        '''
        edges = []
        to_vrtx = ConverterToVertex(v_coll).idx_to_vertex

        # exclude self-loops

        for v in tqdm(range(clique_helper.num_cliques()), desc='Connecting clique pairs', mininterval=1.0,
                         unit='connected clique'):
            for w in range(v + 1, clique_helper.num_cliques()):
                if yes_with_prob(c_graph_info.prob_missing):
                    continue
                start_v = clique_helper.starts_of_cliques[v]
                end_v = clique_helper.starts_of_cliques[v + 1]
                start_w = clique_helper.starts_of_cliques[w]
                end_w = clique_helper.starts_of_cliques[w + 1]

                for f in range(start_v, end_v):
                    for t in range(start_w, end_w):
                        add_edge(f, t, edges, c_graph_info.prob_missing, db_info, graph_info,
                                 # pos_in_prop is for egde_property given as a list, not applicable for cliques-graph
                                 pos_in_prop=0,
                                 to_v=to_vrtx)
                        if len(edges) >= bulk_size:
                            yield edges
                            edges.clear()

    create_graph(db_info)

    # create the cliques

    c_helper = CliquesHelper()

    for clique_i in tqdm(range(c_graph_info.num_cliques), desc='Constructing cliques', mininterval=1.0,
                         unit='clique'):
        size = random.randint(c_graph_info.min_size_clique, c_graph_info.max_size_clique)
        make_and_insert_vertices(db_info, graph_info, c_helper, size, bulk_size, add_part=True)
        make_and_insert_clique_edges(db_info, graph_info, c_helper, clique_i, size, bulk_size,
                                     c_graph_info.prob_missing)

    # create edges between cliques
    for edges in connect_cliques(c_helper, c_graph_info.inter_cliques_density,  # get_num_edges_between_cliques,
                                 db_info.vertices_coll_name, bulk_size, graph_info.isDirected):
        insert_documents(db_info, edges, db_info.edge_coll_name)