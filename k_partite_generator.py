`import random
from typing import Iterable

from tqdm import tqdm

from clique_generator import CliquesHelper
from databaseinfo import DatabaseInfo, GraphInfo, CliquesGraphInfo
from general import ConverterToVertex, yes_with_prob, create_graph, insert_documents
from generator import add_edge, make_and_insert_vertices


def create_k_partite_graph(db_info: DatabaseInfo,
                           graph_info: GraphInfo,
                           parts_graph_info: CliquesGraphInfo,
                           bulk_size: int
                           ):
    def connect_parts(clique_helper: CliquesHelper, density: float,
                        # num_e_between_cliques: Callable[[int, int], int],
                        v_coll: str,
                        bulk_size: int,
                        isDirected: bool) -> Iterable:
        '''
        Given a list parts of disjoint vertex sets (disjointness is not verified), connect every vertex of every part
        with every vertex of every other part.
        :rtype: None
        :param clique_helper:
        :param v_coll:
        :param bulk_size:
        '''
        edges = []
        to_vrtx = ConverterToVertex(v_coll).idx_to_vertex

        # exclude self-loops

        for v in tqdm(range(clique_helper.num_cliques()), desc='Connecting a part', mininterval=1.0,
                         unit='part'):
            for w in range(v + 1, clique_helper.num_cliques()):
                start_v = clique_helper.starts_of_cliques[v]
                end_v = clique_helper.starts_of_cliques[v + 1]
                start_w = clique_helper.starts_of_cliques[w]
                end_w = clique_helper.starts_of_cliques[w + 1]

                for f in range(start_v, end_v):
                    for t in range(start_w, end_w):
                        add_edge(f, t, edges, 0.0, db_info, graph_info,
                                 # pos_in_prop is for egde_property given as a list, not applicable for k-partite graphs
                                 pos_in_prop=0,
                                 to_v=to_vrtx)
                        if len(edges) >= bulk_size:
                            yield edges
                            edges.clear()

    create_graph(db_info)

    # create the cliques

    c_helper = CliquesHelper()

    for _ in tqdm(range(parts_graph_info.num_cliques), desc='Constructing cliques', mininterval=1.0,
                  unit='clique'):
        size = random.randint(parts_graph_info.min_size_clique, parts_graph_info.max_size_clique)
        make_and_insert_vertices(db_info, graph_info, c_helper, size, bulk_size)

    # create edges between cliques
    for edges in connect_parts(c_helper, parts_graph_info.inter_cliques_density,  # get_num_edges_between_cliques,
                               db_info.vertices_coll_name, bulk_size, graph_info.isDirected):
        insert_documents(db_info, edges, db_info.edge_coll_name)