import random
import time
from typing import List, Union, Tuple, Iterable

from tqdm import tqdm, trange

from edges_generator import add_edge, add_smart_edge, connect_parts
from general import yes_with_prob, insert_documents, create_graph
from helper_classes import DatabaseInfo, GraphInfo, CliquesGraphInfo, CliquesHelper
from time_tracking import TimeTracking
from vertices_generator import make_and_insert_vertices, ConverterToVertex


def get_num_edges_between_cliques(size1: int, size2: int):
    return random.randint(1, size1 * size2 + 1)


def make_edges_generalized_clique(db_info: DatabaseInfo,
                                  graph_info: GraphInfo,
                                  bulk_size: int,
                                  prob_missing: float,
                                  first_idx: int,
                                  end_idx: int,
                                  time_tracker: TimeTracking
                                  ):
    edges = []


    # last entry in c_helper.starts_of_cliques is for the next clique
    # if clique_idx >= len(c_helper.starts_of_cliques) - 1:
    #     raise RuntimeError(f'make_edges_generalized_clique: clique {clique_idx} does not exist, nothing to connect.')

    if db_info.isSmart:
        to_v = ConverterToVertex(db_info.vertices_coll_name).idx_to_smart_vertex
        for i in range(first_idx, end_idx):
            smart_val_i = str(i)
            for j in range(i + 1, end_idx):
                add_smart_edge(i, j, edges, prob_missing, db_info, graph_info, to_v, time_tracker, smart_val_i, str(j))
                if len(edges) >= bulk_size:
                    yield edges
                    edges.clear()
            if edges:
                yield edges
                edges.clear()
    else:
        to_v = ConverterToVertex(db_info.vertices_coll_name).idx_to_vertex
        for i in range(first_idx, end_idx):
            for j in range(i + 1, end_idx):
                add_edge(i, j, edges, prob_missing, db_info, graph_info, to_v, time_tracker)
                if len(edges) >= bulk_size:
                    yield edges
                    edges.clear()
            if edges:
                yield edges
                edges.clear()
    if edges:
        yield edges

def make_tournament_edges(smart_attr: str, edge_property: Union[str, None, Tuple[str, List[str]]],
                          vertices_coll_name: str,
                          size: int, bulk_size: int,
                          prob_missing: float):
    edges = []
    to_v = ConverterToVertex(vertices_coll_name).idx_to_vertex
    for i in range(size):
        for j in range(i + 1, size):
            if yes_with_prob(prob_missing):
                return []
            if type(edge_property) is None:
                if random.randint(0, 1):
                    edges.append({"_from": to_v(i   ), "_to": to_v(j)})
                else:
                    edges.append({"_from": to_v(j), "_to": to_v(i)})
            elif len(edge_property) == 2:
                if random.randint(0, 1):
                    edges.append({"_from": to_v(i), "_to": to_v(j),
                                  edge_property[0]: edge_property[1][i * size + j]})
                else:
                    edges.append({"_from": to_v(j), "_to": to_v(i),
                                  edge_property[0]: edge_property[1][j * size + i]})
            else:  # str
                if random.randint(0, 1):
                    edges.append({"_from": to_v(i), "_to": to_v(j),
                                  edge_property: str(random.uniform(0, 1))})
                else:
                    edges.append({"_from": to_v(j), "_to": to_v(i),
                                  edge_property: str(random.uniform(0, 1))})
            if len(edges) >= bulk_size:
                yield edges
                edges.clear()
        if edges:
            yield edges
            edges.clear()
    if edges:
        yield edges


def make_and_insert_clique_edges(db_info: DatabaseInfo, graph_info: GraphInfo,
                                 bulk_size: int, time_tracker: TimeTracking,
                                 prob_missing: float = 0.0,
                                 c_helper: Union[CliquesHelper, None] = None,
                                 clique_idx: Union[int, None] = None,
                                 start_idx: Union[int, None] = None,
                                 end_idx: Union[int, None] = None
                                 ):
    start_time = time.monotonic()
    assert (c_helper is not None and clique_idx is not None and start_idx is None and end_idx is None) or (
            c_helper is None and clique_idx is None and start_idx is not None and end_idx is not None)
    if clique_idx is not None:
        start_idx = c_helper.starts_of_cliques[clique_idx]
        end_idx = c_helper.starts_of_cliques[clique_idx + 1]

    num_edges = int((end_idx - start_idx) * (end_idx - start_idx - 1) / 2 * (1 - prob_missing))

    with tqdm(total=num_edges,
              desc='Creating clique edges',
              mininterval=1.0,
              unit='edges', ncols=100) as pbar:
        for edges in make_edges_generalized_clique(db_info, graph_info, bulk_size, prob_missing, start_idx, end_idx,
                                                   time_tracker):
            start_inserting = time.monotonic()
            insert_documents(db_info, edges, db_info.edge_coll_name)
            time_tracker.insert_edges_time += time.monotonic() - start_inserting
            pbar.update(len(edges))
    time_tracker.make_and_insert_clique_edges_time += time.monotonic() - start_time


def create_one_clique_graph(db_info: DatabaseInfo,
                            bulk_size,
                            num_vertices: int,
                            graph_info: GraphInfo, time_tracker: TimeTracking):
    create_graph(db_info)
    make_and_insert_vertices(db_info, graph_info, num_vertices, bulk_size, time_tracker, add_part=False)
    make_and_insert_clique_edges(db_info, graph_info, bulk_size, time_tracker, start_idx=0, end_idx=num_vertices)


def create_cliques_graph(db_info: DatabaseInfo,
                         graph_info: GraphInfo,
                         c_graph_info: CliquesGraphInfo,
                         bulk_size: int,
                         time_tracker: TimeTracking,
                         be_verbose: bool
                         ):

    create_graph(db_info)

    # create the cliques

    c_helper = CliquesHelper()

    if be_verbose:
        generator = trange(c_graph_info.num_cliques, desc='Constructing cliques', mininterval=1.0, unit='clique')
    else:
        generator = range(c_graph_info.num_cliques)
    for clique_i in generator:
        num_vertices = random.randint(c_graph_info.min_size_clique, c_graph_info.max_size_clique)
        make_and_insert_vertices(db_info, graph_info, num_vertices, bulk_size, time_tracker, add_part=True,
                                 c_helper=c_helper, be_verbose=be_verbose)
        make_and_insert_clique_edges(db_info, graph_info, bulk_size, time_tracker, c_graph_info.prob_missing_one,
                                     c_helper=c_helper, clique_idx=clique_i)

    # create edges_ between cliques
    for edges in connect_parts(c_helper, bulk_size, c_graph_info.prob_missing_all, c_graph_info.prob_missing_one_between,
                                 time_tracker, db_info, graph_info, be_verbose):
        insert_documents(db_info, edges, db_info.edge_coll_name)
