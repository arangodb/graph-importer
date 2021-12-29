import math
import multiprocessing
import random
import time
from typing import List, Union, Tuple, Optional

from tqdm import tqdm, trange

from edges_generator import add_edge, add_smart_edge, make_edges_connect_parts
from general import yes_with_prob, insert_documents, create_graph, graph_exists, get_time_difference_string
from helper_classes import DatabaseInfo, GraphInfo, CliquesGraphInfo, CliquesHelper
from vertices_generator import make_and_insert_vertices, ConverterToVertex


def get_num_edges_between_cliques(size1: int, size2: int):
    return random.randint(1, size1 * size2 + 1)


def make_edges_generalized_clique(db_info: DatabaseInfo,
                                  graph_info: GraphInfo,
                                  bulk_size: int,
                                  prob_missing: float,
                                  first_idx: int,
                                  end_idx: int,
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
                add_smart_edge(i, j, edges, prob_missing, db_info, graph_info, to_v, smart_val_i, str(j))
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
                add_edge(i, j, edges, prob_missing, db_info, graph_info, to_v)
                if len(edges) >= bulk_size:
                    yield edges
                    edges.clear()
            if edges:
                yield edges
                edges.clear()
    if edges:
        yield edges


def make_edges_generalized_clique_piece(db_info: DatabaseInfo,
                                        graph_info: GraphInfo,
                                        bulk_size: int,
                                        prob_missing: float,
                                        start_from_idx: int,
                                        end_from_idx: int,
                                        end_idx: int
                                        ):
    edges = []

    # last entry in c_helper.starts_of_cliques is for the next clique
    # if clique_idx >= len(c_helper.starts_of_cliques) - 1:
    #     raise RuntimeError(f'make_edges_generalized_clique: clique {clique_idx} does not exist, nothing to connect.')

    if db_info.isSmart:
        to_v = ConverterToVertex(db_info.vertices_coll_name).idx_to_smart_vertex
        for i in range(start_from_idx, end_from_idx):
            smart_val_i = str(i)
            for j in range(i + 1, end_idx):
                add_smart_edge(i, j, edges, prob_missing, db_info, graph_info, to_v, smart_val_i, str(j))
                if len(edges) >= bulk_size:
                    yield edges
                    edges.clear()
            if edges:
                yield edges
                edges.clear()
    else:
        to_v = ConverterToVertex(db_info.vertices_coll_name).idx_to_vertex
        for i in range(start_from_idx, end_from_idx):
            for j in range(i + 1, end_idx):
                add_edge(i, j, edges, prob_missing, db_info, graph_info, to_v)
                if len(edges) >= bulk_size:
                    yield edges
                    edges.clear()
            if edges:
                yield edges
                edges.clear()
    if edges:
        yield edges


def make_tournament_edges(edge_property: Union[str, None, Tuple[str, List[str]]],
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
                    edges.append({"_from": to_v(i), "_to": to_v(j)})
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


def make_and_insert_piece(db_info: DatabaseInfo, graph_info: GraphInfo, bulk_size: int, prob_missing: float,
                          start_from_idx: int, end_from_idx: int, end_idx: int, num_edges: int, be_verbose: bool,
                          i: int, num_cores=multiprocessing.cpu_count()):
    def _do_make(do_pbar_update: bool) -> int:
        num_edges_ = 0
        for edges in make_edges_generalized_clique_piece(db_info, graph_info, bulk_size, prob_missing, start_from_idx,
                                                         end_from_idx, end_idx):
            insert_documents(db_info, edges, db_info.edge_coll_name)
            if do_pbar_update:
                pbar.update(len(edges))
            num_edges_ += len(edges)
        return num_edges_

    start = time.monotonic()

    if be_verbose and i == 0:
        with tqdm(total=num_edges,
                  desc=f'Creating clique edges (1/{num_cores})',
                  mininterval=1.0,
                  unit='edges', ncols=100) as pbar:
            num_edges = _do_make(do_pbar_update=True)
    else:
        num_edges = _do_make(do_pbar_update=False)

    if be_verbose:
        print(f'Time process {i:2}: {get_time_difference_string(time.monotonic() - start):10}, {num_edges:6} edges, '
              f'start: {start_from_idx}, end_from: {end_from_idx}')


def make_and_insert_clique_edges(db_info: DatabaseInfo, graph_info: GraphInfo,
                                 bulk_size: int,
                                 prob_missing: float = 0.0,
                                 c_helper: Optional[CliquesHelper] = None,
                                 clique_idx: Optional[int] = None,
                                 start_idx: Optional[int] = None,
                                 end_idx: Optional[int] = None,
                                 be_verbose: bool = True
                                 ):
    """
    Makes all edges of a clique (an edge is missing with probability prob_missing) and insert them into the database
    specified in db_db_info. Either c_helper and clique_idx are given or start_idx and end_idx. In the former case,
    the vertices from clique number clique_idx are connected
    i.e., vertices in [c_helper.starts_of_cliques[clique_idx], c_helper.starts_of_cliques[clique_idx + 1]),
    in the latte case, in [start_idx, end_idx). Parameter graph_info carries the information about the additional
    attribute of edges.
    :param db_info:
    :param graph_info:
    :param bulk_size:
    :param prob_missing:
    :param c_helper:
    :param clique_idx:
    :param start_idx:
    :param end_idx:
    :param be_verbose:
    :return:
    """
    assert (c_helper is not None and clique_idx is not None and start_idx is None and end_idx is None) or (
            c_helper is None and clique_idx is None and start_idx is not None and end_idx is not None)
    if clique_idx is not None:
        start_idx = c_helper.starts_of_cliques[clique_idx]
        end_idx = c_helper.starts_of_cliques[clique_idx + 1]

    n = end_idx - start_idx  # number vertices
    num_edges = int(n * (n - 1) / 2 * (1 - prob_missing))
    num_cores = multiprocessing.cpu_count()

    if num_cores * 100 < end_idx - start_idx:
        # parallelise
        jobs = []

        # Each process i makes edges from each vertex v in [start_i_idx, end_i_idx)
        # to each vertex w in [i+1, end_idx). This is done in make_and_insert_piece().
        # Note that end_idx is the same for all processes and that start_i_idx of process i+1 is
        # end_i_idx of the process i. We need to compute end_i_idx such that the number of produced edges,
        # which is piece_size = num_edges // num_cores is approximately the same for all processes.
        # It's tiresome and error-prone to compute all end_i_idx at once, so we compute the
        # current one in the corresponding iteration where we need it.
        piece_size = num_edges // num_cores
        start_i_idx = start_idx
        for i in range(num_cores):
            # solving for end_i_idx the equality piece_size = sum_{i=0}^end_p_idx (n-i-1)
            # where the right-hand side is the number of edges from vertices v in [0,end_p_idx]
            # to vertices w with v < w <= end_i_idx. (For convenience, we shifted end_i_idx to the left by one).
            # So the first process gets the from vertices in the interval [0, end_p_idx + 1) and,
            # in general, the current process in [start_i_idx, end_p_idx + 1).
            end_i_idx = start_i_idx + int(((2 * n - 1) -
                                           math.sqrt((2 * n - 1) * (2 * n - 1) - 4 * (
                                                   2 - 2 * n + 2 * piece_size))) // 2)  # school math
            end_i_idx = min(end_idx, end_i_idx + 1)  # shift back
            if i == num_cores - 1:
                end_i_idx = end_idx

            process = multiprocessing.Process(target=make_and_insert_piece, args=(
                db_info.copy(), graph_info.copy(), bulk_size, prob_missing, start_i_idx, end_i_idx, end_idx, piece_size,
                be_verbose, i, num_cores))
            process.start()
            jobs.append(process)
            # now update the interval whose first part is away to the previous process
            n -= end_i_idx - start_i_idx
            start_i_idx = end_i_idx

        # for j in jobs:
        #     j.start()
        for j in jobs:
            j.join()
    else:
        make_and_insert_piece(db_info, graph_info, bulk_size, prob_missing, start_idx, end_idx, end_idx, num_edges,
                              be_verbose, 0)


def create_one_clique_graph(db_info: DatabaseInfo,
                            bulk_size,
                            num_vertices: int,
                            graph_info: GraphInfo, be_verbose: bool = True):
    """
    Create a clique with num_vertices vertices in the database.
    :param db_info:
    :param bulk_size:
    :param num_vertices:
    :param graph_info:
    :param be_verbose:
    :return:
    """
    if db_info.overwrite or not graph_exists(db_info):
        create_graph(db_info)
    else:
        if be_verbose:
            print(f'The graph {db_info.graph_name} exists already, skipping. To overwrite, use \'--overwrite\'.')
        return

    make_and_insert_vertices(db_info, graph_info, num_vertices, bulk_size, add_part=False)
    make_and_insert_clique_edges(db_info, graph_info, bulk_size, start_idx=0, end_idx=num_vertices,
                                 be_verbose=be_verbose)


def connect_parts(c_helper: CliquesHelper, bulk_size: int, prob_missing_all: float,
                  prob_missing_one_between: float, db_info: DatabaseInfo, graph_info: GraphInfo,
                  start_i_idx: int, end_i_idx: int, be_verbose: bool, num_cores: int):
    for edges in make_edges_connect_parts(c_helper, bulk_size, prob_missing_all,
                                          prob_missing_one_between,
                                          db_info, graph_info, start_i_idx, end_i_idx, be_verbose):
        insert_documents(db_info, edges, db_info.edge_coll_name)


def create_cliques_graph(db_info: DatabaseInfo,
                         graph_info: GraphInfo,
                         c_graph_info: CliquesGraphInfo,
                         bulk_size: int,
                         be_verbose: bool
                         ) -> None:
    if db_info.overwrite or not graph_exists(db_info):
        create_graph(db_info)
    else:
        if be_verbose:
            print(f'The graph {db_info.graph_name} exists already, skipping. To overwrite, use \'--overwrite\'.')
        return

    # create the cliques

    c_helper = CliquesHelper()

    if be_verbose:
        generator = trange(c_graph_info.num_cliques, desc='Constructing cliques', mininterval=1.0, unit='clique')
    else:
        generator = range(c_graph_info.num_cliques)
    for clique_i in generator:
        num_vertices = random.randint(c_graph_info.min_size_clique, c_graph_info.max_size_clique)
        make_and_insert_vertices(db_info, graph_info, num_vertices, bulk_size, add_part=True,
                                 c_helper=c_helper, be_verbose=be_verbose)
        make_and_insert_clique_edges(db_info, graph_info, bulk_size, c_graph_info.prob_missing_one,
                                     c_helper=c_helper, clique_idx=clique_i, be_verbose=be_verbose)

    # create edges_ between cliques
    n = c_helper.num_cliques()
    num_edges = int(n * (n - 1) / 2)
    num_cores = multiprocessing.cpu_count()

    if num_cores * 100 < num_edges:
        # parallelise
        jobs = []

        piece_size = num_edges // num_cores
        start_i_idx = 0
        for i in range(num_cores):
            end_i_idx = start_i_idx + int(((2 * n - 1) -
                                           math.sqrt((2 * n - 1) * (2 * n - 1) - 4 * (
                                                   2 - 2 * n + 2 * piece_size))) // 2)  # school math
            end_i_idx = min(n, end_i_idx + 1)  # shift back
            if i == num_cores - 1:
                end_i_idx = n
            process = multiprocessing.Process(target=connect_parts,
                                              args=(c_helper, bulk_size, c_graph_info.prob_missing_all,
                                                    c_graph_info.prob_missing_one_between,
                                                    db_info, graph_info, start_i_idx, end_i_idx, be_verbose, i,
                                                    num_cores))
            process.start()
            jobs.append(process)
            # now update the interval whose first part is away to the previous process
            n -= end_i_idx - start_i_idx
            start_i_idx = end_i_idx

    # the logic is as in make_edges_generalized_clique_piece
