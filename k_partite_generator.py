import random
import time
from typing import Iterable

from tqdm import tqdm, trange

import time_tracking
from edges_generator import add_smart_edge, add_edge, connect_parts
from general import create_graph, insert_documents
from helper_classes import DatabaseInfo, GraphInfo, CliquesGraphInfo, CliquesHelper
from vertices_generator import make_and_insert_vertices, ConverterToVertex


def create_k_partite_graph(db_info: DatabaseInfo,
                           graph_info: GraphInfo,
                           parts_graph_info: CliquesGraphInfo,
                           bulk_size: int,
                           time_tracker: time_tracking.TimeTracking,
                           be_verbose = True
                           ):


    create_graph(db_info)

    # create the cliques

    c_helper = CliquesHelper()
    if be_verbose:
        generator_ = trange(parts_graph_info.num_cliques, total=parts_graph_info.num_cliques,
                    desc='Creating parts',
                    mininterval=1.0,
                    unit='parts', ncols=100)
    else:
        generator_ = range(parts_graph_info.num_cliques)
    for i in generator_:
        size = random.randint(parts_graph_info.min_size_clique, parts_graph_info.max_size_clique)
        make_and_insert_vertices(db_info, graph_info, size, bulk_size, time_tracker, add_part=True, c_helper=c_helper)

    # create edges between cliques
    for edges in connect_parts(c_helper, bulk_size, 0.0, 0.0, time_tracker, db_info, graph_info, be_verbose):
        s = time.monotonic()
        insert_documents(db_info, edges, db_info.edge_coll_name)
        time_tracker.insert_edges_time += time.monotonic() - s
