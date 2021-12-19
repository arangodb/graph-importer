import random
import time
from typing import Iterable
from tqdm import tqdm, trange

import time_tracking
from helper_classes import DatabaseInfo, GraphInfo, CliquesGraphInfo, CliquesHelper
from general import create_graph, insert_documents
from edges_generator import add_edge
from vertices_generator import make_and_insert_vertices, ConverterToVertex


def create_k_partite_graph(db_info: DatabaseInfo,
                           graph_info: GraphInfo,
                           parts_graph_info: CliquesGraphInfo,
                           bulk_size: int,
                           time_tracker: time_tracking.TimeTracking
                           ):
    def connect_parts(clique_helper: CliquesHelper, v_coll: str, bulk_size_: int,
                      time_tracker: time_tracking.TimeTracking) -> Iterable:
        """
        Given a list parts of disjoint vertex sets (disjointness is not verified), connect every vertex of every part
        with every vertex of every other part.
        :param time_tracker:
        :rtype: None
        :param clique_helper:
        :param v_coll:
        :param bulk_size_:
        """
        connect_parts_start_time = time.monotonic()
        edges_ = []
        to_vrtx = ConverterToVertex(v_coll).idx_to_vertex

        # exclude self-loops

        for v in tqdm(range(clique_helper.num_cliques()), desc='Connecting a part', mininterval=1.0, unit='part'):
            for w in range(v + 1, clique_helper.num_cliques()):
                start_v = clique_helper.starts_of_cliques[v]
                end_v = clique_helper.starts_of_cliques[v + 1]
                start_w = clique_helper.starts_of_cliques[w]
                end_w = clique_helper.starts_of_cliques[w + 1]

                for f in range(start_v, end_v):
                    for t in range(start_w, end_w):
                        add_edge(f, t, edges_, 0.0, db_info, graph_info, to_v=to_vrtx, time_tracker=time_tracker)
                        if len(edges_) >= bulk_size_:
                            time_tracker.connect_parts_time += time.monotonic() - connect_parts_start_time
                            yield edges_
                            edges_.clear()

    create_graph(db_info)

    # create the cliques

    c_helper = CliquesHelper()

    for i in trange(parts_graph_info.num_cliques, total=parts_graph_info.num_cliques,
                    desc='Creating parts',
                    mininterval=1.0,
                    unit='parts', ncols=100):
        size = random.randint(parts_graph_info.min_size_clique, parts_graph_info.max_size_clique)
        make_and_insert_vertices(db_info, graph_info, size, bulk_size, time_tracker, add_part=True, c_helper=c_helper)

    # create edges between cliques
    for edges in connect_parts(c_helper, db_info.vertices_coll_name, bulk_size, time_tracker):
        s = time.monotonic()
        insert_documents(db_info, edges, db_info.edge_coll_name)
        time_tracker.insert_edges_time += time.monotonic() - s
