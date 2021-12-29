import random
from tqdm import trange

from edges_generator import make_edges_connect_parts
from general import create_graph, insert_documents, graph_exists
from helper_classes import DatabaseInfo, GraphInfo, CliquesGraphInfo, CliquesHelper
from vertices_generator import make_and_insert_vertices


def create_k_partite_graph(db_info: DatabaseInfo,
                           graph_info: GraphInfo,
                           parts_graph_info: CliquesGraphInfo,
                           bulk_size: int,
                           be_verbose=True
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
        generator_ = trange(parts_graph_info.num_cliques, total=parts_graph_info.num_cliques,
                            desc='Creating parts',
                            mininterval=1.0,
                            unit='parts', ncols=100)
    else:
        generator_ = range(parts_graph_info.num_cliques)
    for i in generator_:
        size = random.randint(parts_graph_info.min_size_clique, parts_graph_info.max_size_clique)
        make_and_insert_vertices(db_info, graph_info, size, bulk_size, add_part=True, c_helper=c_helper)

    # create edges between cliques
    # todo finish making parallel
    for edges in make_edges_connect_parts(c_helper, bulk_size, 0.0, 0.0, db_info, graph_info, be_verbose):
        insert_documents(db_info, edges, db_info.edge_coll_name)
