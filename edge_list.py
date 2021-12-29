from tqdm import tqdm

from general import file_reader, insert_documents, create_graph, graph_exists
from helper_classes import DatabaseInfo
from vertices_generator import insert_vertices_unique, ConverterToVertex


def read_and_create_vertices_and_edges(db_info: DatabaseInfo, edges_filename, bulk_size, be_verbose: bool):
    """
    (Almost the same as read_and_create_edges_graphalytics from graphalytics_importer. Duplicate code to avoid if checks
     for every edge.) Read edges from the given file and insert them and the corresponding vertices into the collections
    edges_coll_name and v_coll in bulks of num_vertices bulk_size with smart attribute smart_attribute. The edges must
    be given one edge per line in the form <node id> <node id> [<weight>]. If the weight is not given, Null is inserted.
    Lines starting with '#' or '/' are skipped.
    :param be_verbose:
    :param db_info:
    :param edges_filename:
    :param bulk_size:
    :return:
    """

    def make_edges_and_vertex_indexes():
        edges_ = []
        vertex_indexes_ = set()
        for i in eids:
            if i[0] == '#' or i[0] == '/' or i[0] == '%':
                continue
            e = i.split(' ', 2)
            if len(e) == 2:  # no weight given
                f, t = e
                edges_.append({"_from": to_v(f), "_to": to_v(t)})  # Null will be inserted
            else:  # len == 3
                f, t, w = e
                edges_.append({"_from": to_v(f), "_to": to_v(t), "weight": f'{w}'})
            # add vertices
            # this tests existence just in this bulk, globally checked in insert_vertices_unique
            vertex_indexes_.add(f)
            vertex_indexes_.add(t)

        return edges_, vertex_indexes_

    to_v = ConverterToVertex(db_info.vertices_coll_name).idx_to_vertex

    if be_verbose:
        with tqdm(desc='Importing edges',
                  mininterval=1.0,
                  unit='edges', ncols=100) as pbar:
            for eids in file_reader(edges_filename, bulk_size):
                edges, vertex_indexes = make_edges_and_vertex_indexes()
                insert_vertices_unique(db_info, vertex_indexes)
                insert_documents(db_info, edges, db_info.edge_coll_name)
                pbar.update(len(edges))
    else:
        for eids in file_reader(edges_filename, bulk_size):
            edges, vertex_indexes = make_edges_and_vertex_indexes()
            insert_vertices_unique(db_info, vertex_indexes)
            insert_documents(db_info, edges, db_info.edge_coll_name)


def import_edge_list(db_info: DatabaseInfo, filename, bulk_size, be_verbose: bool):
    if db_info.overwrite or not graph_exists(db_info):
        create_graph(db_info)
        read_and_create_vertices_and_edges(db_info, filename, bulk_size, be_verbose)
    else:
        if be_verbose:
            print(f'The graph {db_info.graph_name} exists already, skipping. To overwrite, use \'--overwrite\'.')
