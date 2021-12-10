from databaseinfo import GraphInfo, DatabaseInfo
from general import file_reader, insert_documents, create_graph, insert_vertices_unique, ConverterToVertex


def read_and_create_vertices_and_edges(db_info: DatabaseInfo, graph_info: GraphInfo, edges_filename, bulk_size):
    '''
    (Almost the same as read_and_create_edges from graphalytics_importer. Duplicate code to avoid if checks for every
    edge.) Read edges from the given file and insert them and the corresponding vertices into the collections
    edges_coll_name and v_coll in bulks of size bulk_size with smart attribute smart_attribute. The edges must be given
    one edge per line in the form <node id> <node id> [<weight>]. If the weight is not given, Null is inserted.
    If isDirected is False, with every edge (a,b) also the edge (b,a) with the same weight is inserted.
    Lines starting with '#' or '/' are skipped.
    :param db_info:
    :param graph_info:
    :param edges_filename:
    :param bulk_size:
    :return:
    '''
    to_v = ConverterToVertex(db_info.vertices_coll_name).idx_to_vertex
    if not graph_info.isDirected:
        bulk_size //= 2
    for eids in file_reader(edges_filename, bulk_size):
        edges = []
        vertex_indexes = set()  # difference to read_and_create_edges
        if eids[0] == '#' or eids[0] == '/':
            continue
        if graph_info.isDirected:
            for i in eids:
                e = i.split(' ', 2)
                if len(e) == 2:  # no weight given
                    f, t = e
                    edges.append({"_from": to_v(f), "_to": to_v(t)})  # Null will be inserted
                else:  # len == 3
                    f, t, w = e
                    edges.append({"_from": to_v(f), "_to": to_v(t), "weight": f'{w}'})
                # add vertices
                # this tests existence just in this bulk, globally checked in insert_vertices_unique
                vertex_indexes.add(f)
                vertex_indexes.add(t)

        else:
            for i in eids:
                e = i.split(' ', 2)
                if len(e) == 2:
                    f, t = e
                    edges.append({"_from": to_v(f), "_to": to_v(t)})  # Null will be inserted for weight
                    edges.append({"_from": to_v(t), "_to": to_v(f)})  # Null will be inserted for weight
                else:
                    f, t, w = e
                    edges.append({"_from": to_v(f), "_to": to_v(t), "weight": f'{w}'})
                    edges.append({"_from": to_v(t), "_to": to_v(f), "weight": f'{w}'})
                # add vertices
                # this tests existence just in this bulk, globally checked in insert_vertices_unique
                vertex_indexes.add(f)
                vertex_indexes.add(t)

        insert_vertices_unique(db_info, vertex_indexes)
        insert_documents(db_info, edges, db_info.edge_coll_name)


def import_edge_list(db_info: DatabaseInfo, graph_info: GraphInfo, filename, bulk_size):
    create_graph(db_info)
    read_and_create_vertices_and_edges(db_info, graph_info, filename, bulk_size)
