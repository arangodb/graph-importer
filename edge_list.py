from general import file_reader, insert_edges, create_graph, insert_vertices_unique


def read_and_create_vertices_and_edges(edges_filename, edges_coll_name, vertices_coll_name, endpoint, bulk_size,
                                       isDirected,
                                       smart_attribute, username, password):
    '''
    (Almost the same as read_and_create_edges from graphalytics_importer. Duplicate code to avoid if checks for every edge.)
     Read edges from the given file and insert them and the corresponding vertices into the collections edges_coll_name
     and vertices_coll_name in bulks of size bulk_size with smart attribute smart_attribute. The edges must be given one
     edge per line in the form <node id> <node id> [<weight>]. If the weight is not given, Null is inserted.
     If isDirected is False, with every edge (a,b) also the edge (b,a) with the same weight is inserted.
     Lines starting with '#' or '/' are skipped.
    :param edges_filename:
    :param edges_coll_name:
    :param vertices_coll_name:
    :param endpoint:
    :param bulk_size:
    :param isDirected:
    :param smart_attribute:
    :param username:
    :param password:
    :return:
    '''
    if not isDirected:
        bulk_size //= 2
    for eids in file_reader(edges_filename, bulk_size):
        edges = []
        vertex_indexes = set()  # difference to read_and_create_edges
        if eids[0] == '#' or eids[0] == '/':
            continue
        if isDirected:
            for i in eids:
                e = i.split(' ', 2)
                if len(e) == 2:  # no weight given
                    f, t = e
                    edges.append({"_from": f"{f}", "_to": f"{t}"})  # Null will be inserted
                else:  # len == 3
                    f, t, w = e
                    edges.append({"_from": f"{f}", "_to": f"{t}", "weight": f'{w}'})
                # add vertices
                # this tests existence just in this bulk, globally checked in insert_vertices_unique
                vertex_indexes.add(f)
                vertex_indexes.add(t)

        else:
            for i in eids:
                e = i.split(' ', 2)
                if len(e) == 2:
                    f, t = e
                    edges.append({"_from": f"{f}", "_to": f"{t}"})  # Null will be inserted for weight
                    edges.append({"_from": f"{t}", "_to": f"{f}"})  # Null will be inserted for weight
                else:
                    f, t, w = e
                    edges.append({"_from": f"{f}", "_to": f"{t}", "weight": f'{w}'})
                    edges.append({"_from": f"{t}", "_to": f"{f}", "weight": f'{w}'})
                # add vertices
                # this tests existence just in this bulk, globally checked in insert_vertices_unique
                vertex_indexes.add(f)
                vertex_indexes.add(t)

        insert_vertices_unique(endpoint, vertices_coll_name, vertex_indexes, smart_attribute, username, password)
        insert_edges(endpoint, edges_coll_name, vertices_coll_name, edges, smart_attribute, username, password)


def import_edge_list(endpoint, filename, bulk_size,
                     make_undirected, graphname, edges_coll_name, vertices_coll_name, replication_factor,
                     number_of_shards, overwrite, smart_attribute, username, password):
    create_graph(endpoint, graphname, vertices_coll_name, edges_coll_name, replication_factor, number_of_shards,
                 overwrite,
                 smart_attribute, username, password)
    read_and_create_vertices_and_edges(filename, edges_coll_name, vertices_coll_name, endpoint, bulk_size,
                                       not make_undirected, smart_attribute, username, password)
