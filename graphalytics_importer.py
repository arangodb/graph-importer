import os
from pathlib import PurePath

from general import file_reader, insert_vertices, insert_edges, create_graph


def import_graphalytics_get_files(directory: str):
    '''
    Append to the directory the filename which is the suffix of directory after the last '/' (or just directory if no '/')
     and then append '.v', '.e' and '.properties' and return all three filenames as absolute paths. todo: test if non-absolute directory works
    :param directory: the directory where Graphalytics files are expected to be. The names of the files should be
            (up to the extensions) the suffix of directory after the last '/'.
    :return: the three filenames
    '''
    graph_name = PurePath(directory).name
    return os.path.join(directory, graph_name + '.v'), \
           os.path.join(directory, graph_name + '.e'), \
           os.path.join(directory, graph_name + '.properties')


def graphalytics_get_directedness(properties_filename: str) -> bool:
    '''
    Return True if the file contains the substring '.directed = true' and false otherwise.
    :param properties_filename:
    :return:
    '''
    with open(properties_filename, 'r') as f:
        contents: str = f.read()
        return '.directed = true' in contents


def read_and_create_vertices(filename, endpoint, bulk_size, vertices_coll_name, smart_attribute, username, password):
    '''
    Read vertices from the given file and insert them into the collection vertices_coll_name in bulks of size
    bulk_size with smart attribute smart_attribute. The vertices must be given one vertex per line as <vertex id>.
    :param filename: the filename of the file containing a list of vertices
    :param endpoint: the server address
    :param bulk_size: the bulk size
    :param vertices_coll_name: the name of the collection to insert vertices into
    :param smart_attribute: the smart attribute
    :param username: the username
    :param password: the password
    :return: None
    '''
    for vids in file_reader(filename, bulk_size):
        vertices = [{f'{smart_attribute}': str(vid), '_key': str(vid) + ':'+ str(vid)} for vid in vids]
        insert_vertices(endpoint, vertices_coll_name, vertices, username, password)


def read_and_create_edges(edges_filename, edges_coll_name, vertices_coll_name, endpoint, bulk_size, isDirected,
                          smart_attribute, username, password):
    '''
    Read edges from the given file and insert them into the collection edges_coll_name in bulks of size
     bulk_size with smart attribute smart_attribute. The edges must be given one edge per line in the form
     <node id> <node id> [<weight>]. If the weight is not given, Null is inserted.
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
        edges = list()
        if eids[0] == '#' or eids[0] == '/':
            continue
        if isDirected:
            for i in eids:
                e = i.split(' ', 2)
                if len(e) == 2:  # no weight given
                    f, t = e
                    edges.append({"_from": f"{vertices_coll_name}/{f}:{f}", "_to": f"{vertices_coll_name}/{t}:{t}"})  # Null will be inserted
                else:
                    f, t, w = e
                    edges.append({"_from": f"{vertices_coll_name}/{f}:{f}", "_to": f"{vertices_coll_name}/{t}:{t}", "weight": f'{w}'})
        else:
            for i in eids:
                e = i.split(' ', 2)
                if len(e) == 2:
                    f, t = e
                    edges.append({"_from": f"{vertices_coll_name}/{f}:{f}", "_to": f"{vertices_coll_name}/{t}:{t}"})  # Null will be inserted for weight
                    edges.append({"_from": f"{vertices_coll_name}/{t}:{t}", "_to": f"{vertices_coll_name}/{f}:{f}"})  # Null will be inserted for weight
                else:
                    f, t, w = e
                    edges.append({"_from": f"{vertices_coll_name}/{f}:{f}", "_to": f"{vertices_coll_name}/{t}:{t}", "weight": f'{w}'})
                    edges.append({"_from": f"{vertices_coll_name}/{t}:{t}", "_to": f"{vertices_coll_name}/{f}:{f}", "weight": f'{w}'})
        insert_edges(endpoint, edges_coll_name, vertices_coll_name, edges, smart_attribute, username, password)


def import_graphalytics(endpoint, vertices_filename, edges_filename, properties_filename, bulk_size,
                        enforce_undirected, graphname, edges_coll_name, vertices_coll_name, replication_factor,
                        number_of_shards, overwrite, smart_attribute, username, password):
    '''
    Create a new smart graph with vertices vertices_coll_name and edges edge_coll_name with given parameters.
     If overwrite is True and the graph and/or the vertex/edge collection exist, they are dropped first.
     Read vertices from vertices_filename and insert them into vertices_coll_name in bulks of size
     bulk_size with smart attribute smart_attribute. The vertices must be given one vertex per line as <vertex id>.
     Read edges from edges_filename and insert them into edges_coll_name in bulks of size
     bulk_size with smart attribute smart_attribute. The edges must be given one edge per line in the form
     <node id> <node id> [<weight>]. If the weight is not given, Null is inserted. The edges are directed
     if enforce_undirected is false and the file properties_filename contains a substring '.directed = true',
     otherwise, with every edge (a,b) also the edge (b,a) with the same weight is inserted.
     Lines starting with '#' or '/' are skipped.
    :param endpoint: the server address
    :param vertices_filename: the name of  the file to read vertices from
    :param edges_filename: the name of the file to read edges from
    :param properties_filename: the name of the file containing information about whether the graph should be directed
    :param bulk_size: the size of bulks
    :param enforce_undirected: whether to make the graph undirected (regardless of the contents of the properties file)
    :param graphname: the name of the graph to be created
    :param edges_coll_name: the name of the edge collection to be created
    :param vertices_coll_name: the name of the vertex collection to be created
    :param replication_factor: the replication factor for the vertices
    :param number_of_shards: the number of shards
    :param overwrite: whether the graph and/or the vertex/edge collections should be overwritten if they exsit
    :param smart_attribute: the smart attribute
    :param username: the user name
    :param password: the password
    :return: None
    '''
    create_graph(endpoint, graphname, vertices_coll_name, edges_coll_name, replication_factor, number_of_shards,
                 overwrite,
                 smart_attribute, username, password)
    read_and_create_vertices(vertices_filename, endpoint, bulk_size, vertices_coll_name, smart_attribute, username,
                             password)
    isDirected = False if enforce_undirected else graphalytics_get_directedness(properties_filename)
    read_and_create_edges(edges_filename, edges_coll_name, vertices_coll_name, endpoint, bulk_size, isDirected,
                          smart_attribute, username, password)
