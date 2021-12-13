import os
import sys
import time
from pathlib import PurePath

from databaseinfo import DatabaseInfo, GraphInfo
from general import file_reader, insert_documents, create_graph, ConverterToVertex, get_time_difference_string


def import_graphalytics_get_files(directory: str):
    '''
    Append to the directory the filename which is the suffix of directory after the last '/'
    (or just directory if no '/') and then append '.v', '.e' and '.properties' and return
    all three filenames as absolute paths. todo: test if non-absolute directory works
    :param directory: the directory where Graphalytics files are expected to be. The names of the files should be
            (up to the extensions) the suffix of directory after the last '/'.
    :return: the three filenames
    '''
    graph_name = PurePath(directory).name
    return os.path.join(directory, graph_name + '.v'), os.path.join(directory, graph_name + '.e'), os.path.join(
        directory, graph_name + '.properties')


def graphalytics_get_property(properties_filename: str, property: str):
    '''

    :param properties_filename:
    :param property: 'num_vertices', 'num_edges', 'isDirected'
    :return:
    '''
    with open(properties_filename, 'r') as f:
        contents: str = f.read()
        substring: str
        if property == 'isDirected':
            substring = '.directed = '
        elif property == 'num_vertices':
            substring = '.vertices = '
        elif property == 'num_edges':
            substring = '.edges = '
        else:
            raise RuntimeError(f'Cannot get property {property} from {properties_filename}.')
        pos = contents.index(substring) + len(substring)
        prop, _ = contents[pos:].split(os.linesep, 1)
        if property == 'num_vertices' or property == 'num_edges':
            prop = int(prop)
        elif property == 'isDirected':
            prop = True if prop == 'true' else False
        return prop


# def graphalytics_get_directedness(properties_filename: str) -> bool:
#     '''
#     Return True if the file contains the substring '.directed = true' and false otherwise.
#     :param properties_filename:
#     :return:
#     '''
#     with open(properties_filename, 'r') as f:
#         contents: str = f.read()
#         return '.directed = true' in contents
# def graphalytics_get_num_vertices(properties_filename: str) -> int:
#     with open(properties_filename, 'r') as f:
#         contents: str = f.read()
#         pos = contents.index('.vertices = ') + len('.vertices = ')
#         num_vertices, _ = contents[pos:].split(os.linesep, 1)
#         return int(num_vertices)


def read_and_create_vertices(vertices_filename, properties_filename, db_info: DatabaseInfo, bulk_size):
    '''
    Read vertices from the given file and insert them into the collection v_coll in bulks of size
    bulk_size with smart attribute smart_attribute. The vertices must be given one vertex per line as <vertex id>.
    :param filename: the filename of the file containing a list of vertices
    :param db_info database info (endpoint, vertices_coll_name, smart_attribute, username, password)
    :param bulk_size: the bulk size
    :return: None
    '''
    count = 0
    num_vertices = graphalytics_get_property(properties_filename, 'num_vertices')
    sys.stdout.write(f"Inserted vertices:            /{num_vertices:,}\x1b[{len(str(f'{num_vertices:,}')) + 12}D")
    sys.stdout.flush()
    start_v = time.time()
    for vids in file_reader(vertices_filename, bulk_size):
        vertices = [{f'{db_info.smart_attribute}': str(vid), '_key': str(vid) + ':' + str(vid)} for vid in vids]
        insert_documents(db_info, vertices, db_info.vertices_coll_name)
        count += len(vertices)
        sys.stdout.write(f'{count:,}')
        sys.stdout.flush()
        sys.stdout.write(f'\x1b[{len(f"{count:,}")}D')
        sys.stdout.flush()
    sys.stdout.write(f'\x1b[{31 + len(str(f"{num_vertices:,}"))}C')
    sys.stdout.flush()
    print()
    print('Time for vertices: ' + get_time_difference_string(time.time() - start_v))


def read_and_create_edges(edges_filename, properties_filename, db_info: DatabaseInfo, graph_info: GraphInfo, bulk_size):
    '''
    Read edges from the given file and insert them into the collection edges_coll_name in bulks of size
     bulk_size with smart attribute smart_attribute. The edges must be given one edge per line in the form
     <node id> <node id> [<weight>]. If the weight is not given, Null is inserted.
     If isDirected is False, with every edge (a,b) also the edge (b,a) with the same weight is inserted.
     Lines starting with '#' or '/' are skipped.
    :param edges_filename:
    :param db_info:
    :param graph_info:
    :param bulk_size:
    :return:
    '''
    count = 0
    num_edges = graphalytics_get_property(properties_filename, 'num_edges')

    if graph_info.isDirected:
        num_edges_string = f'{num_edges:,}'
    else:
        num_edges_string = f'{num_edges:,} (undirected, i.e., {num_edges*2:,} directed)'
    print()
    sys.stdout.write(f"Inserted edges:            /{num_edges_string}\x1b[{len(num_edges_string) + 12}D")
    sys.stdout.flush()

    to_v = ConverterToVertex(db_info.vertices_coll_name).idx_to_vertex
    if not graph_info.isDirected:
        bulk_size //= 2

    start_e = time.time()
    for eids in file_reader(edges_filename, bulk_size):
        edges = list()
        if eids[0] == '#' or eids[0] == '/':
            continue
        if graph_info.isDirected:
            for i in eids:
                e = i.split(' ', 2)
                if len(e) == 2:  # no weight given
                    f, t = e
                    edges.append({"_from": to_v(f), "_to": to_v(t)})  # Null will be inserted
                else:
                    f, t, w = e
                    edges.append({"_from": to_v(f), "_to": to_v(t), "weight": f'{w}'})
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
        insert_documents(db_info, edges, db_info.edge_coll_name)
        count += len(edges)
        sys.stdout.write(f'{count:,}')
        sys.stdout.flush()
        sys.stdout.write(f'\x1b[{len(f"{count:,}")}D')
        sys.stdout.flush()
    sys.stdout.write(f'\x1b[{31 + len(num_edges_string)}')
    print()
    print('Time for edges: ' + get_time_difference_string(time.time() - start_e))

def import_graphalytics(db_info: DatabaseInfo, graph_info: GraphInfo, vertices_filename, edges_filename,
                        properties_filename, bulk_size, isSmart: bool=False):
    '''
    Create a new smart graph with vertices v_coll and edges edge_coll_name with given parameters.
     If db_info.overwrite is True and the graph and/or the vertex/edge collection exist, they are dropped first.
     Read vertices from vertices_filename and insert them into db_info.vertices_coll_name in bulks of size
     bulk_size with smart attribute db_info.smart_attribute. The vertices must be given one vertex per line as
     <vertex id>.
     Read edges from edges_filename and insert them into db_info.edges_coll_name in bulks of size
     bulk_size with smart attribute db_info.smart_attribute. The edges must be given one edge per line in the form
     <node id> <node id> [<weight>]. If the weight is not given, Null is inserted. The edges are directed
     if db_info.isDirected is not given or False and the file properties_filename contains
     a substring '.directed = true', otherwise, with every edge (a,b) also the edge (b,a) with the same weight
     is inserted.
     Lines starting with '#' or '/' are skipped.
    :param isSmart: whether the graph should be a smartGraph
    :param graph_info:
    :param db_info: database info
    :param vertices_filename: the name of  the file to read vertices from
    :param edges_filename: the name of the file to read edges from
    :param properties_filename: the name of the file containing information about whether the graph should be directed
    :param bulk_size: the size of bulks
    :return: None
    '''
    create_graph(db_info)
    read_and_create_vertices(vertices_filename, properties_filename, db_info, bulk_size)
    isDirected = graphalytics_get_property(properties_filename,
                                           'isDirected') if graph_info.isDirected is None or graph_info.isDirected is False else False
    graph_info.isDirected = isDirected
    read_and_create_edges(edges_filename, properties_filename, db_info, graph_info, bulk_size)
