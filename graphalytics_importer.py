import os
import sys
import time
from pathlib import PurePath

from tqdm import tqdm

from general import file_reader, insert_documents, create_graph, get_time_difference_string
from helper_classes import DatabaseInfo
from vertices_generator import ConverterToVertex


def import_graphalytics_get_files(directory: str):
    """
    Append to the directory the filename which is the suffix of directory after the last '/'
    (or just directory if no '/') and then append '.v', '.e' and '.properties' and return
    all three filenames as absolute paths. todo: test if non-absolute directory works
    :param directory: the directory where Graphalytics files are expected to be. The names of the files should be
            (up to the extensions) the suffix of directory after the last '/'.
    :return: the three filenames
    """
    graph_name = PurePath(directory).name
    return os.path.join(directory, graph_name + '.v'), os.path.join(directory, graph_name + '.e')


def get_property_graphalytics(properties_filename: str, property_: str):
    """

    :param properties_filename:
    :param property_: 'num_vertices', 'num_edges'
    :return:
    """
    with open(properties_filename, 'r') as f:
        contents: str = f.read()
        substring: str
        if property_ == 'num_vertices':
            substring = '.vertices = '
        elif property_ == 'num_edges':
            substring = '.edges = '
        else:
            raise RuntimeError(f'Cannot get property_ {property_} from {properties_filename}.')
        pos = contents.index(substring) + len(substring)
        prop, _ = contents[pos:].split(os.linesep, 1)
        if property_ == 'num_vertices' or property_ == 'num_edges':
            prop = int(prop)
        return prop


# def graphalytics_get_directedness(properties_filename: str) -> bool:
#     """
#     Return True if the file contains the substring '.directed = true' and false otherwise.
#     :param properties_filename:
#     :return:
#     """
#     with open(properties_filename, 'r') as f:
#         contents: str = f.read()
#         return '.directed = true' in contents
# def graphalytics_get_num_vertices(properties_filename: str) -> int:
#     with open(properties_filename, 'r') as f:
#         contents: str = f.read()
#         pos = contents.index('.vertices = ') + len('.vertices = ')
#         num_vertices, _ = contents[pos:].split(os.linesep, 1)
#         return int(num_vertices)


def read_and_create_vertices_graphalytics(vertices_filename, properties_filename, db_info: DatabaseInfo, bulk_size,
                                          be_verbose: bool):
    """
    Read vertices from the given file and insert them into the collection v_coll in bulks of num_vertices
    bulk_size with smart attribute smart_attribute. The vertices must be given one vertex per line as <vertex id>.
    :param be_verbose:
    :param vertices_filename: the filename of the file containing a list of vertices
    :param properties_filename: the filename of the file containing properties of the graph
    :param db_info database info (endpoint, vertices_coll_name, smart_attribute, username, password)
    :param bulk_size: the bulk num_vertices
    :return: None
    """
    count = 0
    num_vertices = get_property_graphalytics(properties_filename, 'num_vertices')
    if be_verbose:
        print(f'Number of vertices: {num_vertices}')
    start_v = time.monotonic()
    if be_verbose:
        with tqdm(total=num_vertices, desc='Importing vertices',
                  mininterval=1.0,
                  unit='vertices', ncols=100) as pbar:
            for vids in file_reader(vertices_filename, bulk_size):
                vertices = [{f'{db_info.smart_attribute}': str(vid), '_key': str(vid) + ':' + str(vid)} for vid in vids]
                insert_documents(db_info, vertices, db_info.vertices_coll_name)
                count += len(vertices)
                pbar.update(len(vids))
        print('Time for vertices: ' + get_time_difference_string(time.monotonic() - start_v))
    else:
        for vids in file_reader(vertices_filename, bulk_size):
            vertices = [{f'{db_info.smart_attribute}': str(vid), '_key': str(vid) + ':' + str(vid)} for vid in vids]
            insert_documents(db_info, vertices, db_info.vertices_coll_name)
            count += len(vertices)


def read_and_create_edges_graphalytics(edges_filename, properties_filename, db_info: DatabaseInfo, bulk_size,
                                       be_verbose: bool):
    """
    Read edges from the given file and insert them into the collection edges_coll_name in bulks of num_vertices
     bulk_size with smart attribute smart_attribute. The edges must be given one edge per line in the form
     <node id> <node id> [<weight>]. If the weight is not given, Null is inserted.
     Lines starting with '#' or '/' are skipped.
    :param be_verbose:
    :param properties_filename:
    :param edges_filename:
    :param db_info:
    :param bulk_size:
    :return:
    """
    def make_edges(eids_):
        edges_ = list()
        for i in eids_:
            if i[0] == '#' or i[0] == '/' or i[0] == '%':
                continue
            e = i.split(' ', 2)
            if len(e) == 2:  # no weight given
                f, t = e
                edges_.append({"_from": to_v(f), "_to": to_v(t)})  # Null will be inserted
            else:
                f, t, w = e
                edges_.append({"_from": to_v(f), "_to": to_v(t), "weight": f'{w}'})
        return edges_

    num_edges = get_property_graphalytics(properties_filename, 'num_edges')
    print(f'Number of edges: {num_edges}')
    to_v = ConverterToVertex(db_info.vertices_coll_name).idx_to_vertex

    start_e = time.monotonic()
    if be_verbose:
        with tqdm(total=num_edges, desc='Importing edges',
                  mininterval=1.0,
                  unit='edges', ncols=100) as pbar:
            for eids in file_reader(edges_filename, bulk_size):
                edges = make_edges(eids)
                insert_documents(db_info, edges, db_info.edge_coll_name)
                pbar.update(len(edges))

        print('Time for edges: ' + get_time_difference_string(time.monotonic() - start_e))
    else:
        for eids in file_reader(edges_filename, bulk_size):
            edges = make_edges(eids)
            insert_documents(db_info, edges, db_info.edge_coll_name)


def import_graphalytics(db_info: DatabaseInfo, vertices_filename, edges_filename,
                        properties_filename, bulk_size, be_verbose: bool):
    """
    Create a new smart graph with vertices v_coll and edges edge_coll_name with given parameters.
     If db_info.overwrite is True and the graph and/or the vertex/edge collection exist, they are dropped first.
     Read vertices from vertices_filename and insert them into db_info.vertices_coll_name in bulks of num_vertices
     bulk_size with smart attribute db_info.smart_attribute. The vertices must be given one vertex per line as
     <vertex id>.
     Read edges from edges_filename and insert them into db_info.edges_coll_name in bulks of num_vertices
     bulk_size with smart attribute db_info.smart_attribute. The edges must be given one edge per line in the form
     <node id> <node id> [<weight>]. If the weight is not given, Null is inserted.
     Lines starting with '#' or '/' are skipped.
    :param be_verbose:
    :param db_info: database info
    :param vertices_filename: the name of  the file to read vertices from
    :param edges_filename: the name of the file to read edges from
    :param properties_filename: the name of the file containing information about whether the graph should be directed
    :param bulk_size: the num_vertices of bulks
    :return: None
    """
    create_graph(db_info)
    read_and_create_vertices_graphalytics(vertices_filename, properties_filename, db_info, bulk_size, be_verbose)
    read_and_create_edges_graphalytics(edges_filename, properties_filename, db_info, bulk_size, be_verbose)
