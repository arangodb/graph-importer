import argparse
import os.path
from pathlib import PurePath

import requests


def create_graph(endpoint, graphname, vertices_coll_name, edge_coll_name, replication_factor: int,
                 number_of_shards: int, overwrite: bool, shard_value: str, username: str, password: str):
    '''
    Create a new smart graph with vertices vertices_coll_name and edges edge_coll_name with given parameters.
    If overwrite is True and the graph and/or the vertex/edge collection exist, they are dropped first.
    :param endpoint: the server address
    :param graphname: the name of the grph to create
    :param vertices_coll_name: the name of the vertex collection to create
    :param edge_coll_name: the name of the edge collection to create
    :param replication_factor: the replication factor
    :param number_of_shards: the number of shards
    :param overwrite: whether to overwrite the  graph and the vertex and edge collections
    :param shard_value: the name of the field to shard vertices after
    :param username: the username
    :param password: the password
    :return: None
    '''
    if overwrite:
        # drop the graph (if it exists)
        url = os.path.join(endpoint, '_api/gharial', graphname)
        url = url + '?dropCollections=true'
        requests.delete(url, auth=(username, password))
        # drop edges
        url = os.path.join(endpoint, '_api/collection', edge_coll_name)
        requests.delete(url, auth=(username, password))
        # drop vertices
        url = os.path.join(endpoint, '_api/collection', vertices_coll_name)
        requests.delete(url, auth=(username, password))

    url = os.path.join(endpoint, '_api/gharial')
    response = requests.post(url, auth=(username, password), json={
        "name": graphname,
        "edgeDefinitions": [
            {
                "collection": edge_coll_name,
                "from": [vertices_coll_name],
                "to": [vertices_coll_name]
            }
        ],
        "orphanCollections": [vertices_coll_name],
        "isSmart": "true",
        "options": {
            "replicationFactor": replication_factor,
            "numberOfShards": number_of_shards,
            "smartGraphAttribute": shard_value
        }
    })
    if response.status_code == 409:
        raise RuntimeError(f'The graph or the edge collection already exist. Server response: {response.text}')
    if response.status_code not in [201, 202]:
        raise RuntimeError(f'Invalid response from bulk insert{response.text}')


def insert_vertices(endpoint, vertex_coll_name, vertices, username, password):
    '''
    Insert a vertex or a list of vertices into the vertex collection.
    :param endpoint: the server address
    :param vertex_coll_name: the vertex collection name
    :param vertices: the vertex or (typically) the list of vertices
    :param username: the username
    :param password: the password
    :return: None
    '''
    url = os.path.join(endpoint, "_api/document/", vertex_coll_name)
    response = requests.post(url, json=vertices, auth=(username, password))
    if response.status_code != 202:
        raise RuntimeError(f"Invalid response from bulk insert{response.text}")


def insert_edges(endpoint, edges_coll_name, vertices_coll_name, edges, shard_value, username, password):
    '''
    Insert an edge or (typically) a list of edges into the edge collection.
    :param endpoint: the srever address
    :param edges_coll_name: the name of the edge collection
    :param vertices_coll_name: the name of the vertex collection
    :param edges: the edge or (typically) the list of edges to insert
    :param username: the username
    :param password: the password
    :return: None
    '''
    url = os.path.join(endpoint, "_api/document/", edges_coll_name)
    response = requests.post(url, json=edges, auth=(username, password))
    if response.status_code != 202:
        raise RuntimeError(f"Invalid response from bulk insert{response.text}")
    

def file_reader(filename, bulk_size):
    '''
    Yield bulk_size characters from the file with filename filename or the whole content of the file if it has less characters.
    :param filename: the filename
    :param bulk_size: the number of characters to return at most
    :return: None
    '''
    with open(filename, "r") as f:
        res = list()
        for line in f:
            res.append(line.strip())
            if len(res) == bulk_size:
                yield res
                res = list()
        if len(res) != 0:
            yield res


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


def read_and_create_vertices(filename, endpoint, bulk_size, vertices_coll_name, shard_value, username, password):
    '''
    Read vertices from the given file and insert them into the collection vertices_coll_name in bulks of size
    bulk_size with shard value shard_value. The vertices must be given one vertex per line as <vertex id>.
    :param filename: the filename of the file containing a list of vertices
    :param endpoint: the server address
    :param bulk_size: the bulk size
    :param vertices_coll_name: the name of the collection to insert vertices into
    :param shard_value: the shard value
    :param username: the username
    :param password: the password
    :return: None
    '''
    for vids in file_reader(filename, bulk_size):
        vertices = [{f'{shard_value}': str(vid), '_key': str(vid) + ':'+ str(vid)} for vid in vids]
        insert_vertices(endpoint, vertices_coll_name, vertices, username, password)


def read_and_create_edges(edges_filename, edges_coll_name, vertices_coll_name, endpoint, bulk_size, isDirected,
                          shard_value, username, password):
    '''
    Read edges from the given file and insert them into the collection edges_coll_name in bulks of size
     bulk_size with shard value shard_value. The edges must be given one edge per line in the form
     <node id> <node id> [<weight>]. If the weight is not given, Null is inserted.
     If isDirected is False, with every edge (a,b) also the edge (b,a) with the same weight is inserted.
     Lines starting with '#' or '/' are skipped.
    :param edges_filename:
    :param edges_coll_name:
    :param vertices_coll_name:
    :param endpoint:
    :param bulk_size:
    :param isDirected:
    :param shard_value:
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
                if len(e) == 2: # no weight given
                    f, t = e
                    edges.append({"_from": f"{vertices_coll_name}/{f}:{f}", "_to": f"{vertices_coll_name}/{t}:{t}"}) # Null will be inserted
                else:
                    f, t, w = e
                    edges.append({"_from": f"{vertices_coll_name}/{f}:{f}", "_to": f"{vertices_coll_name}/{t}:{t}", "weight": f'{w}'})
        else:
            for i in eids:
                e = i.split(' ', 2)
                if len(e) == 2:
                    f, t = e
                    edges.append({"_from": f"{vertices_coll_name}/{f}:{f}", "_to": f"{vertices_coll_name}/{t}:{t}"}) # Null will be inserted
                    edges.append({"_from": f"{vertices_coll_name}/{t}:{t}", "_to": f"{vertices_coll_name}/{f}:{f}"}) # Null will be inserted
                else:
                    f, t, w = e
                    edges.append({"_from": f"{vertices_coll_name}/{f}:{f}", "_to": f"{vertices_coll_name}/{t}:{t}", "weight": f'{w}'})
                    edges.append({"_from": f"{vertices_coll_name}/{t}:{t}", "_to": f"{vertices_coll_name}/{f}:{f}", "weight": f'{w}'})
        insert_edges(endpoint, edges_coll_name, vertices_coll_name, edges, shard_value, username, password)


def import_graphalytics(endpoint, vertices_filename, edges_filename, properties_filename, bulk_size,
                        enforce_undirected, graphname, edges_coll_name, vertices_coll_name, replication_factor,
                        number_of_shards, overwrite, shard_value, username, password):
    '''
    Create a new smart graph with vertices vertices_coll_name and edges edge_coll_name with given parameters.
     If overwrite is True and the graph and/or the vertex/edge collection exist, they are dropped first.
     Read vertices from vertices_filename and insert them into vertices_coll_name in bulks of size
     bulk_size with shard value shard_value. The vertices must be given one vertex per line as <vertex id>.
     Read edges from edges_filename and insert them into edges_coll_name in bulks of size
     bulk_size with shard value shard_value. The edges must be given one edge per line in the form
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
    :param shard_value: the shard value
    :param username: the user name
    :param password: the password
    :return: None
    '''
    create_graph(endpoint, graphname, vertices_coll_name, edges_coll_name, replication_factor, number_of_shards,
                 overwrite,
                 shard_value, username, password)
    read_and_create_vertices(vertices_filename, endpoint, bulk_size, vertices_coll_name, shard_value, username,
                             password)
    isDirected = False if enforce_undirected else graphalytics_get_directedness(properties_filename)
    read_and_create_edges(edges_filename, edges_coll_name, vertices_coll_name, endpoint, bulk_size, isDirected,
                          shard_value, username, password)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Import a graph from a file/files to ArangoDB.')

    parser.add_argument('endpoint', type=str, help='Endpoint, e.g. http://localhost:8529/_db/_system')
    parser.add_argument('sourcetype', type=str, nargs='?', default='edge-list', choices=['edge-list', 'graphalytics'],
                        help='Source type')
    parser.add_argument('--dir_graphalytics', '-d', type=str, nargs='?',
                        help='For Graphalytics graphs, the directory containing the files. '
                             'If given, overwrites possible arguments --vgf, --egf and --pgf.')
    parser.add_argument('--vertices_file_graphalytics', '-v', type=str, nargs='?',
                        help='For Graphalytics graphs, the file containing the vertices.')
    parser.add_argument('--edges_file_graphalytics', '-e', type=str, nargs='?',
                        help='For Graphalytics graphs, the file containing the edges.')
    parser.add_argument('--properties_file_graphalytics', '-p', type=str, nargs='?',
                        help='For Graphalytics graphs, the file containing the graph properties.')
    parser.add_argument('--bulk_size', '-s', type=int, nargs='?', default=10000,
                        help='The number of vertices/edges written in one go.')
    parser.add_argument('--enforce_undirected', '-u', action='store_true',  # default: false
                        help='Make the edge relation symmetric. For Graphalytics graphs, '
                             'overwrites the specification from the file.')
    parser.add_argument('--user', nargs='?', default='root', help='User name for the server.')
    parser.add_argument('--pwd', nargs='?', default='', help='Password for the server.')
    parser.add_argument('--graphname', default='importedGraph', help='Name of the new graph in the database.')
    parser.add_argument('--edges', default='e', help='Name of the new edge relation in the database.')
    parser.add_argument('--vertices', default='v', help='Name of the new vertex relation in the database.')
    parser.add_argument('--num_shards', default=5, type=int, help='Number of shards.')
    parser.add_argument('--repl_factor', default=2, type=int, help='Replication factor.')
    parser.add_argument('--shard_value', default='property', help='The name of the field to shard the vertices after.')
    parser.add_argument('--overwrite', action='store_true',  # default: false
                        help='Overwrite the graph and the collection if they already exist.')

    args = parser.parse_args()

    if not args.dir_graphalytics and not (
            args.vertices_file_graphalytics and args.edges_file_graphalytics and args.properties_file_graphalytics):
        raise Exception(
            'With sourcetype graphalytics, either --dir_graphalytics, or all of --vertices_file_graphalytics, --edges_file_graphalytics and --properties_file_graphalytics must be given.')
    # print(f'args.dir_graphalytics: {args.dir_graphalytics}', f'args.vertices_file_graphalytics: {args.vertices_file_graphalytics}')

    if args.sourcetype == 'graphalytics':
        if args.dir_graphalytics:
            vertices_filename, edges_filename, properties_filename = import_graphalytics_get_files(
                args.dir_graphalytics)
        else:
            vertices_filename = args.vertices_file_graphalytics
            edges_filename = args.edges_file_graphalytics
            properties_filename = args.properties_file_graphalytics

        import_graphalytics(args.endpoint, vertices_filename, edges_filename, properties_filename, args.bulk_size,
                            args.enforce_undirected, args.graphname, args.edges, args.vertices, args.repl_factor,
                            args.num_shards, args.overwrite, args.shard_value, args.user, args.pwd)
    else:
        print('The option edge-list is not implemented yet.')
