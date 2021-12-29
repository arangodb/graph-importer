import argparse
import os
import tarfile
import time
from typing import Optional
from urllib.request import urlopen

import zstandard
from tqdm import tqdm

from general import get_time_difference_string
from graphalytics_importer import import_graphalytics, import_graphalytics_get_files
from helper_classes import DatabaseInfo
from start_Pregel import call_pregel_algorithm, print_pregel_status

SMALL_DATASOUCES = {'cit-Patents': 'https://surfdrive.surf.nl/files/index.php/s/mhTyNV2wk5HNAf7/download',
                    'com-friendster': 'https://surfdrive.surf.nl/files/index.php/s/z8PSwZwBma7etRg/download',
                    'datagen-7_5-fb': 'https://surfdrive.surf.nl/files/index.php/s/ypGcsxzrBeh2YGb/download',
                    'datagen-7_6-fb': 'https://surfdrive.surf.nl/files/index.php/s/pxl7rDvzDQJFhfc/download',
                    'datagen-7_7-zf': 'https://surfdrive.surf.nl/files/index.php/s/sstTvqgcyhWVVPn/download',
                    'datagen-7_8-zf': 'https://surfdrive.surf.nl/files/index.php/s/QPSagck1SZTbIA1/download',
                    'datagen-7_9-fb': 'https://surfdrive.surf.nl/files/index.php/s/btdN4uMsW20YJmV/download',
                    'datagen-8_0-fb': 'https://surfdrive.surf.nl/files/index.php/s/lPIRs3QIlrACz86/download',
                    'datagen-8_1-fb': 'https://surfdrive.surf.nl/files/index.php/s/RB5vU9WUtzA00Nz/download',
                    'datagen-8_2-zf': 'https://surfdrive.surf.nl/files/index.php/s/BdQESW3JPg2uMJH/download',
                    'datagen-8_3-zf': 'https://surfdrive.surf.nl/files/index.php/s/35KImcT5RbnZZFb/download',
                    'datagen-8_4-fb': 'https://surfdrive.surf.nl/files/index.php/s/2xB1K9hVe3JSTdH/download',
                    'datagen-8_5-fb': 'https://surfdrive.surf.nl/files/index.php/s/2d8wUj9HGIzime3/download',
                    'datagen-8_6-fb': 'https://surfdrive.surf.nl/files/index.php/s/yyJoaazDGKmLc0k/download',
                    'datagen-8_7-zf': 'https://surfdrive.surf.nl/files/index.php/s/jik4NN4CDnUDmAG/download',
                    'datagen-8_8-zf': 'https://surfdrive.surf.nl/files/index.php/s/Qmi35tpKSjovS5d/download',
                    'datagen-8_9-fb': 'https://surfdrive.surf.nl/files/index.php/s/A8dCtfeqNgSyAOF/download',
                    'datagen-9_0-fb': 'https://surfdrive.surf.nl/files/index.php/s/RFkNmmIOewT3YSd/download',
                    'datagen-9_1-fb': 'https://surfdrive.surf.nl/files/index.php/s/7vJ0i7Ydj67loEL/download',
                    'datagen-9_2-zf': 'https://surfdrive.surf.nl/files/index.php/s/cT4SZT8frlaIkLI/download',
                    'datagen-9_3-zf': 'https://surfdrive.surf.nl/files/index.php/s/DE67JXHTN3jxM7O/download',
                    'datagen-9_4-fb': 'https://surfdrive.surf.nl/files/index.php/s/epHG26pswdJG4kQ/download',
                    'datagen-sf3k-fb': 'https://surfdrive.surf.nl/files/index.php/s/5l6bQq9a6GjZBRq/download',
                    'dota-league': 'https://surfdrive.surf.nl/files/index.php/s/oyOewICGppmn0Jq/download',
                    'example-directed': 'https://surfdrive.surf.nl/files/index.php/s/7hGIIZ6nzxgi0dU/download',
                    'example-undirected': 'https://surfdrive.surf.nl/files/index.php/s/enKFbXmUBP2rxgB/download',
                    'graph500-22': 'https://surfdrive.surf.nl/files/index.php/s/0ix5lmNLsUsbx5W/download',
                    'graph500-23': 'https://surfdrive.surf.nl/files/index.php/s/IIDfjd1ALbWQKhD/download',
                    'graph500-24': 'https://surfdrive.surf.nl/files/index.php/s/FmhO7Xwtd2VYHb9/download',
                    'graph500-25': 'https://surfdrive.surf.nl/files/index.php/s/gDwvrZLQXHr9IN7/download',
                    'graph500-26': 'https://surfdrive.surf.nl/files/index.php/s/GE7kIyBL0PULiRK/download',
                    'graph500-27': 'https://surfdrive.surf.nl/files/index.php/s/l1FRzpAZ2uIddKq/download',
                    'graph500-28': 'https://surfdrive.surf.nl/files/index.php/s/n45KOpNrWZVon04/download',
                    'graph500-29': 'https://surfdrive.surf.nl/files/index.php/s/VSXkomtgPGwZMW4/download',
                    'kgs': 'https://surfdrive.surf.nl/files/index.php/s/L59W21l2jUzAOGf/download',
                    'twitter_mpi': 'https://surfdrive.surf.nl/files/index.php/s/keuUstVmhPAIW3A/download',
                    'wiki-Talk': 'https://surfdrive.surf.nl/files/index.php/s/c5dT1fwzXaNHT8j/download'}

BIG_DATASOURCES = {'datagen-sf10k-fb': ['https://surfdrive.surf.nl/files/index.php/s/mQpAeUD4HIdh88R/download',
                                        'https://surfdrive.surf.nl/files/index.php/s/bLthhT3tQytnlM0/download'
                                        ],
                   'graph500-30': ['https://surfdrive.surf.nl/files/index.php/s/07HY4YvhsFp3awr/download',
                                   'https://surfdrive.surf.nl/files/index.php/s/QMy60s36HBYXliD/download',
                                   'https://surfdrive.surf.nl/files/index.php/s/K0SsxPKogKZu86P/download',
                                   'https://surfdrive.surf.nl/files/index.php/s/E5ZgpdUyDxVMP9O/download'
                                   ]}

ALL_DATASOUCES_NAMES = list(SMALL_DATASOUCES.keys()) + list(BIG_DATASOURCES.keys())


def get_arguments():
    parser = argparse.ArgumentParser(description='Import a graph from a file/files to ArangoDB.')

    # global
    parser.add_argument('endpoint', type=str, help='Endpoint, e.g. http://localhost:8529/_db/_system')
    parser.add_argument('--bulk_size', type=int, nargs='?', default=10000,
                        help='The number of vertices/edges written in one go.')
    parser.add_argument('--silent', action='store_true',  # default: False
                        help='Print progress and statistics.')
    parser.add_argument('--overwrite_file', action='store_true',  # default: False
                        help='Whether to overwrite the file if it exists.')
    parser.add_argument('--sleep_time', type=int, default=1, help='Time in seconds to wait before requesting '
                                                                  'the status of the Pregel program again.')
    parser.add_argument('--remove_archive', action='store_true',  # default: False
                        help='Whether to remove the archive file.')
    parser.add_argument('--remove_graph_files', action='store_true',  # default: False
                        help='Whether to remove the graph files file extracted from the archive.')
    parser.add_argument('--target_directory', action='store_true',  # default: False
                        help='The directory to extract downloaded files.')
    parser.add_argument('dataset', choices=ALL_DATASOUCES_NAMES + ['all'],
                        help='The dataset, either \'all\' or one of.' + str(ALL_DATASOUCES_NAMES))

    # database parameters
    parser.add_argument('--user', nargs='?', default='root', help='User name for the server.')
    parser.add_argument('--pwd', nargs='?', default='', help='Password for the server.')
    parser.add_argument('--graphname', default='generatedGraph', help='Name of the new graph in the database.')
    parser.add_argument('--edge_collection_name', default='e', help='Name of the new edge collection in the database.')
    parser.add_argument('--vertex_collection_name', default='v', help='Name of the new vertex collection'
                                                                      ' in the database.')
    parser.add_argument('--make_smart', action='store_true',  # default: false
                        help='Create a smart graph.')
    parser.add_argument('--num_shards', default=5, type=int, help='Number of shards.')
    parser.add_argument('--repl_factor', default=2, type=int, help='Replication factor.')
    parser.add_argument('--smart_attribute', default='smartProp',
                        help='The name of the attribute to shard the vertices after.')
    parser.add_argument('--overwrite', action='store_true',  # default: false
                        help='Overwrite the graph and the collection if they already exist.')

    # pregel specific
    parser.add_argument('--store', action='store_true',  # default: False
                        help='Whether the results computed by the Pregel algorithm '
                             'are written back into the source collections.')
    parser.add_argument('--maxGSS', type=int,
                        help='Execute a fixed number of iterations (or until the threshold is met).')
    parser.add_argument('--parallelism', type=int,
                        help='The maximum number of parallel threads that will execute the Pregel algorithm.')
    parser.add_argument('--asynchronous', action='store_true',
                        help='Algorithms which support asynchronous mode will run without synchronized '
                             'global iterations.')
    parser.add_argument('--resultField', type=str,
                        help='The attribute of vertices to write the result into.')
    parser.add_argument('--useMemoryMaps', action='store_true',  # default: False
                        help='Whether to use disk based files to store temporary results.')
    parser.add_argument('--shardKeyAttribute', help='The shard key that edge collections are sharded after.')

    parser.add_argument('algorithm', help='''The name of the Gregel algorithm, one of:
                                            pagerank - Page Rank; 
                                            sssp - Single-Source Shortest Path; 
                                            connectedcomponents - Connected Components;
                                            wcc - Weakly Connected Components;
                                            scc - Strongly Connected Components;
                                            hits - Hyperlink-Induced Topic Search;
                                            effectivecloseness - Effective Closeness;
                                            linerank - LineRank;
                                            labelpropagation - Label Propagation;
                                            slpa - Speaker-Listener Label Propagation''')

    parser.add_argument('--pagerank_threshold', type=float,
                        help='Execute until the value changes in the vertices are at most the threshold.')
    parser.add_argument('--pagerank_sourceField', type=str,
                        help='The attribute of vertices to read the initial rank value from.')

    parser.add_argument('--sssp_source', help='The vertex ID to calculate distances from.')
    parser.add_argument('--sssp_resultField', help='The vertex ID to calculate distances from.')

    arguments = parser.parse_args()

    # check parameters
    if arguments.make_smart and not arguments.smart_attribute:
        raise RuntimeError('If --make_smart is given, then also --smart_attribute must be given.')

    return arguments


def download(url: str, filename: Optional[str] = None, append: bool = False, be_verbose: bool = True):
    """
    Saves the file form the given url in the current directory.

    :param append:
    :param url:
    :param filename:
    :param be_verbose:
    :return: the filename
    """
    if os.path.isfile(filename) and not append:
        return

    response = urlopen(url)
    # file_size = int(response.info.getheaders("Content-Length")[0])
    file_size = response.length
    current_file_size = 0
    block_size = 10 * 1024 * 1024  # 10MB
    pbar = tqdm(total=file_size, desc='Reading file',
                mininterval=1.0,
                unit='byte', ncols=100)
    mode = 'ab' if append else 'wb'
    with open(filename, mode) as f:
        if be_verbose:
            print(f'Downloading {file_size} bytes from {url} to {filename}.')
        while True:
            buffer = response.read(block_size)
            if not buffer:
                break
            current_file_size += len(buffer)
            pbar.update(len(buffer))
            f.write(buffer)
    pbar.close()

    return filename


if __name__ == "__main__":
    args = get_arguments()
    overwrite = args.overwrite
    be_verbose = not args.silent
    dataset = args.dataset
    target_directory = args.target_directory + os.sep + dataset if args.target_directory else dataset

    filename = dataset + '.tar.zst'

    if dataset in BIG_DATASOURCES:
        append = False
        for url in BIG_DATASOURCES[dataset]:
            download(url, filename, append=append, be_verbose=be_verbose)
            append = True
    elif dataset in SMALL_DATASOUCES:
        url = SMALL_DATASOUCES[dataset]
        download(url, filename, be_verbose=be_verbose)

    db_info = DatabaseInfo(args.endpoint, args.graphname, args.vertex_collection_name,
                           args.edge_collection_name, args.make_smart,
                           args.repl_factor, args.num_shards, args.overwrite, args.smart_attribute,
                           '', 'weight', args.user, args.pwd)

    dctx = zstandard.ZstdDecompressor()
    with open(filename, 'rb') as ifh:
        with open('output.tar', 'wb') as ofh:
            dctx.copy_stream(ifh, ofh)

    tar_file = tarfile.open('output.tar')
    tar_file.extractall(target_directory)

    vertices_filename, edges_filename, properties_filename = import_graphalytics_get_files(target_directory)
    start = time.monotonic()
    import_graphalytics(db_info, vertices_filename, edges_filename, properties_filename, args.bulk_size,
                        not args.silent)

    params = dict()
    if args.store:
        params['store'] = 'true'
    if args.maxGSS:
        params['maxGSS'] = args.maxGSS
    if args.parallelism:
        params['parallelism'] = args.parallelism
    if args.asynchronous:
        params['async'] = 'true'
    if args.resultField:
        params['resultField'] = args.resultField
    if args.useMemoryMaps:
        params['useMemoryMaps'] = 'true'
    if args.shardKeyAttribute:
        params['shardKeyAttribute'] = args.shardKeyAttribute

        # pagerank
    if args.algorithm == 'pagerank':
        if args.pagerank_threshold:
            params['threshold'] = args.pagerank_threshold
        if args.pagerank_sourceField:
            params['sourceField'] = args.pagerank_sourceField

        algorithm_id = call_pregel_algorithm(db_info, 'pagerank', args.edge_collection_name,
                                             args.vertex_collection_name,
                                             params).strip('"')
        print_pregel_status(db_info, algorithm_id, args.sleep_time)

    if not args.silent:
        print('Total time: ' + get_time_difference_string(time.monotonic() - start))