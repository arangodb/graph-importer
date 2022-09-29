import argparse
import os
import tarfile
import time
from typing import Optional
from urllib.request import urlopen

import zstandard
from tqdm import tqdm

from arguments import make_global_parameters, make_database_parameters, make_pregel_parameters
from general import get_time_difference_string, arangodIsRunning
from graphalytics_importer import import_graphalytics, import_graphalytics_get_files_from_directory
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

BIG_DATASOURCES = {} #{'datagen-sf10k-fb': ['https://surfdrive.surf.nl/files/index.php/s/mQpAeUD4HIdh88R/download',
#                                         'https://surfdrive.surf.nl/files/index.php/s/bLthhT3tQytnlM0/download'
#                                         ],
#                    'graph500-30': ['https://surfdrive.surf.nl/files/index.php/s/07HY4YvhsFp3awr/download',
#                                    'https://surfdrive.surf.nl/files/index.php/s/QMy60s36HBYXliD/download',
#                                    'https://surfdrive.surf.nl/files/index.php/s/K0SsxPKogKZu86P/download',
#                                    'https://surfdrive.surf.nl/files/index.php/s/E5ZgpdUyDxVMP9O/download'
#                                    ]}

ALL_DATASOUCES_NAMES = list(SMALL_DATASOUCES.keys()) + list(BIG_DATASOURCES.keys())


def get_arguments():
    parser = argparse.ArgumentParser(description='Import a graph from a file/files to ArangoDB.')

    # global
    make_global_parameters(parser)
    make_database_parameters(parser)
    make_pregel_parameters(parser)

    parser.add_argument('--remove_archive', action='store_true',  # default: False
                        help='Whether to remove the archive file.')
    parser.add_argument('--target_directory', action='store_true',  # default: False
                        help='The directory to extract downloaded files.')
    parser.add_argument('dataset', choices=ALL_DATASOUCES_NAMES + ['all'],
                        help='The dataset, either \'all\' or one of.' + str(ALL_DATASOUCES_NAMES))

    arguments = parser.parse_args()

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
    # read arguments
    args = get_arguments()
    overwrite = args.overwrite
    be_verbose = not args.silent
    dataset = args.dataset
    target_directory = args.target_directory + os.sep + dataset if args.target_directory else dataset

    # prepare data structures
    db_info = DatabaseInfo(args.endpoint, args.graphname, args.vertex_collection_name,
                           args.edge_collection_name, True,
                           args.repl_factor, args.num_shards, args.overwrite, args.smart_attribute,
                           '', 'weight', args.user, args.pwd)

    if not arangodIsRunning():
        raise RuntimeError('The process "arangod" is not running, please, run it first.')

    #   parameters for Pregel
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

    # download
    filename = dataset + '.tar.zst'
    if dataset in BIG_DATASOURCES:
        append = False
        for url in BIG_DATASOURCES[dataset]:
            download(url, filename, append=append, be_verbose=be_verbose)
            append = True
    elif dataset in SMALL_DATASOUCES:
        url = SMALL_DATASOUCES[dataset]
        download(url, filename, be_verbose=be_verbose)

    # extract
    dctx = zstandard.ZstdDecompressor()
    with open(filename, 'rb') as ifh:
        with open('output.tar', 'wb') as ofh:
            dctx.copy_stream(ifh, ofh)

    tar_file = tarfile.open('output.tar')
    tar_file.extractall(target_directory)

    # find graphalytics files
    vertices_filename, edges_filename, properties_filename = import_graphalytics_get_files_from_directory(target_directory)

    # import
    start = time.monotonic()
    import_graphalytics(db_info, vertices_filename, edges_filename, properties_filename, args.bulk_size,
                        not args.silent)

    # execute
    #   pagerank
    if args.algorithm == 'pagerank':
        if args.pr_threshold:
            params['threshold'] = args.pr_threshold
        if args.pr_sourceField:
            params['sourceField'] = args.pr_sourceField

        algorithm_id = call_pregel_algorithm(db_info, 'pagerank', params).strip('"')
        if not args.silent:
            print(f'Pregel algorithm with id {algorithm_id} started.')
        if not args.no_watch:
            print_pregel_status(db_info, algorithm_id, args.sleep_time, args.extended_info, args.max_num_states)

    # print statistics
    if not args.silent:
        print('Total time: ' + get_time_difference_string(time.monotonic() - start))
