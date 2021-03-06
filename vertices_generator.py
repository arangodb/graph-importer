import os
import random
from typing import Union, Optional

import requests
import tqdm

from general import insert_documents
from helper_classes import DatabaseInfo, GraphInfo, VertexOrEdgeProperty, CliquesHelper


def prepare_vertices(db_info: DatabaseInfo, graph_info: GraphInfo, part_label: str, start_idx: int, end_idx: int):
    """
    Create vertex document.
    :param db_info:
    :param graph_info:
    :param part_label:
    :param start_idx:
    :param end_idx:
    :return:
    """
    docs = []
    for vid in range(start_idx, end_idx):
        if db_info.isSmart:  # smart_attribute exists and makes sense
            if db_info.smart_attribute != 'part':
                doc = {f'{db_info.smart_attribute}': str(vid), "_key": f'{vid}:{vid}'}
                if part_label != "":
                    doc['part'] = part_label
            else:  # db_info.smart_attribute == 'part'
                doc = {'_key': f'{part_label}:{vid}', 'part': f'{part_label}'}
            if graph_info.vertex_property.type == 'random':
                doc[db_info.additional_vertex_attribute] = str(random.uniform(float(graph_info.vertex_property.min),
                                                                              float(graph_info.vertex_property.max)))
        else:
            doc = {'_key': str(vid)}
            if part_label != "":
                doc['part'] = part_label
            if graph_info.vertex_property.type == 'random':
                doc[db_info.additional_vertex_attribute] = str(
                    random.uniform(float(graph_info.vertex_property.min), float(graph_info.vertex_property.max)))
        docs.append(doc)
    return docs


def make_vertices(graph_info: GraphInfo,
                  db_info: DatabaseInfo,
                  size: int,
                  bulk_size: int,
                  add_part: bool = True):
    """
    Yield size many vertices in bulks of size bulk_size starting from id = graph_info.next_id. If add_part is True,
    all created vertices have an attribute with name described in db_info and value graph_info.next_id.
    :param graph_info:
    :param db_info:
    :param size:
    :param bulk_size:
    :param add_part:
    :return:
    """
    c_begin = graph_info.next_id
    c_end = graph_info.next_id + size
    if add_part:
        part_value = str(c_begin)
    else:
        part_value = ""  # don't add

    while graph_info.next_id + bulk_size <= c_end:
        yield prepare_vertices(db_info, graph_info, part_value, graph_info.next_id, graph_info.next_id + bulk_size)
        graph_info.next_id += bulk_size
    yield prepare_vertices(db_info, graph_info, part_value, graph_info.next_id, c_end)
    graph_info.next_id = c_end


def make_and_insert_vertices(db_info: DatabaseInfo, graph_info: GraphInfo, size: int, bulk_size: int,
                             add_part: bool = True, c_helper: Union[CliquesHelper, None] = None,
                             be_verbose: bool = True):
    """
    Make size many vertices and insert them into the database specified in db_info. The ids are consecutive, they start
    with graph_info.next_id, which is updated to graph_info.next_id + size. If c_helper is given, it is updated with
    a new clique.
    :param db_info:
    :param graph_info:
    :param size:
    :param bulk_size:
    :param add_part:
    :param c_helper:
    :param be_verbose: if True, produces a proress bar
    :return:
    """
    if be_verbose:
        pbar = tqdm.tqdm(total=size, desc='Creating vertices', mininterval=1.0, unit='vertices', ncols=100)
    for vertices in make_vertices(graph_info, db_info, size, bulk_size, add_part):
        insert_documents(db_info, vertices, db_info.vertices_coll_name)
        if be_verbose:
            pbar.update(len(vertices))
        if c_helper:
            c_helper.update(size)
    if be_verbose:
        pbar.close()


def get_vertex_property(args) -> VertexOrEdgeProperty:
    if not args.vertex_property_type or args.vertex_property_type == 'none':
        v_property = None
    elif args.vertex_property_type == 'random':
        if len(args.vertex_property) != 2:
            raise RuntimeError(
                'If --vertex_property_type is \'random\', --vertex_property must have exactly two arguments.')
        v_property = VertexOrEdgeProperty('random', float(args.vertex_property[0]), float(args.vertex_property[1]))
    else:  # remains list values
        if args.graphtype == 'clique':
            if len(args.vertex_property) < args.size:
                raise RuntimeError(
                    'If --vertex_property_type is \'list\' and --graphtype == \'clique\', --vertex_property must have '
                    'exactly --num_vertices many arguments.')
        if args.graphtype == 'cliques-graph':
            raise RuntimeError(
                'If --graphtype == \'cliques-graph\', --vertex_property_type == \'list\' is not allowed.')
        v_property = VertexOrEdgeProperty('list', val_list=list(args.vertex_property))
    return v_property


def insert_vertices_unique(db_info: DatabaseInfo, vertices):
    """
    Insert vertices into the database described in db_info, only vertices with a new attribute smartProp are inserted.
    :param db_info:
    :param vertices:
    :return:
    """
    doc = dict()
    vertices = list(vertices)
    q = f'''
    let vertex_ids = (
            FOR vertex IN @@vertex_coll
                RETURN vertex.smartProp
                )
    FOR v in @vertices
        FILTER TO_STRING(v) NOT IN vertex_ids
        INSERT {{ {db_info.smart_attribute} : v }} INTO @@vertex_coll
    '''
    doc['query'] = q
    doc['bindVars'] = {'vertices': vertices, '@vertex_coll': db_info.vertices_coll_name}
    url = os.path.join(db_info.endpoint, f"_api/cursor/")
    response = requests.post(url, json=doc, auth=(db_info.username, db_info.password))
    if response.status_code != 201:
        raise RuntimeError(f'Invalid response from server during insert_vertices_unique: {response.text}')


class ConverterToVertex:
    def __init__(self, vertex_coll_name: str):
        self.vertex_coll_name = vertex_coll_name

    def idx_to_smart_vertex(self, idx: Union[int, str], smart_value: Optional[str] = None) -> str:
        if smart_value:
            return f'{self.vertex_coll_name}/{smart_value}:{idx}'
        else:
            return f'{self.vertex_coll_name}/{idx}:{idx}'

    def idx_to_vertex(self, idx: Union[int, str]):
        return f"{self.vertex_coll_name}/{idx}"
