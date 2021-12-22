import random
import time
from typing import List, Dict, Callable, Union, Optional, Iterable

from tqdm import trange

import time_tracking
from general import yes_with_prob
from helper_classes import DatabaseInfo, GraphInfo, VertexOrEdgeProperty, CliquesHelper
from time_tracking import TimeTracking
from vertices_generator import ConverterToVertex


def append_smart_edges(edges: List[Dict], f: int, t: int, to_v: Callable[[Union[int, str], str], str],
                       smart_val_f: str, smart_val_t: str, attr_name: Optional[str] = None,
                       attr_value: Optional[str] = None):
    """

    :param smart_val_t:
    :param smart_val_f:
    :param edges:
    :param f:
    :param t:
    :param to_v: converts (id, smart_value) to the _id value of a smart vertex
    :param attr_name:
    :param attr_value:
    :return:
    """
    doc = {"_from": to_v(f, smart_val_f), "_to": to_v(t, smart_val_t)}
    if attr_name:
        doc[attr_name] = attr_value
    edges.append(doc)


def append_edges(edges: List[Dict], f: int, t: int, to_v: Callable[[Union[int, str]], str],
                 attr_name: Optional[str] = None, attr_value: Optional[str] = None):
    """

    :param edges:
    :param f:
    :param t:
    :param to_v: converts (id, smart_value) to the _id value of a smart vertex
    :param attr_name:
    :param attr_value:
    :param smart_val:
    :param db_info:
    :return:
    """
    doc = {"_from": to_v(f), "_to": to_v(t)}
    if attr_name:
        doc[attr_name] = attr_value
    edges.append(doc)


def add_smart_edge(i: int, j: int, edges: List, prob_missing: float, db_info: DatabaseInfo, graph_info: GraphInfo,
                   to_v: Callable[[Union[int, str]], str], time_tracker: TimeTracking,
                   smart_val_i: str, smart_val_j: str):
    assert db_info.isSmart
    s = time.monotonic()
    if yes_with_prob(prob_missing):
        return False
    if graph_info.edge_property.type == 'none':
        append_smart_edges(edges, i, j, to_v, smart_val_i, smart_val_j)
    else:  # graph_info.edge_property.type == 'random':
        append_smart_edges(edges, i, j, to_v, smart_val_i, smart_val_j, db_info.edge_coll_name,
                           str(random.uniform(graph_info.edge_property.min, graph_info.edge_property.max)))

    time_tracker.add_edge_time += time.monotonic() - s


def add_edge(i: int, j: int, edges: List, prob_missing: float, db_info: DatabaseInfo, graph_info: GraphInfo,
             to_v: Callable[[Union[int, str]], str], time_tracker: TimeTracking):
    assert not db_info.isSmart
    s = time.monotonic()
    if yes_with_prob(prob_missing):
        return False
    if graph_info.edge_property.type == 'none':
        append_edges(edges, i, j, to_v)
    else:  # graph_info.edge_property.type == 'random':
        append_edges(edges, i, j, to_v, db_info.edge_attribute,
                     str(random.uniform(graph_info.edge_property.min, graph_info.edge_property.max)))

    time_tracker.add_edge_time += time.monotonic() - s


def get_edge_property(a) -> Union[None, VertexOrEdgeProperty]:
    if not a.edge_property_type or a.edge_property_type == 'none':
        return None
    elif a.edge_property_type == 'random':
        if len(a.edge_property) != 2:
            raise RuntimeError(
                'If --edge_property_type is \'random\', --edge_property must have exactly two arguments.')
        return VertexOrEdgeProperty('random', float(a.edge_property[0]), float(a.edge_property[1]))
    else:  # remains list values
        if a.graphtype == 'clique':
            if len(a.edge_property) < a.size * a.size:
                raise RuntimeError(
                    'If --edge_property_type is \'list\' and --graphtype == \'clqiue\', --edge_property must have '
                    'at least (--num_vertices)^2 many arguments.')
        if a.graphtype == 'cliques-graph':
            if len(a.edge_property) < (a.num_cliques * a.max_size_clique) ** 2:
                raise RuntimeError(
                    'If --edge_property_type is \'list\' and --graphtype == \'clqiues-graph\', '
                    '--edge_property must have '
                    'at least (a.num_cliques * a.max_size_clique)^2 many arguments.')

        return VertexOrEdgeProperty('list', val_list=list(a.edge_property))


def connect_parts(clique_helper: CliquesHelper, bulk_size_: int, prob_missing_all: float, prob_missing_one: float,
                  time_tracker_: time_tracking.TimeTracking, db_info: DatabaseInfo, graph_info: GraphInfo,
                  be_verbose: bool = True) -> Iterable:
    """
    Given a list parts of disjoint vertex sets (disjointness is not verified), connect every vertex of every part
    with every vertex of every other part.
    :param prob_missing_all:
    :param prob_missing_one:
    :param graph_info:
    :param db_info:
    :param be_verbose:
    :param time_tracker_:
    :rtype: None
    :param clique_helper:
    :param v_coll:
    :param bulk_size_:
    """
    connect_parts_start_time = time.monotonic()
    edges_ = []
    to_vrtx = ConverterToVertex(db_info.vertices_coll_name).idx_to_smart_vertex if db_info.isSmart else \
        ConverterToVertex(db_info.vertices_coll_name).idx_to_vertex

    if be_verbose:
        generator_ = trange(clique_helper.num_cliques(), desc='Connecting parts to each other', mininterval=1.0,
                            unit='connecting a part to all others')
    else:
        generator_ = range(clique_helper.num_cliques())

    if db_info.isSmart:
        if db_info.smart_attribute == 'part':
            for c1 in generator_:
                start_1 = clique_helper.starts_of_cliques[c1]
                end_1 = clique_helper.starts_of_cliques[c1 + 1]
                smart_value_1 = str(start_1)
                for c2 in range(c1 + 1, clique_helper.num_cliques()):
                    if yes_with_prob(prob_missing_all):
                        continue
                    start_2 = clique_helper.starts_of_cliques[c2]
                    end_2 = clique_helper.starts_of_cliques[c2 + 1]
                    smart_value_2 = str(start_2)
                    for f in range(start_1, end_1):
                        for t in range(start_2, end_2):
                            add_smart_edge(f, t, edges_, prob_missing_one, db_info, graph_info, to_vrtx,
                                           time_tracker_, smart_value_1, smart_value_2)
                            if len(edges_) >= bulk_size_:
                                time_tracker_.connect_parts_time += time.monotonic() - connect_parts_start_time
                                yield edges_
                                edges_.clear()
        else:
            for c1 in generator_:
                start_1 = clique_helper.starts_of_cliques[c1]
                end_1 = clique_helper.starts_of_cliques[c1 + 1]
                for c2 in range(c1 + 1, clique_helper.num_cliques()):
                    if yes_with_prob(prob_missing_all):
                        continue
                    start_2 = clique_helper.starts_of_cliques[c2]
                    end_2 = clique_helper.starts_of_cliques[c2 + 1]
                    for f in range(start_1, end_1):
                        smart_value_f = str(f)
                        for t in range(start_2, end_2):
                            add_smart_edge(f, t, edges_, prob_missing_one, db_info, graph_info, to_vrtx,
                                           time_tracker_, smart_val_i=smart_value_f, smart_val_j=str(t))
                            if len(edges_) >= bulk_size_:
                                time_tracker_.connect_parts_time += time.monotonic() - connect_parts_start_time
                                yield edges_
                                edges_.clear()
    else:
        for c1 in generator_:
            start_1 = clique_helper.starts_of_cliques[c1]
            end_1 = clique_helper.starts_of_cliques[c1 + 1]
            for c2 in range(c1 + 1, clique_helper.num_cliques()):
                if yes_with_prob(prob_missing_all):
                    continue
                start_2 = clique_helper.starts_of_cliques[c2]
                end_2 = clique_helper.starts_of_cliques[c2 + 1]
                for f in range(start_1, end_1):
                    for t in range(start_2, end_2):
                        add_edge(f, t, edges_, prob_missing_one, db_info, graph_info, to_vrtx, time_tracker_)
                        if len(edges_) >= bulk_size_:
                            time_tracker_.connect_parts_time += time.monotonic() - connect_parts_start_time
                            yield edges_
                            edges_.clear()
    if edges_:
        yield edges_
