import random
import time
from typing import List, Dict, Callable, Union

from helper_classes import DatabaseInfo, GraphInfo, VertexOrEdgeProperty
from general import yes_with_prob
from time_tracking import TimeTracking


def append_edges(edges: List[Dict], f: int, t: int, to_v: Callable[[Union[int, str]], str],
                 attr_name: str = None, attr_value: str = None):
    doc = {"_from": to_v(f), "_to": to_v(t)}
    if attr_name:
        doc[attr_name] = attr_value
    edges.append(doc)


def add_edge(i: int, j: int, edges: List, prob_missing: float, db_info: DatabaseInfo, graph_info: GraphInfo,
             to_v: Callable[[Union[int, str]], str], time_tracker: TimeTracking):
    s = time.monotonic()
    if yes_with_prob(prob_missing):
        return False
    if graph_info.edge_property.type == 'none':
        append_edges(edges, i, j, to_v)
    elif graph_info.edge_property.type == 'random':
        append_edges(edges, i, j, to_v, db_info.edge_coll_name,
                     str(random.uniform(graph_info.edge_property.min, graph_info.edge_property.max)))
    else:
        raise RuntimeError(
            f"Wrong vertex property kind: {graph_info.vertex_property.type}. "
            f"Allowed values are \'none\' and \'random\'.")
    time_tracker.add_edge_time += time.monotonic() - s


def get_edge_property(a) -> Union[None, VertexOrEdgeProperty]:
    if not a.edge_property_type or a.edge_property_type == 'none':
        return None
    elif a.edge_property_type == 'random':
        if len(a.edge_prop) != 2:
            raise RuntimeError(
                'If --edge_property_type is \'random\', --edge_prop must have exactly two arguments.')
        return VertexOrEdgeProperty('random', float(a.edge_prop[0]), float(a.edge_prop[1]))
    else:  # remains list values
        if a.graphtype == 'clique':
            if len(a.edge_prop) < a.size * a.size:
                raise RuntimeError(
                    'If --edge_property_type is \'list\' and --graphtype == \'clqiue\', --edge_prop must have '
                    'at least (--num_vertices)^2 many arguments.')
        if a.graphtype == 'cliques-graph':
            if len(a.edge_prop) < (a.num_cliques * a.max_size_clique) ** 2:
                raise RuntimeError(
                    'If --edge_property_type is \'list\' and --graphtype == \'clqiues-graph\', --edge_prop must have '
                    'at least (a.num_cliques * a.max_size_clique)^2 many arguments.')

        return VertexOrEdgeProperty('list', val_list=list(a.edge_prop))
