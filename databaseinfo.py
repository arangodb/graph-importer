from typing import List, Callable


class DatabaseInfo:
    def __init__(self, endpoint: str,
                 graph_name: str, vertices_coll_name: str, edge_coll_name: str,
                 replication_factor: int, number_of_shards: int,
                 overwrite: bool,
                 smart_attribute: str, additional_vertex_attribute: str,
                 edge_attribute: str,
                 username: str, password: str):
        self.replication_factor = replication_factor
        self.number_of_shards = number_of_shards
        self.overwrite = overwrite
        self.smart_attribute = smart_attribute
        self.additional_vertex_attribute = additional_vertex_attribute
        self.edge_attribute = edge_attribute
        self.username = username
        self.password = password
        self.edge_coll_name = edge_coll_name
        self.vertices_coll_name = vertices_coll_name
        self.graph_name = graph_name
        self.endpoint = endpoint


class VertexOrEdgeProperty:
    def __init__(self, kind: str, mi: float = 0.0, ma: float = 1.0, val_list: List[str] = None):
        self.list = val_list
        self.min = mi
        self.max = ma
        self.type = kind


class CliquesGraphInfo:
    inter_cliques_density: float

    def __init__(self,
                 num_cliques: int, min_size_clique: int, max_size_clique: int,
                 prob_missing: float, inter_cliques_density: float, density_between_two_cliques: float):
        '''
        Information for cliques-graph construction. The graph is the result of the following construction.
        Make a graph with num_cliques many vertices, add edges with probability inter_cliques_density.
        Then replace every vertex v by a set V_v of vertices of size randomly chosen between min_size_clique and
        max_size_clique with equal probability. Remove edges vw and insert every edge between V_v and V_w with
        probability density_between_two_cliques.
        :param num_cliques: number of cliques
        :param min_size_clique:
        :param max_size_clique:
        :param prob_missing:
        :param inter_cliques_density:
        :param density_between_two_cliques:
        '''
        self.density_between_two_cliques = density_between_two_cliques
        self.num_cliques = num_cliques
        self.min_size_clique = min_size_clique
        self.max_size_clique = max_size_clique
        self.prob_missing = prob_missing
        self.inter_cliques_density = inter_cliques_density


class GraphInfo:
    def __init__(self, hasSelfLoops: bool, isDirected: bool,
                 vertex_property: VertexOrEdgeProperty,
                 edge_property: VertexOrEdgeProperty
                 ):
        '''
        Information for graph construction.
        :param hasSelfLoops: whether each vertex has a self-loop
        :param isDirected: whether the graph is directed. If not, the edge relation is symmetric.
        :param vertex_property:
        :param edge_property:
        '''
        self.hasSelfLoops = hasSelfLoops
        self.isDirected = isDirected
        self.vertex_property = vertex_property
        self.edge_property = edge_property
        self.next_id: int = 0
