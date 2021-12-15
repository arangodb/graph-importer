# Graph Importer

This repository contains command line Python scripts to import graphs from external sources as files into a running
instance of `arangod`
and to generate graphs in a running instance.

# Importing Graphs

The graphs be stored in a local file. Two formats are accepted: graphs from the
[Graphalytics repository](https://graphalytics.org/) and graphs saved in one file as a list of edges, possiblly with
weights. In the following we describe the accepted formats. It is expected that the files are well-formed.

## Grapahlytics Format

A Graphalytics graph is stored in multiple files. Our script uses only three of them:

- the file containing vertices have the extension `.v` and list vertex ids, which are natural numbers, one vertex per
  line, e.g.,

  ```
    123
    321
    543
    43
  ```

- the edge file describes edges one edge per line as a pair of vertex ids or a triple vertex id, vertex id, weight. The
  latter is a real number. As a separator, the space character is used, e.g.,

  ```
    123 321 2.322
    43 321 43.2
  ```

- the property file contains (among other information) the substring `.directed = ` followed by `true`
  or `false`.

## Edge list format

A graph is stored in a single file that has the same format as the edge files in Graphalytics format except that it may
contain comment lines starting with `#`, `%` or `/` and the weighs are any sequences of characters without whitespaces.

## How to import

The import script is `importer.py`. You can call with the option `-h` to obtain detailed information on its options that
we describe here as well. The examples are given for a *nix system.

- The only necessary parameter is the address of the server running an ArangoDB instance:

```
  python3 importer.py http://localhost:8529/_db/_system
```

- the format is either `graphalytics` or `edge-list`:

```
  python3 importer.py http://localhost:8529/_db/_system edge-list
```

- database options. These options describe the connection to the database and the objects that will be created in the
  database. Currently, only smart graphs are created.
    - `--user`: username
    - `--pwd`: user password
    - `--graphname`: the name of the graph in the database
    - `--vertices`: the name of the new vertex collection that will be created in the database
    - `--edges`: the name of the new edge collection that will be created in the database
    - `--num_shards`: the number of shards for the new collections
    - `--repl_factor`: replication factor for the new collections
    - `--smart_attribute`: the smart attribute for the vertex collection
    - `--overwrite`: a flag to indicate that any of the graph and both collections (edges and vertices)
      should be deleted if they exist before graph creation
- translation options:
    - `--enforce_undirected`: make the graph undirected even if the corresponding option in the property file tells the
      opposite; this means, for each edge (v,w), insert also the edge (w,v) with the same edge if the latter exists. If
      the edge (w,v) is also contained in the edge file (with its own weight), both edges (w,v) are inserted.
    - `--bulk_size`: the number of vertices/edges that are internally inserted into the database in one iteration. Every
      iteration corresponds to one interaction with the database.
- graphalytics options describe the input files. There are two ways to do this: by giving all three files explicitly or
  by giving the directory containing the three files. In the latter case, certain conditions must be fulfilled:
    1. The names of the files must be identical to the name of the directory containing it.
    2. The vertex file must have the extension `.v`.
    3. The edge file must have the extension `.e`.
    4. The property file must have the extension `.properties`.

  Example: the directory is `./abc/def` and the file names are `def.v`, `def.e` and `def.properties`.

    - `--vertices_file_graphalytics`: the file containing the vertices
    - `--edges_file_graphalytics`: the file containing the edges
    - `--edges_file_graphalytics`: the file containing the edges
    - `--properties_file_graphalytics`: the file containing the properties
    - `--dir_graphalytics`: the directory containing (at least) the three files
- edge list properties:
    - `--edges_file_edge_list`: the file containing the list of edges

# Generating Graphs

The script name is `generator.py`. It can create two types of graphs: undirected cliques and the cliques graphs. An
undirected clique is a graph where every vertex has an edge to every other vertex and, possibly, self-loops. A cliques
graph is a graph that is the result of the following construction.

Make a graph with `num_cliques` many vertices, add edges with probability `inter_cliques_density`. Then replace every
vertex `v` by a set `V` of vertices of size randomly chosen between `min_size_clique` and
`max_size_clique` with equal probability. Add edges between vertices of `V` where an edge is _not_ added with
probability `prob_missing`. (We call the subgraph induced by `V` a clique although it is not necessarily a complete
subgraph. The idea is that if `prob_missing` is very low, the resulting subgraph is "almost"
a clique.) Replace edges `(v,w)` by `num_edges_between_cliques(|V_v|, |V_w|)` edges between the cliques, choosing
endpoints in the cliques randomly with equal distribution.

## How to Generate

The script `generator.py` has at least two arguments:

- the address of the server running an ArangoDB instance and
- the graph type (`clique` or `cliques-graph`), e.g.,

```
   python3 importer.py http://localhost:8529/_db/_system clique 
```

- `-h`: print the help and terminate
- database parameters. These options describe the connection to the database and the objects that will be created in the
  database. Currently, only smart graphs are created.
    - `--user`: username
    - `--pwd`: user password
    - `--graphname`: the name of the graph in the database
    - `--vertices`: the name of the new vertex collection that will be created in the database
    - `--edges`: the name of the new edge collection that will be created in the database
    - `--num_shards`: the number of shards for the new collections
    - `--repl_factor`: replication factor for the new collections
    - `--smart_attribute`: the smart attribute for the vertex collection
    - `--overwrite`: a flag to indicate that any of the graph and both collections (edges and vertices)
      should be deleted if they exist before graph creation
    - `--make_smart`: whether the graph should be smart
- graph parameters:
    - vertex attributes. There are three values that a vertex can have as attributes besides the system attributes:
      (1) the id, (2) to which part of the graph it belongs (e.g., the part in a k-partite graph of the clique in a
      cliques-graph) and (3) an additional attribute, which currently can be only a random real value. If the graph is
      not smart (`--make_smart` is not given), the id is written into the attribute `id`, the part (in graphs where it
      makes sense, currently in clqiues-graphs and in k-partite graphs) into the attribute `part` and the additional
      value (which is optional) into the attribute given in the parameter
      `--additional_vertex_attribute`. The latter cannot be `id` or `part`. If the graph is smart, the smart attribute
      name is given in `--smart_attribute` (the default is `smartProp`). It is possible that the smart attribute name
      is `id` or `part`; in this case, the ids are written into the attribute `id`.
        - `--vertex_property_type`: one of `none`, `random`. If this parameter is not given or is `none`, no additional
          vertex attribute is created. Otherwise, the attribute name is given in `--additional_vertex_attribute`
          and the values are determined by the parameter `--vertex_property`.
        - `--additional_vertex_attribute`: if the vertices should have another (besides `smart_attribute` and `id`) 
          attribute, which is determined by the parameter `--vertex_property_type`, its name is given here
        - `--vertex_property`: if `--vertex_property_type` is random, two space separated numbers `a`, `b` with 
          `a <= b`. The real value is computed randomly with equal distribution between `a` and `b`.
        - `--edge_attribute`: if the edges should have an attribute, which is determined by the parameter
          `--edge_property_type`, its name is given here
        - `--edge_property_type`: one of `none`, `random`. If this parameter is not given or is `none`, the standard
          edge attribute `weight` with property `Null` is created. Otherwise, the attribute name is given 
          in `--edge_attribute`  and the values are determined by the parameter `--edge_property`.
        - `--edge_property`: if `--edge_property_type` is random, two space separated numbers `a`, `b` with `a <= b`.
          The real value is computed randomly with equal distribution between `a` and `b`.
- clique parameters:
    - `--size`: the number of vertices in the clique
- cliques graph parameters:
    - `--num_cliques`: the number of cliques
    - `--min_size_clique`: the (non-strict) lower bound for the size of a clique
    - `--max_size_clique`: the (non-strict) upper bound for the size of a clique
    - `--prob_missing`: the probability for an edge in a clique to be missing
    - `--inter_cliques_density`: the probability there are some edges between two cliques
    - `--density_between_two_cliques`: the density of edges between two cliques, i.e., if the cliques have sizes s1 and
      s2, '
      'and there are m edges between the two cliques, the density is m/(s1*s2).
