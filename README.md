# Graph Importer

This repository contains command line Python scripts to import graphs from external sources as files into a running
instance of `arangod`
and to generate graphs in a running instance.

# TL;DR:

- importing a [Graphalytics](https://graphalytics.org/) graph:

```commandline
wget https://atlarge.ewi.tudelft.nl/graphalytics/zip/wiki-Talk.zip
unzip wiki-Talk.zip
python importer.py http://localhost:8529/_db/_system graphalytics --dir_graphalytics wiki-Talk
```

- importing a graph saved as a list of edges:

```commandline
python importer.py http://localhost:8529/_db/_system edge-list --edges_file_edge_list /PATH/GRAPH_FILE 
```

- generate a clique (only one direction for every undirected edge):

```commandline
python generator.py http://localhost:8529/_db/_system clique \
    --num_vertices 1000 --graphname Clique --vertex_collection_name cliqueVertices --edge_collection_name cliqueEdges \
    --overwrite --vertex_property_type random 
    --vertex_property 0.1 0.9 --edge_property_type random --edge_property 0.2 0.8
```

This will create a clique graph on 1000 vertices in the database. The vertex collection will be `cliqueVertices`, the
edge collection `cliqueEdges`, the graph itself `Clique`. Any existing object with the same name will be overwritten.
The vertices will have a random number between `0.1` and `0.9` as an attribute, the edges - between `0.2` and `0.8`.

- Generating a "cliques-graph": a disjoint union of 100 cliques such that
    - in a clique, an edge is missing with probability `0.4`;
    - any two cliques are connected with probability `0.7`;
    - an edge between two connected cliques exists with probability `0.3`.

```commandline
python generator.py http://localhost:8529/_db/_system cliques-graph 
    --num_cliques 100 --min_size_clique 12 --max_size_clique 12 
    --prob_missing_one 0.4 --prob_missing_all 0.7 --density_between_two_cliques 0.3
```

- Generating the 20-partite complete graph with parts of random size between 30 and 35. The constructed graph will be
  smart with smart attribute `part`

```commandline
python generator.py http://localhost:8529/_db/_system k-partite 
    --num_parts 20 --min_size_clique 30 --max_size_clique 35 
    --make_smart --smart_attribute part --overwrite
```

# Importing Graphs

The graphs must be stored in local files. Two formats are accepted: graphs from the
[Graphalytics repository](https://graphalytics.org/) and graphs saved in one file as a list of edges, possiblly with
weights. In the following we describe the accepted formats. It is expected that the files are well-formed.

## Graphalytics Format

A Graphalytics graph is stored in multiple files. Our script uses only two of them:

- the file containing vertices has the extension `.v` and lists vertex ids, which are natural numbers, one vertex per
  line, e.g.,

  ```
    123
    321
    543
    43
  ```

- the edge file describes edges one edge per line as a pair of vertex ids or a triple (vertex id, vertex id, weight).
  The latter is a real number. As a separator, the space character is used, e.g.,

  ```
    123 321 2.322
    43 321 43.2
  ```

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

- the format is either `graphalytics` or `edge-list` (default is `edge-list`):

```
  python3 importer.py http://localhost:8529/_db/_system edge-list
```

- _database options._ These options describe the connection to the database and the objects that will be created in the
  database.
    - `--user`: username, default is `root`
    - `--pwd`: user password, default is the empty string
    - `--graphname`: the name of the graph in the database, default is `importedGraph`
    - `--vertex_collection_name`: the name of the new vertex collection that will be created in the database, default
      is `vertices`
    - `--edge_collection_name`: the name of the new edge collection that will be created in the database, default
      is `edges`
    - `--make_smart`: make a SmartGraph (only available if the database is run in the Enterprise edition). Deafult is
      `False`. For smart graphs, further options are available (they are ignored for non-smart graphs):
    - `--num_shards`: the number of shards for the new collections, default is 5
    - `--repl_factor`: replication factor for the new collections, default is 2
    - `--smart_attribute`: the smart attribute for the vertex collection, default is `smartProp`
    - `--overwrite`: a flag to indicate that any of the graph and both collections (edges and vertices)
      should be deleted if they exist before graph creation, default is `False`
- _translation options_:
    - `--bulk_size`: the maximum number of vertices/edges that are internally inserted into the database in one database
      interaction, default is 10000
- _graphalytics options_ describe the input files. There are two ways to do this: by giving all three files explicitly
  or by giving the directory containing the three files. In the latter case, certain conditions must be fulfilled:
    1. The names of the files must be identical to the name of the directory containing it.
    2. The vertex file must have the extension `.v`.
    3. The edge file must have the extension `.e`.
    4. The properties file must have the extension `.properties`.

  Example: the directory is `./abc/def` and the file names are `def.v` and `def.e`and `def.properties`.

    - `--vertices_file_graphalytics`: the file containing the vertices, if not given, `--dir_graphalytics` will be used
    - `--edges_file_graphalytics`: the file containing the edges, if not given, `--dir_graphalytics` will be used
    - `--properties_file_graphalytics`: the file containing the graph properties, if not given, `--dir_graphalytics`
      will be used
    - `--dir_graphalytics`: the directory containing (at least) the two files, default is the current directory
- edge list properties:
    - `--edges_file_edge_list`: the file containing the list of edges, default is `graph.txt`

- verbosity:
    - `-- silent`: do not print time statistics, progress bar and what is being currently done, default is `False`

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
- the graph type (`clique`, `cliques-graph`or `k-partite`), e.g.,

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
