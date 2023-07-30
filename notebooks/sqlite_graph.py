### sqlite_graph.py

# This is a very simple implementation of a relational database approach to storing and 
# querying labeled property graphs as described in [Schmid 2019].

## References

# Schmid, M., 2019, December. On efficiently storing huge property graphs in relational 
# database management systems. In Proceedings of the 21st International Conference on 
# Information Integration and Web-based Applications & Services (pp. 344-352).

# Bornea, M.A., Dolby, J., Kementsietsidis, A., Srinivas, K., Dantressangle, P., Udrea, O. and 
# Bhattacharjee, B., 2013, June. Building an efficient RDF store over a relational database. 
# In Proceedings of the 2013 ACM SIGMOD International Conference on Management of Data (pp. 121-132).

# Sun, W., Fokoue, A., Srinivas, K., Kementsietsidis, A., Hu, G. and Xie, G., 2015, May. 
# Sqlgraph: An efficient relational-based property graph store. In Proceedings of 
# the 2015 ACM SIGMOD International Conference on Management of Data (pp. 1887-1901).

import hashlib
from sqlite_utils import Database

### column_pair: a simple function for hashing of edge labels to column pairs

# Schmid's approach uses a graph coloring algorithm to derive a hashing function that avoids 
# collisions in the adjacency tables. We didn't get around to doing that yet. 
# Instead, collisions be damned; we'll just hash the edge label to one of 
# ```NUMBER_OF_COLUMN_PAIRS``` column pairs. 

NUMBER_OF_COLUMN_PAIRS = 100

def column_pair(label):
    m = hashlib.md5()
    m.update(str.encode(label))
    digest = m.hexdigest()
    n = int(digest, 16) % NUMBER_OF_COLUMN_PAIRS
    return ( f'label_{n}', f'edges_{n}' )

### generate_adjacency_tables: a utility function for generating adjacency tables from edges

# Schmid uses column triples in his adjacency table schema, with one column 
# containing an array of edge ids, another column containing an array of target vertex ids, 
# and a column containing the edge label common to all of those. His ```unshred_edges``` 
# Common Table Expression (CTE) then uses PostgreSQL's ```UNNEST``` function to convert entries 
# in the outgoing adjacency table to an intermediate result that enables subsequent CTEs or 
# queries to process matching edges. Since SQLite doesn't have an equivalent to ```UNNEST```, 
# we instead opt to use column pairs, one with the edge label and the other containing 
# an array of JSON objects, each of which contains the edge and target vertex ids for 
# a given edge that shares the edge label.

def generate_adjacency_tables(db):
    out_neighborhoods = {}
    in_neighborhoods = {}
    for row in db.execute("select sid, eid, tid, label from edge").fetchall():
        (sid, eid, tid, label) = row
        label_column, edges_column = column_pair(label)

        if sid not in out_neighborhoods:
            out_neighborhoods[sid] = { 'vid': sid }
        if label_column not in out_neighborhoods[sid]:
            out_neighborhoods[sid][label_column] = label
        if edges_column not in out_neighborhoods[sid]:
            out_neighborhoods[sid][edges_column] = []
        out_neighborhoods[sid][edges_column].append({ 'eid': eid, 'tid': tid })
        
        if tid not in in_neighborhoods:
            in_neighborhoods[tid] = { 'vid': tid }
        if label_column not in in_neighborhoods[tid]:
            in_neighborhoods[tid][label_column] = label
        if edges_column not in in_neighborhoods[tid]:
            in_neighborhoods[tid][edges_column] = []
        in_neighborhoods[tid][edges_column].append({ 'eid': eid, 'sid': sid })       
        
    db['outgoing'].insert_all([ out_neighborhoods[sid] for sid in out_neighborhoods ], pk='vid')
    db['incoming'].insert_all([ in_neighborhoods[tid] for tid in in_neighborhoods ], pk='vid')

### out_neighborhood_cte: a utility function for generating a CTE for edges with targets in 
### the out neighborhood of a given vertex and edge label

# Here we define generate a Common Table Expression (CTE) corresponding to the combination of 
# ```unshred_edges``` and ```gather_edges``` in Schmid's article, with the source vertex id and 
# edge label as arguments.

def out_neighborhood_cte(vid, label):
    label_column, edges_column = column_pair(label)
    return (
        f'select vid, json_extract(value, "$.eid") as eid, {label_column} as label,'
        ' json_extract(value, "$.tid") as tid' 
        f' from outgoing, json_each(outgoing.{edges_column})'
        f' where vid = "{vid}" and {label_column} = "{label}"'
    )

### in_neighborhood_cte: a utility function for generating a CTE for edges with targets in 
### the in neighborhood of a given vertex and edge label

def in_neighborhood_cte(vid, label):
    label_column, edges_column = column_pair(label)
    return (
        f'select vid, json_extract(value, "$.eid") as eid, {label_column} as label,'
        ' json_extract(value, "$.sid") as sid' 
        f' from incoming, json_each(incoming.{edges_column})'
        f' where vid = "{vid}" and {label_column} = "{label}"'
    )

