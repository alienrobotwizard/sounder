--
-- Given an adjacency pair representation of a directed graph
-- calculate its out degree, in degree, and degree.
--
%default PAIRS   'data/seinfeld_network.tsv'
%default DEGDIST 'data/seinfeld_network_deg_dist'

adj_pairs  = LOAD '$PAIRS' AS (node_a:chararray, node_b:chararray);        
out_node   = FOREACH adj_pairs GENERATE node_a AS node;
in_node    = FOREACH adj_pairs GENERATE node_b AS node;
grouped    = COGROUP out_node BY node OUTER, in_node BY node;
degrees    = FOREACH grouped
             {
               out_degree = COUNT(out_node);
               in_degree  = COUNT(in_node);
               degree     = out_degree + in_degree;
               GENERATE
                 group      AS node,
                 out_degree AS out_degree,
                 in_degree  AS in_degree,
                 degree     AS degree
               ;
             };

rmf $DEGDIST
STORE degrees INTO '$DEGDIST';
