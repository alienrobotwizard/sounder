%default NETWORK     'data/seinfeld_network.tsv'
%default OUT_CLIQUE  'data/seinfeld_n1_clique'
%default N0_SEED     'kramer'

links = LOAD '$NETWORK' AS (node_a:chararray, node_b:chararray);

-- Extract all edges that originate or terminate on the seed (n0)
e1_edges = FILTER links BY (node_a == '$N0_SEED') OR (node_b == '$N0_SEED');

--
-- From e1_edges, find all nodes in the in or out 1-neighborhood
-- (the nodes at radius 1 from our seed)
--
n1_out          = FOREACH e1_edges GENERATE node_a AS node;
n1_in           = FOREACH e1_edges GENERATE node_b AS node;
n1_out_plus_in  = UNION n1_out, n1_in;
n1_nodes_only   = FILTER n1_out_plus_in BY (node != '$N0_SEED');
n1              = DISTINCT n1_nodes_only;
                                                                                                                                                        
-- Find all edges that originate in n1
e2_out_edges_j  = JOIN links BY node_a, n1 BY node USING 'replicated';
e2_out_edges    = FOREACH e2_out_edges_j GENERATE node_a, node_b;

-- Among those edges, find those that terminate in n1 as well
clique_edges_j  = JOIN e2_out_edges BY node_b, n1 BY node USING 'replicated';
clique_edges    = FOREACH clique_edges_j GENERATE node_a, node_b;

-- Save the result
rmf $OUT_CLIQUE;
STORE clique_edges INTO '$OUT_CLIQUE';
