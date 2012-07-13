%default NETWORK     'data/seinfeld_network.tsv'
%default OUT_CLIQUE  'data/seinfeld_n1_clique'
%default N0_SEED     'kramer'

links = load '$NETWORK' as (node_a:chararray, node_b:chararray);

-- Extract all edges that originate or terminate on the seed (n0)
e1_edges = filter links by (node_a == '$N0_SEED') or (node_b == '$N0_SEED');

--
-- From e1_edges, find all nodes in the in or out 1-neighborhood
-- (the nodes at radius 1 from our seed)
--
n1_out = foreach e1_edges generate node_a as node;
n1_in  = foreach e1_edges generate node_b as node;
n1     = distinct (filter (union n1_out, n1_in) by node != '$N0_SEED');
                                                                                                                                                         
-- Find all edges that originate in n1
e2_out_edges = foreach (join links by node_a, n1 by node using 'replicated') generate
                 links::node_a as node_a, links::node_b as node_b;
 
-- Among those edges, find those that terminate in n1 as well
clique_edges = foreach (join e2_out_edges by node_b, n1 by node using 'replicated') generate
                 e2_out_edges::node_a as node_a, e2_out_edges::node_b as node_b;

-- Save the result
rmf $OUT_CLIQUE;
store clique_edges into '$OUT_CLIQUE';
