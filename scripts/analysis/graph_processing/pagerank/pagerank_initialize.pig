--
-- Create initial graph on which to iterate the pagerank algorithm.
--
%default ADJLIST  '../data/seinfeld_network.tsv'
%default INITGRPH '../data/pagerank_graph_000'

--
-- Generate a unique list of nodes with in links to cogroup on. This allows
-- us to treat the case where nodes have in links but no out links.
--
network     = LOAD '$ADJLIST' AS (node_a:chararray, node_b:chararray);
cut_rhs     = FOREACH network GENERATE node_b;
uniq_rhs    = DISTINCT cut_rhs;
list_links  = COGROUP network BY node_a, uniq_rhs BY node_b;
count_links = FOREACH list_links
              {
                  -- if network.node_b is empty there are no out links, set to dummy value
                  out_links = (IsEmpty(network.node_b) ? {('dummy')} : network.node_b);
                  GENERATE
                      group     AS node_a,
                      1.0f      AS rank,
                      out_links AS out_links
                  ;
              };

rmf $INITGRPH;
STORE count_links INTO '$INITGRPH';
