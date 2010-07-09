--
-- Given an adjacency pair representation of a network
-- calculate its out degree, in degree, and degree. Order
-- the result by degree.
--
%default PAIRS       'data/seinfeld_network.tsv'
%default GRAPHCOUNTS 'data/seinfeld_network_deg_dist.tsv'
        
pairs         = LOAD '$PAIRS' AS (node_a:chararray, node_b:chararray);
out_list      = GROUP pairs BY node_a;
out_counts    = FOREACH out_list GENERATE group AS node, COUNT(pairs) AS num_out_links;
in_list       = GROUP pairs BY node_b;
in_counts     = FOREACH in_list GENERATE group AS node, COUNT(pairs) AS num_in_links;
joined        = JOIN out_counts BY node FULL OUTER, in_counts BY node;
node_counts   = FOREACH joined
                {
                    node_name = (out_counts::node IS NOT NULL?out_counts::node:in_counts::node);
                    degree    = out_counts::num_out_links + in_counts::num_in_links;
                    GENERATE
                        node_name                 AS node,
                        out_counts::num_out_links AS num_out_links,
                        in_counts::num_in_links   AS num_in_links,
                        degree                    AS degree
                    ;
                };

ordered = ORDER node_counts BY degree DESC;

rmf $GRAPHCOUNTS;
STORE node_counts INTO '$GRAPHCOUNTS';
