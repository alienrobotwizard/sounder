%default NETWORK     'seinfeld_network.tsv'
%default GRAPHCOUNTS 'seinfeld_degree_dist.tsv'
        
links         = LOAD '$NETWORK' AS (node_a:chararray, node_b:chararray);
-- get out degree distribution
out_list      = GROUP links BY node_a;
out_counts    = FOREACH out_list GENERATE group AS node, COUNT(links) AS num_out_links;

-- get in degree distribution
in_list       = GROUP links BY node_b;
in_counts     = FOREACH in_list GENERATE group AS node, COUNT(links) AS num_in_links;

-- join together
joined      = JOIN out_counts BY node, in_counts BY node;
node_counts = FOREACH joined GENERATE
                                  out_counts::node                           AS node,
                                  out_counts::num_out_links                  AS num_out_links,
                                  in_counts::num_in_links                    AS num_in_links,
                                  ((float)num_out_links/(float)num_in_links) AS ratio
                               ;
ordered     = ORDER node_counts BY ratio DESC;

rmf $GRAPHCOUNTS;
STORE ordered INTO '$GRAPHCOUNTS';
