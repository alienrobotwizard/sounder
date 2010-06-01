%default PAIRS       'seinfeld_network.tsv'
%default GRAPHCOUNTS 'seinfeld_network_deg_dist.tsv'
        
pairs         = LOAD '$PAIRS' AS (node_a:chararray, node_b:chararray);
out_list      = GROUP pairs BY node_a;
out_counts    = FOREACH out_list GENERATE group AS node, COUNT(pairs) AS num_out_links;
in_list       = GROUP pairs BY node_b;
in_counts     = FOREACH in_list GENERATE group AS node, COUNT(pairs) AS num_in_links;
joined        = JOIN out_counts BY node FULL OUTER, in_counts BY node;
node_counts   = FOREACH joined
        {
        node_name = (out_counts::node IS NOT NULL?out_counts::node:in_counts::node);
        GENERATE
                node_name                                  AS node,
                out_counts::num_out_links                  AS num_out_links,
                in_counts::num_in_links                    AS num_in_links,
                ((float)num_out_links/(float)num_in_links) AS ratio
                ;
};
ordered = ORDER node_counts BY ratio DESC;

rmf $GRAPHCOUNTS;
STORE node_counts INTO '$GRAPHCOUNTS';
