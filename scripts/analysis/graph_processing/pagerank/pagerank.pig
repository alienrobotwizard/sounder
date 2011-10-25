%default CURR_ITER_FILE '../data/pagerank_graph_000'
%default NEXT_ITER_FILE '../data/pagerank_graph_001'
%default DAMP   '0.85f' -- naively accepting that given in the wikipedia article on pagerank...

network      = LOAD '$CURR_ITER_FILE' AS (node_a:chararray, rank:float, out_links:bag { link:tuple (node_b:chararray) });
sent_shares  = FOREACH network GENERATE FLATTEN(out_links) AS node_b, (float)(rank / (float)SIZE(out_links)) AS share:float;
sent_links   = FOREACH network GENERATE node_a, out_links;
rcvd_shares  = COGROUP sent_links BY node_a INNER, sent_shares BY node_b;
next_iter    = FOREACH rcvd_shares
               {
                   raw_rank    = (float)SUM(sent_shares.share);
                   -- treat the case that a node has no in links                   
                   damped_rank = ((raw_rank IS NOT NULL AND raw_rank > 1.0e-12f) ? raw_rank*$DAMP + 1.0f - $DAMP : 0.0f);
                   GENERATE
                       group         AS node_a,
                       damped_rank   AS rank,
                       FLATTEN(sent_links.out_links) -- hack, should only be one bag, unbag it
                   ;
               };

rmf $NEXT_ITER_FILE;
STORE next_iter INTO '$NEXT_ITER_FILE';
