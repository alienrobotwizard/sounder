--
-- Create initial graph on which to iterate the pagerank algorithm.
--
%default ADJLIST  '../data/seinfeld_network.tsv'
%default INITGRPH '../data/pagerank_graph_000'
        
network = LOAD '$ADJLIST' AS (rsrc:chararray, user_a:chararray, user_b:chararray);

cut_links   = FOREACH network GENERATE user_a, user_b;
list_links  = GROUP cut_links BY user_a;
count_links = FOREACH list_links
              {
                  num_out_links = COUNT(cut_links);
                  GENERATE
                      group           AS user_a,
                      1.0             AS rank,
                      num_out_links   AS num_out_links,
                      cut_links.user_b  AS out_links
                  ;
              };

rmf $INITGRPH;
STORE count_links INTO '$INITGRPH';
