--
-- Create initial graph on which to iterate the pagerank algorithm.
--
%default ADJLIST  'data/seinfeld_network.tsv'
%default INITGRPH 'data/pagerank_graph_000'
        
network = LOAD '$ADJLIST' AS (user_a:chararray, user_b:chararray);

list_links  = GROUP network BY user_a;
count_links = FOREACH list_links
              {
                  num_out_links = COUNT(network);
                  GENERATE
                      group           AS user_a,
                      1.0             AS rank,
                      num_out_links   AS num_out_links,
                      network.user_b  AS out_links
                  ;
              };

rmf $INITGRPH;
STORE count_links INTO '$INITGRPH';
