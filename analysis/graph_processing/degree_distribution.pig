--
-- Given an adjacency pair representation of a directed graph
-- calculate its out degree, in degree, and degree.
--
%default PAIRS   'data/seinfeld_network.tsv'
%default DEGDIST 'data/seinfeld_network_deg_dist'

pairs   = LOAD '$PAIRS' AS (v1:chararray, v2:chararray);        
pairs_o = FOREACH pairs GENERATE v1 AS v;
pairs_i = FOREACH pairs GENERATE v2 AS v;
pairs_g = COGROUP pairs_o BY v OUTER, pairs_i BY v;
degrees = FOREACH pairs_g {
               out_degree = COUNT(pairs_o);
               in_degree  = COUNT(pairs_i);
               degree     = out_degree + in_degree;
               GENERATE
                 group      AS vertex,
                 out_degree AS out_degree,
                 in_degree  AS in_degree,
                 degree     AS degree
               ;
             };

rmf $DEGDIST
STORE degrees INTO '$DEGDIST';
