--
-- Given an adjacency pair representation of a directed graph
-- calculate its out degree, in degree, and degree.
--
%default PAIRS   'data/seinfeld_network.tsv'
%default DEGDIST 'data/seinfeld_network_deg_dist'

pairs   = load '$PAIRS' as (v1:chararray, v2:chararray);

-- Projecting the pairs relation twice is necessary since we cant cogroup
-- a relation with itself.
pairs_o = foreach pairs generate v1 as v;
pairs_i = foreach pairs generate v2 as v;

degrees = foreach (cogroup pairs_o by v, pairs_i by v) {
            out_degree = COUNT(pairs_o);
            in_degree  = COUNT(pairs_i);
            degree     = out_degree + in_degree;
            generate
              group      as vertex,
              out_degree as out_degree,
              in_degree  as in_degree,
              degree     as degree;
          };

rmf $DEGDIST
store degrees into '$DEGDIST';
