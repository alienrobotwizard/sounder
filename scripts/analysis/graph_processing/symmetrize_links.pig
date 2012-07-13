--
-- Extract symmetric links from a directed graph
--
%default PAIRS 'data/seinfeld_network.tsv'
%default SYM   'data/seinfeld_network_symmetric.tsv'
        
links         = load '$PAIRS' AS (v1:chararray, v2:chararray);
links_ordered = foreach links generate (v1 < v2 ? v1 : v2) as v1, (v1 < v2 ? v2 : v1) as v2;

counted = foreach (group links_ordered by (v1, v2)) generate
                FLATTEN(group) as (v1, v2), COUNT(links_ordered) as num;

symmetrized = foreach (filter counted by num >= 2) generate v1, v2;

rmf $SYM;
store symmetrized into '$SYM';
