--
-- Extract symmetric links from a directed graph
--
%default PAIRS 'data/seinfeld_network.tsv'
%default SYM   'data/seinfeld_network_symmetric.tsv'
        
links   = LOAD '$PAIRS' AS (v1:chararray, v2:chararray);
links_o = FOREACH links { -- order pairs lexigraphically
            first  = (v1 < v2 ? v1 : v2);
            second = (v1 < v2 ? v2 : v1);
            GENERATE first AS v1, second AS v2;
          };
links_g = GROUP links_o BY (v1, v2);
links_c = FOREACH links_g GENERATE FLATTEN(group) AS (v1, v2), COUNT(links_o) AS num;
links_f = FILTER links_c BY num >= 2; -- if link shows up twice it's symmetric
links_s = FOREACH links_f GENERATE v1, v2;

rmf $SYM;
STORE links_s INTO '$SYM';
