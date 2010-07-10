--
-- Given adjacency pairs, output only the symmetric ones 
--
%default PAIRS 'data/seinfeld_network.tsv'
%default SYM   'data/seinfeld_network_symmetric.tsv'
        
links   = LOAD '$PAIRS' AS (node_a:chararray, node_b:chararray);
ordered = FOREACH links -- order pairs alphabetically
          {
              first  = (node_a <= node_b ? node_a : node_b);
              second = (node_a <= node_b ? node_b : node_a);
              GENERATE first AS node_a, second AS node_b;
          };

grouped   = GROUP ordered BY (node_a, node_b);
counts    = FOREACH grouped GENERATE
                FLATTEN(group) AS (node_a, node_b),
                COUNT(ordered) AS num_pairs
            ;
filtered  = FILTER counts BY num_pairs >= 2;
symmetric = FOREACH filtered GENERATE node_a, node_b;

rmf $SYM;
STORE symmetric_links INTO '$SYM';
