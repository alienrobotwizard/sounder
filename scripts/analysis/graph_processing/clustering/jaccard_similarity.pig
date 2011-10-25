--
-- Computes the jaccard (structural) similarity of nodes in a directed graph.
--
-- Note, it is critical for this algorithm that there are no duplicate
-- edges in the input data.
--
%default GRAPH '../data/graph.tsv'

        
edges     = LOAD '$GRAPH' AS (v1:chararray, v2:chararray);
edges_dup = LOAD '$GRAPH' AS (v1:chararray, v2:chararray);

--
-- Augment the edges with the sizes of their outgoing adjacency lists. Note that
-- if a self join was possible we would only have to do this once.
--
grouped_edges = GROUP edges BY v1;
aug_edges     = FOREACH grouped_edges GENERATE FLATTEN(edges) AS (v1, v2), COUNT(edges) AS v1_out;

grouped_dups  = GROUP edges_dup BY v1;
aug_dups      = FOREACH grouped_dups GENERATE FLATTEN(edges_dup) AS (v1, v2), COUNT(edges_dup) AS v1_out;

--
-- Compute the sizes of the intersections of outgoing adjacency lists
--
edges_joined  = JOIN aug_edges BY v2, aug_dups BY v2;
intersection  = FOREACH edges_joined {
                  --
                  -- results in:
                  -- (X, Y, |X| + |Y|)
                  -- 
                  added_size = aug_edges::v1_out + aug_dups::v1_out;
                  GENERATE
                    aug_edges::v1 AS v1,
                    aug_dups::v1  AS v2,
                    added_size    AS added_size
                  ;
                };

intersect_grp   = GROUP intersection BY (v1, v2);
intersect_sizes = FOREACH intersect_grp {
                    --
                    -- results in:
                    -- (X, Y, |X /\ Y|, |X| + |Y|)
                    --
                    intersection_size = (double)COUNT(intersection);
                    GENERATE
                      FLATTEN(group)               AS (v1, v2),
                      intersection_size            AS intersection_size,
                      MAX(intersection.added_size) AS added_size -- hack, we only need this one time
                    ;
                  };

similarities = FOREACH intersect_sizes {
                 --
                 -- results in:
                 -- (X, Y, |X /\ Y|/|X U Y|)
                 --
                 similarity = (double)intersection_size/((double)added_size-(double)intersection_size);
                 GENERATE
                   v1         AS v1,
                   v2         AS v2,
                   similarity AS similarity
                 ;
               };

DUMP similarities;
