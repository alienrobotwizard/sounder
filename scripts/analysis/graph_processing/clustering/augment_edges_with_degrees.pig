%default GRAPH '../data/graph.tsv'
%default AUG   '../data/augmented_graph'

--
-- Attach the degree of each vertex in a list of edges to
-- the edge itself as metadata with only 2 MR jobs.
--
edges     = LOAD '$GRAPH' AS (v1:chararray, v2:chararray);
edges_dup = FOREACH edges GENERATE v1, v2; --FIXME: when can we cogroup a relation with itself?
edges_cog = COGROUP edges BY v1 OUTER, edges_dup BY v2;
v1_deg    = FOREACH edges_cog GENERATE FLATTEN(edges)     AS (v1, v2), COUNT(edges) + COUNT(edges_dup) AS deg_v1;
v2_deg    = FOREACH edges_cog GENERATE FLATTEN(edges_dup) AS (v1, v2), COUNT(edges) + COUNT(edges_dup) AS deg_v2;
v_deg     = COGROUP v1_deg BY (v1, v2), v2_deg BY (v1, v2);
edges_deg = FOREACH v_deg GENERATE FLATTEN(group) AS (v1, v2), FLATTEN(v1_deg.deg_v1) AS deg_v1, FLATTEN(v2_deg.deg_v2) AS deg_v2;

rmf $AUG
STORE edges_deg INTO '$AUG';
