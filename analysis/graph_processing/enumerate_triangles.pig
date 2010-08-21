--
-- Need to use the output from "augment_edges_with_degrees.pig"
--
-- See: http://www.computer.org/portal/web/csdl/doi/10.1109/MCSE.2009.120 for full discussion
--
%default AUG   'data/augmented_graph'
%default TRI   'data/enumerated_triangles'
        
edges   = LOAD '$AUG' AS (v1:chararray, v2:chararray, deg_v1:int, deg_v2:int);
edges_f = FILTER edges BY (deg_v1 > 1) AND (deg_v2 > 1); -- a vertex with deg = 1 can't possibly form triangles
edges_o = FOREACH edges_f {
            -- order vertices by degree, order lexigraphically in case of tie
            first  = (deg_v1 <= deg_v2 ? v1 : v2);
            second = (deg_v1 <= deg_v2 ? v2 : v1);
            t1     = (deg_v1 == deg_v2 AND first > second ? second : first);
            t2     = (deg_v1 == deg_v2 AND first > second ? first  : second);
            GENERATE t1 AS v1, t2 AS v2;
          };

edges_g  = GROUP edges_o BY v1;
edges_gf = FILTER edges_g BY COUNT(edges_o) > 1;

-- stream reduce output through a simple wukong script
triads   = STREAM edges_gf THROUGH `open_triads.rb --map` AS (
             v1:chararray,    v2:chararray,
             t1_v1:chararray, t1_v2:chararray,
             t2_v1:chararray, t2_v2:chararray
           );

edges_only = FOREACH edges {
               first  = (v1 < v2 ? v1 : v2);
               second = (v1 < v2 ? v2 : v1);
               GENERATE first AS v1, second AS v2;
             };
-- first two fields form the theoretical closing edge (use this for friend suggestion...),
-- a simple join with real edges pulls out only those that actually exist
triangles    = JOIN triads BY (v1, v2), edges_only BY (v1, v2);
triangles_fg = FOREACH triangles GENERATE triads::v1, triads::v2, triads::t1_v1, triads::t1_v2, triads::t2_v1, triads::t2_v2;

rmf $TRI
STORE triangles_fg INTO '$TRI';
