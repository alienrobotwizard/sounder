--
-- Calculate the euclidean distance from every point in a
-- given set to every other point in that set. Expensive!
--
REGISTER /usr/lib/pig/contrib/piggybank/java/piggybank.jar;
%default POINTS 'data/points.tsv'
%default DISTS  'data/points_with_distances.tsv'


points = LOAD '$POINTS' AS (x1:double, x2:double);
dupes  = LOAD '$POINTS' AS (x1:double, x2:double);
pairs  = CROSS points, dupes;
dists  = FOREACH pairs
         {
             sq_dist = (points::x1 - dupes::x1)*(points::x1 - dupes::x1) + (points::x2 - dupes::x2)*(points::x2 - dupes::x2);
             GENERATE
                 points::x1 AS p1_x1,
                 points::x2 AS p1_x2,
                 dupes::x1  AS p2_x1,
                 dupes::x2  AS p2_x2,
                 org.apache.pig.piggybank.evaluation.math.SQRT(sq_dist) AS dist -- you can skip this for most applications
             ;
         };

rmf $DISTS;
STORE dists INTO '$DISTS';
