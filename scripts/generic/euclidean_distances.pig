--
-- Calculate the euclidean distance from every point in a
-- given set to every other point in that set. Expensive!
--

%default POINTS 'data/points.tsv'
%default DISTS  'data/points_with_distances.tsv'

points = load '$POINTS' as (x1:double, x2:double);
dupes  = foreach points generate x1, x2;
dists  = foreach (cross dupes, points) {
           sq_dist = (points::x1 - dupes::x1)*(points::x1 - dupes::x1) + (points::x2 - dupes::x2)*(points::x2 - dupes::x2);
           generate
             points::x1, points::x2, dupes::x1, dupes::x2,
             SQRT(sq_dist) as dist;
         };

rmf $DISTS;
store dists into '$DISTS';
