--
-- Script for generating a kdtree from a set of 2 dimensional points. Specifically,
-- makes use of the quadkey to partition space into non-overlapping tiles.
--
register '../../../../udf/target/sounder-1.0-SNAPSHOT.jar';

define GetQuadkey sounder.pig.geo.GetQuadkey();
define KDTree     sounder.pig.points.KDTree();

--
-- Load data with required fields
--
data = load '$POINTS' as (id:chararray, lat:double, lng:double);

--
-- Partition the space by assigning every point to a tile as ZL=7
--
with_key = foreach data generate id, TOTUPLE(lng, lat) as point, GetQuadkey(lng, lat, 7) as quadkey;

--
-- generate a k-d tree for every tile
--
trees = foreach (group with_key by quadkey) {
          tree = KDTree(with_key.(id, point));
          generate
            group as quadkey,
            FLATTEN(tree) as (id:chararray, is_root:int, axis:int, above_child:chararray, below_child:chararray, point:tuple(lng:double, lat:double));
        };

--
-- Next we need to flatten and split the trees
--
flat_trees = foreach trees generate id..below_child, flatten(point) as (lng, lat);

split flat_trees into roots if is_root==1, branches if is_root==0;


store roots    into '$ROOTS';
store branches into '$BRANCHES';
