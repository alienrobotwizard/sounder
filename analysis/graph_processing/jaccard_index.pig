-- Take two input sets,X and Y, and compute their jaccard similarity index
--
-- in pseudo code:
--
-- jaccard_index = COUNT(intersection(X,Y)) / COUNT(union(X,Y))
--
%default SETX 'data/set_x'
%default SETY 'data/set_y'
        
x = LOAD '$SETX' AS (element:float);
y = LOAD '$SETY' AS (element:float);

-- perform intersection and union (in an efficient way?)
union_xy       = UNION x,y;
union_xy_g     = GROUP union_xy BY element;
intersect_xy_g = FILTER union_xy_g BY COUNT(union_xy) == 2;
intersect_xy   = FOREACH intersect_xy_g GENERATE group;

-- calculate sizes, there must be a better way
union_xy_sz      = GROUP union_xy ALL;
union_xy_sz_fg   = FOREACH union_xy_sz GENERATE COUNT(union_xy) AS union_size;
intrsct_xy_sz    = GROUP intersect_xy ALL;
intrsct_xy_sz_fg = FOREACH intrsct_xy_sz GENERATE COUNT(intersect_xy) AS intrsct_size;

-- finally
jaccard_idx      = CROSS union_xy_sz_fg, intrsct_xy_sz_fg;
jaccard_idx_fg   = FOREACH jaccard_idx GENERATE (float)intrsct_xy_sz_fg::intrsct_size / (float)union_xy_sz_fg::union_size;
DUMP jaccard_idx_fg;
