--
-- Read in a bag of tuples (timeseries for this example) and divide the
-- numeric column by its maximum.
--
%default DATABAG 'data/timeseries.tsv'
        
data       = LOAD '$DATABAG' AS (month:chararray, count:int);
accumulate = GROUP data ALL;
calc_max   = FOREACH accumulate GENERATE FLATTEN(data), MAX(data.count) AS max_count;
normalize  = FOREACH calc_max GENERATE data::month AS month, data::count AS count, (float)data::count / (float)max_count AS normed_count;
DUMP normalize;
