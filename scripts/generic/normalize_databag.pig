--
-- Read in a bag of tuples (timeseries for this example) and divide the
-- numeric column by its maximum.
--
%default DATABAG 'data/timeseries.tsv'
        
data       = load '$DATABAG' as (month:chararray, count:int);
calc_max   = foreach (group data all) generate
               FLATTEN(data) as (month, count), MAX(data.count) as max_count;
normalized = foreach calc_max generate month, count, (float)count / (float)max_count as normalized_count;
dump normalized;
