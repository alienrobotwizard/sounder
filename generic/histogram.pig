--
-- Given a grossly simplified site log (ip_address, user_id, timestamp) generate
-- a count of how many times each has logged in.
--
%default LOG 'data/sitelog.tsv'
%default OUT 'data/visits_hist.tsv'
        
log_data   = LOAD '$LOG' AS (ip_address:chararray, user:chararray, timestamp:long);
cut_data   = FOREACH log_data GENERATE user;
accumulate = GROUP cut_data BY user;
histogram  = FOREACH accumulate GENERATE group AS user, COUNT(cut_data) AS num_visits;

rmf $OUT;
STORE histogram INTO '$OUT';
