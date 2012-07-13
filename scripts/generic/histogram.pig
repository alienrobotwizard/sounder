--
-- Given a grossly simplified site log (ip_address, user_id, timestamp) generate
-- a count of how many times each has logged in.
--
%default LOG 'data/sitelog.tsv'
%default OUT 'data/visits_hist.tsv'
        
log_data   = load '$LOG' as (ip_address:chararray, user:chararray, timestamp:long);
cut_data   = foreach log_data generate user;
histogram  = foreach (group cut_data by user) generate
               group as user, COUNT(cut_data) as num_visits;

rmf $OUT;
store histogram into '$OUT';
