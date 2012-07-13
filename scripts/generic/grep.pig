--
-- Extract lines from a file that match a given regexp. 
--
        
%default RAW 'data/raw_text'
%default OUT 'data/matches'        
%default REGEXP '.*HEART.*'

text = load '$RAW' as (raw_text:chararray);        
out  = filter text by UPPER(raw_text) matches '$REGEXP';

rmf $OUT;
store out into '$OUT';
