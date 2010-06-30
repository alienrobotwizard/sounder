--
-- Extract lines from a file that match a given regexp. 
--
REGISTER /usr/lib/pig/contrib/piggybank/java/piggybank.jar
        
%default RAW 'data/raw_text'
%default OUT 'data/matches'        
%default REGEXP '.*HEART.*'

text = LOAD '$RAW' AS (raw_text:chararray);        
out  = FILTER text BY org.apache.pig.piggybank.evaluation.string.UPPER(raw_text) MATCHES '$REGEXP';

rmf $OUT;
STORE out INTO '$OUT';
