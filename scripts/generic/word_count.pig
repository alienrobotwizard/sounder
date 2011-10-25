--
-- Word count from raw text. Illustrates the canonical map-reduce
-- example as well as contrib udfs like lower and replace.
--
REGISTER /usr/local/share/pig/contrib/piggybank/java/piggybank.jar;
%default RAW          'data/raw_text'
%default TOKEN_COUNTS 'data/token_counts.tsv'
        
text      = LOAD '$RAW' USING TextLoader();
tokens    = FOREACH text GENERATE FLATTEN(TOKENIZE($0)) AS token;
lowered   = FOREACH tokens GENERATE org.apache.pig.piggybank.evaluation.string.LOWER(token) AS token;
rectified = FOREACH lowered GENERATE org.apache.pig.piggybank.evaluation.string.REPLACE(token, '(\\.|\\:|\\!|\\?)', '') AS token; --yuck
grouped   = GROUP rectified BY token;
counts    = FOREACH grouped GENERATE group AS token, COUNT(rectified) AS count;

rmf $TOKEN_COUNTS;
STORE counts INTO '$TOKEN_COUNTS';
