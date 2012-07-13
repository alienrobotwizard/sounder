--
-- Word count from raw text. Illustrates the canonical map-reduce
-- example as well as contrib udfs like lower and replace.
--
%default RAW          'data/raw_text'
%default TOKEN_COUNTS 'data/token_counts.tsv'
        
text      = load '$RAW' using TextLoader();
tokens    = foreach text generate FLATTEN(TOKENIZE($0)) as token;
cleaned   = foreach tokens generate REPLACE(LOWER(token), '(\\.|\\:|\\!|\\?)', '') as token;
counts    = foreach (group cleaned by token) generate group as token, COUNT(cleaned) as token_count;

rmf $TOKEN_COUNTS;
store counts into '$TOKEN_COUNTS';
