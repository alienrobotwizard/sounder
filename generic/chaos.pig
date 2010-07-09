REGISTER /usr/lib/pig/contrib/piggybank/java/piggybank.jar;
%default ATTRACTOR 'data/points.tsv'
%default EPS '0.5'

attractor = LOAD '$ATTRACTOR' AS (x1:double, x2:double);
sample_1  = SAMPLE attractor 0.01;
sample_2  = SAMPLE attractor 0.01;
crossed   = CROSS sample_1, sample_2;
dists     = FOREACH crossed
            {
                sq_dist = (sample_1::x1 - sample_2::x1)*(sample_1::x1 - sample_2::x1) + (sample_1::x2 - sample_2::x2)*(sample_1::x2 - sample_2::x2);
                GENERATE
                    sample_1::x1  AS p1_x1,
                    sample_1::x2  AS p1_x2,
                    sample_2::x1  AS p2_x1,
                    sample_2::x2  AS p2_x2,
                    org.apache.pig.piggybank.evaluation.math.SQRT(sq_dist) AS dist -- you can skip this for most applications
                ;
            };

sample_size     = GROUP dists ALL;
dists_with_size = FOREACH sample_size GENERATE FLATTEN(dists), SIZE(dists) AS n_sample;
to_count        = FILTER dists_with_size BY dist < $EPS;
cut_to_count    = FOREACH to_count GENERATE 1, n_sample;
grouped         = GROUP cut_to_count ALL;
corr_val        = FOREACH grouped GENERATE (float)SIZE(cut_to_count) AS sz, MAX(cut_to_count.n_sample) AS n_sample;
