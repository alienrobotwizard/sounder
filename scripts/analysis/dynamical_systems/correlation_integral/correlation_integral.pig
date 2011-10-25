--
-- Correlation Integral
-- 
-- C(e) = lim N->inf (1/N^2)*SUM( H( e - distance(x_i,x_j) ) )
--
-- where H is the Heaviside step function { 0 if distance(x_i,x_j) > e, 1 if distance(x_i,x_j) < e }
-- where x_i and x_j are different points on the attractor
--
-- Here we take a sample n < N to calculate it for a given e (EPS)
--
%default ATTRACTOR    'data/points.tsv'
%default SAMPLE_FRAC  '0.1'  -- higher precentage = better accuracy but slower and more intensive        
%default SAMPLE_SIZE  '100'  -- should be less than or equal to what is pulled out from the SAMPLE command
%default EPS          '0.5'  -- independent variable in the correlation integral
%default OUT          'data/corr_iter_00'
        
attractor = LOAD '$ATTRACTOR' AS (x1:double, x2:double);
first     = SAMPLE attractor $SAMPLE_FRAC;
second    = SAMPLE attractor $SAMPLE_FRAC;
sample_1  = LIMIT first  $SAMPLE_SIZE;
sample_2  = LIMIT second $SAMPLE_SIZE;
crossed   = CROSS sample_1, sample_2; -- careful
dists     = FOREACH crossed {
              sq_dist = (sample_1::x1 - sample_2::x1)*(sample_1::x1 - sample_2::x1) + (sample_1::x2 - sample_2::x2)*(sample_1::x2 - sample_2::x2);
              GENERATE sq_dist AS sq_dist;
            };
to_sum    = FILTER dists BY sq_dist < (double)$EPS*(double)$EPS; -- only keep points inside circle of radius eps
only_ones = FOREACH to_sum GENERATE 1;
grouped   = GROUP only_ones ALL;
corr      = FOREACH grouped GENERATE (float)COUNT(only_ones)/((float)$SAMPLE_SIZE*(float)$SAMPLE_SIZE) AS corr_val, $EPS AS eps;;

STORE corr INTO '$OUT';
