register '../../../udf/target/sounder-1.0-SNAPSHOT.jar';

define Zip sounder.pig.tuple.Zip();

/*
 * Run one iteration of gradient descent for the given weights and features
 * with step size alpha. Returns the updated weights.
 *
 * features - Relation with the following schema:
 *  {response:double, vector:tuple(f0:double,f1:double,...,fN:double)}
 *
 * w - Relation with **exactly one tuple** with the following schema:
 *  {weights:tuple(w0:double,w1:double,...,wN:double)}
 *
 * alpha - Step size. Schema:
 *   alpha:double
 */
define sounder_lm_gradient_descent(features, w, alpha) returns new_weights {
  
  --
  -- Use a scalar cast to zip weights with their appropriate features
  --
  weighted = foreach $features {
             zipped = Zip($w.weights, vector);
             generate
               response as response,
               vector   as vector,
               zipped   as zipped:bag{t:tuple(weight:double, feature:double, dimension:int)};
           };

  --
  -- Compute dot product of weights with feature vectors. First part of
  -- step adjustment.
  --
  dot_prod = foreach weighted {
               dots        = foreach zipped generate weight*feature as product;
               dot_product = SUM(dots.product);
               diff        = (dot_product-response);

               generate
                 flatten(zipped) as (weight, feature, dimension), diff as diff;
             };
  
  scaled = foreach dot_prod generate dimension, weight, feature*diff as feature_diff;

  --
  -- Compute step diff along each dimension. Uses combiners
  --
  steps = foreach (group scaled by (dimension,weight)) {
            factor      = ($alpha/(double)COUNT(scaled));
            weight_step = factor*SUM(scaled.feature_diff);
            new_weight  = group.weight - weight_step;
            generate
              group.dimension as dimension,
              new_weight      as weight;
          };

  --
  -- A group all is acceptable here since the previous step reduces the
  -- size down to the number of features.
  --
  $new_weights = foreach (group steps all) {
                   in_order = order steps by dimension asc;
                   as_tuple = BagToTuple(in_order.weight);
                   generate
                     as_tuple;
                 };
};
