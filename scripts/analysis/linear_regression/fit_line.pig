import 'linear_model.pig';

--
-- Consider linear regression w/gradient descent for stupid simple line fitting case
--
weights  = load '$input_weights' as (w0:double, w1:double);
features = load '$data'          as (x:double, y:double);

weights  = foreach weights generate TOTUPLE(w0,w1) as weights:tuple(w0:double, w1:double);
features = foreach features generate y as response, TOTUPLE(1.0, x) as vector:tuple(f0:double, f1:double);

new_weights = sounder_lm_gradient_descent(features, weights, 0.1);
new_weights = foreach new_weights generate flatten($0);

store new_weights into '$output_weights';
