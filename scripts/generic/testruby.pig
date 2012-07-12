register '../../udf/src/main/ruby/testruby.rb' using jruby as myfuncs;

data = load 'data/points.tsv' as (x:double, y:double);

x_squared = foreach data generate myfuncs.square(x);

dump x_squared;
