#!/usr/bin/env ruby

require 'rubygems'
require 'fileutils'

#
# 1. initialize weights
# 2. descend
# 3. check convergence criteria
# 4. repeat 2,3 until converged
# 5. profit
#

def cleanup data_dir
  Dir["#{data_dir}/weight-*"].each do |f|
    FileUtils.rm_r(f)
  end  
end

#
# Initialize the weights randomly
#
def initialize_weights data_dir
  weights = [rand,rand]
  f = File.open(File.join(data_dir, 'weight-0'), 'w')
  f.puts(weights.join("\t"))
  f.close
end

def descend data_dir, iteration
  input_weights  = File.join(data_dir, "weight-#{iteration}")
  output_weights = File.join(data_dir, "weight-#{iteration+1}")

  pig     = File.join(ENV['PIG_HOME'], 'bin', 'pig')
  pig_cmd = "#{pig} -x local -p weights=#{input_weights} -p weights_new=#{output_weights} gradient_descent.pig"
  %x{#{pig_cmd}}
end

data_dir = "data"

cleanup(data_dir)
initialize_weights(data_dir)
1000.times do |i|
  descend(data_dir, i)
end
