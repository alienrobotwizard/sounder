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

EPS = 0.001

def cleanup data_dir
  Dir["#{data_dir}/weight-*"].each do |f|
    FileUtils.rm_r(f)
  end  
end

#
# Initialize the weights randomly
#
def initialize_weights data_dir, num_features
  weights = []
  num_features.times{ weights << rand }
  
  f = File.open(File.join(data_dir, 'weight-0'), 'w')
  f.puts(weights.join("\t"))
  f.close
end

def descend data_dir, iteration
  input_weights  = File.join(data_dir, "weight-#{iteration}")
  output_weights = File.join(data_dir, "weight-#{iteration+1}")

  pig     = File.join(ENV['PIG_HOME'], 'bin', 'pig')
  pig_cmd = "#{pig} -x local -p weights=#{input_weights} -p weights_new=#{output_weights} fit_line.pig"
  %x{#{pig_cmd}}
end

def converged? data_dir, iteration
  input_weights  = File.join(data_dir, "weight-#{iteration}/part-r-00000")
  output_weights = File.join(data_dir, "weight-#{iteration+1}/part-r-00000")

  x1 = File.readlines(input_weights).first.strip.split("\t").map{|x| x.to_f }
  x2 = File.readlines(output_weights).first.strip.split("\t").map{|x| x.to_f }

  d = Math.sqrt(x2.zip(x1).inject(0.0){|d,pair| d += (pair.first - pair.last)**2; d})

  (d < EPS)
end

data_dir     = ARGV[0]
num_features = ARGV[1].to_i

cleanup(data_dir)
initialize_weights(data_dir, num_features)
descend(data_dir,0)
descend(data_dir,1)

i = 2
loop do 
  descend(data_dir, i)
  break if converged?(data_dir, i)
  i += 1
end

puts File.read(File.join(data_dir, "weight-#{i}/part-r-00000"))
