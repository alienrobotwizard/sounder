#!/usr/bin/env ruby

#
# Pass these in via args
#
raise "\n\nUsage: ./iterate.rb /path/to/attractor <num_iters> /path/to/output_dir\n" unless ARGV.length == 3
attractor = ARGV[0]
num_iters = ARGV[1]
work_dir  = ARGV[2]

#
# Parameters that are constant throughout the calculation
#
eps_min         = 0.001
eps_increment   = 0.025
sample_size     = 1000
sample_fraction = 0.001

#
# Sample the same attractor num_iters times
#
eps = eps_min
num_iters.to_i.times do |i|
  next_dir  = File.join(work_dir, "corr_iter_#{i}")
  system "pig -p ATTRACTOR=#{attractor} -p OUT=#{next_dir} -p EPS=#{eps} -p SAMPLE_SIZE=#{sample_size} -p SAMPLE_FRAC=#{sample_fraction} correlation_integral.pig"
  eps += eps_increment
end

