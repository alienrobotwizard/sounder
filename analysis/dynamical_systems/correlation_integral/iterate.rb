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
sample_size     = 5000
sample_fraction = 0.001

iter = 0
eps  = eps_min
num_iters.to_i.times do
  next_file = "corr_iter_" + iter.to_s
  next_dir  = File.join(work_dir, next_file)
  system %Q{ pig -param ATTRACTOR=#{attractor} -param OUT=#{next_dir} -param EPS=#{eps} -param SAMPLE_SIZE=#{sample_size} -param SAMPLE_FRAC=#{sample_fraction} correlation_integral.pig }
  iter += 1
  eps += eps_increment
end

