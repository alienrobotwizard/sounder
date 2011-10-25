#!/usr/bin/env ruby

def henon_map
  steps = 1000000
  a = 1.4
  b = 0.3
  x = 0.631354477
  y = 0.189406343
  
  steps.times do
    x_old = x
    x = y + 1 - a*x*x
    y = b*x_old
    puts [x,y].join("\t") + "\n"
  end
end

henon_map
