#!/usr/bin/env ruby

require 'rubygems'
require 'wukong'
require 'wukong/and_pig'

#
# Here we need to receive data of the form:
#
# (D,{(D,C),(D,E)})
#
# and emit data of the form:
#
# (C,E,D,C,D,E)
#
class TriadMapper < Wukong::Streamer::RecordStreamer
  def process group, bag, &blk
    yield_triads(bag, &blk)
  end

  def yield_triads pigbag, &blk
    edges = pigbag.from_pig_bag
    pairs = edges.zip(edges[1..-1]).reject{|pair| pair.include?(nil)}
    pairs.each do |pair|
      key = [pair[0].last, pair[1].last].sort
      yield [key, pair].flatten
    end    
  end
  
end

Wukong::Script.new(TriadMapper, nil).run
