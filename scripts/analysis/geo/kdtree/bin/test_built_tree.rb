#!/usr/bin/env ruby
require 'rubygems'
require 'json'

#
# Utility script for testing the k-d tree built with pig
#

#
# The fields are expected to be in this order in the input
#
FIELDS = %w[
  point_id
  is_root
  axis
  above_child
  below_child
  lng
  lat
]

class KDTree
  attr_accessor :points, :other_roots

  Node = Struct.new(:id, :axis, :left, :right, :coords)

  def initialize(roots, branches)

    # First, add all the branches to the tree (represented as a hashmap)
    @points = branches.inject({ }) do |tree,l|
      node          = line_to_node(l)
      tree[node.id] = node
      tree
    end

    # Next, nominate a root to be "the root"
    roots = roots.map{|root| line_to_node(root) }.sort{|x,y| x.coords[0] <=> y.coords[0]}
    @root = nominate_root(roots)

    # Add other roots to tree by insertion
    roots.each do |node|
      if (@root.id != node.id)
        add_point(@root, node)
      end
    end

    @dimensions = 2
    @nearest    = []
  end

  #
  # Convert the tsv input line into a node object
  #
  def line_to_node line
    point = FIELDS.zip(line.strip.split("\t")).inject({}){|hsh, arr| hsh[arr.first] = arr.last; hsh}
    point["lng"]  = point["lng"].to_f
    point["lat"]  = point["lat"].to_f
    point["axis"] = point["axis"].to_i

    left  = (point["below_child"].empty? ? nil : point["below_child"])
    right = (point["above_child"].empty? ? nil : point["above_child"])

    Node.new(point["point_id"], point["axis"], left, right, [point["lng"], point["lat"]])
  end

  #
  # Given a sorted array of 'root nodes', select one to be the top root
  #
  def nominate_root roots
    roots[roots.size/2+1]
  end

  #
  # Node is the current node being looked at, point is the point being inserted
  #
  def add_point node, point
    axis = node.axis
    if point.coords[axis] <= node.coords[axis]
      if node.left.nil?
        node.left = point.id
        @points[node.id]  = node
        @points[point.id] = point
      else
        add_point(@points[node.left], point)
      end
    else
      if node.right.nil?
        node.right = point.id
        @points[node.id]  = node
        @points[point.id] = point
      else
        add_point(@points[node.right], point)
      end
    end
  end

  def find_nearest(target, k_nearest)
    @nearest = []
    nearest(@root, target, k_nearest, 0)
  end

  def distance2(node, target)
    return nil if node.nil? or target.nil?
    c = (node.coords[0] - target[0])
    d = (node.coords[1] - target[1])
    c * c + d * d
  end

  def check_nearest(nearest, node, target, k_nearest)
    d = distance2(node, target)
    if nearest.size < k_nearest || d < nearest.last[0]
      nearest.pop if nearest.size >= k_nearest
      nearest << [d, node.id]
      nearest.sort! { |a, b| a[0] <=> b[0] }
    end
    nearest
  end

  def nearest(node, target, k_nearest, depth)
    axis = node.axis
    if node.left.nil? && node.right.nil? # Leaf node
      @nearest = check_nearest(@nearest, node, target, k_nearest)
      return
    end

    # Go down the nearest split
    if node.right.nil? || (node.left && target[axis] <= node.coords[axis])
      nearer = @points[node.left]
      further = @points[node.right]
    else
      nearer = @points[node.right]
      further = @points[node.left]
    end
    nearest(nearer, target, k_nearest, depth+1)

    # See if we have to check other side
    if further
      if @nearest.size < k_nearest || (target[axis] - node.coords[axis])**2 < @nearest.last[0]
        nearest(further, target, k_nearest, depth+1)
      end
    end

    @nearest = check_nearest(@nearest, node, target, k_nearest)
  end

end

roots    = File.readlines(ARGV[0])
branches = File.readlines(ARGV[1])

kdtree   = KDTree.new(roots, branches)

# my house
test_point = [-97.699013, 30.265016]
nearest = kdtree.find_nearest(test_point, 1)
nearest.map do |pnt|
  dist = pnt.first
  id   = pnt.last
  p dist
  p kdtree.points[id]
end
