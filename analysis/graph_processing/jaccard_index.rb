require 'rubygems'
require 'wukong'
require 'set'


#
# Returns jaccard similarity index between two objects
#
def jaccard_index(x, y)
  set1 = Set.new(x)
  set2 = Set.new(y)
  set1.intersection(set2).size.to_f / set1.union(set2).size.to_f
end

