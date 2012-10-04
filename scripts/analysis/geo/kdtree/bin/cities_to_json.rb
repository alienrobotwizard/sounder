#!/usr/bin/env ruby

require 'rubygems'
require 'json'

#
# Little utility script to convert points from geonames into geojson
# for viewing with qgis
#

#
# The fields are expected to be in this order in the input
#
FIELDS = %w[
  geonameid
  name
  longitude
  latitude
]

input_tsv   = ARGV[0]
output_json = ARGV[0]

#
# Read data from input tsv, convert each point to a geojson feature,
# serialize the results as one geojson featurecollection.
#
data     = File.readlines(input_tsv)
features = data.map do |x|
  props  = FIELDS.zip(x.strip.split("\t")).inject({}){|hsh, arr| hsh[arr.first] = arr.last; hsh}
  coords = [props["longitude"].to_f, props["latitude"].to_f]
  feature = {
    :type     => "Feature",
    :geometry => {
      :type => "Point",
      :coordinates => coords
    },
    :properties => props.reject{|x,y| x =~ /lat|lon/}
  }
end

result = {
  :type     => "FeatureCollection",
  :features => features
}

File.open(output_json, 'w').puts(result.to_json)
