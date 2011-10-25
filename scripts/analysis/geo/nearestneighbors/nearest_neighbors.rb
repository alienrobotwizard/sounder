#!/usr/bin/env jruby

require 'java'

#
# You might consider changing this to point to where you have
# pig installed...
#
jar  = "/usr/lib/pig/pig-0.8.1-cdh3u1-core.jar"
conf = "/etc/hadoop/conf"

$CLASSPATH << conf
require jar

import org.apache.pig.ExecType
import org.apache.pig.PigServer
import org.apache.pig.FuncSpec

class NearestNeighbors

  attr_accessor :points, :k, :min_zl, :runmode

  #
  # Create a new nearest neighbors instance
  # for the given points, k neighbors to find,
  # a optional minimum zl (1-21) and optional
  # hadoop run mode (local or mapreduce)
  #
  def initialize points, k, min_zl=20, runmode='mapreduce'
    @points  = points
    @k       = k
    @min_zl  = min_zl
    @runmode = runmode
  end

  #
  # Run the nearest neighbors algorithm
  #
  def run
    start_pig_server
    register_jars_and_functions
    run_algorithm
    stop_pig_server
  end

  #
  # Actually runs all the pig queries for
  # the algorithm. Stops if all neighbors
  # have been found or if min_zl is reached
  #
  def run_algorithm
    start_nearest_neighbors(points, k, 22)
    if run_nearest_neighbors(k, 22)
      21.downto(min_zl) do |zl|
        iterate_nearest_neighbors(k, zl)
        break unless run_nearest_neighbors(k,zl)
      end
    end
  end

  #
  # Registers algorithm initialization queries
  #
  def start_nearest_neighbors(input, k, zl)
    @pig.register_query(PigQueries.load_points(input))
    @pig.register_query(PigQueries.generate_initial_quadkeys(zl))
  end

  #
  # Registers algorithm iteration queries
  #
  def iterate_nearest_neighbors k, zl
    @pig.register_query(PigQueries.load_prior_done(zl))
    @pig.register_query(PigQueries.load_prior_not_done(zl))
    @pig.register_query(PigQueries.union_priors(zl))
    @pig.register_query(PigQueries.trim_quadkey(zl))
  end

  #
  # Runs one iteration of the algorithm
  #
  def run_nearest_neighbors(k, zl)
    @pig.register_query(PigQueries.group_by_quadkey(zl))
    @pig.register_query(PigQueries.nearest_neighbors(k, zl))
    @pig.register_query(PigQueries.split_results(k, zl))

    if !@pig.exists_file("done#{zl}")
      @pig.store("done#{zl}", "done#{zl}")
      not_done = @pig.store("not_done#{zl}", "not_done#{zl}")
      not_done.get_results.has_next?
    else
      true
    end
  end

  #
  # Start a new pig server with the specified run mode
  #
  def start_pig_server
    @pig = PigServer.new(runmode)
  end

  #
  # Stop the running pig server
  #
  def stop_pig_server
    @pig.shutdown
  end

  #
  # Register the jar that contains the nearest neighbors
  # and quadkeys udfs and define functions for them.
  #
  def register_jars_and_functions
    @pig.register_jar('../../../../udf/target/sounder-1.0-SNAPSHOT.jar')
    @pig.register_function('GetQuadkey',       FuncSpec.new('sounder.pig.geo.GetQuadkey()'))
    @pig.register_function('NearestNeighbors', FuncSpec.new('sounder.pig.geo.nearestneighbors.NearestNeighbors()'))
  end

  #
  # A simple class to contain the pig queries
  #
  class PigQueries

    #
    # Load the geonames points. Obviously,
    # this should be modified to accept a
    # variable schema.
    #
    def self.load_points geonames
      "points = LOAD '#{geonames}' AS (
     geonameid:         int,
     name:              chararray,
     asciiname:         chararray,
     alternatenames:    chararray,
     latitude:          double,
     longitude:         double,
     feature_class:     chararray,
     feature_code:      chararray,
     country_code:      chararray,
     cc2:               chararray,
     admin1_code:       chararray,
     admin2_code:       chararray,
     admin3_code:       chararray,
     admin4_code:       chararray,
     population:        long,
     elevation:         int,
     gtopo30:           int,
     timezone:          chararray,
     modification_date: chararray
   );"
    end

    #
    # Query to generate quadkeys at the specified zoom level
    #
    def self.generate_initial_quadkeys(zl)
      "projected#{zl} = FOREACH points GENERATE GetQuadkey(longitude, latitude, #{zl}) AS quadkey, geonameid, longitude, latitude, {};"
    end

    #
    # Load previous iteration's done points
    #
    def self.load_prior_done(zl)
      "prior_done#{zl+1} = LOAD 'done#{zl+1}/part*' AS (
     quadkey:   chararray,
     geonameid: int,
     longitude: double,
     latitude:  double,
     neighbors: bag {t:tuple(neighbor_id:int, distance:double)}
   );"
    end

    #
    # Load previous iteration's not done points
    #
    def self.load_prior_not_done(zl)
      "prior_not_done#{zl+1} = LOAD 'not_done#{zl+1}/part*' AS (
     quadkey:   chararray,
     geonameid: int,
     longitude: double,
     latitude:  double,
     neighbors: bag {t:tuple(neighbor_id:int, distance:double)}
   );"
    end

    #
    # Union the previous iterations points that are done
    # with the points that are not done
    #
    def self.union_priors zl
      "prior_neighbors#{zl+1} = UNION prior_done#{zl+1}, prior_not_done#{zl+1};"
    end

    #
    # Chop off one character of precision from the existing
    # quadkey to go one zl down.
    #
    def self.trim_quadkey zl
      "projected#{zl} = FOREACH prior_neighbors#{zl+1} GENERATE
     SUBSTRING(quadkey, 0, #{zl}) AS quadkey,
     geonameid AS geonameid,
     longitude AS longitude,
     latitude  AS latitude,
     neighbors AS neighbors;"
    end

    #
    # Group the points by quadkey
    #
    def self.group_by_quadkey zl
      "grouped#{zl}  = FOREACH (GROUP projected#{zl} BY quadkey) GENERATE group AS quadkey, projected#{zl}.(geonameid, longitude, latitude, $4) AS points_bag;"
    end

    #
    # Run the nearest neighbors udf on all the points for
    # a given quadkey
    #
    def self.nearest_neighbors(k, zl)
      "nearest_neighbors#{zl} = FOREACH grouped#{zl} GENERATE quadkey AS quadkey, FLATTEN(NearestNeighbors(#{k}l, points_bag)) AS (
     geonameid: int,
     longitude: double,
     latitude:  double,
     neighbors: bag {t:tuple(neighbor_id:int, distance:double)}
   );"
    end

    #
    # Split the results into done and not_done relations
    # The algorithm is done when 'not_done' contains
    # no more tuples.
    #
    def self.split_results(k, zl)
      "SPLIT nearest_neighbors#{zl} INTO done#{zl} IF COUNT(neighbors) >= #{k}l, not_done#{zl} IF COUNT(neighbors) < #{k}l;"
    end
  end

end

NearestNeighbors.new(ARGV[0], ARGV[1]).run
