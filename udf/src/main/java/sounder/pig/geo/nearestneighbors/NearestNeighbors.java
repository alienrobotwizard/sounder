package sounder.pig.geo.nearestneighbors;

import java.io.IOException;
import java.util.PriorityQueue;
import java.util.Iterator;
import java.util.Comparator;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;

public class NearestNeighbors extends EvalFunc<DataBag> {
    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    private static BagFactory bagFactory = BagFactory.getInstance();
    
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() < 2 || input.isNull(0) || input.isNull(1))
            return null;

        Long k = (Long)input.get(0);
        DataBag points = (DataBag)input.get(1);      // {(id,lng,lat,{(n1,n1_dist)...})}
        DataBag result = bagFactory.newDefaultBag(); 

        for (Tuple pointA : points) {
            DataBag neighborsBag = (DataBag)pointA.get(3);
            if (neighborsBag.size() < k) {
                PriorityQueue<Tuple> neighbors = toDistanceSortedQueue(k.intValue(), neighborsBag);
                Double x1 = Math.toRadians((Double)pointA.get(1));
                Double y1 = Math.toRadians((Double)pointA.get(2));
                
                for (Tuple pointB : points) {
                    if (pointA!=pointB) {
                        Double x2 = Math.toRadians((Double)pointB.get(1));
                        Double y2 = Math.toRadians((Double)pointB.get(2));
                        Double distance = haversineDistance(x1,y1,x2,y2);

                        // Add this point as a neighbor if pointA has no neighbors
                        if (neighbors.size()==0) {
                            Tuple newNeighbor = tupleFactory.newTuple(2);
                            newNeighbor.set(0, pointB.get(0));
                            newNeighbor.set(1, distance);
                            neighbors.add(newNeighbor);
                        }

                        Tuple furthestNeighbor = neighbors.peek();
                        Double neighborDist = (Double)furthestNeighbor.get(1);
                        if (distance < neighborDist) {
                            Tuple newNeighbor = tupleFactory.newTuple(2);
                            newNeighbor.set(0, pointB.get(0));
                            newNeighbor.set(1, distance);
                            
                            if (neighbors.size() < k) {                                
                                neighbors.add(newNeighbor);
                            } else {
                                neighbors.poll(); // remove farthest
                                neighbors.add(newNeighbor);
                            }
                        }
                    }
                }
                // Should now have a priorityqueue containing a sorted list of neighbors
                // create new result tuple and add to result bag
                Tuple newPointA = tupleFactory.newTuple(4);
                newPointA.set(0, pointA.get(0));
                newPointA.set(1, pointA.get(1));
                newPointA.set(2, pointA.get(2));
                newPointA.set(3, fromQueue(neighbors));
                result.add(newPointA);
            } else {
                result.add(pointA);
            }                    
        }
        return result;
    }

    // Ensure sorted by descending
    private PriorityQueue<Tuple> toDistanceSortedQueue(int k, DataBag bag) {
        PriorityQueue<Tuple> q = new PriorityQueue<Tuple>(k,
                                                          new Comparator<Tuple>() {
                                                              public int compare(Tuple t1, Tuple t2) {
                                                                  try {
                                                                      Double dist1 = (Double)t1.get(1);
                                                                      Double dist2 = (Double)t2.get(1);
                                                                      return dist2.compareTo(dist1);
                                                                  } catch (ExecException e) {
                                                                      throw new RuntimeException("Error comparing tuples", e);
                                                                  }
                                                              };
                                                          });
        for (Tuple tuple : bag) q.add(tuple);            
        return q;
    }

    private DataBag fromQueue(PriorityQueue<Tuple> q) {
        DataBag bag = bagFactory.newDefaultBag();
        for (Tuple tuple : q) bag.add(tuple);
        return bag;
    }
    
    private Double haversineDistance(Double x1, Double y1, Double x2, Double y2) {
        double a = Math.pow(Math.sin((x2-x1)/2), 2)
            + Math.cos(x1) * Math.cos(x2) * Math.pow(Math.sin((y2-y1)/2), 2);

        return (2 * Math.asin(Math.min(1, Math.sqrt(a))));
    }
}
