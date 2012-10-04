package sounder.pig.points;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.BagFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;

/**
   Constructs a k-d tree from the passed in databag containing
   points. NOTE: This is intended as a proof-of-concept and is
   unlikely to be production worthy.
 */
public class KDTree extends EvalFunc<DataBag> {
    private static Comparator<KDPoint> comparators[];
    private static final Integer ID_FIELD = 0;
    private static final Integer IS_ROOT_FIELD = 1;
    private static final Integer AXIS_FIELD = 2;
    private static final Integer ABOVE_CHILD_FIELD = 3;
    private static final Integer BELOW_CHILD_FIELD = 4;
    private static final Integer POINT_FIELD = 5;
    
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() < 1 || input.isNull(0)) { return null; }

        DataBag points = (DataBag)input.get(0);       // {(id, point:(x1,x2,...,xK))}
        KDPoint[] asPoints = toPoints(points);

        return generateTree(asPoints);        
    }

    /**
       Check if the input tuple can make a valid KDPoint object
     */
    private boolean isValidPoint(Tuple t) throws ExecException {
        if (t.isNull(0) || t.isNull(1)) { return false; }
        return true;
    }

    /**
       Construct an array of KDPoint objects from the passed in DataBag
       of tuples
     */
    private KDPoint[] toPoints(DataBag points) throws ExecException {
        KDPoint[] result = new KDPoint[((Long)points.size()).intValue()];
        int idx = 0;
        for (Tuple t : points) {
            if (isValidPoint(t)) {
                result[idx] = new KDPoint(t);
                idx++;
            }
        }
        return result;
    }

    /**
       Recursively generate a k-d tree from the passed in array of points
     */
    private DataBag generateTree(KDPoint[] points) throws ExecException {
        if (points.length == 0) { return null; }

        int maxD = points[0].getDimensionality();
        comparators = new Comparator[maxD];
        for (int i = 0; i < maxD; i++) {
            comparators[i] = new KDPointComparator(i);
        }
        KDPoint root = generate(0, maxD, points, 0, points.length-1);
        root.isRoot = true;
        return root.toBag();
    }

    private KDPoint generate(int d, int maxD, KDPoint[] points, int left, int right) throws ExecException {
        if (right < left) { return null; }
        if (right == left) {
            KDPoint returnPoint = points[left];
            if (returnPoint != null) { returnPoint.setAxis(d); }
            return returnPoint;
        }

        int m = (right-left)/2;
        // Yes, sort every time. Not super efficient
        Arrays.sort(points, left, right+1, comparators[d]);

        KDPoint medianPoint = points[left+m];
        medianPoint.setAxis(d);
        
        if (++d >= maxD) { d = 0; }
        
        medianPoint.setBelowChild(generate(d, maxD, points, left, left+m-1));
	medianPoint.setAboveChild(generate(d, maxD, points, left+m+1, right));
        return medianPoint;
    }

    /**
       Set the appropriate output schema so pig doesn't get confused
     */
    public Schema outputSchema(Schema input) {
        Schema schema = null;
        try {
            schema = Utils.getSchemaFromString("result:bag{t:tuple(id:chararray, is_root:int, axis:int, above_child:chararray, below_child:chararray, point:tuple(lng:double, lat:double))}");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return schema;
    }
    
    /**
       Simple representation of a multi-dimensional point
     */
    class KDPoint {
        
        final String pointId;
        final int dimensionality;
        public boolean isRoot;
        protected String aboveChildId;
        protected String belowChildId;
        protected KDPoint aboveChild; // Above child, right in 2-D case
        protected KDPoint belowChild; // Below child, left in 2-D case
        protected Integer axis; // Splitting axis for this node (0 or 1) in 2-D case
        double values[];

        /**
           Construct a KDPoint from the passed in tuple representation
         */
        public KDPoint(Tuple pointTuple) throws ExecException {
            this.pointId = (String)pointTuple.get(0);
            Tuple point = (Tuple)pointTuple.get(1);
            
            int d = this.dimensionality = point.size();
            values = new double[d];
            for (int i = 0; i < d; i++) {
                values[i] = (Double)point.get(i);
            }
        }

        public KDPoint getAboveChild() { return aboveChild; }
        public String getAboveChildId() { return aboveChildId; }
        public KDPoint getBelowChild() { return belowChild; }
        public String getBelowChildId() { return belowChildId; }
        public Integer getAxis() { return axis; }
        public String getPointId() { return pointId; }
        public int getDimensionality() { return dimensionality; }
        public double getCoordinate(int d) { return values[d]; }

        public void setAboveChild(KDPoint child) {
            this.aboveChild = child;
            if (child != null) { this.aboveChildId = child.getPointId(); }
        }
        
        public void setAboveChildId(String childId) { this.aboveChildId = childId; }
        
        public void setBelowChild(KDPoint child) {
            this.belowChild = child;
            if (child != null) { this.belowChildId = child.getPointId(); }
        }
        
        public void setBelowChildId(String childId) { this.belowChildId = childId; }
        
        public void setAxis(Integer axis) { this.axis = axis; }

        public Tuple toTuple() throws ExecException {
            TupleFactory tfact = TupleFactory.getInstance();
            Tuple result = tfact.newTuple(6);
            Tuple point = tfact.newTuple(dimensionality);

            for (int i = 0; i < dimensionality; i++) {
                point.set(i, values[i]);
            }
            
            result.set(0, pointId);
            result.set(1, (isRoot ? 1 : 0));
            result.set(2, axis);
            result.set(3, aboveChildId);
            result.set(4, belowChildId);
            result.set(5, point);
            return result;
        }

        public DataBag toBag() throws ExecException {
            DataBag result = BagFactory.getInstance().newDefaultBag();
            result.add(toTuple());
            if (aboveChild != null) {
                result.addAll(aboveChild.toBag());
            }

            if (belowChild != null) {
                result.addAll(belowChild.toBag());
            }
            return result;
        }
    }

    /**
       Simple comparator class for sorting KDPoints along a particular dimension
     */
    public class KDPointComparator implements Comparator<KDPoint> {
        public final int d;
        public static final double epsilon = 1E-9;
        
        public KDPointComparator (int d) {
            this.d = d;
	}

        public int compare(KDPoint p1, KDPoint p2) {
            double d1 = p1.getCoordinate(d);
            double d2 = p2.getCoordinate(d);
            if (lesser(d1, d2)) { return -1; }
            if (same(d1, d2)) { return 0; }		
            return +1;
	}

        public double value(double x) {
            if ((x >= 0) && (x <= epsilon)) { return 0.0; }
            
            if ((x < 0) && (-x <= epsilon)) { return 0.0; }
            
            return x;
	}

        public boolean lesser(double x, double y) { return value(x-y) < 0; }

        public boolean same (double d1, double d2) {
            if (Double.isNaN(d1)) { return Double.isNaN(d2); }
            
            if (d1 == d2) { return true; }
            
            if (Double.isInfinite(d1)) { return false; }
            
            return value (d1-d2) == 0;
	}
    }
}
