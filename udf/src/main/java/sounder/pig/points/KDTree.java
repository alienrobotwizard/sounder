package sounder.pig.points;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.BagFactory;
import org.apache.pig.backend.executionengine.ExecException;
    
public class KDTree extends EvalFunc<DataBag> {
    private static Comparator<KDPoint> comparators[];
    
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() < 1 || input.isNull(0)) { return null; }

        DataBag points = (DataBag)input.get(0); // {(id,point:tuple(x1,x2,...,xK)}
        
        KDPoint[] asPoints = toPoints(points);
        DataBag result = generateTree(asPoints);
        return result;        
    }

    private KDPoint[] toPoints(DataBag points) throws ExecException {
        KDPoint[] result = new KDPoint[((Long)points.size()).intValue()];
        int idx = 0;
        for (Tuple t : points) {
            if (!t.isNull(0) && !t.isNull(1)) {
                String pointId = (String)t.get(0);
                Tuple point = (Tuple)t.get(1);
                result[idx] = new KDPoint(pointId, point);
                idx++;
            }
        }
        return result;
    }

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
        if (right == left) { return points[left]; }

        int m = (right-left)/2;
        Arrays.sort(points, left, right, comparators[d]);

        KDPoint medianPoint = points[left+m];
        if (++d >= maxD) { d = 0; }

        medianPoint.setBelowChild(generate(d, maxD, points, left, left+m-1));
	medianPoint.setAboveChild(generate(d, maxD, points, left+m+1, right));
        return medianPoint;
    }
    
    /**
       Simple representation of a multi-dimensional point
     */
    class KDPoint {
        
        final String pointId;
        final int dimensionality;
        public boolean isRoot;
        protected KDPoint aboveChild;
        protected KDPoint belowChild;
        double values[];

        /**
           Construct a KDPoint from the passed in tuple representation
         */
        public KDPoint(String pointId, Tuple point) throws ExecException {
            this.pointId = pointId;
            this.isRoot = false;
            int d = this.dimensionality = point.size();
            values = new double[d];
            for (int i = 0; i < d; i++) {
                values[i] = (Double)point.get(i);
            }
        }

        public KDPoint getAboveChild() { return aboveChild; }
        public KDPoint getBelowChild() { return belowChild; }
        
        public String getPointId() {
            return pointId;
        }
        
        public int getDimensionality() {
            return dimensionality;
        }

        public double getCoordinate(int d) {
            return values[d];
        }

        public void setAboveChild(KDPoint child) { this.aboveChild = child; }
        public void setBelowChild(KDPoint child) { this.belowChild = child; }

        public Tuple toTuple() throws ExecException {
            TupleFactory tfact = TupleFactory.getInstance();
            Tuple result = tfact.newTuple(5);
            Tuple point = tfact.newTuple(dimensionality);

            for (int i = 0; i < dimensionality; i++) {
                point.set(i, values[i]);
            }
            
            result.set(0, pointId);
            result.set(1, (isRoot ? 1 : 0));
            result.set(2, (aboveChild != null ? aboveChild.getPointId() : null));
            result.set(3, (belowChild != null ? belowChild.getPointId() : null));
            result.set(4, point);
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
            if ((x >= 0) && (x <= epsilon)) {
                return 0.0;
            }
            
            if ((x < 0) && (-x <= epsilon)) {
                return 0.0;
            }
            
            return x;
	}

        public boolean lesser(double x, double y) {
		return value(x-y) < 0;
	}

        public boolean same (double d1, double d2) {
            if (Double.isNaN(d1)) {
                return Double.isNaN(d2);
            }
            
            if (d1 == d2) return true;
            
            if (Double.isInfinite(d1)) {
                return false;
            }
            
            return value (d1-d2) == 0;
	}
    }
}
