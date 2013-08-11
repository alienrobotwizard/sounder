package sounder.pig.points;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.List;
import java.util.ArrayList;
import java.lang.InterruptedException;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;

/**
   Given a space (1 or more linearly independent dimensions) represented
   as a csv file of the form:
   <p>
   dimension_1_start, dimension_1_end, dimension_1_num
   dimension_2_start, dimension_2_end, dimension_2_num
   .
   .
   .
   dimension_N_start, dimension_N_end, dimension_N_num
   
   produces a rectangular tiling of the space.
   Each "tile" is a multidimensional rectangle with sides of lengths:

   (dim1_max - dim1_min)/dim1_num
   (dim2_max - dim2_min)/dim2_num
   .
   .
   .
   (dimN_max - dimN_min)/dimN_num

   and is represented as a single input split (for maximum parallelism) or
   several tiles per split as determined by the numSplits value in the
   constructor. Each tile is returned as a tuple.
   
 */
public class RectangularSpaceLoader extends LoadFunc {

    protected RecordReader in = null;
    protected int numSplits;
    
    public RectangularSpaceLoader() {
        this("-1");
    }

    /**
       Allows for user to specify desired number of input splits.
       Note that this is an approximate value since
       tilesPerSplit = numTiles/numSplits will in general have
       some remainder. This remainder will go into an additional
       split.
     */
    public RectangularSpaceLoader(String numSplits) {
        this.numSplits = Integer.parseInt(numSplits);
    }

    @Override
    public Tuple getNext() throws IOException {
        try {

            boolean notDone = in.nextKeyValue();
            if (!notDone) {
                return null;
            }
            
            ArrayWritable point = (ArrayWritable)in.getCurrentValue();
            Writable[] values = point.get();

            Tuple result = TupleFactory.getInstance().newTuple(values.length);
            for (int i = 0; i < values.length; i++) {
                result.set(i, ((DoubleWritable)values[i]).get());
            }
            return result;
        } catch (InterruptedException e) { // Just do what PigStorage() does
            int errCode = 6018;
            String errMsg = "Error while reading input";
            throw new ExecException(errMsg, errCode,
                    PigException.REMOTE_ENVIRONMENT, e);
        }
    }

    @Override
    public InputFormat getInputFormat() {        
        return new SpaceInputFormat(numSplits);
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) {
        in = reader;
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        // Don't let pig combine splits since our input splits are NOT blocks of files
        job.getConfiguration().setBoolean("pig.noSplitCombination", true);
        FileInputFormat.setInputPaths(job, location);
    }

    public class SpaceInputFormat extends FileInputFormat<NullWritable, ArrayWritable> {

        private int numSplits; // Importantly this is the _desired_ number of splits, not actual splits
        
        public SpaceInputFormat(int numSplits) {
            this.numSplits = numSplits;
        }
        
        @Override
        public RecordReader<NullWritable, ArrayWritable> createRecordReader(InputSplit split, TaskAttemptContext context) {
            return new SpaceRecordReader();
        }

        /**
           Most of the work is done here.

           (1) The file containing the spatial specification
           is read and parsed.
           (2) Each dimension in the specification is expanded
           (3) The dimensions are crossed in such a way to create
           a rectangular tiling of the space
           (4) Each tile is represented by its "lower left" coordinates.
           Each such tile is an single input split.
         */
        @Override
        public List<InputSplit> getSplits(JobContext context) throws IOException {
            Path[] list = getInputPaths(context);
            List<InputSplit> result = new ArrayList<InputSplit>();

            // Only handle one spatial specification
            if (list.length == 1) { 
                Path input = list[0];
                ArrayList<ArrayList<Double>> spaceSpec = readSpace(input, context);
                
                ArrayList<ArrayList<Double>> expandedDims = expandDimensions(spaceSpec);
                
                ArrayList<ArrayList<Double>> exploded = rectangularize(expandedDims, expandedDims.size()-1);

                int pointsPerSplit = (numSplits == -1 ? 1 : exploded.size()/numSplits);

                int currentPoint = 0;
                while (currentPoint < exploded.size()) {
                    ArrayList<ArrayList<Double>> splitPoints = new ArrayList<ArrayList<Double>>();
                    while (splitPoints.size() < pointsPerSplit && currentPoint < exploded.size()) {
                        ArrayList<Double> point = exploded.get(currentPoint);
                        splitPoints.add(point);
                        currentPoint += 1;
                    }
                    result.add(new SpaceSplit(splitPoints, spaceSpec.size()));
                }
            }
            return result;
        }

        @Override
        public boolean isSplitable(JobContext context, Path path) {
            return false;
        }

        /**
           Returns the contents of the file specified by path as a string 
         */
        private String readFile(Path path, JobContext context) throws IOException {
            FileSystem fs = path.getFileSystem(context.getConfiguration());
            FSDataInputStream fileIn = fs.open(path);
            
            long sz = fs.getFileStatus(path).getLen();
            byte[] contents = new byte[(int)sz];

            IOUtils.readFully(fileIn, contents, 0, contents.length);

            return new String(contents);
        }
        
        /**
           Read the spatial specification from path. Spatial specification is a csv of the form:

           dimension_1_start,dimension_1_end,dimension_1_num
           dimension_2_start,dimension_2_end,dimension_2_num
           .
           .
           .
           dimension_N_start,dimension_N_end,dimension_N_num

           where each line (a dimension specification) contains
           exactly 3 double values.
         */
        private ArrayList<ArrayList<Double>> readSpace(Path path, JobContext context) throws IOException {

            String spec = readFile(path, context);            
            String[] dims = spec.split("\n");

            int numDimensions = dims.length;
            int requiredFields = 3;           // Number of fields required in each dimensional spec
            
            ArrayList<ArrayList<Double>> spaceSpec = new ArrayList<ArrayList<Double>>(numDimensions);

            for (int i = 0; i < numDimensions; i++) {
                if (dims[i] != null) {
                    String[] fields = dims[i].split(",");
                    if (fields.length == requiredFields) {
                        ArrayList dimSpec = new ArrayList<Double>(requiredFields);
                    
                        for (int j = 0; j < requiredFields; j++) {
                            dimSpec.add(j,Double.parseDouble(fields[j]));
                        }
                        
                        spaceSpec.add(i, dimSpec);
                    }
                }                
            }
            return spaceSpec;
        }

        /**
           Identical to python numpy's "linspace" function and R's seq function.
           <p>
           Eg. linearSpace(0.0, 1.0, 3.0) returns:
           {0.0, 0.5, 1.0}
        */
        private ArrayList<Double> linearSpace(Double start, Double end, Double num) {
            if (start == null || end == null || num == null) {
                return null;
            }

            int resultSize = (num.intValue() > 0 ? num.intValue() : 0);
            ArrayList<Double> expanded = new ArrayList<Double>(resultSize);

            if (resultSize == 1 || start.equals(end)) {
                for (int i = 0; i < resultSize; i++) {
                    expanded.add(i, start);
                }
            } else {
                Double dx = (end - start)/(num-1.0);
                Double x = start;
                for (int i = 0; i < resultSize; i++) {
                    expanded.add(i, x);
                    x += dx;
                }
            }
            return expanded;
        }
        
        /**
           Each dimensional specification is expanded to create a linear space. 
         */
        private ArrayList<ArrayList<Double>> expandDimensions(ArrayList<ArrayList<Double>> spaceSpec) {
            ArrayList<ArrayList<Double>> expanded = new ArrayList<ArrayList<Double>>(spaceSpec.size());
            for (ArrayList<Double> dimSpec : spaceSpec) {
                if (dimSpec.size() == 3) {
                    expanded.add( linearSpace(dimSpec.get(0), dimSpec.get(1), dimSpec.get(2)) );
                }
            }
            return expanded;
        }

        /**
           Create a new set of points by appending each element from d2 to the end of each point in d1. Consider:
           <p>
           d1 = {
             {0},
             {1},
             {2},
             {3}
           };
           d2 = {
             {0,1,2}
           };
           crossDimensions(d1,d2) yields:
           {
             {0,0}, {0,1}, {0,2},
             {1,0}, {1,1}, {1,2},
             {2,0}, {2,1}, {2,2},
             {3,0}, {3,1}, {3,3}
           }
           
         */
        private ArrayList<ArrayList<Double>> crossDimensions(ArrayList<ArrayList<Double>> d1, ArrayList<Double> d2) {
            int numPoints = d1.size()*d2.size();
            ArrayList<ArrayList<Double>> result = new ArrayList<ArrayList<Double>>(numPoints);

            if (numPoints > 0) {
                int numDimensions = d1.get(0).size()+1; // Each coordinate will have this many entries
                int idx = 0;
                for (ArrayList<Double> x : d1) {
                    for (Double y : d2) {
                        ArrayList<Double> point = new ArrayList<Double>(numDimensions);
                        point.addAll(x);
                        point.add(y);
                        result.add(idx, point);
                        idx += 1;
                    }
                }
            }
            return result;
        }

        /**
           Recursively cross dimensions to get a rectangular space.
         */
        private ArrayList<ArrayList<Double>> rectangularize(ArrayList<ArrayList<Double>> dimensions, int idx) {
            if (idx == 0) {
                // Only have a single dimension, just return the input
                return dimensions;  
            } else if (idx == 1) {
                
                // For the first crossing it is necessary to take each element of the first dimension and create
                // a "point" from it. In further crossings elements from the other dimensions will be appended
                // to these points.
                ArrayList<Double> first = dimensions.get(0);
                ArrayList<ArrayList<Double>> firstPoints = new ArrayList<ArrayList<Double>>(first.size());
                for (Double x : first) {
                    ArrayList<Double> asPoint = new ArrayList<Double>();
                    asPoint.add(x);
                    firstPoints.add(asPoint);
                }
                
                return crossDimensions(firstPoints, dimensions.get(1));
            } else {
                return crossDimensions(rectangularize(dimensions, idx-1), dimensions.get(idx));
            }
        }
    }

    /**
       Represents a single rectangular "tile" of the split space. The coordinates are for the bottom left
       of the tile in a right handed coordinate system.
     */
    public static class SpaceSplit extends org.apache.hadoop.mapreduce.InputSplit implements org.apache.hadoop.mapred.InputSplit {
        private ArrayList<ArrayList<Double>> points = null;
        private int numPoints = 0;
        private int dimension = 0;

        public SpaceSplit() {
        }
        
        public SpaceSplit(ArrayList<ArrayList<Double>> points, int dimension) {
            this.points = points;
            this.numPoints = points.size();
            this.dimension = dimension; // dimensionality of each point, uniform
        }

        @Override
        public long getLength() {
            return 0l;
        }

        @Override
        public String[] getLocations() {
            return new String[]{};
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.numPoints = in.readInt();
            this.dimension = in.readInt();
            this.points = new ArrayList<ArrayList<Double>>(numPoints);
            for (int i = 0; i < numPoints; i++) {
                ArrayList<Double> point = new ArrayList<Double>();
                for (int j = 0; j < dimension; j++) {
                    point.add(j, in.readDouble());
                }
                points.add(i, point);
            }
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(numPoints);
            out.writeInt(dimension);
            for (List<Double> point : points) {
                for (Double coordinate : point) {
                    out.writeDouble(coordinate);
                }
            }
        }
        
        public ArrayList<ArrayList<Double>> getPoints() {
            return points;
        }

        public int getNumPoints() {
            return numPoints;
        }

        public int getDimension() {
            return dimension;
        }

        public List<Double> getPoint(int index) {
            return points.get(index);
        }

        public String toString() {
            StringBuffer b = new StringBuffer();
            b.append("SpaceSplit( ");
            b.append("numPoints:");
            b.append(numPoints);
            b.append(", firstPoint:[");
            if (numPoints > 0) {
                List<Double> point = getPoint(0);
                for (int i = 0; i < point.size(); i++) {
                    b.append(point.get(i));
                    if (i < point.size()-1) {
                        b.append(",");
                    }
                }
            }
            b.append("] )");
            return b.toString();
        }
    }
    
                                   
    public class SpaceRecordReader extends RecordReader<NullWritable, ArrayWritable> {
        private SpaceSplit split = null;
        private boolean done = false;
        private int currentPoint;
        
        public SpaceRecordReader() {
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            this.split = (SpaceSplit)split;
            this.currentPoint = 0;
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            done = (currentPoint == split.getNumPoints());
            return !done;
        }

        @Override
        public NullWritable getCurrentKey() throws IOException, InterruptedException {
            return NullWritable.get();
        }

        @Override
        public ArrayWritable getCurrentValue() throws IOException, InterruptedException {            
            DoubleWritable[] coordinates = new DoubleWritable[split.getDimension()];
            List<Double> values = split.getPoint(currentPoint);
            for (int i = 0; i < split.getDimension(); i++) {
                coordinates[i] = new DoubleWritable(values.get(i));
            }
            currentPoint += 1;
            return new ArrayWritable(DoubleWritable.class, coordinates);
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return 0.0f;
        }

        @Override
        public void close() {
        }
    }
}
