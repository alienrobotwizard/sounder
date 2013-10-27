package sounder.pig.tuple;

import java.util.List;
import java.util.ArrayList; // maybe use something else; LinkedList?
import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.BagFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
   (w1,w2,...,wN) * (f1,f2,...,fN) => {(w1,f1),(w2,f2),...,(wN,fN)}
 */
public class Zip extends EvalFunc<DataBag> {

    protected static TupleFactory tf = TupleFactory.getInstance();
    protected static BagFactory bf = BagFactory.getInstance();

    @Override
    public Schema outputSchema(Schema input) {

        try {
            List<Schema.FieldSchema> outputTupleFields = new ArrayList<Schema.FieldSchema>(input.size()+1);
        
            for (Schema.FieldSchema fieldSchema : input.getFields()) {
                // Assumption is that fieldSchema is itself a tuple schema and all the
                // fields have the same type as the first field
                Schema.FieldSchema s = fieldSchema.schema.getField(0);
                outputTupleFields.add(s);
            }
            outputTupleFields.add(new Schema.FieldSchema("order", DataType.INTEGER)); // Important to maintain position
            
            Schema.FieldSchema tupleSchema = new Schema.FieldSchema("t", new Schema(outputTupleFields), DataType.TUPLE);
            Schema.FieldSchema bagSchema = new Schema.FieldSchema("b", new Schema(tupleSchema), DataType.BAG);
            return new Schema(bagSchema);
        } catch (FrontendException e) {
            e.printStackTrace();
        }
        
        return null;
    }
    
    @Override
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() < 2)
            return null;

        Tuple first = (Tuple)input.get(0);

        List<Tuple> results = initialize(first);
        
        for (int i = 1; i < input.size(); i++) {
            Tuple next = (Tuple)input.get(i);
            concatTuple(results, next);
        }
        
        return toBag(results);
    }

    private List<Tuple> initialize(Tuple first) throws ExecException {
        List<Tuple> results = new ArrayList<Tuple>(first.size());
        for (int i = 0; i < first.size(); i++) {
            results.add(tf.newTuple(first.get(i)));
        }
        return results;
    }

    private void concatTuple(List<Tuple> tuples, Tuple input) throws ExecException {
        for (int i = 0; i < tuples.size(); i++) {
            Tuple t = tuples.get(i);
            if (i < input.size()) {
                t.append(input.get(i));
            } else {
                t.append(null);
            }
        }        
    }

    private DataBag toBag(List<Tuple> tuples) {
        DataBag result = bf.newDefaultBag();
        int position = 0;
        for (Tuple t : tuples) {
            t.append(position);
            result.add(t);
            position++;
        }
        return result;
    }
    
}
