package sounder.pig.tuple;

import static org.junit.Assert.*;

import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.BagFactory;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class ZipTest {

    @Test
    public void testUDF() throws Exception {
        Zip udf = new Zip();
    
        TupleFactory tf = TupleFactory.getInstance();

        Tuple t1 = tf.newTuple(Arrays.asList("w1","w2","w3","w4"));
        Tuple t2 = tf.newTuple(Arrays.asList("f1","f2","f3"));
        Tuple input = tf.newTuple(Arrays.asList(t1,t2));
       
        DataBag result = udf.exec(input);
        // {(w1,f1,0),(w2,f2,1),(w3,f3,2),(w4,null,3)}        

        for (Tuple t : result) {
            assertEquals(t.size(), 3);
            assertFalse(t.isNull(0));
            assertFalse(t.isNull(2));
            if (t.get(2).equals(3)) { // (w4,null,3)
                assertTrue(t.isNull(1));
            }
        }
    }
}
