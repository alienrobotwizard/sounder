package sounder.pig.points;

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

public class KDTreeTest {

    @Test
    public void testUDF() throws Exception {
        KDTree udf = new KDTree();
    
        TupleFactory tf = TupleFactory.getInstance();
        DataBag input = BagFactory.getInstance().newDefaultBag();

        Tuple p1 = tf.newTuple(2);
        Tuple p2 = tf.newTuple(2);
        Tuple p3 = tf.newTuple(2);
        Tuple p4 = tf.newTuple(2);
        Tuple p5 = tf.newTuple(2);
        Tuple p6 = tf.newTuple(2);

        p1.set(0, "p1");
        p1.set(1, tf.newTuple(Arrays.asList(7.0d, 2.0d)));

        p2.set(0, "p2");
        p2.set(1, tf.newTuple(Arrays.asList(9.0d, 6.0d)));

        p3.set(0, "p3");
        p3.set(1, tf.newTuple(Arrays.asList(8.0d, 1.0d)));

        p4.set(0, "p4");
        p4.set(1, tf.newTuple(Arrays.asList(5.0d, 4.0d)));

        p5.set(0, "p5");
        p5.set(1, tf.newTuple(Arrays.asList(4.0d, 7.0d)));

        p6.set(0, "p6");
        p6.set(1, tf.newTuple(Arrays.asList(2.0d, 3.0d)));

        input.add(p1);
        input.add(p2);
        input.add(p3);
        input.add(p4);
        input.add(p5);
        input.add(p6);

        DataBag result = udf.exec(tf.newTuple(input));

        // {(p4,1,0,p1,p6,(5.0,4.0)),(p1,0,1,p2,p3,(7.0,2.0)),(p2,0,0,,,(9.0,6.0)),(p3,0,0,,,(8.0,1.0)),(p6,0,1,p5,,(2.0,3.0)),(p5,0,0,,,(4.0,7.0))}

        /**
                  p4
                /    \
               p6    p1
                \    / \
                p5  p3  p2
         */
        assertEquals(result.size(), 6l);
        for (Tuple point : result) {
            assertEquals(point.size(), 6);
            assertFalse(point.isNull(0));
            assertFalse(point.isNull(1));
            assertFalse(point.isNull(2));
            assertFalse(point.isNull(5));
            String pointId = (String)point.get(0);
            if (pointId.equals("p1")) {
                assertEquals(point.get(3), "p2");
                assertEquals(point.get(4), "p3");
            } else if (pointId.equals("p2") || pointId.equals("p3") || pointId.equals("p5")) {
                assertTrue(point.isNull(3));
                assertTrue(point.isNull(4));
            } else if (pointId.equals("p4")) {
                assertEquals(point.get(3), "p1");
                assertEquals(point.get(4), "p6");
            } else if (pointId.equals("p6")) {
                assertEquals(point.get(3), "p5");
                assertTrue(point.isNull(4));
            }
        }
    }
}
