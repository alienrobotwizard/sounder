package sounder.pig.json;

import static org.junit.Assert.*;

import org.junit.Ignore;
import org.junit.Test;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.BagFactory;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class ToJsonTest {

    @Test
    public void testUDF() throws Exception {
        
        ToJson udf = new ToJson();

        String schemaString = "int_field:int, long_field:long, float_field:float, double_field:double, string_field:chararray, bag_field:bag{t:tuple(bag_element:chararray)}";
        Schema schema = Utils.getSchemaFromString(schemaString);
        udf.outputSchema(schema);  // Pull the input schema
        
        TupleFactory tf = TupleFactory.getInstance();
        Tuple input = tf.newTuple(6);

        input.set(0, 1);             // int_field
        input.set(1, 1L);            // long_field
        input.set(2, 1.0f);          // float_field
        input.set(3, 1.0);           // double_field
        input.set(4, "Some string"); // string_field

        DataBag bagField = BagFactory.getInstance().newDefaultBag();
        Tuple t = tf.newTuple(1);
        t.set(0, "I'm in a bag!");
        bagField.add(t);
        
        input.set(5, bagField);      // bag_field

        String expectedResult = "{\"int_field\":1,\"long_field\":1,\"float_field\":1.0,\"double_field\":1.0,\"string_field\":\"Some string\",\"bag_field\":[{\"bag_element\":\"I'm in a bag!\"}]}";
        String output = udf.exec(input);
        System.out.println("Got ["+output+"] from ToJson udf");
        assertEquals(expectedResult, output);
    }
}
