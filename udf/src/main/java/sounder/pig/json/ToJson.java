package sounder.pig.json;

import java.io.StringWriter;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Map;
    
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

/**
   Creates a JSON string representing the passed in tuple. To do this
   the schema the tuple is given in the pig script is used. Clearly, if
   the schema for the tuple isn't well defined in the script, and default
   field names and types ($1, $2, etc as field names and bytearrays as field
   types) then the results of ToJson won't be well defined either. The process
   for jsonizing an arbitrary tuple is as follows:
   <p>
   (1) A new json object is started to represent the tuple. It has no name since
   the only name associated with it is that of the relation itself.
   <p>
   (2) As fields are encountered they are dealt with depending on their type.
    - Strings and Numbers are simply written as fields in the json. The name is
      the name given by the pig schema and the value is the value pulled from
      the tuple itself, cast to the appropriate type.
    - DataBags become arrays where the name is the name given by the pig schema.
      <b>NOTE</b>: It is assumed that each tuple in the databag has the same schema.
      In this case the name of the tuple itself inside the databag schema is
      irrelevant and ignored. Consider the following schema:
      "my_bag:bag{t:tuple(some_field:chararray)}" The name of the tuple, t, isn't
      providing any new information. Hence, the serialization of a flat databag
      will be a json array containing objects.
    - DataByteArrays are not dealt with and simply skipped at this time.
    - Maps are not dealt with and simply skipped at this time.
    - Tuples have two cases. See the note about DataBags above for the first case.
      In the second case, they're encountered inside an object and the name is
      important. Unfortunately, the second case isn't dealt with at the moment.

 */
public class ToJson extends EvalFunc<String> {
    private transient static Logger log = LoggerFactory.getLogger(ToJson.class);
    public static final String UDFCONTEXT_SCHEMA_KEY = "json.input_field_schema";
    private static final String defaultContext = "default_context";
    private String context;
    private JsonFactory jsonFactory = new JsonFactory();

    public ToJson() {
        this(defaultContext);
    }

    /**
     * Pass in a unique value for the script for the context, e.g. a relation name.
     * @param context
     */
    public ToJson(String context) {
        this.context = context;
    }

    public String exec(Tuple input) throws IOException {
        UDFContext context = UDFContext.getUDFContext();
        Properties property = context.getUDFProperties(ToJson.class);
        String schemaString = property.getProperty(getSchemaKey());

        try {
            Schema schema = Utils.getSchemaFromString(schemaString);
            List<Schema.FieldSchema> fields = schema.getFields();

            StringWriter stringer = new StringWriter();
            JsonGenerator jsonator = jsonFactory.createJsonGenerator(stringer);

            jsonator.writeStartObject();
            // Use the pig schema to create json object from input tuple
            for (int i = 0; i < input.size(); i++) {
                Object o = input.get(i);
                Schema.FieldSchema fieldSchema = fields.get(i);
                jsonize(jsonator, fieldSchema, o);
            }
            jsonator.writeEndObject();
            jsonator.close();
            return stringer.toString();
        } catch (Exception e) {
            e.printStackTrace();
            // cant parse schema
            return null;
        }
    }

    private void jsonize(JsonGenerator jsonator, Schema.FieldSchema fieldSchema, Object thing) throws IOException {
        if (thing instanceof String) {
            jsonator.writeStringField(fieldSchema.alias, (String)thing);
        } else if (thing instanceof Integer) {
            jsonator.writeNumberField(fieldSchema.alias, (Integer)thing);
        } else if (thing instanceof Long) {
            jsonator.writeNumberField(fieldSchema.alias, (Long)thing);
        } else if (thing instanceof Float) {
            jsonator.writeNumberField(fieldSchema.alias, (Float)thing);
        } else if (thing instanceof Double) {
            jsonator.writeNumberField(fieldSchema.alias, (Double)thing);
        } else if (thing instanceof DataBag) {
            jsonator.writeArrayFieldStart(fieldSchema.alias);
            List<Schema.FieldSchema> tupleFields = fieldSchema.schema.getFields();
            for (Tuple t : (DataBag)thing) {
                jsonize(jsonator, tupleFields.get(0), t); 
            }
            jsonator.writeEndArray();
        } else if (thing instanceof Map) {
            // best of luck :)
        } else if (thing instanceof DataByteArray) {
            // nope.
        } else if (thing instanceof Tuple) {
            List<Schema.FieldSchema> fields = fieldSchema.schema.getFields();
            jsonator.writeStartObject();
            for (int i = 0; i < ((Tuple)thing).size(); i++) {
                Object o = ((Tuple)thing).get(i);
                Schema.FieldSchema field = fields.get(i);
                jsonize(jsonator, field, o);
            }
            jsonator.writeEndObject();
        }
    }
    
    public Schema outputSchema(Schema input) {
        
        UDFContext context = UDFContext.getUDFContext();
        Properties property = context.getUDFProperties(ToJson.class);

        String stringSchema = input.toString();

        if (stringSchema.startsWith("{(")) {
            property.setProperty(getSchemaKey(), stringSchema.substring(2,stringSchema.length()-2));
        } else if (stringSchema.startsWith("{")) {
            property.setProperty(getSchemaKey(), stringSchema.substring(1,stringSchema.length()-1));
        } else {
            property.setProperty(getSchemaKey(), stringSchema);
        }
        return super.outputSchema(input);
    }

    private String getSchemaKey() {
        return UDFCONTEXT_SCHEMA_KEY + '.' + context;
    }
}
