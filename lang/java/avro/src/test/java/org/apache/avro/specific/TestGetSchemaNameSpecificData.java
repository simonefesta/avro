package org.apache.avro.specific;

import com.sun.org.apache.xml.internal.utils.URI;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;

@RunWith(Parameterized.class)
public class TestGetSchemaNameSpecificData {

  private Object obj;
  private Object expected;

  public TestGetSchemaNameSpecificData(Object expected, Object obj) {
    configure(expected, obj);
  }

  public void configure(Object expected, Object obj) {
    this.expected = expected;
    this.obj = obj;
  }

  @Parameterized.Parameters
  public static Collection<?> getParameter() throws MalformedURLException, URI.MalformedURIException {
    // creating complex types, see :
    // https://avro.apache.org/docs/current/spec.html#schema_primitive

    String strRecord = "{\"namespace\": \"record\",\n" + " \"type\": \"record\",\n" + " \"name\": \"example\",\n"
        + " \"fields\": [\n" + "     {\"name\": \"name\", \"type\": \"string\"}\n" + " ]\n" + "}";
    Schema schemaRecord = new Schema.Parser().parse(strRecord);

    String strEnum = "{\"type\": \"enum\",\n" + "  \"name\": \"enum\",\n"
        + "  \"symbols\" : [\"ARRAY\", \"INT\", \"SPADES\", \"HEARTS\"]\n" + "}";
    Schema schemaEnum = new Schema.Parser().parse(strEnum);

    String strArray = "{\"type\": \"array\",\n" + "\"items\" : \"string\"}";
    Schema schemaArray = new Schema.Parser().parse(strArray);

    String strFixed = "{\"type\": \"fixed\",\n" + " \"size\": 16,\n" + " \"name\": \"fixed\"}";
    Schema schemaFixed = new Schema.Parser().parse(strFixed);
    byte[] supportFixedByte = new byte[16];
    String bigIntegerVal = "2";
    String url = "http://www.example.com/docs/resource1.html";

    return Arrays.asList(new Object[][] { { "null", null }, { "boolean", true }, { "int", 5 }, { "long", 2147483649L },
        { "float", 3.2f }, { "double", 581216732.323433 }, { "bytes", ByteBuffer.allocate(1) }, { "string", "s" },
        // complex type
        { "record.example", new GenericData.Record(schemaRecord) },
        { "enum", new GenericData.EnumSymbol(schemaEnum, strEnum) },
        { "array", new GenericData.Array<>(2, schemaArray) }, { "map", new HashMap<>() },
        { "fixed", new GenericData.Fixed(schemaFixed, supportFixedByte) }, { null, new Object() },
        // coverage
        { "string", new BigDecimal(2) }, { "string", new File("src/test") },
        { "string", new BigInteger(bigIntegerVal) }, { "string", new URL(url) },

    });

  }

  @Test
  public void testGetSchemaName() {
    String actual;
    try {
      actual = SpecificData.get().getSchemaName(obj);
    } catch (Exception e) {
      actual = null;
    }
    Assert.assertEquals(expected, actual);
  }
}
