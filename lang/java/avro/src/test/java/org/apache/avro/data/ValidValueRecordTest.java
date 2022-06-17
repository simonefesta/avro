package org.apache.avro.data;

import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Before;

import org.junit.Test;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class ValidValueRecordTest {

  private Object expected;
  private Schema.Field field, fieldUnionNull, fieldUnionNotNull;
  private Object value;
  private Schema.Type type;

  public ValidValueRecordTest(Object expected, Schema.Type type, Object value) {
    configure(expected, type, value);

  }

  public void configure(Object expected, Schema.Type type, Object value) {
    this.expected = expected;
    this.type = type;
    this.value = value;
  }

  @Parameterized.Parameters
  public static Collection getParameters() {

    return Arrays.asList(new Object[][] {

        { true, Schema.Type.INT, new Object() }, { true, Schema.Type.NULL, null }, { false, Schema.Type.LONG, null },
        { false, Schema.Type.FLOAT, null }, { false, Schema.Type.DOUBLE, null }, { false, Schema.Type.BYTES, null },
        { false, Schema.Type.BOOLEAN, null }, { false, Schema.Type.STRING, null },

    });
  }

  @Before
  public void setup() {
    field = new Schema.Field("field", Schema.create(type));
    Schema unionNull = Schema.createUnion(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.NULL));
    Schema unionNotNull = Schema.createUnion(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.FLOAT));
    fieldUnionNull = new Schema.Field("fieldUnionNull", unionNull);
    fieldUnionNotNull = new Schema.Field("fieldUnionNotNull", unionNotNull);

  }

  @Test
  public void testValidValue() {
    boolean actual;
    try {
      actual = RecordBuilderBase.isValidValue(field, value);
      Assert.assertEquals(expected, actual);
    } catch (Exception e) {
      Assert.fail("Exception instead a true/false return in validValue");
    }

  }

  @Test
  public void testValidValueUnionNotNull() {
    boolean actual;
    try {
      actual = RecordBuilderBase.isValidValue(fieldUnionNotNull, null);
      Assert.assertFalse(actual);
    } catch (Exception e) {
      Assert.fail("Exception instead a true/false return in UnionNotNull");
    }
  }

  @Test
  public void testValidValueUnionNull() {
    boolean actual;
    try {
      actual = RecordBuilderBase.isValidValue(fieldUnionNull, null);
      Assert.assertTrue(actual);
    } catch (Exception e) {
      Assert.fail("Exception instead a true/false return in UnionNull");
    }
  }

}
