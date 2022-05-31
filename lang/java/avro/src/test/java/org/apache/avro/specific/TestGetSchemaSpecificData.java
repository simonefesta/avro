package org.apache.avro.specific;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;

import org.junit.Assert;
import java.util.Arrays;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Collection;
import java.nio.ByteBuffer;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestGetSchemaSpecificData {

  private java.lang.reflect.Type type;
  private Object expected;

  ArrayList<String> array = new ArrayList<>();
  Map<String, String> map = new HashMap<>(); // il fatto che key = String è 'imposto' da CreateSchema

  public TestGetSchemaSpecificData(Object expected, java.lang.reflect.Type type) {
    configure(expected, type);
  }

  private void configure(Object expected, java.lang.reflect.Type type) {
    this.expected = expected;
    this.type = type;
  }

  @Parameterized.Parameters
  public static Collection<?> getParameter() throws NoSuchFieldException {

    return Arrays.asList(new Object[][] { { Schema.Type.INT, Integer.class }, { Schema.Type.BOOLEAN, Boolean.class },
        { Schema.Type.FLOAT, Float.class }, { Schema.Type.DOUBLE, Double.class }, { Schema.Type.STRING, String.class },
        { AvroTypeException.class, null }, { Schema.Type.BYTES, ByteBuffer.class },
        { Schema.Type.ARRAY, TestGetSchemaSpecificData.class.getDeclaredField("array").getGenericType() }, // tipi di
                                                                                                           // dati
                                                                                                           // complessi
        { Schema.Type.MAP, TestGetSchemaSpecificData.class.getDeclaredField("map").getGenericType() },

        { AvroRuntimeException.class, java.io.IOException.class }, { Schema.Type.NULL, Void.class } });

  }

  @Test
  public void testGetSchemaName() {
    Schema.Type actual;
    try {
      actual = SpecificData.get().getSchema(type).getType();

      Assert.assertEquals(expected, actual);
    } catch (Exception e) {
      Assert.assertEquals(expected, e.getClass());
    }

  }

}
