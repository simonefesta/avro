package org.apache.avro.specific;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
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

  private void configure(Object expected, Object obj) {
    this.expected = expected;
    this.obj = obj;
  }

  @Parameterized.Parameters
  public static Collection<?> getParameter() {

    return Arrays.asList(new Object[][] { { "int", 5 }, { "null", null }, { "map", new HashMap<>() },
        { "boolean", true }, { null, new Object() }, { "float", 3.2f },
        // coverage
        { "string", new BigDecimal(2) }

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
