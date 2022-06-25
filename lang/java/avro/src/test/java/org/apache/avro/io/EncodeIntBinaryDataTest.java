package org.apache.avro.io;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Enclosed.class)
public class EncodeIntBinaryDataTest {
  @RunWith(Parameterized.class)
  public static class EncodeIntBinaryDataTest1 {

    private byte[] buf; // dove scrivo
    private int n, pos; // quanto scrivo, e partendo da dove
    private int expected; // quanto ho scritto effettivamente

    public EncodeIntBinaryDataTest1(int expected, int n, byte[] buf, int pos) {
      configure(expected, n, buf, pos);
    }

    public void configure(int expected, int n, byte[] buf, int pos) {
      this.expected = expected;
      this.n = n;
      this.buf = buf;
      this.pos = pos;
    }

    @Parameterized.Parameters
    public static Collection<?> getParameter() {
      return Arrays.asList(new Object[][] {
          // category partition
          // expected //n //buf //pos
          { 0, -1, new byte[5], -1 }, { 0, 0, new byte[0], 0 }, { 1, 1, new byte[1], 0 }, { 0, 1, null, 1 },
          { 1, -1, new byte[5], 1 }, { 0, 1, new byte[5], 6 }, { 0, 128, new byte[1], 0 },
          // coverage
          { 2, 64, new byte[5], 0 }, { 3, 131072, new byte[5], 0 }, { 4, 1048576, new byte[5], 0 },
          { 5, 134217728, new byte[5], 0 },
          // pitest
          { 2, 8191, new byte[10], 0 }, { 3, 1048448, new byte[10], 0 }, { 4, 134201344, new byte[10], 0 },

      });

    }

    @Test
    public void testEncodeInt() {
      int actual;
      try {
        actual = BinaryData.encodeInt(n, buf, pos);
      } catch (Exception e) {
        actual = 0;
      }

      Assert.assertEquals(expected, actual);

    }
  }

  @RunWith(JUnit4.class)
  public static class EncodeIntBinaryDataTest2 {

    @Test
    public void increasePIT() {
      byte[] actualBuf = new byte[5];
      BinaryData.encodeInt(Integer.MAX_VALUE, actualBuf, 0);
      byte[] bufExpected = new byte[] { -2, -1, -1, -1, 15 };
      String actualBufToString = Arrays.toString(actualBuf);
      String bufExpectedToString = Arrays.toString(bufExpected);
      Assert.assertEquals(actualBufToString, bufExpectedToString);
    }

  }

}
