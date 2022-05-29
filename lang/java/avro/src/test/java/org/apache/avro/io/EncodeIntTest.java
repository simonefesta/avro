package org.apache.avro.io;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class EncodeIntTest {

  private byte[] buf; // dove scrivo
  private int n, pos; // quanto scrivo, e partendo da dove
  private int expected; // quanto ho scritto effettivamente

  public EncodeIntTest(int expected, int n, byte[] buf, int pos) {
    configure(expected, n, buf, pos);
  }

  private void configure(int expected, int n, byte[] buf, int pos) {
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
        { 1, 1, new byte[5], 1 }, { 0, -1, new byte[5], -1 }, { 0, 0, new byte[0], 0 }

    });

  }

  @Test
  public void TestCompare() {
    int actual;
    try {
      actual = BinaryData.encodeInt(n, buf, pos);

      System.out.println(actual);
    } catch (Exception e) {
      actual = 0;
    }

    Assert.assertEquals(expected, actual);

  }

}
