package org.apache.avro.io;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class CompareBytesTest {

  private byte[] b1, b2; // bytes to compare
  private int s1, s2; // start of byte to compare
  private int l1, l2; // lenght of byte[]
  private int expected;

  public CompareBytesTest(int expected, byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    configure(expected, b1, s1, l1, b2, s2, l2);
  }

  private void configure(int expected, byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    this.expected = expected;
    this.b1 = b1;
    this.b2 = b2;
    this.s1 = s1;
    this.s2 = s2;
    this.l1 = l1;
    this.l2 = l2;
  }

  @Parameterized.Parameters
  public static Collection<?> getParameter() {
    return Arrays.asList(new Object[][] {
        // category partition
        // expected //b1 //s1 //l1 //b2 //s2 //l2
        { 0, new byte[0], 0, 0, new byte[0], 0, 0 }, { 0, new byte[1], 0, -1, new byte[1], 0, -1 },
        { -1, "testb1".getBytes(), 1, 6, "testb2".getBytes(), 1, 6 },

    });

  }

  @Test
  public void TestCompare() {
    int actual;
    actual = BinaryData.compareBytes(b1, s1, l1, b2, s2, l2);
    Assert.assertEquals(expected, actual);

  }

}
