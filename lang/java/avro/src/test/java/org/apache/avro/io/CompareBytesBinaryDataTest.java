package org.apache.avro.io;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class CompareBytesBinaryDataTest {

  private byte[] b1, b2; // bytes to compare
  private int s1, s2; // start of byte to compare
  private int l1, l2; // lenght of byte[]
  private int expected;
  public static final int ERRORVALUE = -100; // CompareBytes ritorna un int. Definiamo ERRORVALUE come il valore di int
                                             // che definisce l'errore nell'esecuzione del metodo.
  // Se da un test che reputiamo corretto ci aspettiamo un risultato uguale
  // all'int scelto per errorvalue, devo cambiare errorvalue, che viene usato solo
  // per expected errors.

  public CompareBytesBinaryDataTest(int expected, byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    configure(expected, b1, s1, l1, b2, s2, l2);
  }

  public void configure(int expected, byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
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
        { 0, new byte[0], 0, 0, new byte[0], 0, 0 }, { 0, new byte[0], -1, -1, new byte[0], -1, -1 },
        { -1, "testb1".getBytes(), 0, 6, "testb2".getBytes(), 0, 6 },
        { ERRORVALUE, "testb1".getBytes(), 7, 6, "testb2".getBytes(), 7, 6 },
        { ERRORVALUE, "testb2".getBytes(), 0, 6, null, 0, 1 }, { ERRORVALUE, null, 1, 1, null, 5, 6 },

        // coverage
        { 1, "testb1".getBytes(), 0, 6, "testb0".getBytes(), 0, 6 }, // questo nuovo 'test' copre il caso in cui b1 sia
                                                                     // più grande di
        // b2, infatti nei test precedenti mi aspettavo sempre expected
        // <=0
        // pit
        { -1, "testb1".getBytes(), 1, 4, "testb2".getBytes(), 1, 5 }, // fix mutation on return
        { -1, "testc1".getBytes(), 0, 6, "testc15".getBytes(), 0, 7 }, // fix mutation on for cycle #1
        { 1, "testc15".getBytes(), 0, 7, "testc1".getBytes(), 0, 6 } // fix mutation on for cycle #2

    });

  }

  @Test
  public void testCompareBytes() {
    int actual = ERRORVALUE;
    try {
      actual = BinaryData.compareBytes(b1, s1, l1, b2, s2, l2);
      Assert.assertEquals(expected, actual);
    } catch (Exception e) {
      Assert.assertEquals(expected, actual);
    }

  }

}
