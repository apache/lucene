package org.apache.lucene.util;

import org.apache.lucene.tests.util.LuceneTestCase;

import java.util.Arrays;
import java.util.Random;

import static org.apache.lucene.util.Simple64.BITS;
import static org.apache.lucene.util.Simple64.COUNTS;
import static org.apache.lucene.util.Simple64.MASKS;

public class TestSimple64 extends LuceneTestCase {

  /** Verify selector table invariants. */
  public void testSelectorTable() {
    for (int s = 0; s < 14; s++) {
      // bits must fit inside the 60 data bits
      assertTrue("selector " + s + ": COUNTS[s]*BITS[s] <= 60",
          (long) COUNTS[s] * BITS[s] <= 60);
      // max value must be representable as a non-negative int
      assertTrue("selector " + s + ": MASKS[s] <= Integer.MAX_VALUE",
          MASKS[s] <= Integer.MAX_VALUE);
    }
    // selector 13 must cover Integer.MAX_VALUE
    assertTrue("selector 13 covers Integer.MAX_VALUE",
        MASKS[13] >= Integer.MAX_VALUE);
  }

  /** Each selector should be chosen when values exactly hit its upper bound. */
  public void testSelectorBoundaries() {
    for (int s = 0; s < 14; s++) {
      int maxVal = (int) MASKS[s];
      int[] input = new int[COUNTS[s]];
      Arrays.fill(input, maxVal);
      long word = Simple64.encode(input, 0, input.length);
      assertEquals("selector for bits=" + BITS[s], s, (int)(word >>> 60));
    }
  }

  public void testRoundtripSmallValues() {
    // value=2 needs 2 bits → selector 1 (30 × 2 bits)
    int[] input = new int[30];
    Arrays.fill(input, 2);
    long word = Simple64.encode(input, 0, 30);
    assertEquals("selector=1 for value=2", 1, (int)(word >>> 60));
    int[] out = new int[30];
    assertEquals("decoded count", 30, Simple64.decode(word, out, 0));
    assertArrayEquals("decoded values", input, out, 30);
  }

  public void testRoundtripAllSelectors() {
    for (int s = 0; s < 14; s++) {
      int count  = COUNTS[s];
      int maxVal = (int) MASKS[s];
      int[] input = new int[count];
      Arrays.fill(input, maxVal);
      long word = Simple64.pack(s, input, 0, count);   // force this selector
      int[] out = new int[count];
      assertEquals("selector " + s + " decode count", count, Simple64.decode(word, out, 0));
      assertArrayEquals("selector " + s + " values", input, out, count);
    }
  }

  /** Selector 13 must handle Integer.MAX_VALUE. */
  public void testIntegerMaxValue() {
    int[] input = {Integer.MAX_VALUE};
    long word = Simple64.encode(input, 0, 1);
    assertEquals("selector=13 for Integer.MAX_VALUE", 13, (int)(word >>> 60));
    int[] out = new int[1];
    Simple64.decode(word, out, 0);
    assertEquals("decoded Integer.MAX_VALUE", Integer.MAX_VALUE, out[0]);
  }

  public void testEncodeAll() {
    int[] input = new int[35];
    Random rng = new Random(42);
    for (int i = 0; i < 35; i++) input[i] = rng.nextInt(10) + 1;
    long[] longs = new long[35];
    int numLongs = Simple64.encodeAll(input, 0, 35, longs, 0);
    int[] decoded = new int[35];
    Simple64.decodeAll(longs, 0, decoded, 0, 35);
    assertArrayEquals("encodeAll roundtrip", input, decoded, 35);
  }

  public void testSuffixLengths() {
    int[] input = {3,1,4,1,5,9,2,6,5,3,5,8,9,7,9,3,2,3,8,4,6,2,6,4,3,3,8,3};
    int len = input.length;
    long[] longs = new long[len];
    int numLongs = Simple64.encodeAll(input, 0, len, longs, 0);
    int[] decoded = new int[len];
    Simple64.decodeAll(longs, 0, decoded, 0, len);
    assertArrayEquals("suffix roundtrip", input, decoded, len);
  }

  public void testNegativeValueThrows() {
    boolean threw = false;
    try { Simple64.encode(new int[]{1, -1, 3}, 0, 3); }
    catch (IllegalArgumentException e) { threw = true; }
    assertTrue("negative value throws", threw);
  }

  public void testInvalidSelectorThrows() {
    long badWord = 15L << 60;
    boolean threw = false;
    try { Simple64.decode(badWord, new int[10], 0); } catch (IllegalArgumentException e) { threw = true; }
    assertTrue("invalid selector throws on decode", threw);
    threw = false;
    try { Simple64.count(badWord); } catch (IllegalArgumentException e) { threw = true; }
    assertTrue("invalid selector throws on count", threw);
  }

  public void testCountWithoutDecode() {
    // value=63 needs 6 bits → selector 5 (10 × 6 bits)
    int[] input = new int[10];
    Arrays.fill(input, 63);
    long word = Simple64.encode(input, 0, 10);
    assertEquals("selector=5 for value=63", 5, (int)(word >>> 60));
    assertEquals("count()==10", 10, Simple64.count(word));
  }

  public void testFuzz() {
    Random rng = new Random(12345);
    int rounds = 20_000;
    int errors = 0;
    for (int r = 0; r < rounds; r++) {
      int len = rng.nextInt(60) + 1;
      // random bit-width 1..31 to exercise all selectors including 13
      int maxBits = rng.nextInt(31) + 1;
      int maxVal  = (maxBits == 31) ? Integer.MAX_VALUE : (1 << maxBits) - 1;
      int[] input = new int[len];
      for (int i = 0; i < len; i++)
        input[i] = (int)(((long) rng.nextInt() & 0x7FFFFFFFL) % ((long) maxVal + 1));
      long[] longs = new long[len + 1];
      Simple64.encodeAll(input, 0, len, longs, 0);
      int[] decoded = new int[len];
      Simple64.decodeAll(longs, 0, decoded, 0, len);
      if (!Arrays.equals(input, decoded)) errors++;
    }
    assertEquals("fuzz errors", 0, errors);
  }

  private void assertArrayEquals(String msg, int[] expected, int[] actual, int len) {
    assertTrue(msg, Arrays.equals(Arrays.copyOf(expected, len), Arrays.copyOf(actual, len)));
  }
}
