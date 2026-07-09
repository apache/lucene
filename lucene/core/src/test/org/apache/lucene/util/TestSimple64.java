/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.util;

import java.util.Arrays;
import java.util.Random;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestSimple64 extends LuceneTestCase {

  /** Verify selector table invariants. */
  public void testSelectorTable() {
    for (int s = 0; s < Simple64.numSelectors(); s++) {
      // bits must fit inside the 60 data bits
      assertTrue(
          "selector " + s + ": COUNTS[s]*BITS[s] <= 60",
          (long) Simple64.selectorCount(s) * Simple64.selectorBits(s) <= 60);
      // max value must be representable as a non-negative int
      assertTrue(
          "selector " + s + ": MASKS[s] <= Integer.MAX_VALUE",
          Simple64.selectorMask(s) <= Integer.MAX_VALUE);
    }
    // selector 13 must cover Integer.MAX_VALUE
    assertTrue(
        "selector 13 covers Integer.MAX_VALUE", Simple64.selectorMask(13) >= Integer.MAX_VALUE);
  }

  /** Each selector should be chosen when values exactly hit its upper bound. */
  public void testSelectorBoundaries() {
    for (int s = 0; s < Simple64.numSelectors(); s++) {
      int maxVal = (int) Simple64.selectorMask(s);
      int[] input = new int[Simple64.selectorCount(s)];
      Arrays.fill(input, maxVal);
      long word = Simple64.encodeOneLong(input, 0, input.length);
      assertEquals("selector for bits=" + Simple64.selectorBits(s), s, (int) (word >>> 60));
    }
  }

  public void testRoundtripSmallValues() {
    // value=2 needs 2 bits → selector 1 (30 × 2 bits)
    int[] input = new int[30];
    Arrays.fill(input, 2);
    long word = Simple64.encodeOneLong(input, 0, 30);
    assertEquals("selector=1 for value=2", 1, (int) (word >>> 60));
    int[] out = new int[30];
    assertEquals("decoded count", 30, Simple64.decodeOneLong(word, out, 0));
    assertArrayEquals("decoded values", input, out);
  }

  public void testRoundtripAllSelectors() {
    for (int s = 0; s < Simple64.numSelectors(); s++) {
      int count = Simple64.selectorCount(s);
      int maxVal = (int) Simple64.selectorMask(s);
      int[] input = new int[count];
      Arrays.fill(input, maxVal);
      long word = Simple64.encodeOneLong(input, 0, count);
      assertEquals("selector " + s, s, (int) (word >>> 60));
      int[] out = new int[count];
      assertEquals("selector " + s + " decode count", count, Simple64.decodeOneLong(word, out, 0));
      assertArrayEquals("selector " + s + " values", input, out);
    }
  }

  public void testFastPathSelectors0To3() {
    Random rng = new Random(7);
    for (int s = 0; s <= 3; s++) {
      int[] input = new int[Simple64.selectorCount(s)];
      int maxVal = (int) Simple64.selectorMask(s);
      for (int i = 0; i < input.length; i++) {
        input[i] = rng.nextInt(maxVal + 1);
      }
      input[0] = maxVal;
      long word = Simple64.encodeOneLong(input, 0, input.length);
      assertEquals("selector " + s + " fast path", s, (int) (word >>> 60));
      int[] out = new int[input.length];
      Simple64.decodeOneLong(word, out, 0);
      assertArrayEquals("selector " + s + " values", input, out);
    }
  }

  /** Selector 13 must handle Integer.MAX_VALUE. */
  public void testIntegerMaxValue() {
    int[] input = {Integer.MAX_VALUE};
    long word = Simple64.encodeOneLong(input, 0, 1);
    assertEquals("selector=13 for Integer.MAX_VALUE", 13, (int) (word >>> 60));
    int[] out = new int[1];
    Simple64.decodeOneLong(word, out, 0);
    assertEquals("decoded Integer.MAX_VALUE", Integer.MAX_VALUE, out[0]);
  }

  public void testIntegerMaxValueArray() {
    int valueCount = 2 + random().nextInt(127);
    int[] input = new int[valueCount];
    Arrays.fill(input, Integer.MAX_VALUE);
    long[] longs = new long[input.length];
    int numLongs = Simple64.encodeAll(input, 0, input.length, longs, 0);
    assertTrue("Simple64 should not expand the number of values", numLongs <= input.length);

    int[] decoded = new int[input.length];
    assertEquals(numLongs, Simple64.decodeAll(longs, 0, decoded, 0, input.length));
    assertArrayEquals(input, decoded);
  }

  public void testZeroValuesAreAllowed() {
    int[] input = {3, 0, 5, 0, 8};
    long[] longs = new long[input.length];
    int numLongs = Simple64.encodeAll(input, 0, input.length, longs, 0);
    assertTrue("Simple64 should not expand the number of values", numLongs <= input.length);

    int[] decoded = new int[input.length];
    assertEquals(numLongs, Simple64.decodeAll(longs, 0, decoded, 0, input.length));
    assertArrayEquals(input, decoded);
  }

  public void testAllZeroValues() {
    int[] input = new int[Simple64.MAX_VALUES_PER_LONG];
    long word = Simple64.encodeOneLong(input, 0, input.length);
    assertEquals(0, (int) (word >>> 60));
    assertEquals(0L, word);

    int[] decoded = new int[input.length];
    assertEquals(input.length, Simple64.decodeOneLong(word, decoded, 0));
    assertArrayEquals(input, decoded);
  }

  public void testEncodeOneLongAll() {
    int[] input = new int[35];
    Random rng = new Random(42);
    for (int i = 0; i < 35; i++) {
      input[i] = rng.nextInt(10) + 1;
    }
    long[] longs = new long[35];
    int numLongs = Simple64.encodeAll(input, 0, 35, longs, 0);
    if (numLongs < longs.length) {
      assert longs[numLongs] == 0;
    }
    int[] decoded = new int[35];
    Simple64.decodeAll(longs, 0, decoded, 0, 35);
    assertArrayEquals("encodeAll roundtrip", input, decoded);
  }

  public void testEncodeOneLongAllAndDecodeOneLongAllWithOffsets() {
    int[] input = {7, 2, 6, 6, 5, 1, 8, 2, 7, 2, 3, 4, 5, 6, 7, 8, 9};
    long[] longs = new long[input.length + 4];
    Arrays.fill(longs, -1L);
    int numLongs = Simple64.encodeAll(input, 0, input.length, longs, 2);
    assertEquals(-1L, longs[1]);
    if (numLongs + 2 < longs.length) {
      assertEquals(-1L, longs[numLongs + 2]);
    }

    int[] decoded = new int[input.length + 6];
    Arrays.fill(decoded, -1);
    int consumed = Simple64.decodeAll(longs, 2, decoded, 3, input.length);
    assertEquals(numLongs, consumed);
    assertEquals(-1, decoded[2]);
    assertEquals(-1, decoded[input.length + 3]);
    assertArrayEquals(input, Arrays.copyOfRange(decoded, 3, 3 + input.length));
  }

  public void testDecodeOneLongWithOutOffset() {
    int[] input = new int[Simple64.selectorCount(3)];
    Arrays.fill(input, 8);
    long word = Simple64.encodeOneLong(input, 0, input.length);
    int[] out = new int[input.length + 4];
    Arrays.fill(out, -1);
    assertEquals(input.length, Simple64.decodeOneLong(word, out, 2));
    assertEquals(-1, out[1]);
    assertEquals(-1, out[input.length + 2]);
    assertArrayEquals(input, Arrays.copyOfRange(out, 2, 2 + input.length));
  }

  public void testSuffixLengths() {
    int[] input = {
      3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5, 8, 9, 7, 9, 3, 2, 3, 8, 4, 6, 2, 6, 4, 3, 3, 8, 3
    };
    int len = input.length;
    long[] longs = new long[len];
    int numLongs = Simple64.encodeAll(input, 0, len, longs, 0);
    if (numLongs < longs.length) {
      assert longs[numLongs] == 0;
    }
    int[] decoded = new int[len];
    Simple64.decodeAll(longs, 0, decoded, 0, len);
    assertArrayEquals("suffix roundtrip", input, decoded);
  }

  public void testCountWithoutDecodeOneLong() {
    // value=63 needs 6 bits → selector 5 (10 × 6 bits)
    int[] input = new int[10];
    Arrays.fill(input, 63);
    long word = Simple64.encodeOneLong(input, 0, 10);
    assertEquals("selector=5 for value=63", 5, (int) (word >>> 60));
    assertEquals("count()==10", 10, Simple64.count(word));
  }

  public void testInvalidInputs() {
    expectThrows(IllegalArgumentException.class, () -> Simple64.encodeOneLong(new int[1], 0, 0));
    expectThrows(
        IllegalArgumentException.class, () -> Simple64.encodeOneLong(new int[] {-1}, 0, 1));
    expectThrows(
        IllegalArgumentException.class,
        () -> Simple64.encodeAll(new int[] {1, -1, 2}, 0, 3, new long[3], 0));

    long invalidSelector = 14L << 60;
    expectThrows(IllegalArgumentException.class, () -> Simple64.count(invalidSelector));
    expectThrows(
        IllegalArgumentException.class,
        () -> Simple64.decodeOneLong(invalidSelector, new int[1], 0));
    expectThrows(
        IllegalArgumentException.class,
        () -> Simple64.decodeAll(new long[] {invalidSelector}, 0, new int[1], 0, 1));
  }

  public void testFuzz() {
    Random rng = new Random(12345);
    int rounds = 20_000;
    for (int r = 0; r < rounds; r++) {
      int len = rng.nextInt(60) + 1;
      // random bit-width 1..31 to exercise all selectors including 13
      int maxBits = rng.nextInt(31) + 1;
      int maxVal = (maxBits == 31) ? Integer.MAX_VALUE : (1 << maxBits) - 1;
      int[] input = new int[len];
      for (int i = 0; i < len; i++) {
        input[i] = (int) (((long) rng.nextInt() & 0x7FFFFFFFL) % ((long) maxVal + 1));
      }
      long[] longs = new long[len + 1];
      int numLongs = Simple64.encodeAll(input, 0, len, longs, 0);
      assertTrue("Simple64 should not expand the number of values", numLongs <= len);
      if (numLongs < longs.length) {
        assertEquals(0L, longs[numLongs]);
      }
      int[] decoded = new int[len];
      Simple64.decodeAll(longs, 0, decoded, 0, len);
      if (Arrays.equals(input, decoded) == false) {
        fail(
            "Simple64 fuzz roundtrip failed: round="
                + r
                + ", len="
                + len
                + ", maxBits="
                + maxBits
                + ", maxVal="
                + maxVal
                + ", numLongs="
                + numLongs
                + ", longs="
                + Arrays.toString(Arrays.copyOf(longs, numLongs))
                + ", input="
                + Arrays.toString(input)
                + ", decoded="
                + Arrays.toString(decoded));
      }
    }
  }
}
