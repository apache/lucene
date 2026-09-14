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
package org.apache.lucene.analysis.icu;

import com.ibm.icu.text.Transliterator;
import com.ibm.icu.text.Transliterator.Position;
import java.util.Arrays;
import java.util.Random;
import org.apache.lucene.tests.analysis.BaseTokenStreamTestCase;

public class TestCircularReplaceable extends BaseTokenStreamTestCase {

  private static CircularReplaceable newInstanceInit(int size, int shift) {
    CircularReplaceable ret = new CircularReplaceable(size);
    if (true) return ret;
    int i = 0;
    while (i < shift) {
      final char[] sink = new char[1];
      ret.append(" ");
      ret.flush(sink, 0, 1, ++i, null);
    }
    return ret;
  }

  private static CircularReplaceable newInstanceInit(int size, Random r) {
    int actualInternalCapacity = Integer.highestOneBit(size) << 1;
    // +1 allows to completely wrap head back to idx=0
    return newInstanceInit(size, r.nextInt(actualInternalCapacity + 1));
  }

  private static CircularReplaceable newInstanceInit(int size) {
    return newInstanceInit(size, random());
  }

  private static void populate(CircularReplaceable r, int i) {
    char[] runner = new char[1];
    for (; ; ) {
      for (char c = 'a'; c <= 'z'; c++) {
        if (i-- == 0) {
          return;
        }
        runner[0] = c;
        r.append(runner, 0, 1);
      }
    }
  }

  private static String getReplacementText(int i) {
    char[] builder = new char[i];
    int offset = random().nextInt(26);
    final boolean lowercaseReplacement = random().nextBoolean() && false;
    final char from = lowercaseReplacement ? 'a' : 'A';
    final char to = (char) (from + 26);
    for (; ; ) {
      for (char c = from; c < to; c++) {
        if (offset > 0) {
          offset--;
          continue;
        }
        if (i-- == 0) {
          return new String(builder);
        }
        builder[i] = c;
      }
    }
  }

  public void testCopyVsReplace() {
    final boolean pipeline = true; // random().nextBoolean();
    for (int i = 0; i < 10000; i++) {
      // System.err.println("round "+i);
      int bound = 32;
      int initCopy = random().nextInt(bound);
      int initReplace = random().nextInt(bound);
      int size = random().nextInt(bound);

      // negative offset, from `*.length()`
      int destOff = size == 0 ? 0 : random().nextInt(size);

      // negative offset of dest limit
      int destLimitOff = destOff == 0 ? 0 : random().nextInt(destOff);

      String replacement = getReplacementText(random().nextInt(size + 1));
      String replace1 = replacement;
      CRStruct copy = new CRStruct(initCopy, size);
      CRStruct replace = new CRStruct(initReplace, size);
      String pre = replace.r.toString();
      int expectSize = testCopyVsReplace(copy, replace, size, destOff, destLimitOff, replacement);
      String mid = replace.r.toString();
      String midOr = "r:" + Arrays.toString(replace.r.offsetCorrect);
      String midOc = "c:" + Arrays.toString(copy.r.offsetCorrect);
      try {
        assertEquals(
            "headDiff for len=" + replace.r.length() + ", " + replace.r.toString(),
            replace.r.headDiff(),
            copy.r.headDiff());
      } catch (AssertionError er) {
        System.err.println("Stage1: " + pre + " => " + mid + " (" + replace1 + ")");
        System.err.println("mid-" + midOr);
        System.err.println("mid-" + midOc);
        throw er;
      }
      if (pipeline) {
        size = expectSize;
        destOff = size == 0 ? 0 : random().nextInt(size); // negative offset, from `*.length()`
        destLimitOff =
            destOff == 0 ? 0 : random().nextInt(destOff); // negative offset of dest limit
        replacement = getReplacementText(random().nextInt(size + 1));
        expectSize = testCopyVsReplace(copy, replace, size, destOff, destLimitOff, replacement);
      }
      String end = replace.r.toString();
      String endOr = "r:" + Arrays.toString(replace.r.offsetCorrect);
      String endOc = "c:" + Arrays.toString(copy.r.offsetCorrect);
      char[] replaceSink = new char[expectSize];
      char[] copySink = new char[expectSize];
      final int[] replaceOffsets = new int[expectSize]; // possibly oversized
      final int[] copyOffsets = new int[expectSize]; // possibly oversized
      replace.r.flush(
          replaceSink,
          0,
          expectSize,
          replace.r.length(),
          new CircularReplaceable.OffsetCorrectionRegistrar(
              (offset, diff) -> {
                replaceOffsets[offset - replace.preLen] = diff;
                return 0;
              }));
      copy.r.flush(
          copySink,
          0,
          expectSize,
          copy.r.length(),
          new CircularReplaceable.OffsetCorrectionRegistrar(
              (offset, diff) -> {
                copyOffsets[offset - copy.preLen] = diff;
                return 0;
              }));
      assertArrayEquals(replaceSink, copySink);
      try {
        assertArrayEquals(
            "r:"
                + Arrays.toString(replaceOffsets)
                + ", "
                + replace.r.headDiff()
                + ", c:"
                + Arrays.toString(copyOffsets)
                + ", "
                + copy.r.headDiff(),
            replaceOffsets,
            copyOffsets);
        assertEquals(
            "headDiff for len=" + replace.r.length() + ", " + end,
            replace.r.headDiff(),
            copy.r.headDiff());
      } catch (AssertionError er) {
        System.err.println("Stage1: " + pre + " => " + mid + " (" + replace1 + ")");
        System.err.println("Stage2: " + mid + " => " + end + " (" + replacement + ")");
        System.err.println("mid-" + midOr);
        System.err.println("mid-" + midOc);
        System.err.println("end-" + endOr);
        System.err.println("end-" + endOc);
        throw er;
      }
    }
  }

  private static class CRStruct {
    private final CircularReplaceable r;
    private final int preLen;

    private CRStruct(int initSize, int populate) {
      r = newInstanceInit(initSize);
      preLen = r.length();
      populate(r, populate);
    }
  }

  /**
   * It is important that simple "replace" and and "complex replace" (a distinct sequence of "copy"
   * and "replace" operations as executed by ICU `StringReplacer`) be functionally equivalent. Here
   * we replicate "simple" and "complex" replace operations, and check for parity against random
   * data
   */
  private int testCopyVsReplace(
      CRStruct copyStruct,
      CRStruct replaceStruct,
      int size,
      final int destOff,
      final int destLimitOff,
      String replacement) {
    CircularReplaceable copy = copyStruct.r;
    CircularReplaceable replace = replaceStruct.r;
    final int copyPreLen = copyStruct.preLen;
    final int replacePreLen = replaceStruct.preLen;
    final int replaceLen = replace.length();
    // String pre = replace.toString();
    replace.replace(replaceLen - destOff, replaceLen - destLimitOff, replacement);
    // System.err.println(pre+" => "+replace.toString() + " ("+replacement+")");
    final int copyLen = copy.length();
    // here we mimic the behavior of "complex" replacement:
    // 1: copy the leading context character. This triggers "workingBuffer mode"
    if (destOff == copyLen) {
      copy.replace(copyLen, copyLen, "\uFFFF");
    } else {
      copy.copy(copyLen - destOff - 1, copyLen - destOff, copyLen);
    }
    assertEquals(copyLen + 1, copy.length()); // +1 for lead context
    // 2: copy the trailing context character; this is a bit weird because if we're at the
    // end, the trailing context character will have been just copied from the _leading_ context
    // character! But apparently that's how Replacers do it; for our purposes it shouldn't matter
    copy.copy(copyLen - destLimitOff, copyLen - destLimitOff + 1, copy.length());
    assertEquals(copyLen + 2, copy.length()); // +2 for lead/trail context
    // 3: directly insert replacement between the two copied context codepoints
    copy.replace(copyLen + 1, copyLen + 1, replacement);
    assertEquals(replacement.length() + 2, copy.length() - copyLen); // +2 for lead/trail context
    // 4: copy the replacement text from the working buffer, inserting *before* the key text in
    // the source buffer
    copy.copy(copyLen + 1, copy.length() - 1, copyLen - destOff);
    // 5: delete working buffer (-2 to also delete lead/trail context)
    copy.replace(copy.length() - replacement.length() - 2, copy.length(), "");
    // 6: delete key text from source
    copy.replace(copy.length() - destOff, copy.length() - destLimitOff, "");
    assertEquals(replace.length() - replacePreLen, copy.length() - copyPreLen);
    String replaceOut = replace.toString();
    String copyOut = copy.toString();
    int expectSize = size + replacement.length() - (destOff - destLimitOff);
    assertEquals(expectSize, replaceOut.length());
    assertEquals(expectSize, copyOut.length());
    assertEquals(replaceOut, copyOut);
    return expectSize;
  }

  public void testDoubleReplace() throws Exception {
    CircularReplaceable r;
    char[] charSink;
    int[] expectedOffsets;
    r = new CircularReplaceable("abcd");
    r.replace(1, 3, "RQPO");
    charSink = new char[r.length()];
    int[] offsets1 = new int[r.length()];
    assertEquals("aRQPOd", r.toString());
    expectedOffsets = new int[] {0, 0, 0, -1, -2, 0};
    r.flush(
        charSink,
        0,
        r.length(),
        r.length(),
        new CircularReplaceable.OffsetCorrectionRegistrar(
            (offset, diff) -> {
              offsets1[offset] = diff;
              return 0;
            }));
    assertArrayEquals(
        "e:" + Arrays.toString(expectedOffsets) + ", a:" + Arrays.toString(offsets1),
        expectedOffsets,
        offsets1);
    r = new CircularReplaceable("abcd");
    r.replace(1, 3, "RQPO");
    assertEquals("aRQPOd", r.toString());
    r.replace(1, 5, "O");
    charSink = new char[r.length()];
    int[] offsets2 = new int[r.length()];
    assertEquals("aOd", r.toString());
    expectedOffsets = new int[] {0, 0, 1};
    r.flush(
        charSink,
        0,
        r.length(),
        r.length(),
        new CircularReplaceable.OffsetCorrectionRegistrar(
            (offset, diff) -> {
              offsets2[offset] = diff;
              return 0;
            }));
    assertArrayEquals(
        "e:"
            + Arrays.toString(expectedOffsets)
            + "/0"
            + ", a:"
            + Arrays.toString(offsets2)
            + "/"
            + r.headDiff(),
        expectedOffsets,
        offsets2);
  }

  public void testDoubleReplace2() throws Exception {
    CircularReplaceable r;
    char[] charSink;
    int[] expectedOffsets;
    r = new CircularReplaceable("abcd");
    r.replace(1, 3, "RQPON");
    charSink = new char[r.length()];
    int[] offsets1 = new int[r.length()];
    assertEquals("aRQPONd", r.toString());
    expectedOffsets = new int[] {0, 0, 0, -1, -2, -3, 0};
    r.flush(
        charSink,
        0,
        r.length(),
        r.length(),
        new CircularReplaceable.OffsetCorrectionRegistrar(
            (offset, diff) -> {
              offsets1[offset] = diff;
              return 0;
            }));
    assertArrayEquals(
        "e:" + Arrays.toString(expectedOffsets) + ", a:" + Arrays.toString(offsets1),
        expectedOffsets,
        offsets1);
    r = new CircularReplaceable("abcd");
    r.replace(1, 3, "RQPON");
    assertEquals("aRQPONd", r.toString());
    r.replace(1, 6, "ON");
    charSink = new char[r.length()];
    int[] offsets2 = new int[r.length()];
    assertEquals("aONd", r.toString());
    expectedOffsets = new int[] {0, 0, 0, 0};
    r.flush(
        charSink,
        0,
        r.length(),
        r.length(),
        new CircularReplaceable.OffsetCorrectionRegistrar(
            (offset, diff) -> {
              offsets2[offset] = diff;
              return 0;
            }));
    assertArrayEquals(
        "e:"
            + Arrays.toString(expectedOffsets)
            + "/0, a:"
            + Arrays.toString(offsets2)
            + "/"
            + r.headDiff(),
        expectedOffsets,
        offsets2);
  }

  public void testDoubleReplace3() throws Exception {
    CircularReplaceable r;
    char[] charSink;
    int[] expectedOffsets;
    r = new CircularReplaceable("abcdefghijklmnopqrstuvwxyzabcd");
    r.replace(10, 26, "TSRQPONMLKJIHG");
    charSink = new char[r.length()];
    int[] offsets1 = new int[r.length()];
    assertEquals("abcdefghijTSRQPONMLKJIHGabcd", r.toString());
    expectedOffsets =
        new int[] {
          0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0
        };
    r.flush(
        charSink,
        0,
        r.length(),
        r.length(),
        new CircularReplaceable.OffsetCorrectionRegistrar(
            (offset, diff) -> {
              offsets1[offset] = diff;
              return 0;
            }));
    assertArrayEquals(
        "e:" + Arrays.toString(expectedOffsets) + ", a:" + Arrays.toString(offsets1),
        expectedOffsets,
        offsets1);
    r = new CircularReplaceable("abcdefghijklmnopqrstuvwxyzabcd");
    r.replace(10, 26, "TSRQPONMLKJIHG");
    assertEquals("abcdefghijTSRQPONMLKJIHGabcd", r.toString());

    // target key "NM" is both prefix and suffix of replacement val
    r.replace(16, 18, "NMLKJIHGFEDCBAZYXWVUTSRQPONM");

    charSink = new char[r.length()];
    int[] offsets2 = new int[r.length()];
    assertEquals("abcdefghijTSRQPONMLKJIHGFEDCBAZYXWVUTSRQPONMLKJIHGabcd", r.toString());
    // below if `replace` associates "NM" as prefix, not suffix.
    expectedOffsets =
        new int[] {
          0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -1, -2, -3, -4, -5, -6, -7, -8, -9,
          -10, -11, -12, -13, -14, -15, -16, -17, -18, -19, -20, -21, -22, -23, -24, -25, -26, 0, 0,
          0, 0, 0, 0, -24, 0, 0, 0
        };
    // below if `replace` associates "NM" as suffix, not prefix.
    /*
    expectedOffsets = new int[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -1, -2, -3, -4, -5, -6, -7, -8,
        -9, -10, -11, -12, -13, -14, -15, -16, -17, -18, -19, -20, -21, -22, -23, -24, -25, -26, 0, 0, 0, 0, 0, 0,
        0, -24, 0, 0, 0};

    */
    r.flush(
        charSink,
        0,
        r.length(),
        r.length(),
        new CircularReplaceable.OffsetCorrectionRegistrar(
            (offset, diff) -> {
              offsets2[offset] = diff;
              return 0;
            }));
    assertArrayEquals(
        "e:"
            + Arrays.toString(expectedOffsets)
            + "/0, a:"
            + Arrays.toString(offsets2)
            + "/"
            + r.headDiff(),
        expectedOffsets,
        offsets2);
  }

  public void testTrimPrefixShrink() throws Exception {
    CircularReplaceable r = new CircularReplaceable("abcdefg");
    r.replace(1, 4, "b");
    char[] charSink = new char[r.length()];
    int[] offsets = new int[r.length()];
    assertEquals("abefg", r.toString());
    int[] expectedOffsets = new int[] {0, 0, 2, 0, 0};
    r.flush(
        charSink,
        0,
        r.length(),
        r.length(),
        new CircularReplaceable.OffsetCorrectionRegistrar(
            (offset, diff) -> {
              offsets[offset] = diff;
              return 0;
            }));
    assertArrayEquals(expectedOffsets, offsets);
  }

  public void testTrimPrefixGrow() throws Exception {
    CircularReplaceable r = new CircularReplaceable("abcdefg");
    r.replace(1, 4, "bcXX");
    char[] charSink = new char[r.length()];
    int[] offsets = new int[r.length()];
    assertEquals("abcXXefg", r.toString());
    int[] expectedOffsets = new int[] {0, 0, 0, 0, -1, 0, 0, 0};
    r.flush(
        charSink,
        0,
        r.length(),
        r.length(),
        new CircularReplaceable.OffsetCorrectionRegistrar(
            (offset, diff) -> {
              offsets[offset] = diff;
              return 0;
            }));
    assertArrayEquals(expectedOffsets, offsets);
  }

  public void testTrimPrefixFullGrow() throws Exception {
    CircularReplaceable r = new CircularReplaceable("abcdefg");
    r.replace(1, 3, "bcXX");
    char[] charSink = new char[r.length()];
    int[] offsets = new int[r.length()];
    assertEquals("abcXXdefg", r.toString());
    int[] expectedOffsets = new int[] {0, 0, 0, -1, -2, 0, 0, 0, 0};
    r.flush(
        charSink,
        0,
        r.length(),
        r.length(),
        new CircularReplaceable.OffsetCorrectionRegistrar(
            (offset, diff) -> {
              offsets[offset] = diff;
              return 0;
            }));
    assertArrayEquals(
        "e:" + Arrays.toString(expectedOffsets) + ", a:" + Arrays.toString(offsets),
        expectedOffsets,
        offsets);
  }

  public void testTrimSuffixShrink() throws Exception {
    CircularReplaceable r = new CircularReplaceable("abcdefg");
    r.replace(1, 4, "d");
    char[] charSink = new char[r.length()];
    int[] offsets = new int[r.length()];
    assertEquals("adefg", r.toString());
    // This one is odd! because the top-level has "d" replacing "bcd", we actually want "d" to
    // inherit "b"s
    // offset; the offset diff should thus be applied to "e".
    int[] expectedOffsets = new int[] {0, 0, 2, 0, 0};
    r.flush(
        charSink,
        0,
        r.length(),
        r.length(),
        new CircularReplaceable.OffsetCorrectionRegistrar(
            (offset, diff) -> {
              offsets[offset] = diff;
              return 0;
            }));
    assertArrayEquals(
        "e:" + Arrays.toString(expectedOffsets) + ", a:" + Arrays.toString(offsets),
        expectedOffsets,
        offsets);
  }

  public void testTrimSuffixGrow() throws Exception {
    CircularReplaceable r = new CircularReplaceable("abcdefg");
    r.replace(1, 4, "XXcd");
    char[] charSink = new char[r.length()];
    int[] offsets = new int[r.length()];
    assertEquals("aXXcdefg", r.toString());
    int[] expectedOffsets = new int[] {0, 0, -1, 0, 0, 0, 0, 0};
    r.flush(
        charSink,
        0,
        r.length(),
        r.length(),
        new CircularReplaceable.OffsetCorrectionRegistrar(
            (offset, diff) -> {
              offsets[offset] = diff;
              return 0;
            }));
    assertArrayEquals(
        "e:" + Arrays.toString(expectedOffsets) + ", a:" + Arrays.toString(offsets),
        expectedOffsets,
        offsets);
  }

  public void testTrimSuffixFullGrow() throws Exception {
    CircularReplaceable r = new CircularReplaceable("abcdefg");
    r.replace(1, 3, "XXbc");
    char[] charSink = new char[r.length()];
    int[] offsets = new int[r.length()];
    assertEquals("aXXbcdefg", r.toString());
    int[] expectedOffsets = new int[] {0, 0, -1, -2, 0, 0, 0, 0, 0};
    r.flush(
        charSink,
        0,
        r.length(),
        r.length(),
        new CircularReplaceable.OffsetCorrectionRegistrar(
            (offset, diff) -> {
              offsets[offset] = diff;
              return 0;
            }));
    assertArrayEquals(
        "e:" + Arrays.toString(expectedOffsets) + ", a:" + Arrays.toString(offsets),
        expectedOffsets,
        offsets);
  }

  public void testRtx() throws Exception {
    Transliterator t =
        Transliterator.createFromRules(
            "X_ROUND_TRIP", "a > bc; ::Null; bc > a;", Transliterator.FORWARD);
    String in = "a a a a a ";
    int[] expectedOffsets = new int[0];
    check(t, in, in, expectedOffsets);
  }

  public void testExpand() throws Exception {
    Transliterator t =
        Transliterator.createFromRules("X_EXPAND", "a > bc;", Transliterator.FORWARD);
    String in = "a a a a a ";
    String expected = "bc bc bc bc bc ";
    int[] expectedOffsets = new int[] {1, -1, 4, -2, 7, -3, 10, -4, 13, -5};
    check(t, in, expected, expectedOffsets);
  }

  private void check(Transliterator t, String in, String expected, int[] expectedOffsets) {
    CircularReplaceable r = new CircularReplaceable();
    char[] expectedChars = expected.toCharArray();
    final int[] baseOffsetIndices = new int[expectedOffsets.length >> 1];
    for (int i = 0; i < baseOffsetIndices.length; i++) {
      baseOffsetIndices[i] = expectedOffsets[i << 1];
    }
    Position p = new Position();
    p.contextStart = p.start = 0;
    for (int i = 0; i < 5; i++) {
      r.append(in);
      p.limit = p.contextLimit = r.length();
      t.finishTransliteration(r, p);
      char[] actual = new char[expectedChars.length];
      final int[] counter = new int[1];
      final int[] offsetMetadata = new int[expectedOffsets.length];
      int len = r.length();
      r.flush(
          actual,
          0,
          actual.length,
          len,
          new CircularReplaceable.OffsetCorrectionRegistrar(
              (offset, diff) -> {
                offsetMetadata[counter[0]++] = offset;
                offsetMetadata[counter[0]++] = diff;
                return 0;
              }));
      assertEquals(0, r.headDiff());
      assertArrayEquals(expectedChars, actual);
      assertArrayEquals(
          "run "
              + i
              + "; expected:"
              + Arrays.toString(expectedOffsets)
              + ", actual: "
              + Arrays.toString(offsetMetadata),
          expectedOffsets,
          offsetMetadata);
      for (int j = 0; j < baseOffsetIndices.length; j++) {
        expectedOffsets[j << 1] = baseOffsetIndices[j] + len;
      }
    }
  }

  public void testInsert1() throws Exception {
    int from = 0;
    int to = 0;
    String replace = "X";
    String expected = "Xabcdefg";
    int[] expectedOffsets = new int[] {1, -1};
    checkSimple(from, to, replace, expected, expectedOffsets);
  }

  public void testInsert2() throws Exception {
    int from = 0;
    int to = 0;
    String replace = "XX";
    String expected = "XXabcdefg";
    int[] expectedOffsets = new int[] {1, -1, 2, -2};
    checkSimple(from, to, replace, expected, expectedOffsets);
  }

  public void testInsert3() throws Exception {
    int from = 1;
    int to = 1;
    String replace = "XX";
    String expected = "aXXbcdefg";
    int[] expectedOffsets = new int[] {2, -1, 3, -2};
    checkSimple(from, to, replace, expected, expectedOffsets);
  }

  public void testInsert4() throws Exception {
    int from = 6;
    int to = 6;
    String replace = "XX";
    String expected = "abcdefXXg";
    int[] expectedOffsets = new int[] {7, -1, 8, -2};
    checkSimple(from, to, replace, expected, expectedOffsets);
  }

  public void testInsert5() throws Exception {
    int from = 7;
    int to = 7;
    String replace = "XX";
    String expected = "abcdefgXX";
    int[] firstExpectedOffsets = new int[] {8, -1};
    int[] expectedOffsets = new int[] {0, -1, 8, -2};
    checkSimple(from, to, replace, expected, firstExpectedOffsets, expectedOffsets);
  }

  public void testInsert6() throws Exception {
    int from = 7;
    int to = 7;
    String replace = "X";
    String expected = "abcdefgX";
    int[] firstExpectedOffsets = new int[0];
    int[] expectedOffsets = new int[] {0, -1};
    checkSimple(from, to, replace, expected, firstExpectedOffsets, expectedOffsets);
  }

  public void testContract1() throws Exception {
    int from = 1;
    int to = 4;
    String replace = "X";
    String expected = "aXefg";
    int[] expectedOffsets = new int[] {2, 2};
    checkSimple(from, to, replace, expected, expectedOffsets);
  }

  public void testDelete1() throws Exception {
    int from = 1;
    int to = 4;
    String replace = "";
    String expected = "aefg";
    int[] expectedOffsets = new int[] {1, 3};
    checkSimple(from, to, replace, expected, expectedOffsets);
  }

  public void testDelete2() throws Exception {
    int from = 0;
    int to = 3;
    String replace = "";
    String expected = "defg";
    int[] expectedOffsets = new int[] {0, 3};
    checkSimple(from, to, replace, expected, expectedOffsets);
  }

  public void testExpand1() throws Exception {
    int from = 0;
    int to = 1;
    String replace = "XX";
    String expected = "XXbcdefg";
    int[] expectedOffsets = new int[] {1, -1};
    checkSimple(from, to, replace, expected, expectedOffsets);
  }

  public void testExpand2() throws Exception {
    int from = 1;
    int to = 2;
    String replace = "XX";
    String expected = "aXXcdefg";
    int[] expectedOffsets = new int[] {2, -1};
    checkSimple(from, to, replace, expected, expectedOffsets);
  }

  public void testExpand3() throws Exception {
    int from = 6;
    int to = 7;
    String replace = "XX";
    String expected = "abcdefXX";
    int[] expectedOffsets = new int[] {7, -1};
    checkSimple(from, to, replace, expected, expectedOffsets);
  }

  public void testExpand4() throws Exception {
    int from = 0;
    int to = 1;
    String replace = "XXX";
    String expected = "XXXbcdefg";
    int[] expectedOffsets = new int[] {1, -1, 2, -2};
    checkSimple(from, to, replace, expected, expectedOffsets);
  }

  public void testExpand5() throws Exception {
    int from = 1;
    int to = 2;
    String replace = "XXX";
    String expected = "aXXXcdefg";
    int[] expectedOffsets = new int[] {2, -1, 3, -2};
    checkSimple(from, to, replace, expected, expectedOffsets);
  }

  public void testExpand6() throws Exception {
    int from = 6;
    int to = 7;
    String replace = "XXX";
    String expected = "abcdefXXX";
    int[] expectedOffsets = new int[] {7, -1, 8, -2};
    checkSimple(from, to, replace, expected, expectedOffsets);
  }

  private void checkSimple(int from, int to, String replace, String expected, int[] expectedOffsets)
      throws Exception {
    checkSimple(from, to, replace, expected, expectedOffsets, expectedOffsets);
  }

  private void checkSimple(
      int from,
      int to,
      String replace,
      String expected,
      int[] firstExpectedOffsets,
      int[] expectedOffsets)
      throws Exception {
    CircularReplaceable r = new CircularReplaceable();
    char[] expectedChars = expected.toCharArray();
    final int[] baseOffsetIndices = new int[expectedOffsets.length >> 1];
    for (int i = 0; i < baseOffsetIndices.length; i++) {
      baseOffsetIndices[i] = expectedOffsets[i << 1];
    }
    final int expectedHeadDiff = firstExpectedOffsets == expectedOffsets ? 0 : -1;
    int[] passExpectedOffsets = firstExpectedOffsets;
    int len = 0;
    for (int i = 0; i < 10; i++) {
      r.append("abcdefg");
      r.replace(len + from, len + to, replace);
      assertEquals(expectedHeadDiff, r.headDiff());
      char[] actual = new char[expectedChars.length];
      final int[] counter = new int[1];
      final int[] offsetMetadata = new int[passExpectedOffsets.length];
      r.flush(
          actual,
          0,
          actual.length,
          len = r.length(),
          new CircularReplaceable.OffsetCorrectionRegistrar(
              (offset, diff) -> {
                // it's hard to correct expected diffs for multiple passes, so we pass a new
                // registrar for each pass (thus we only need correction for _offsets_).
                offsetMetadata[counter[0]++] = offset;
                offsetMetadata[counter[0]++] = diff;
                return 0;
              }));
      assertEquals(expectedHeadDiff, r.headDiff());
      assertArrayEquals(expectedChars, actual);
      assertArrayEquals(
          "actual:" + Arrays.toString(offsetMetadata) + " for pass " + i,
          passExpectedOffsets,
          offsetMetadata);
      passExpectedOffsets = expectedOffsets; // subsequent passes may expect different offsets;
      for (int j = 0; j < baseOffsetIndices.length; j++) {
        passExpectedOffsets[j << 1] = baseOffsetIndices[j] + len;
      }
    }
  }

  public void testMultiOp() throws Exception {
    for (int i = 1; i <= 3; i++) {
      testMultiOp(i);
    }
  }

  private void testMultiOp(int toPhase) throws Exception {
    CircularReplaceable r = new CircularReplaceable();
    r.append("abcdefg");
    r.replace(0, 0, "XX");
    String expected = "XXabcdefg";
    int[] expectedOffsets;
    assertEquals(expected, r.toString());
    if (toPhase == 1) {
      expectedOffsets = new int[] {1, -1, 2, -2};
    } else {
      r.replace(2, 3, ""); // delete 'a'
      expected = "XXbcdefg";
      assertEquals(expected, r.toString());
      if (toPhase == 2) {
        expectedOffsets = new int[] {1, -1};
      } else if (toPhase == 3) {
        r.replace(3, 4, "XX"); // replace/expand 'c'
        expected = "XXbXXdefg";
        assertEquals(expected, r.toString());
        expectedOffsets = new int[] {1, -1, 4, -2};
      } else {
        throw new IllegalArgumentException("illegal phase: " + toPhase);
      }
    }
    char[] expectedChars = expected.toCharArray();
    char[] actual = new char[expectedChars.length];
    final int[] counter = new int[1];
    final int[] offsetMetadata = new int[expectedOffsets.length];
    int len = r.length();
    r.flush(
        actual,
        0,
        actual.length,
        len,
        new CircularReplaceable.OffsetCorrectionRegistrar(
            (offset, diff) -> {
              // it's hard to correct expected diffs for multiple passes, so we pass a new registrar
              // for each pass (thus we only need correction for _offsets_).
              offsetMetadata[counter[0]++] = offset;
              offsetMetadata[counter[0]++] = diff;
              return 0;
            }));
    assertEquals(0, r.headDiff());
    assertArrayEquals(expectedChars, actual);
    assertArrayEquals("actual:" + Arrays.toString(offsetMetadata), expectedOffsets, offsetMetadata);
  }

  public void testHeadInsertAndDelete() throws Exception {
    // this checks that headDiff is added and removed
    for (int i = 1; i <= 3; i++) {
      testHeadInsertAndDelete(i);
    }
  }

  private void testHeadInsertAndDelete(int toPhase) throws Exception {
    CircularReplaceable r = new CircularReplaceable();
    r.append("abcdefg");
    r.replace(7, 7, "XX");
    String expected = "abcdefgXX";
    int[] expectedOffsets;
    assertEquals(expected, r.toString());
    assertEquals(-1, r.headDiff());
    if (toPhase == 1) {
      expectedOffsets = new int[] {8, -1};
    } else {
      r.replace(8, 9, ""); // delete last 'X'
      expected = "abcdefgX";
      assertEquals(expected, r.toString());
      assertEquals(-1, r.headDiff());
      if (toPhase == 2) {
        expectedOffsets = new int[0];
      } else if (toPhase == 3) {
        r.replace(7, 8, ""); // delete remaining 'X'
        expected = "abcdefg";
        assertEquals(expected, r.toString());
        assertEquals(0, r.headDiff());
        expectedOffsets = new int[0];
      } else {
        throw new IllegalArgumentException("illegal phase: " + toPhase);
      }
    }
    char[] expectedChars = expected.toCharArray();
    char[] actual = new char[expectedChars.length];
    final int[] counter = new int[1];
    final int[] offsetMetadata = new int[expectedOffsets.length];
    int len = r.length();
    r.flush(
        actual,
        0,
        actual.length,
        len,
        new CircularReplaceable.OffsetCorrectionRegistrar(
            (offset, diff) -> {
              // it's hard to correct expected diffs for multiple passes, so we pass a new registrar
              // for each pass (thus we only need correction for _offsets_).
              offsetMetadata[counter[0]++] = offset;
              offsetMetadata[counter[0]++] = diff;
              return 0;
            }));
    assertArrayEquals(expectedChars, actual);
    assertArrayEquals("actual:" + Arrays.toString(offsetMetadata), expectedOffsets, offsetMetadata);
  }

  public void testHeadInsertAndExpand() throws Exception {
    // this checks that headDiff is added and shifted forward
    for (int i = 1; i <= 4; i++) {
      testHeadInsertAndExpand(i);
    }
  }

  private void testHeadInsertAndExpand(int toPhase) throws Exception {
    CircularReplaceable r = new CircularReplaceable();
    r.append("abcdefg");
    r.replace(7, 7, "XX");
    String expected = "abcdefgXX";
    int[] expectedOffsets;
    assertEquals(expected, r.toString());
    assertEquals(-1, r.headDiff());
    if (toPhase == 1) {
      expectedOffsets = new int[] {8, -1};
    } else {
      r.replace(8, 9, ""); // delete last 'X'
      expected = "abcdefgX";
      assertEquals(expected, r.toString());
      assertEquals(-1, r.headDiff());
      if (toPhase == 2) {
        expectedOffsets = new int[0];
      } else {
        r.replace(1, 1, "XX");
        expected = "aXXbcdefgX";
        assertEquals(expected, r.toString());
        assertEquals(-1, r.headDiff()); // ensure headDiff got shifted forward
        if (toPhase == 3) {
          expectedOffsets = new int[] {2, -1, 3, -2};
        } else if (toPhase == 4) {
          r.replace(9, 10, ""); // delete remaining 'X'
          expected = "aXXbcdefg";
          assertEquals(expected, r.toString());
          assertEquals(0, r.headDiff()); // ensure headDiff got deleted
          expectedOffsets = new int[] {2, -1, 3, -2};
        } else {
          throw new IllegalArgumentException("illegal phase: " + toPhase);
        }
      }
    }
    char[] expectedChars = expected.toCharArray();
    char[] actual = new char[expectedChars.length];
    final int[] counter = new int[1];
    final int[] offsetMetadata = new int[expectedOffsets.length];
    int len = r.length();
    r.flush(
        actual,
        0,
        actual.length,
        len,
        new CircularReplaceable.OffsetCorrectionRegistrar(
            (offset, diff) -> {
              // it's hard to correct expected diffs for multiple passes, so we pass a new registrar
              // for each pass (thus we only need correction for _offsets_).
              offsetMetadata[counter[0]++] = offset;
              offsetMetadata[counter[0]++] = diff;
              return 0;
            }));
    assertArrayEquals(expectedChars, actual);
    assertArrayEquals("actual:" + Arrays.toString(offsetMetadata), expectedOffsets, offsetMetadata);
  }
}
