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
package org.apache.lucene.util.compress;

import java.io.IOException;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.ArrayUtil;

public class TestDecompressLZ4 extends LuceneTestCase {
  /**
   * Decodes a single match that starts copying at the first byte of a preset dictionary. A distance
   * smaller than the length makes the match overlap its own output, so the expected output is the
   * dictionary repeated with a period of {@code distance}.
   */
  public void testOverlappingMatchesWithDictionaryAndPartialReads() throws IOException {
    final byte[] lastLiterals = {1, 2, 3, 4, 5};
    final int[] distances = {1, 2, 3, 7, 8, 15, 16, 17, 31, 32, 63, 64, 65, 255, 65535};
    // Covers both sides of the extension byte boundaries and of the bulk copy threshold.
    final int[] lengths = {
      4,
      7,
      8,
      15,
      16,
      17,
      18,
      19,
      31,
      32,
      63,
      64,
      65,
      255,
      256,
      257,
      LZ4.MIN_BULK_OVERLAP_LEN - 1,
      LZ4.MIN_BULK_OVERLAP_LEN,
      LZ4.MIN_BULK_OVERLAP_LEN + 1,
      511,
      512,
      513,
      1024,
      131089
    };

    for (int distance : distances) {
      for (int length : lengths) {
        byte[] dictionary = new byte[distance];
        random().nextBytes(dictionary);
        byte[] compressed = encodeMatchAndLastLiterals(distance, length, lastLiterals);

        // The decoder stops once it has at least the requested number of bytes, so a request that
        // the match satisfies leaves the whole match decoded and the last literals unread.
        for (int requested : new int[] {1, Math.min(63, length), length, length + 5}) {
          byte[] dest = new byte[distance + length + lastLiterals.length];
          System.arraycopy(dictionary, 0, dest, 0, distance);
          boolean literalsRead = requested > length;

          int end = LZ4.decompress(new ByteArrayDataInput(compressed), requested, dest, distance);

          assertEquals(distance + length + (literalsRead ? lastLiterals.length : 0), end);
          assertArrayEquals(dictionary, ArrayUtil.copyOfSubArray(dest, 0, distance));
          for (int i = 0; i < length; i++) {
            assertEquals(dictionary[i % distance], dest[distance + i]);
          }
          if (literalsRead) {
            assertArrayEquals(
                lastLiterals, ArrayUtil.copyOfSubArray(dest, distance + length, dest.length));
          }
        }
      }
    }
  }

  /** Encodes a sequence with just a match, then a final sequence with five literals. */
  private static byte[] encodeMatchAndLastLiterals(int distance, int length, byte[] lastLiterals)
      throws IOException {
    assert lastLiterals.length == 5;
    var out = ByteBuffersDataOutput.newResettableInstance();

    // Token: no literals in the high bits, length - MIN_MATCH in the low bits. A value of 15 means
    // that the rest of the length follows the distance as extension bytes.
    int encodedLength = length - LZ4.MIN_MATCH;
    out.writeByte((byte) Math.min(15, encodedLength));
    out.writeShort((short) distance);
    if (encodedLength >= 15) {
      int remaining = encodedLength - 15;
      while (remaining >= 255) {
        out.writeByte((byte) 255);
        remaining -= 255;
      }
      out.writeByte((byte) remaining);
    }

    // Token: five literals in the high bits, and no match since the block ends with the literals.
    out.writeByte((byte) (lastLiterals.length << 4));
    out.writeBytes(lastLiterals, lastLiterals.length);
    return out.toArrayCopy();
  }

  public void testDecompressOffset0() {
    byte[] input =
        new byte[] {
          // token
          0xE,
          // offset 0 (invalid)
          0,
          0,
          // last literal
          // token
          7 << 4,
          // literal
          0,
          0,
          0,
          0,
          0,
          0,
          0
        };

    byte[] output = new byte[18];

    var e =
        assertThrows(
            IOException.class,
            () -> LZ4.decompress(new ByteArrayDataInput(input), output.length, output, 0));
    assertEquals("offset 0 is invalid", e.getMessage());
  }

  public void testDecompressOffsetBeyondOutput() {
    // A match offset must not point before the start of the output, otherwise the match would
    // reference bytes that this call never decompressed.
    byte[] input =
        new byte[] {
          // token: 0 literals, match length 4 (MIN_MATCH)
          0x0,
          // offset 8, which is greater than the 0 bytes decompressed so far
          8,
          0,
          // last literals
          // token
          7 << 4,
          // literals
          0,
          0,
          0,
          0,
          0,
          0,
          0
        };

    byte[] output = new byte[18];

    var e =
        assertThrows(
            IOException.class,
            () -> LZ4.decompress(new ByteArrayDataInput(input), output.length, output, 0));
    assertEquals("match offset 8 is invalid, only 0 bytes are available", e.getMessage());
  }

  public void testDecompressOffsetBeyondOutputWithDictionary() {
    // With a preset dictionary the match offset may reach into the dictionary that the caller
    // placed in dest[dOff-dictLen:dOff], so the bound is dOff, not the number of bytes that this
    // call decompressed.
    byte[] input =
        new byte[] {
          // token: 0 literals, match length 4 (MIN_MATCH)
          0x0,
          // offset 5, one byte past the 4-byte dictionary
          5,
          0,
          // last literals
          // token
          7 << 4,
          // literals
          0,
          0,
          0,
          0,
          0,
          0,
          0
        };

    byte[] output = new byte[22];
    final int dictLen = 4;

    var e =
        assertThrows(
            IOException.class,
            () -> LZ4.decompress(new ByteArrayDataInput(input), 18, output, dictLen));
    assertEquals("match offset 5 is invalid, only 4 bytes are available", e.getMessage());
  }
}
