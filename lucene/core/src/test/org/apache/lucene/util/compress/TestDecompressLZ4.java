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
  public void testOverlappingMatchesWithDictionaryAndPartialReads() throws IOException {
    for (int distance : new int[] {1, 2, 3, 7, 8, 15, 16, 17, 63, 64, 65, 255, 65535}) {
      for (int length : new int[] {4, 63, 64, 65, 1024, 131089}) {
        byte[] dictionary = new byte[distance];
        random().nextBytes(dictionary);
        var encoded = ByteBuffersDataOutput.newResettableInstance();
        // A match against the preset dictionary, followed by five terminal literals.
        encoded.writeByte((byte) Math.min(15, length - 4));
        encoded.writeShort((short) distance);
        if (length >= 19) {
          int remaining = length - 19;
          while (remaining >= 255) {
            encoded.writeByte((byte) 255);
            remaining -= 255;
          }
          encoded.writeByte((byte) remaining);
        }
        encoded.writeByte((byte) 0x50);
        encoded.writeBytes(new byte[] {1, 2, 3, 4, 5}, 5);
        byte[] compressed = encoded.toArrayCopy();
        for (int requested : new int[] {1, Math.min(63, length), length, length + 5}) {
          byte[] dest = new byte[distance + length + 5];
          System.arraycopy(dictionary, 0, dest, 0, distance);
          int end = LZ4.decompress(new ByteArrayDataInput(compressed), requested, dest, distance);
          assertEquals(distance + (requested <= length ? length : length + 5), end);
          assertArrayEquals(dictionary, ArrayUtil.copyOfSubArray(dest, 0, distance));
          for (int i = 0; i < length; i++) {
            assertEquals(dictionary[i % distance], dest[distance + i]);
          }
          if (requested > length) {
            assertArrayEquals(
                new byte[] {1, 2, 3, 4, 5},
                ArrayUtil.copyOfSubArray(dest, distance + length, dest.length));
          }
        }
      }
    }
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
