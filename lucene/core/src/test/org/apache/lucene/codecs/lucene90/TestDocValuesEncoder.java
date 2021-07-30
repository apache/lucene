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
package org.apache.lucene.codecs.lucene90;

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NumericUtils;

public class TestDocValuesEncoder extends LuceneTestCase {

  public void testRandomValues() throws IOException {
    long[] arr = new long[DocValuesEncoder.BLOCK_SIZE];
    for (int i = 0; i < DocValuesEncoder.BLOCK_SIZE; ++i) {
      arr[i] = random().nextLong();
    }
    arr[4] = Long.MAX_VALUE / 2;
    arr[100] = Long.MIN_VALUE / 2 - 1;
    final long expectedNumBytes =
        2 // token
            + (DocValuesEncoder.BLOCK_SIZE * 64) / Byte.SIZE; // data
    doTest(arr, expectedNumBytes);
  }

  public void testAllEqual() throws IOException {
    long[] arr = new long[DocValuesEncoder.BLOCK_SIZE];
    Arrays.fill(arr, 3);
    final long expectedNumBytes = 2; // token + min value
    doTest(arr, expectedNumBytes);
  }

  public void testSmallPositiveValues() throws IOException {
    long[] arr = new long[DocValuesEncoder.BLOCK_SIZE];
    for (int i = 0; i < DocValuesEncoder.BLOCK_SIZE; ++i) {
      arr[i] = (i + 2) & 0x03; // 2 bits per value
    }
    final long expectedNumBytes =
        1 // token
            + (DocValuesEncoder.BLOCK_SIZE * 2) / Byte.SIZE; // data
    doTest(arr, expectedNumBytes);
  }

  public void testSmallPositiveValuesWithOffset() throws IOException {
    long[] arr = new long[DocValuesEncoder.BLOCK_SIZE];
    for (int i = 0; i < DocValuesEncoder.BLOCK_SIZE; ++i) {
      arr[i] = 1000 + ((i + 2) & 0x03); // 2 bits per value
    }
    final long expectedNumBytes =
        3 // token + min value (1000 -> 2 bytes)
            + (DocValuesEncoder.BLOCK_SIZE * 2) / Byte.SIZE; // data
    doTest(arr, expectedNumBytes);
  }

  public void testSmallPositiveValuesWithNegativeOffset() throws IOException {
    long[] arr = new long[DocValuesEncoder.BLOCK_SIZE];
    for (int i = 0; i < DocValuesEncoder.BLOCK_SIZE; ++i) {
      arr[i] = -1000 + ((i + 2) & 0x03); // 2 bits per value
    }
    final long expectedNumBytes =
        3 // token + min value (-1000 -> 2 bytes)
            + (DocValuesEncoder.BLOCK_SIZE * 2) / Byte.SIZE; // data
    doTest(arr, expectedNumBytes);
  }

  /**
   * Integers as doubles, GCD compression should help here given high numbers of trailing zeroes.
   */
  public void testIntegersAsDoubles() throws IOException {
    long[] arr = new long[DocValuesEncoder.BLOCK_SIZE];
    for (int i = 0; i < DocValuesEncoder.BLOCK_SIZE; ++i) {
      arr[i] = NumericUtils.doubleToSortableLong((i + 2) & 0x03); // 0, 1 or 2
    }
    final long expectedNumBytes =
        9 // token + GCD (8 bytes)
            + DocValuesEncoder.BLOCK_SIZE * 12 / Byte.SIZE; // 12 bits per value -> 26 longs
    doTest(arr, expectedNumBytes);
  }

  public void testDecreasingValues() throws IOException {
    long[] arr = new long[DocValuesEncoder.BLOCK_SIZE];
    for (int i = 0; i < DocValuesEncoder.BLOCK_SIZE; ++i) {
      arr[i] = 1000 - 30 * i;
    }
    final long expectedNumBytes = 4; // token + min value + delta
    doTest(arr, expectedNumBytes);
  }

  public void testTwoValues() throws IOException {
    long[] arr = new long[DocValuesEncoder.BLOCK_SIZE];
    for (int i = 0; i < DocValuesEncoder.BLOCK_SIZE; ++i) {
      arr[i] = i % 3 == 1 ? 42 : 100;
    }
    final long expectedNumBytes =
        3 // token + min value (1 byte) + GCD (1 byte)
            + (DocValuesEncoder.BLOCK_SIZE * 1) / Byte.SIZE; // data
    doTest(arr, expectedNumBytes);
  }

  /** Monotonic timestamps: delta coding + GCD compression. */
  public void testMonotonicTimestamps() throws IOException {
    long offset = 2 * 60 * 60 * 1000; // 2h offset = timezone
    // a timestamp near Jan 1st 2021
    long first = (2021 - 1970) * 365L * 24 * 60 * 60 * 1000 + offset;
    long granularity = 24L * 60 * 60 * 1000; // 1 day

    long[] arr = new long[DocValuesEncoder.BLOCK_SIZE];
    arr[0] = first;
    for (int i = 1; i < DocValuesEncoder.BLOCK_SIZE; ++i) {
      arr[i] = arr[i - 1] + (5 + i % 4) * granularity; // increment by a multiple of granularity
    }
    final long expectedNumBytes =
        16 // token + min delta + GCD + first
            + (DocValuesEncoder.BLOCK_SIZE * 2) / Byte.SIZE; // data
    doTest(arr, expectedNumBytes);
  }

  public void testZeroOrMinValue() throws IOException {
    long[] arr = new long[DocValuesEncoder.BLOCK_SIZE];
    for (int i = 0; i < DocValuesEncoder.BLOCK_SIZE; ++i) {
      arr[i] = i % 3 == 1 ? Long.MIN_VALUE : 0;
    }
    final long expectedNumBytes =
        10 // token + GCD (9 byte)
            + (DocValuesEncoder.BLOCK_SIZE * 1) / Byte.SIZE; // data
    doTest(arr, expectedNumBytes);
  }

  private void doTest(long[] arr, long expectedNumBytes) throws IOException {
    final long[] expected = arr.clone();
    DocValuesEncoder encoder = new DocValuesEncoder();
    try (Directory dir = newDirectory()) {
      try (IndexOutput out = dir.createOutput("tests.bin", IOContext.DEFAULT)) {
        encoder.encode(arr, out);
        assertEquals(expectedNumBytes, out.getFilePointer());
      }
      try (IndexInput in = dir.openInput("tests.bin", IOContext.DEFAULT)) {
        long[] decoded = new long[DocValuesForUtil.BLOCK_SIZE];
        for (int i = 0; i < decoded.length; ++i) {
          decoded[i] = random().nextLong();
        }
        encoder.decode(in, decoded);
        assertEquals(in.length(), in.getFilePointer());
        assertArrayEquals(expected, decoded);
      }
    }
  }
}
