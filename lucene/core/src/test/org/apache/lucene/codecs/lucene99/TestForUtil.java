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
package org.apache.lucene.codecs.lucene99;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.packed.PackedInts;

public class TestForUtil extends LuceneTestCase {

  public void testEncodeDecode() throws IOException {
    final int iterations = RandomNumbers.randomIntBetween(random(), 50, 1000);
    final int[] values = new int[iterations * ForUtil.BLOCK_SIZE];

    for (int i = 0; i < iterations; ++i) {
      final int bpv = TestUtil.nextInt(random(), 1, 31);
      for (int j = 0; j < ForUtil.BLOCK_SIZE; ++j) {
        values[i * ForUtil.BLOCK_SIZE + j] =
            RandomNumbers.randomIntBetween(random(), 0, (int) PackedInts.maxValue(bpv));
      }
    }

    final Directory d = new ByteBuffersDirectory();
    final long endPointer;

    {
      // encode
      IndexOutput out = d.createOutput("test.bin", IOContext.DEFAULT);
      final ForUtil forUtil = new ForUtil();

      for (int i = 0; i < iterations; ++i) {
        long[] source = new long[ForUtil.BLOCK_SIZE];
        long or = 0;
        for (int j = 0; j < ForUtil.BLOCK_SIZE; ++j) {
          source[j] = values[i * ForUtil.BLOCK_SIZE + j];
          or |= source[j];
        }
        final int bpv = PackedInts.bitsRequired(or);
        out.writeByte((byte) bpv);
        forUtil.encode(source, bpv, out);
      }
      endPointer = out.getFilePointer();
      out.close();
    }

    {
      // decode
      IndexInput in = d.openInput("test.bin", IOContext.READONCE);
      final ForUtil forUtil = new ForUtil();
      for (int i = 0; i < iterations; ++i) {
        final int bitsPerValue = in.readByte();
        final long currentFilePointer = in.getFilePointer();
        final long[] restored = new long[ForUtil.BLOCK_SIZE];
        forUtil.decode(bitsPerValue, in, restored);
        int[] ints = new int[ForUtil.BLOCK_SIZE];
        for (int j = 0; j < ForUtil.BLOCK_SIZE; ++j) {
          ints[j] = Math.toIntExact(restored[j]);
        }
        assertArrayEquals(
            Arrays.toString(ints),
            ArrayUtil.copyOfSubArray(values, i * ForUtil.BLOCK_SIZE, (i + 1) * ForUtil.BLOCK_SIZE),
            ints);
        assertEquals(forUtil.numBytes(bitsPerValue), in.getFilePointer() - currentFilePointer);
      }
      assertEquals(endPointer, in.getFilePointer());
      in.close();
    }

    d.close();
  }

  public void testPrefixSum() throws IOException {
    try (Directory dir = newDirectory()) {
      try (IndexOutput out = dir.createOutput("ints", IOContext.DEFAULT)) {
        for (int i = 0; i < 64; ++i) {
          out.writeLong(random().nextLong());
        }
      }
      final ForUtil forUtil = new ForUtil();
      try (IndexInput in = dir.openInput("ints", IOContext.DEFAULT)) {
        for (int bpv = 1; bpv <= 24; ++bpv) {
          final int offset = random().nextInt(8);
          long base = random().nextInt(1_000_000);

          long[] expected = new long[128];
          in.seek(offset);
          forUtil.decode(bpv, in, expected);
          expected[0] += base;
          for (int i = 1; i < 128; ++i) {
            expected[i] += expected[i-1];
          }

          long[] actual = new long[128];
          in.seek(offset);
          forUtil.decodeAndPrefixSum(bpv, in, base, actual);
          assertArrayEquals(expected, actual);
        }
      }
    }
  }
}
