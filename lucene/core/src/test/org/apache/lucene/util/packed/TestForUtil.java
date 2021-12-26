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
package org.apache.lucene.util.packed;

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

public class TestForUtil extends LuceneTestCase {

  static final int[] SUPPORTED_BITS_PER_VALUE = BlockWriter.SUPPORTED_BITS_PER_VALUE;

  public void testEncodeDecode() throws IOException {
    for (int iter = 0; iter < 100; iter++) {
      final int iterations = RandomNumbers.randomIntBetween(random(), 50, 1000);
      final long[] values = new long[iterations * ForUtil.BLOCK_SIZE];

      for (int i = 0; i < iterations; ++i) {
        final int bpv = SUPPORTED_BITS_PER_VALUE[TestUtil.nextInt(random(), 0, SUPPORTED_BITS_PER_VALUE.length - 1)];
        for (int j = 0; j < ForUtil.BLOCK_SIZE; ++j) {
          values[i * ForUtil.BLOCK_SIZE + j] =
                  RandomNumbers.randomLongBetween(random(), 0, PackedInts.maxValue(bpv));
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
          final int bpv = BlockWriter.unsignedBitsRequired(or);
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
          ForUtil.Decoder decoder = forUtil.decoder(bitsPerValue);
          decoder.decode(in, restored);
          assertArrayEquals(
                  Arrays.toString(restored),
                  ArrayUtil.copyOfSubArray(values, i * ForUtil.BLOCK_SIZE, (i + 1) * ForUtil.BLOCK_SIZE),
                  restored);
          assertEquals(forUtil.numBytes(bitsPerValue), in.getFilePointer() - currentFilePointer);
        }
        assertEquals(endPointer, in.getFilePointer());
        in.close();
      }

      d.close();
    }
  }
}
