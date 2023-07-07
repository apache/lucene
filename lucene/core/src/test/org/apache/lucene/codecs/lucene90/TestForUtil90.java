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

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.internal.vectorization.ForUtil90;
import org.apache.lucene.internal.vectorization.VectorizationProvider;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.packed.PackedInts;

public class TestForUtil90 extends LuceneTestCase {

  private static final VectorizationProvider VPROVIDER = VectorizationProvider.getInstance();

  public void testEncodeDecode() throws IOException {
    final int iterations = RandomNumbers.randomIntBetween(random(), 50, 1000);
    final int[] values = new int[iterations * ForUtil90.BLOCK_SIZE];

    for (int i = 0; i < iterations; ++i) {
      final int bpv = TestUtil.nextInt(random(), 1, 31);
      for (int j = 0; j < ForUtil90.BLOCK_SIZE; ++j) {
        values[i * ForUtil90.BLOCK_SIZE + j] =
            RandomNumbers.randomIntBetween(random(), 0, (int) PackedInts.maxValue(bpv));
      }
    }

    final Directory d = new ByteBuffersDirectory();
    final long endPointer;

    {
      // encode
      IndexOutput out = d.createOutput("test.bin", IOContext.DEFAULT);
      final ForUtil90 forUtil = VPROVIDER.newForUtil90();

      for (int i = 0; i < iterations; ++i) {
        int[] source = new int[ForUtil90.BLOCK_SIZE];
        int maxValue = 0;
        for (int j = 0; j < ForUtil90.BLOCK_SIZE; ++j) {
          source[j] = values[i * ForUtil90.BLOCK_SIZE + j];
          maxValue |= source[j];
        }
        final int bpv = PackedInts.bitsRequired(maxValue);
        out.writeByte((byte) bpv);
        forUtil.encode(source, bpv, out);
      }
      endPointer = out.getFilePointer();
      out.close();
    }

    {
      // decode
      IndexInput in = d.openInput("test.bin", IOContext.READONCE);
      final ForUtil90 forUtil = VPROVIDER.newForUtil90();
      for (int i = 0; i < iterations; ++i) {
        final int bitsPerValue = in.readByte();
        final long currentFilePointer = in.getFilePointer();
        final int[] restored = new int[ForUtil90.BLOCK_SIZE];
        forUtil.decode(bitsPerValue, in, restored);
        assertArrayEquals(
            Arrays.toString(restored),
            ArrayUtil.copyOfSubArray(
                values, i * ForUtil90.BLOCK_SIZE, (i + 1) * ForUtil90.BLOCK_SIZE),
            restored);
        assertEquals(forUtil.numBytes(bitsPerValue), in.getFilePointer() - currentFilePointer);
      }
      assertEquals(endPointer, in.getFilePointer());
      in.close();
    }

    d.close();
  }
}
