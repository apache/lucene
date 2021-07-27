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
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.packed.PackedInts;

public class TestDocValuesForUtil extends LuceneTestCase {

  public void testEncodeDecode() throws IOException {
    final int iterations = RandomNumbers.randomIntBetween(random(), 50, 1000);
    final long[] values = new long[iterations * DocValuesForUtil.BLOCK_SIZE];
    final int[] bpvs = new int[iterations];

    for (int i = 0; i < iterations; ++i) {
      final int bpv = TestUtil.nextInt(random(), 1, 64);
      bpvs[i] = bpv;
      for (int j = 0; j < DocValuesForUtil.BLOCK_SIZE; ++j) {
        values[i * DocValuesForUtil.BLOCK_SIZE + j] =
            bpv == 64
                ? random().nextLong()
                : TestUtil.nextLong(random(), 0, PackedInts.maxValue(bpv));
      }
    }

    final Directory d = new ByteBuffersDirectory();
    final long endPointer;

    {
      // encode
      IndexOutput out = d.createOutput("test.bin", IOContext.DEFAULT);
      final DocValuesForUtil forUtil = new DocValuesForUtil();

      for (int i = 0; i < iterations; ++i) {
        long[] source = new long[DocValuesForUtil.BLOCK_SIZE];
        for (int j = 0; j < DocValuesForUtil.BLOCK_SIZE; ++j) {
          source[j] = values[i * DocValuesForUtil.BLOCK_SIZE + j];
        }
        out.writeByte((byte) bpvs[i]);
        forUtil.encode(source, bpvs[i], out);
      }
      endPointer = out.getFilePointer();
      out.close();
    }

    {
      // decode
      IndexInput in = d.openInput("test.bin", IOContext.READONCE);
      final DocValuesForUtil forUtil = new DocValuesForUtil();
      final long[] restored = new long[DocValuesForUtil.BLOCK_SIZE];
      for (int i = 0; i < iterations; ++i) {
        final int bitsPerValue = in.readByte();
        forUtil.decode(bitsPerValue, in, restored);
        assertArrayEquals(
            Arrays.toString(restored),
            ArrayUtil.copyOfSubArray(
                values, i * DocValuesForUtil.BLOCK_SIZE, (i + 1) * DocValuesForUtil.BLOCK_SIZE),
            restored);
      }
      assertEquals(endPointer, in.getFilePointer());
      in.close();
    }

    d.close();
  }
}
