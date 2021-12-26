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
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

import java.io.IOException;

public class TestBlockReader extends LuceneTestCase {

  static final int[] SUPPORTED_BITS_PER_VALUE = BlockWriter.SUPPORTED_BITS_PER_VALUE;

  public void testWriteAndRead() throws IOException {
    for (int iter = 0; iter < 100; iter++) {
      final int valueCount = RandomNumbers.randomIntBetween(random(), 5, 100000);
      final long[] values = new long[valueCount];

      final int bpv = SUPPORTED_BITS_PER_VALUE[TestUtil.nextInt(random(), 0, SUPPORTED_BITS_PER_VALUE.length - 1)];
      for (int i = 0; i < valueCount; ++i) {
        values[i] = RandomNumbers.randomLongBetween(random(), 0, PackedInts.maxValue(bpv));
      }

      final Directory d = new ByteBuffersDirectory();
      IndexOutput output = d.createOutput("test.bin", IOContext.DEFAULT);
      BlockWriter blockWriter = new BlockWriter(output, bpv);
      for (long l : values) {
        blockWriter.add(l);
      }
      blockWriter.finish();
      output.close();

      IndexInput input = d.openInput("test.bin", IOContext.READONCE);
      BlockReader blockReader = new BlockReader(input, bpv, valueCount);
      for (int i = 0; i < valueCount; i++) {
        assertEquals(values[i], blockReader.get(i));
      }
      input.close();
      d.close();
    }
  }
}
