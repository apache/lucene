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
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.packed.PackedInts;

public class TestGroupVInt extends LuceneTestCase {

  public void testEncodeDecode() throws IOException {
    long[] values = new long[ForUtil.BLOCK_SIZE];
    long[] restored = new long[ForUtil.BLOCK_SIZE];
    final int iterations = atLeast(100);

    final GroupVIntWriter w = new GroupVIntWriter();
    byte[] encoded = new byte[(int) (Integer.BYTES * ForUtil.BLOCK_SIZE * 1.25)];

    for (int i = 0; i < iterations; i++) {
      final int bpv = TestUtil.nextInt(random(), 1, 31);
      final int numValues = TestUtil.nextInt(random(), 1, ForUtil.BLOCK_SIZE);

      // encode
      for (int j = 0; j < numValues; j++) {
        values[j] = RandomNumbers.randomIntBetween(random(), 0, (int) PackedInts.maxValue(bpv));
      }
      w.writeValues(new ByteArrayDataOutput(encoded), values, numValues);

      // decode
      GroupVIntReader.readValues(new ByteArrayDataInput(encoded), restored, numValues);
      assertArrayEquals(
          ArrayUtil.copyOfSubArray(values, 0, numValues),
          ArrayUtil.copyOfSubArray(restored, 0, numValues));
    }
  }
}
