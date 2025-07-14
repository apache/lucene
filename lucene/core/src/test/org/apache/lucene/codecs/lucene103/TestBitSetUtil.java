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
package org.apache.lucene.codecs.lucene103;

import static org.apache.lucene.codecs.lucene103.Lucene103PostingsFormat.BLOCK_SIZE;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.FixedBitSet;

public class TestBitSetUtil extends LuceneTestCase {

  public void testDenseBitsetToArray() throws Exception {
    for (int iter = 0; iter < 1000; iter++) {
      FixedBitSet bitSet = new FixedBitSet(BLOCK_SIZE + random().nextInt(200));
      while (bitSet.cardinality() < BLOCK_SIZE) {
        bitSet.set(random().nextInt(bitSet.length()));
      }

      int[] array = new int[BLOCK_SIZE + 1];
      int from, to, size;
      if (random().nextBoolean()) {
        from = 0;
        to = bitSet.length();
      } else {
        from = random().nextInt(bitSet.length());
        to = from + random().nextInt(bitSet.length() - from);
      }

      int base = random().nextInt(1000);
      size = BitSetUtil.denseBitsetToArray(bitSet, from, to, base, array);

      assertEquals(bitSet.cardinality(from, to), size);

      int[] index = new int[] {0};
      bitSet.forEach(from, to, base, bit -> assertEquals(bit, array[index[0]++]));
      assertEquals(size, index[0]);
    }
  }
}
