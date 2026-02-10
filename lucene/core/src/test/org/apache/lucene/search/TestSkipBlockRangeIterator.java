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
package org.apache.lucene.search;

import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.util.FixedBitSet;

public class TestSkipBlockRangeIterator extends BaseDocValuesSkipperTests {

  public void testSkipBlockRangeIterator() throws Exception {

    DocValuesSkipper skipper = docValuesSkipper(10, 20, true);
    SkipBlockRangeIterator it = new SkipBlockRangeIterator(skipper, 10, 20);

    assertEquals(0, it.nextDoc());
    assertEquals(256, it.docIDRunEnd());
    assertEquals(100, it.advance(100));
    assertEquals(768, it.advance(300));
    assertEquals(1024, it.docIDRunEnd());
    assertEquals(1100, it.advance(1100));
    assertEquals(1280, it.docIDRunEnd());
    assertEquals(1792, it.advance(1500));
    assertEquals(2048, it.docIDRunEnd());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.advance(2050));
  }

  public void testIntoBitSet() throws Exception {
    DocValuesSkipper skipper = docValuesSkipper(10, 20, true);
    SkipBlockRangeIterator it = new SkipBlockRangeIterator(skipper, 10, 20);
    assertEquals(768, it.advance(300));
    FixedBitSet bitSet = new FixedBitSet(2048);
    it.intoBitSet(1500, bitSet, 768);

    FixedBitSet expected = new FixedBitSet(2048);
    expected.set(0, 512);

    assertEquals(expected, bitSet);
  }
}
