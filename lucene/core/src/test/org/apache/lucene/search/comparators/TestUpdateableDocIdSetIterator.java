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
package org.apache.lucene.search.comparators;

import java.io.IOException;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.FixedBitSet;

public class TestUpdateableDocIdSetIterator extends LuceneTestCase {

  public void testNextDoc() throws IOException {
    UpdateableDocIdSetIterator iterator = new UpdateableDocIdSetIterator();
    iterator.update(DocIdSetIterator.range(10, 20));
    assertEquals(10, iterator.nextDoc());
    iterator.update(DocIdSetIterator.range(10, 12));
    assertEquals(11, iterator.nextDoc());
    DocIdSetIterator in = DocIdSetIterator.range(10, 15);
    in.advance(14);
    iterator.update(in);
    assertEquals(14, iterator.nextDoc());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.nextDoc());
  }

  public void testAdvance() throws IOException {
    UpdateableDocIdSetIterator iterator = new UpdateableDocIdSetIterator();
    iterator.update(DocIdSetIterator.range(10, 20));
    assertEquals(12, iterator.advance(12));
    iterator.update(DocIdSetIterator.range(10, 15));
    assertEquals(13, iterator.advance(13));
    DocIdSetIterator in = DocIdSetIterator.range(10, 20);
    in.advance(15);
    iterator.update(in);
    assertEquals(15, iterator.advance(15));
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.advance(20));
  }

  public void testIntoBitSet() throws IOException {
    UpdateableDocIdSetIterator iterator = new UpdateableDocIdSetIterator();
    iterator.update(DocIdSetIterator.range(10, 18));
    assertEquals(10, iterator.nextDoc());
    FixedBitSet bitSet = new FixedBitSet(100);
    iterator.intoBitSet(15, bitSet, 5);
    assertEquals(15, iterator.docID());
    FixedBitSet expected = new FixedBitSet(100);
    expected.set(5, 10);
    assertEquals(expected, bitSet);

    iterator.update(DocIdSetIterator.range(12, 25));
    bitSet.clear();
    iterator.intoBitSet(20, bitSet, 8);
    assertEquals(20, iterator.docID());
    expected.clear();
    expected.set(7, 12);
    assertEquals(expected, bitSet);

    DocIdSetIterator in = DocIdSetIterator.range(15, 25);
    in.advance(23);
    iterator.update(in);
    bitSet.clear();
    iterator.intoBitSet(30, bitSet, 10);
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.docID());
    expected.clear();
    expected.set(13, 15);
    assertEquals(expected, bitSet);
  }

  public void testDocIDRunEnd() throws IOException {
    UpdateableDocIdSetIterator iterator = new UpdateableDocIdSetIterator();
    iterator.update(DocIdSetIterator.range(10, 18));
    assertEquals(10, iterator.nextDoc());
    assertEquals(18, iterator.docIDRunEnd());
    assertEquals(10, iterator.docID()); // no side effect

    iterator.update(DocIdSetIterator.range(8, 25));
    assertEquals(25, iterator.docIDRunEnd());
    assertEquals(10, iterator.docID()); // no side effect

    DocIdSetIterator in = DocIdSetIterator.range(5, 25);
    in.advance(12);
    iterator.update(in);
    assertEquals(11, iterator.docIDRunEnd());
    assertEquals(10, iterator.docID()); // no side effect
    assertEquals(12, iterator.nextDoc());
  }
}
