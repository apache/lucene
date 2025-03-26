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

import java.io.IOException;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;

public class TestDISIDocIdStream extends LuceneTestCase {

  private static FixedBitSet randomBitSet() {
    FixedBitSet set = new FixedBitSet(100);
    for (int i = TestUtil.nextInt(random(), 0, 5);
        i < set.length();
        i += TestUtil.nextInt(random(), 1, 5)) {
      set.set(i);
    }
    return set;
  }

  public void testForEach() throws IOException {
    FixedBitSet bitSet = randomBitSet();
    BitSetIterator iterator = new BitSetIterator(bitSet, bitSet.approximateCardinality());
    iterator.advance(42);
    DocIdStream stream = new DISIDocIdStream(iterator, 80, new FixedBitSet(40));

    BitSetIterator expected = new BitSetIterator(bitSet, bitSet.approximateCardinality());
    expected.advance(42);

    stream.forEach(
        doc -> {
          assertEquals(expected.docID(), doc);
          assertTrue(doc < 80);
          expected.nextDoc();
        });
    assertTrue(expected.docID() >= 80);
  }

  public void testCount() throws IOException {
    FixedBitSet bitSet = randomBitSet();
    BitSetIterator iterator = new BitSetIterator(bitSet, bitSet.approximateCardinality());
    iterator.advance(42);
    DocIdStream stream = new DISIDocIdStream(iterator, 80, new FixedBitSet(40));
    assertEquals(bitSet.cardinality(42, 80), stream.count());
  }

  public void testForEachUpTo() throws IOException {
    FixedBitSet bitSet = randomBitSet();
    BitSetIterator iterator = new BitSetIterator(bitSet, bitSet.approximateCardinality());
    iterator.advance(42);
    DocIdStream stream = new DISIDocIdStream(iterator, 80, new FixedBitSet(40));

    BitSetIterator expected = new BitSetIterator(bitSet, bitSet.approximateCardinality());
    expected.advance(42);

    assertTrue(stream.mayHaveRemaining());
    stream.forEach(40, _ -> fail());

    assertTrue(stream.mayHaveRemaining());
    stream.forEach(
        60,
        doc -> {
          assertEquals(expected.docID(), doc);
          assertTrue(doc < 60);
          expected.nextDoc();
        });
    assertTrue(expected.docID() >= 60);

    assertTrue(stream.mayHaveRemaining());
    stream.forEach(
        120,
        doc -> {
          assertEquals(expected.docID(), doc);
          assertTrue(doc < 80);
          expected.nextDoc();
        });
    assertTrue(expected.docID() >= 80);

    assertFalse(stream.mayHaveRemaining());
  }

  public void testCountUpTo() throws IOException {
    FixedBitSet bitSet = randomBitSet();
    BitSetIterator iterator = new BitSetIterator(bitSet, bitSet.approximateCardinality());
    iterator.advance(42);
    DocIdStream stream = new DISIDocIdStream(iterator, 80, new FixedBitSet(40));

    assertTrue(stream.mayHaveRemaining());
    assertEquals(0, stream.count(40));

    assertTrue(stream.mayHaveRemaining());
    assertEquals(bitSet.cardinality(42, 60), stream.count(60));

    assertTrue(stream.mayHaveRemaining());
    assertEquals(bitSet.cardinality(60, 80), stream.count(120));

    assertFalse(stream.mayHaveRemaining());
  }

  public void testMixForEachCountUpTo() throws IOException {
    FixedBitSet bitSet = randomBitSet();
    BitSetIterator iterator = new BitSetIterator(bitSet, bitSet.approximateCardinality());
    iterator.advance(42);
    DocIdStream stream = new DISIDocIdStream(iterator, 80, new FixedBitSet(40));

    BitSetIterator expected = new BitSetIterator(bitSet, bitSet.approximateCardinality());

    assertTrue(stream.mayHaveRemaining());
    stream.forEach(40, _ -> fail());

    assertTrue(stream.mayHaveRemaining());
    assertEquals(bitSet.cardinality(42, 50), stream.count(50));

    assertTrue(stream.mayHaveRemaining());
    expected.advance(50);
    stream.forEach(
        60,
        doc -> {
          assertEquals(expected.docID(), doc);
          assertTrue(doc < 60);
          expected.nextDoc();
        });
    assertTrue(expected.docID() >= 60);

    assertTrue(stream.mayHaveRemaining());
    assertEquals(bitSet.cardinality(60, 70), stream.count(70));

    assertTrue(stream.mayHaveRemaining());
    expected.advance(70);
    stream.forEach(
        120,
        doc -> {
          assertEquals(expected.docID(), doc);
          assertTrue(doc < 80);
          expected.nextDoc();
        });
    assertTrue(expected.docID() >= 80);

    assertFalse(stream.mayHaveRemaining());
  }
}
