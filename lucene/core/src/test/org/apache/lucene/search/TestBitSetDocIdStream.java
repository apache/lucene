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
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;

public class TestBitSetDocIdStream extends LuceneTestCase {

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
    int base = 42;
    BitSetDocIdStream stream = new BitSetDocIdStream(bitSet, base);
    BitSetIterator iterator = new BitSetIterator(bitSet, bitSet.approximateCardinality());

    stream.forEach(doc -> {
      assertEquals(base + iterator.nextDoc(), doc);
    });
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.nextDoc());
  }

  public void testCount() throws IOException {
    FixedBitSet bitSet = randomBitSet();
    int base = 42;
    BitSetDocIdStream stream = new BitSetDocIdStream(bitSet, base);
    assertEquals(bitSet.cardinality(), stream.count());
  }

  public void testIntoArray() {
    FixedBitSet bitSet = randomBitSet();
    BitSetIterator bitSetIterator = new BitSetIterator(bitSet, 0L);
    int base = 42;
    BitSetDocIdStream stream = new BitSetDocIdStream(bitSet, base);
    int[] array = new int[16];

    int o = array.length;
    int count = array.length;

    for (int expected = bitSetIterator.nextDoc();
        expected != DocIdSetIterator.NO_MORE_DOCS;
        expected = bitSetIterator.nextDoc()) {
      if (o == count) {
        count = stream.intoArray(array);
        o = 0;
      }
      assertEquals(expected + base, array[o++]);
    }
    assertEquals(count, o);
    assertEquals(0, stream.intoArray(array));
  }

  public void testIntoBitSet() throws IOException {
    FixedBitSet bitSet = randomBitSet();
    int base = 42;
    BitSetDocIdStream stream = new BitSetDocIdStream(bitSet, base);
    FixedBitSet dest = new FixedBitSet(bitSet.length());

    stream.intoBitSet(DocIdSetIterator.NO_MORE_DOCS, dest, base);

    assertEquals(bitSet, dest);
    assertFalse(stream.mayHaveRemaining());
  }

  public void testForEachUpTo() throws IOException {
    FixedBitSet bitSet = randomBitSet();
    int base = 42;
    BitSetDocIdStream stream = new BitSetDocIdStream(bitSet, base);
    BitSetIterator iterator = new BitSetIterator(bitSet, bitSet.approximateCardinality());

    assertTrue(stream.mayHaveRemaining());
    stream.forEach(20, _ -> fail());

    assertTrue(stream.mayHaveRemaining());
    stream.forEach(
        100,
        doc -> {
          assertEquals(base + iterator.nextDoc(), doc);
        });
    assertEquals(bitSet.prevSetBit(99 - base), iterator.docID());

    assertTrue(stream.mayHaveRemaining());
    stream.forEach(
        200,
        doc -> {
          assertEquals(base + iterator.nextDoc(), doc);
        });
    assertEquals(bitSet.prevSetBit(bitSet.length() - 1), iterator.docID());

    assertFalse(stream.mayHaveRemaining());
  }

  public void testCountUpTo() throws IOException {
    FixedBitSet bitSet = randomBitSet();
    int base = 42;
    BitSetDocIdStream stream = new BitSetDocIdStream(bitSet, base);

    assertTrue(stream.mayHaveRemaining());
    assertEquals(0, stream.count(20));

    assertTrue(stream.mayHaveRemaining());
    assertEquals(bitSet.cardinality(0, 100 - 42), stream.count(100));

    assertTrue(stream.mayHaveRemaining());
    assertEquals(bitSet.cardinality(100 - 42, bitSet.length()), stream.count(200));

    assertFalse(stream.mayHaveRemaining());
  }

  public void testIntoArrayUpTo() throws IOException {
    FixedBitSet bitSet = randomBitSet();
    BitSetIterator bitSetIterator = new BitSetIterator(bitSet, 0L);
    bitSetIterator.nextDoc();
    int base = 42;
    BitSetDocIdStream stream = new BitSetDocIdStream(bitSet, base);
    int[] array = new int[16];

    int o = array.length;
    int count = array.length;

    for (int upTo = 0; upTo < bitSet.length(); ) {
      int newUpTo = Math.min(upTo + random().nextInt(40), 100);

      for (int expected = bitSetIterator.docID();
          expected < newUpTo;
          expected = bitSetIterator.nextDoc()) {
        if (o == count) {
          count = stream.intoArray(base + newUpTo, array);
          o = 0;
        }
        assertEquals(expected + base, array[o++]);
      }
      assertEquals(count, o);
      assertEquals(0, stream.intoArray(base + newUpTo, array));

      upTo = newUpTo;
    }
  }

  public void testMixForEachCountUpTo() throws IOException {
    FixedBitSet bitSet = randomBitSet();
    int base = 42;
    BitSetDocIdStream stream = new BitSetDocIdStream(bitSet, base);
    BitSetIterator iterator = new BitSetIterator(bitSet, bitSet.approximateCardinality());

    assertTrue(stream.mayHaveRemaining());
    stream.forEach(20, _ -> fail());

    assertTrue(stream.mayHaveRemaining());
    assertEquals(0, stream.count(30));

    assertTrue(stream.mayHaveRemaining());
    stream.forEach(
        100,
        doc -> {
          assertEquals(base + iterator.nextDoc(), doc);
        });
    assertEquals(bitSet.prevSetBit(99 - base), iterator.docID());

    assertTrue(stream.mayHaveRemaining());
    assertEquals(bitSet.cardinality(100 - 42, 120 - 42), stream.count(120));

    assertTrue(stream.mayHaveRemaining());
    iterator.advance(bitSet.prevSetBit(120 - 1 - 42));
    stream.forEach(
        200,
        doc -> {
          assertEquals(base + iterator.nextDoc(), doc);
        });
    assertEquals(bitSet.prevSetBit(bitSet.length() - 1), iterator.docID());

    assertFalse(stream.mayHaveRemaining());
  }

  public void testFork() throws IOException {
    FixedBitSet bitSet = randomBitSet();
    int base = 42;
    BitSetDocIdStream stream = new BitSetDocIdStream(bitSet, base);
    int upTo = random().nextInt(bitSet.length() + 1);
    assertEquals(bitSet.cardinality(0, upTo), stream.count(base + upTo));

    List<Integer> expected = new ArrayList<>();
    for (int i = upTo; i < bitSet.length(); ++i) {
      if (bitSet.get(i)) {
        expected.add(base + i);
      }
    }

    DocIdStream fork1 = stream.fork();
    DocIdStream fork2 = fork1.fork();
    assertNotNull(fork1);
    assertNotNull(fork2);

    assertEquals(expected, toList(fork1));
    assertFalse(fork1.mayHaveRemaining());
    assertEquals(expected.size(), fork2.count());
    assertFalse(fork2.mayHaveRemaining());

    // Consuming forks doesn't advance the original stream
    assertEquals(expected, toList(stream));
    assertFalse(stream.mayHaveRemaining());

    // Exhausted streams can be forked too
    DocIdStream fork3 = stream.fork();
    assertFalse(fork3.mayHaveRemaining());
    assertEquals(0, fork3.count());
  }

  private static List<Integer> toList(DocIdStream stream) throws IOException {
    List<Integer> docs = new ArrayList<>();
    stream.forEach(docs::add);
    return docs;
  }
}
