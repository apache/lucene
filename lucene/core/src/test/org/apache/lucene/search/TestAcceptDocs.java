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
import java.util.Arrays;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;

public class TestAcceptDocs extends LuceneTestCase {

  public void testValidation() {
    // iterator supplier must be non-null
    expectThrows(NullPointerException.class, () -> AcceptDocs.fromIteratorSupplier(null, null, 1));

    // iterator supplier may not produce null iterators
    expectThrows(
        NullPointerException.class,
        () -> AcceptDocs.fromIteratorSupplier(() -> null, null, 1).iterator());

    // Bits length != maxDoc
    expectThrows(
        IllegalArgumentException.class, () -> AcceptDocs.fromLiveDocs(new Bits.MatchNoBits(3), 4));
  }

  public void testIteratorIgnoresDeletedDocs() throws IOException {
    int maxDoc = 5;
    int deletedDoc = 3;
    FixedBitSet liveDocs = new FixedBitSet(maxDoc);
    liveDocs.set(0, liveDocs.length());
    liveDocs.clear(deletedDoc);

    Bits liveDocsBits = liveDocs.asReadOnlyBits();

    AcceptDocs bitsAcceptDocs = AcceptDocs.fromLiveDocs(liveDocsBits, maxDoc);
    AcceptDocs iteratorAcceptDocs =
        AcceptDocs.fromIteratorSupplier(() -> DocIdSetIterator.all(maxDoc), liveDocsBits, maxDoc);

    for (AcceptDocs acceptDocs : Arrays.asList(bitsAcceptDocs, iteratorAcceptDocs)) {
      Bits acceptBits = acceptDocs.bits();
      assertEquals(maxDoc, acceptBits.length());
      for (int i = 0; i < maxDoc; ++i) {
        assertEquals(i != deletedDoc, acceptBits.get(i));
      }

      DocIdSetIterator iterator = acceptDocs.iterator();
      for (int i = 0; i < maxDoc; ++i) {
        if (i != deletedDoc) {
          assertEquals(i, iterator.nextDoc());
        }
      }
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.nextDoc());
    }
  }

  public void testIteratorIsNew() throws IOException {
    int maxDoc = 5;
    AcceptDocs bitsAcceptDocs = AcceptDocs.fromLiveDocs(null, maxDoc);
    AcceptDocs iteratorAcceptDocs =
        AcceptDocs.fromIteratorSupplier(() -> DocIdSetIterator.all(maxDoc), null, maxDoc);

    for (AcceptDocs acceptDocs : Arrays.asList(bitsAcceptDocs, iteratorAcceptDocs)) {
      DocIdSetIterator iterator = acceptDocs.iterator();
      assertEquals(-1, iterator.docID());
      iterator.nextDoc();
      iterator = acceptDocs.iterator();
      assertEquals(-1, iterator.docID());

      // Triggers lazy loading of matches into a bit set when created from an iterator
      acceptDocs.bits();

      iterator = acceptDocs.iterator();
      assertEquals(-1, iterator.docID());
      iterator.nextDoc();
      iterator = acceptDocs.iterator();
      assertEquals(-1, iterator.docID());
    }
  }

  /**
   * A {@link BitSetIterator} that counts calls to {@link #nextDoc()}. It extends {@link
   * BitSetIterator} rather than wrapping one so that the bulk {@link BitSetIterator#intoBitSet}
   * implementation remains visible to callers.
   */
  private static class CountingBitSetIterator extends BitSetIterator {

    int nextDocCalls;

    CountingBitSetIterator(BitSet bits, long cost) {
      super(bits, cost);
    }

    @Override
    public int nextDoc() {
      nextDocCalls++;
      return super.nextDoc();
    }
  }

  /**
   * Dense accept docs must be loaded through {@link DocIdSetIterator#intoBitSet}, which {@link
   * BitSetIterator} implements by copying whole words, and only then be masked with live docs via
   * {@link Bits#applyMask}. Filtering the iterator upfront would fall back to one {@link
   * DocIdSetIterator#nextDoc()} call per matching doc. Both approaches return the same bits, so
   * this can only be observed by counting calls on the source iterator.
   */
  public void testDenseIteratorIsConsumedInBulkWhenSegmentHasDeletions() throws IOException {
    int iters = atLeast(20);
    for (int iter = 0; iter < iters; ++iter) {
      int maxDoc = TestUtil.nextInt(random(), 1024, 1 << 16);

      // Match every `step`th doc, so that the cost is always >= maxDoc >> 7 and AcceptDocs loads
      // matches into a FixedBitSet.
      FixedBitSet filter = new FixedBitSet(maxDoc);
      int step = TestUtil.nextInt(random(), 1, 16);
      for (int doc = random().nextInt(step); doc < maxDoc; doc += step) {
        filter.set(doc);
      }
      int filterCardinality = filter.cardinality();
      assertTrue(filterCardinality > (maxDoc >> 7));

      // A segment that has deletions, ie. a non-null liveDocs: none, one, or many deleted docs.
      FixedBitSet liveDocs = new FixedBitSet(maxDoc);
      liveDocs.set(0, maxDoc);
      int deleteCount =
          switch (random().nextInt(3)) {
            case 0 -> 0;
            case 1 -> 1;
            default -> TestUtil.nextInt(random(), 2, maxDoc);
          };
      for (int i = 0; i < deleteCount; ++i) {
        liveDocs.clear(random().nextInt(maxDoc));
      }

      CountingBitSetIterator iterator = new CountingBitSetIterator(filter, filterCardinality);
      int[] iteratorsPulled = new int[1];
      AcceptDocs acceptDocs =
          AcceptDocs.fromIteratorSupplier(
              () -> {
                iteratorsPulled[0]++;
                return iterator;
              },
              liveDocs.asReadOnlyBits(),
              maxDoc);

      Bits acceptBits = acceptDocs.bits();

      // The bit set must be built from a single bulk pass over the source iterator.
      assertEquals(1, iteratorsPulled[0]);
      assertTrue(
          "iterator was consumed one doc at a time: "
              + iterator.nextDocCalls
              + " nextDoc() calls for "
              + filterCardinality
              + " matching docs",
          iterator.nextDocCalls <= 2);

      // Loading in bulk must not change the result.
      FixedBitSet expected = filter.clone();
      expected.and(liveDocs);
      assertEquals(expected, acceptBits);
      assertEquals(expected.cardinality(), acceptDocs.cost());
    }
  }

  /**
   * The bits exposed by {@link AcceptDocs#bits()} are always the matches of the iterator
   * intersected with live docs, on both sides of the {@code maxDoc >> 7} dense/sparse boundary and
   * whatever the number of deleted docs.
   */
  public void testRandomBitsAreMatchesIntersectedWithLiveDocs() throws IOException {
    int iters = atLeast(100);
    for (int iter = 0; iter < iters; ++iter) {
      int maxDoc = TestUtil.nextInt(random(), 1, 5000);
      int threshold = maxDoc >> 7; // AcceptDocs' dense/sparse boundary

      int targetCardinality =
          switch (random().nextInt(6)) {
            case 0 -> 0;
            case 1 -> Math.max(0, threshold - 1);
            case 2 -> threshold;
            case 3 -> threshold + 1;
            case 4 -> maxDoc;
            default -> TestUtil.nextInt(random(), 0, maxDoc);
          };
      targetCardinality = Math.min(targetCardinality, maxDoc);

      FixedBitSet filter = new FixedBitSet(maxDoc);
      for (int cardinality = 0; cardinality < targetCardinality; ) {
        if (filter.getAndSet(random().nextInt(maxDoc)) == false) {
          cardinality++;
        }
      }
      long cost = filter.cardinality();

      // None, one or many deleted docs -- and sometimes a segment with no deletions at all.
      FixedBitSet liveDocs = new FixedBitSet(maxDoc);
      liveDocs.set(0, maxDoc);
      Bits liveDocsBits;
      if (random().nextInt(5) == 0) {
        liveDocsBits = null; // no deletions
      } else {
        int deleteCount =
            switch (random().nextInt(3)) {
              case 0 -> 0;
              case 1 -> 1;
              default -> TestUtil.nextInt(random(), 1, maxDoc);
            };
        for (int i = 0; i < deleteCount; ++i) {
          liveDocs.clear(random().nextInt(maxDoc));
        }
        liveDocsBits = liveDocs.asReadOnlyBits();
      }

      FixedBitSet expected = filter.clone();
      expected.and(liveDocs);

      // Half of the time, hide the fact that the source is a BitSetIterator behind a
      // FilterDocIdSetIterator, which uses the default impl of intoBitSet, so that the generic
      // path gets the same coverage as the optimized one.
      boolean opaque = random().nextBoolean();
      AcceptDocs acceptDocs =
          AcceptDocs.fromIteratorSupplier(
              () -> {
                DocIdSetIterator iterator = new BitSetIterator(filter, cost);
                return opaque ? new FilterDocIdSetIterator(iterator) : iterator;
              },
              liveDocsBits,
              maxDoc);

      Bits acceptBits = acceptDocs.bits();
      assertEquals(maxDoc, acceptBits.length());
      for (int doc = 0; doc < maxDoc; ++doc) {
        assertEquals(
            "doc=" + doc + " maxDoc=" + maxDoc + " cardinality=" + cost,
            expected.get(doc),
            acceptBits.get(doc));
      }
      assertEquals(expected.cardinality(), acceptDocs.cost());
      if (acceptBits instanceof FixedBitSet fixedBitSet) {
        assertEquals(expected, fixedBitSet);
      }

      // The iterator must agree with the bits.
      DocIdSetIterator iterator = acceptDocs.iterator();
      for (int doc = expected.nextSetBit(0);
          doc != DocIdSetIterator.NO_MORE_DOCS;
          doc = doc + 1 >= maxDoc ? DocIdSetIterator.NO_MORE_DOCS : expected.nextSetBit(doc + 1)) {
        assertEquals(doc, iterator.nextDoc());
      }
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.nextDoc());
    }
  }
}
