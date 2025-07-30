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
import java.util.List;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;

/**
 * Higher-level abstraction for document acceptance filtering. Can be consumed in either
 * random-access (Bits) or sequential (DocIdSetIterator) pattern.
 *
 * @lucene.experimental
 */
public abstract class AcceptDocs {

  /**
   * Random access to the accepted documents.
   *
   * @return Bits instance for random access, or null if not available
   * @throws IOException if an I/O error occurs
   */
  public abstract Bits getBits() throws IOException;

  /**
   * Get an iterator of accepted docs.
   *
   * @return DocIdSetIterator for sequential access
   * @throws IOException if an I/O error occurs
   */
  public abstract DocIdSetIterator getIterator() throws IOException;

  /**
   * Return an approximation of the number of accepted documents.
   *
   * @return approximate cost
   */
  public abstract long cost();

  /**
   * Create AcceptDocs from a Bits instance.
   *
   * @param bits the Bits instance for random access
   * @return AcceptDocs wrapping the Bits
   */
  public static AcceptDocs fromBits(Bits bits) {
    if (bits == null) {
      return new AcceptAllDocs();
    }
    return new BitsAcceptDocs(bits);
  }

  /**
   * Create AcceptDocs from a DocIdSetIterator.
   *
   * @param iterator a DocIdSetIterator iterator
   * @return AcceptDocs wrapping the iterator
   */
  public static AcceptDocs fromIterator(DocIdSetIterator iterator, int maxDoc) {
    if (iterator == null) {
      return new AcceptAllDocs();
    }
    return new IteratorAcceptDocs(iterator, maxDoc);
  }

  /**
   * Create AcceptDocs from a conjunction of DocIdSetIterators
   *
   * @param disiList list of required iterators
   * @return AcceptDocs wrapping the conjunction
   */
  public static AcceptDocs fromConjunction(List<DocIdSetIterator> disiList, int maxDoc) {
    if (disiList.isEmpty()) {
      return new AcceptAllDocs();
    }
    if (disiList.size() == 1) {
      return new IteratorAcceptDocs(disiList.getFirst(), maxDoc);
    }
    return new IteratorAcceptDocs(ConjunctionDISI.createConjunction(disiList, List.of()), maxDoc);
  }

  /** Impl that accepts all documents. */
  private static class AcceptAllDocs extends AcceptDocs {
    @Override
    public Bits getBits() {
      return null;
    }

    @Override
    public DocIdSetIterator getIterator() {
      return DocIdSetIterator.all(Integer.MAX_VALUE);
    }

    @Override
    public long cost() {
      return Integer.MAX_VALUE;
    }
  }

  /** Impl backed by Bits */
  private static class BitsAcceptDocs extends AcceptDocs {
    private final Bits bits;
    private DocIdSetIterator iterator;
    private final long cost;

    BitsAcceptDocs(Bits bits) {
      this.bits = bits;
      // estimated cost
      if (bits instanceof BitSet bitSet) {
        this.cost = bitSet.cardinality();
      } else {
        this.cost = bits.length();
      }
    }

    @Override
    public Bits getBits() {
      return bits;
    }

    @Override
    public DocIdSetIterator getIterator() {
      if (iterator == null) {
        if (bits instanceof BitSet bitSet) {
          iterator = new BitSetIterator(bitSet, cost);
        } else {
          FixedBitSet bitSet = new FixedBitSet(bits.length());
          for (int i = 0; i < bits.length(); i++) {
            if (bits.get(i)) {
              bitSet.set(i);
            }
          }
          iterator = new BitSetIterator(bitSet, bitSet.cardinality());
        }
      }
      return iterator;
    }

    @Override
    public long cost() {
      return cost;
    }
  }

  /** Impl backed by DocIdSetIterator */
  private static class IteratorAcceptDocs extends AcceptDocs {
    private final DocIdSetIterator iterator;
    private final long cost;
    private Bits bits;
    private final int maxDoc;

    IteratorAcceptDocs(DocIdSetIterator iterator, int maxDoc) {
      this.iterator = iterator;
      this.cost = iterator.cost();
      this.maxDoc = maxDoc;
    }

    @Override
    public Bits getBits() throws IOException {
      if (bits == null) {
        bits = BitSet.of(iterator, maxDoc);
      }
      return bits;
    }

    @Override
    public DocIdSetIterator getIterator() {
      return iterator;
    }

    @Override
    public long cost() {
      return cost;
    }
  }
}
