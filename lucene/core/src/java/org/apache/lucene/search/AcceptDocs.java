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
import org.apache.lucene.index.LeafReader;
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
  public abstract Bits bits() throws IOException;

  /**
   * Get an iterator of accepted docs.
   *
   * @return DocIdSetIterator for sequential access
   * @throws IOException if an I/O error occurs
   */
  public abstract DocIdSetIterator iterator() throws IOException;

  /**
   * Return an approximation of the number of accepted documents.
   *
   * @return approximate cost
   */
  public abstract int cost();

  /**
   * Create AcceptDocs from a {@link Bits} instance representing live documents. A {@code null}
   * instance is interpreted as matching all documents, like in {@link LeafReader#getLiveDocs()}.
   *
   * @param bits the Bits instance for random access
   * @param maxDoc the number of documents in the reader
   * @return AcceptDocs wrapping the Bits
   */
  public static AcceptDocs fromLiveDocs(Bits bits, int maxDoc) {
    if (bits instanceof BitSet bitSet) {
      return new BitSetAcceptDocs(bitSet);
    }
    return new BitsAcceptDocs(bits, maxDoc);
  }

  /**
   * Create AcceptDocs from a DocIdSetIterator, optionally filtered by live documents.
   *
   * @param iterator a DocIdSetIterator iterator
   * @param liveDocs Bits representing live documents, or {@code null} if no deleted docs
   * @param maxDoc the number of documents in the reader
   * @return AcceptDocs wrapping the iterator
   */
  public static AcceptDocs fromIterator(DocIdSetIterator iterator, Bits liveDocs, int maxDoc)
      throws IOException {
    BitSet bitSet = createBitSet(iterator, liveDocs, maxDoc);
    return new BitSetAcceptDocs(bitSet);
  }

  private static BitSet createBitSet(DocIdSetIterator iterator, Bits liveDocs, int maxDoc)
      throws IOException {
    if (liveDocs == null && iterator instanceof BitSetIterator bitSetIterator) {
      // If we already have a BitSet and no deletions, reuse the BitSet
      return bitSetIterator.getBitSet();
    } else {
      int threshold = maxDoc >> 7; // same as BitSet#of
      if (iterator.cost() >= threshold) {
        // take advantage of Disi#intoBitset and Bits#applyMask
        FixedBitSet bitSet = new FixedBitSet(maxDoc);
        bitSet.or(iterator);
        if (liveDocs != null) {
          liveDocs.applyMask(bitSet, 0);
        }
        return bitSet;
      } else {
        FilteredDocIdSetIterator filterIterator =
            new FilteredDocIdSetIterator(iterator) {
              @Override
              protected boolean match(int doc) {
                return liveDocs == null || liveDocs.get(doc);
              }
            };
        return BitSet.of(filterIterator, maxDoc); // create a sparse bitset
      }
    }
  }

  /** Impl backed by Bits, expected to be somewhat dense. */
  private static class BitsAcceptDocs extends AcceptDocs {
    private final Bits bits;
    private final int maxDoc;

    BitsAcceptDocs(Bits bits, int maxDoc) {
      this.bits = bits;
      this.maxDoc = maxDoc;
    }

    @Override
    public Bits bits() {
      return bits;
    }

    @Override
    public DocIdSetIterator iterator() {
      DocIdSetIterator iterator = DocIdSetIterator.all(maxDoc);
      if (bits != null) {
        iterator =
            new FilteredDocIdSetIterator(iterator) {
              @Override
              protected boolean match(int doc) {
                return bits.get(doc);
              }
            };
      }
      return iterator;
    }

    @Override
    public int cost() {
      // We have no better estimate. This should be ok in practice since background merges should
      // keep the number of deletes under control (< 20% by default).
      return maxDoc;
    }
  }

  /** Impl backed by a {@link BitSet} */
  private static class BitSetAcceptDocs extends AcceptDocs {

    private final BitSet bitSet;
    private int cardinality = -1;

    BitSetAcceptDocs(BitSet bitSet) {
      this.bitSet = bitSet;
    }

    @Override
    public Bits bits() throws IOException {
      return bitSet;
    }

    @Override
    public DocIdSetIterator iterator() throws IOException {
      return new BitSetIterator(bitSet, cost());
    }

    @Override
    public synchronized int cost() {
      if (cardinality == -1) {
        cardinality = bitSet.cardinality();
      }
      return cardinality;
    }
  }
}
