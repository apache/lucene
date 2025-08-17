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
   * <p><b>NOTE</b>: This must not be called after {@link #iterator()}.
   *
   * @return Bits instance for random access, or null if all documents are accepted
   * @throws IOException if an I/O error occurs
   */
  public abstract Bits bits() throws IOException;

  /**
   * Get an iterator of accepted docs.
   *
   * <p><b>NOTE</b>: This method doesn't return a new iterator, but a reference to a shared
   * instance.
   *
   * @return DocIdSetIterator for sequential access
   * @throws IOException if an I/O error occurs
   */
  public abstract DocIdSetIterator iterator() throws IOException;

  /**
   * Return an approximation of the number of accepted documents. This is typically useful to decide
   * whether to consume these accept docs using random access ({@link #bits()}) or sequential access
   * ({@link #iterator()}).
   *
   * <p><b>NOTE</b>: This must not be called after {@link #iterator()}.
   *
   * @return approximate cost
   */
  public abstract int cost() throws IOException;

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
      DocIdSetIterator iterator = new BitSetIterator(bitSet, bitSet.approximateCardinality());
      return new DocIdSetIteratorAcceptDocs(iterator, null, maxDoc);
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
    return new DocIdSetIteratorAcceptDocs(iterator, liveDocs, maxDoc);
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
        if (liveDocs != null) {
          iterator =
              new FilteredDocIdSetIterator(iterator) {
                @Override
                protected boolean match(int doc) {
                  return liveDocs.get(doc);
                }
              };
        }
        return BitSet.of(iterator, maxDoc); // create a sparse bitset
      }
    }
  }

  /** Impl backed by Bits, expected to be somewhat dense. */
  private static class BitsAcceptDocs extends AcceptDocs {
    private final Bits bits;
    private final DocIdSetIterator iterator;
    private final int maxDoc;

    BitsAcceptDocs(Bits bits, int maxDoc) {
      this.bits = bits;
      this.maxDoc = maxDoc;
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
      this.iterator = iterator;
    }

    @Override
    public Bits bits() {
      return bits;
    }

    @Override
    public DocIdSetIterator iterator() {
      return iterator;
    }

    @Override
    public int cost() {
      // We have no better estimate. This should be ok in practice since background merges should
      // keep the number of deletes under control (< 20% by default).
      return maxDoc;
    }
  }

  /** Impl backed by a {@link DocIdSetIterator} */
  private static class DocIdSetIteratorAcceptDocs extends AcceptDocs {

    private final Bits liveDocs;
    private final int maxDoc;

    private DocIdSetIterator iterator;
    private BitSet bitSet = null;
    private int cardinality = -1;

    DocIdSetIteratorAcceptDocs(DocIdSetIterator iterator, Bits liveDocs, int maxDoc) {
      this.iterator = iterator;
      this.liveDocs = liveDocs;
      this.maxDoc = maxDoc;
      if (iterator.docID() != -1) {
        throw new IllegalArgumentException(
            "Iterator must be unpositioned, but got current doc ID: " + iterator.docID());
      }
    }

    /**
     * The {@link BitSet} is computed lazily so that it is never created in case only the {@link
     * #iterator} is needed
     */
    private void loadIntoBitSetIfNecessary() throws IOException {
      // Usage of AcceptDocs should be confined to a single thread, so this doesn't need
      // synchronization.
      if (iterator.docID() != -1) {
        throw new IllegalStateException(
            "It is illegal to call #cost() or #bits() after #iterator()");
      }
      if (bitSet == null) {
        bitSet = createBitSet(iterator, liveDocs, maxDoc);
        cardinality = bitSet.cardinality();
        iterator = new BitSetIterator(bitSet, cardinality);
      }
    }

    @Override
    public Bits bits() throws IOException {
      loadIntoBitSetIfNecessary();
      return bitSet;
    }

    @Override
    public DocIdSetIterator iterator() throws IOException {
      return iterator;
    }

    @Override
    public int cost() throws IOException {
      loadIntoBitSetIfNecessary();
      return cardinality;
    }
  }
}
