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
import java.util.Objects;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOSupplier;

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
   * @return Bits instance for random access, or null if all documents are accepted
   * @throws IOException if an I/O error occurs
   */
  public abstract Bits bits() throws IOException;

  /**
   * Create a new iterator of accepted docs. There accepted docs already ignore deleted docs.
   *
   * <p><b>NOTE</b>: If you also plan on calling {@link #bits()} or {@link #cost()}, it is
   * recommended to call these methods before {@link #iterator()} for better performance.
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
    return new BitsAcceptDocs(bits, maxDoc);
  }

  /**
   * Create AcceptDocs from an {@link IOSupplier} of {@link DocIdSetIterator}, optionally filtered
   * by live documents.
   *
   * @param iteratorSupplier a DocIdSetIterator iterator
   * @param liveDocs Bits representing live documents, or {@code null} if no deleted docs.
   * @param maxDoc the number of documents in the reader
   * @return AcceptDocs wrapping the iterator
   */
  public static AcceptDocs fromIteratorSupplier(
      IOSupplier<DocIdSetIterator> iteratorSupplier, Bits liveDocs, int maxDoc) {
    return new DocIdSetIteratorAcceptDocs(iteratorSupplier, liveDocs, maxDoc);
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
        return BitSet.of(
            AcceptDocs.getFilteredDocIdSetIterator(iterator, liveDocs),
            maxDoc); // create a sparse bitset
      }
    }
  }

  /**
   * Impl backed by Bits, expected to be somewhat dense, except when a {@link BitSet} is provided,
   * in which case it's not necessarily dense.
   */
  private static class BitsAcceptDocs extends AcceptDocs {
    private final Bits bits;
    private final int maxDoc;

    BitsAcceptDocs(Bits bits, int maxDoc) {
      if (bits != null && bits.length() != maxDoc) {
        throw new IllegalArgumentException(
            "Bits length = " + bits.length() + " != maxDoc = " + maxDoc);
      }
      this.bits = bits;
      if (bits instanceof BitSet bitSet) {
        this.maxDoc = Objects.requireNonNull(bitSet).cardinality();
      } else {
        this.maxDoc = maxDoc;
      }
    }

    @Override
    public Bits bits() {
      return bits;
    }

    @Override
    public DocIdSetIterator iterator() {
      if (bits instanceof BitSet bitSet) {
        return new BitSetIterator(bitSet, maxDoc);
      }
      return AcceptDocs.getFilteredDocIdSetIterator(DocIdSetIterator.all(maxDoc), bits);
    }

    @Override
    public int cost() {
      // We have no better estimate. This should be ok in practice since background merges should
      // keep the number of deletes under control (< 20% by default).
      return maxDoc;
    }
  }

  /**
   * Impl backed by a {@link DocIdSetIterator}, which lazily creates a {@link BitSet} if {@link
   * #cost()} or {@link #bits()} are called.
   */
  private static class DocIdSetIteratorAcceptDocs extends AcceptDocs {

    private final IOSupplier<DocIdSetIterator> iteratorSupplier;
    private final Bits liveDocs;
    private final int maxDoc;
    private BitSet acceptBitSet;
    private int cardinality;

    DocIdSetIteratorAcceptDocs(
        IOSupplier<DocIdSetIterator> iteratorSupplier, Bits liveDocs, int maxDoc) {
      this.iteratorSupplier = Objects.requireNonNull(iteratorSupplier);
      this.liveDocs = liveDocs;
      this.maxDoc = maxDoc;
    }

    private void createBitSetAcceptDocsIfNecessary() throws IOException {
      if (acceptBitSet == null) {
        acceptBitSet = Objects.requireNonNull(createBitSet(iterator(), liveDocs, maxDoc));
        cardinality = acceptBitSet.cardinality();
      }
    }

    @Override
    public Bits bits() throws IOException {
      createBitSetAcceptDocsIfNecessary();
      return acceptBitSet;
    }

    @Override
    public int cost() throws IOException {
      createBitSetAcceptDocsIfNecessary();
      return acceptBitSet.cardinality();
    }

    @Override
    public DocIdSetIterator iterator() throws IOException {
      if (acceptBitSet != null) {
        return new BitSetIterator(acceptBitSet, cardinality);
      }
      DocIdSetIterator iterator = Objects.requireNonNull(iteratorSupplier.get());
      return AcceptDocs.getFilteredDocIdSetIterator(iterator, liveDocs);
    }
  }

  private static DocIdSetIterator getFilteredDocIdSetIterator(
      DocIdSetIterator iterator, Bits liveDocs) {
    if (liveDocs != null) {
      iterator =
          new FilteredDocIdSetIterator(iterator) {
            @Override
            protected boolean match(int doc) {
              return liveDocs.get(doc);
            }
          };
    }
    return iterator;
  }
}
