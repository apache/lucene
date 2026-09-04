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
package org.apache.lucene.util;

import java.util.Locale;
import org.apache.lucene.search.DocIdSetIterator;

/**
 * {@link LiveDocs} implementation optimized for dense deletions.
 *
 * <p>This implementation stores LIVE documents using {@link FixedBitSet}, which is the traditional
 * approach used by Lucene. This provides:
 *
 * <ul>
 *   <li>O(1) random access via {@link #get(int)}
 *   <li>Memory usage proportional to maxDoc
 *   <li>Efficient iteration over live documents
 * </ul>
 *
 * <p>This is most efficient when deletions are dense. For sparser deletions, {@link SparseLiveDocs}
 * should be used instead.
 *
 * <p><b>Standard semantics:</b> Set bits represent LIVE documents, which is the traditional Lucene
 * approach:
 *
 * <ul>
 *   <li>{@link #get(int)} returns {@code true} if doc is LIVE (bit IS set in liveDocs)
 *   <li>{@link #deletedDocsIterator()} iterates documents where bit is NOT set in liveDocs
 * </ul>
 *
 * <p><b>Immutability:</b> This class is immutable once constructed. Instances are typically created
 * using the {@link Builder} via {@link #builder(FixedBitSet, int)}.
 *
 * @lucene.experimental
 */
public final class DenseLiveDocs implements LiveDocs {

  private final FixedBitSet liveDocs;
  private final int maxDoc;
  // Cached at construction for performance. Safe because this class is immutable.
  // Eliminates repeated cardinality() and subtraction operations.
  private final int deletedCount;

  /**
   * Creates a builder for constructing DenseLiveDocs instances.
   *
   * @param liveDocs bit set where set bits represent LIVE documents
   * @param maxDoc the maximum document ID (exclusive)
   * @return a new builder instance
   */
  public static Builder builder(FixedBitSet liveDocs, int maxDoc) {
    return new Builder(liveDocs, maxDoc);
  }

  /** Builder for creating DenseLiveDocs instances with optional pre-computed deleted count. */
  public static final class Builder {
    private final FixedBitSet liveDocs;
    private final int maxDoc;
    private Integer deletedCount;

    private Builder(FixedBitSet liveDocs, int maxDoc) {
      this.liveDocs = liveDocs;
      this.maxDoc = maxDoc;
    }

    /**
     * Sets the pre-computed deleted document count, avoiding cardinality computation.
     *
     * @param deletedCount the number of deleted documents
     * @return this builder
     */
    public Builder withDeletedCount(int deletedCount) {
      this.deletedCount = deletedCount;
      return this;
    }

    /**
     * Builds the DenseLiveDocs instance.
     *
     * @return a new DenseLiveDocs instance
     * @throws IllegalArgumentException if deletedCount is outside valid range [0, maxDoc]
     */
    public DenseLiveDocs build() {
      int count = deletedCount != null ? deletedCount : (maxDoc - liveDocs.cardinality());

      if (count < 0 || count > maxDoc) {
        throw new IllegalArgumentException(
            "deletedCount=" + count + " is outside valid range [0, " + maxDoc + "]");
      }

      assert count == (maxDoc - liveDocs.cardinality())
          : "deletedCount="
              + count
              + " does not match maxDoc - liveDocs.cardinality()="
              + (maxDoc - liveDocs.cardinality());

      return new DenseLiveDocs(liveDocs, maxDoc, count);
    }
  }

  private DenseLiveDocs(final FixedBitSet liveDocs, int maxDoc, int deletedCount) {
    assert liveDocs.length() >= maxDoc;
    this.maxDoc = maxDoc;
    this.liveDocs = liveDocs;
    this.deletedCount = deletedCount;
  }

  @Override
  public boolean get(int index) {
    return liveDocs.get(index);
  }

  @Override
  public int length() {
    return maxDoc;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Live documents are stored in a {@link FixedBitSet}, so the mask is applied with a word-wise
   * AND through {@link FixedBitSet#andRange}, which auto-vectorizes. Writing {@code w} for the
   * number of bits of {@code bitSet} that this instance covers, this reads and writes {@code w/64}
   * words whatever {@code bitSet} contains. The default implementation walks {@code bitSet} with
   * {@link FixedBitSet#nextSetBit}, which reads the same {@code w/64} words one at a time, and
   * additionally calls {@link #get} once per set bit. One vectorized word AND is roughly an order
   * of magnitude cheaper than one {@code get}, so this implementation is the cheaper of the two
   * from a handful of set bits per window upwards, and the gap grows linearly with the number of
   * set bits from there. Callers therefore do not need to measure the density of {@code bitSet}
   * first, and most do not: below the crossover the two are within a few tens of nanoseconds per
   * window, and for a {@code w} of a few thousand bits {@link FixedBitSet#cardinality} costs about
   * as much as the mask itself.
   *
   * @throws IllegalArgumentException if a bit of {@code bitSet} at or beyond {@code maxDoc -
   *     offset} is set
   */
  @Override
  public void applyMask(FixedBitSet bitSet, int offset) {
    // Note: Some scorers don't track maxDoc and may thus call this method with an offset that is
    // beyond maxDoc.
    // The mask is bounded on maxDoc rather than on liveDocs.length(): the backing bit set is only
    // required to be at least maxDoc long, and bits beyond maxDoc are not part of this Bits.
    int length = Math.min(bitSet.length(), maxDoc - offset);
    if (length >= 0) {
      FixedBitSet.andRange(liveDocs, offset, bitSet, 0, length);
    }
    if (length < bitSet.length()
        && bitSet.nextSetBit(Math.max(0, length)) != DocIdSetIterator.NO_MORE_DOCS) {
      throw new IllegalArgumentException("Some bits are set beyond the end of live docs");
    }
  }

  @Override
  public DocIdSetIterator liveDocsIterator() {
    return new BitSetIterator(liveDocs, maxDoc - deletedCount);
  }

  @Override
  public DocIdSetIterator deletedDocsIterator() {
    return new FilteredDocIdSetIterator(maxDoc, deletedCount, doc -> !liveDocs.get(doc));
  }

  @Override
  public int deletedCount() {
    return deletedCount;
  }

  /**
   * Returns the memory usage in bytes.
   *
   * @return estimated memory usage in bytes
   */
  public long ramBytesUsed() {
    return liveDocs.ramBytesUsed();
  }

  @Override
  public String toString() {
    return "DenseLiveDocs(maxDoc="
        + maxDoc
        + ", deleted="
        + deletedCount()
        + ", deletionRate="
        + String.format(Locale.ROOT, "%.2f%%", 100.0 * deletedCount() / maxDoc)
        + ")";
  }
}
