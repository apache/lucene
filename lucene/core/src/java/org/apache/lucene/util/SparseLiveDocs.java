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
 * {@link LiveDocs} implementation optimized for sparse deletions.
 *
 * <p>This implementation stores DELETED documents using {@link SparseFixedBitSet}, which provides:
 *
 * <ul>
 *   <li>O(1) random access via {@link #get(int)}
 *   <li>O(deletedDocs) iteration via {@link #deletedDocsIterator()}
 *   <li>Memory usage proportional to number of deleted documents, not total documents
 * </ul>
 *
 * <p>This is most efficient when deletions are sparse. For denser deletions, {@link DenseLiveDocs}
 * should be used instead.
 *
 * <p><b>Inverted semantics:</b> Unlike typical live docs that store which documents are live, this
 * stores which documents are DELETED. Therefore:
 *
 * <ul>
 *   <li>{@link #get(int)} returns {@code true} if doc is LIVE (bit is NOT set in deletedDocs)
 *   <li>{@link #deletedDocsIterator()} iterates documents where bit IS set in deletedDocs
 * </ul>
 *
 * <p><b>Immutability:</b> This class is immutable once constructed. Instances are typically created
 * using the {@link Builder} via {@link #builder(SparseFixedBitSet, int)}.
 *
 * @lucene.experimental
 */
public final class SparseLiveDocs implements LiveDocs {

  private final SparseFixedBitSet deletedDocs;
  private final int maxDoc;
  // Cached at construction for performance. Safe because this class is immutable.
  // Eliminates repeated O(n) cardinality() calls.
  private final int deletedCount;

  /**
   * Creates a builder for constructing SparseLiveDocs instances.
   *
   * @param deletedDocs bit set where set bits represent DELETED documents
   * @param maxDoc the maximum document ID (exclusive)
   * @return a new builder instance
   */
  public static Builder builder(SparseFixedBitSet deletedDocs, int maxDoc) {
    return new Builder(deletedDocs, maxDoc);
  }

  /** Builder for creating SparseLiveDocs instances with optional pre-computed deleted count. */
  public static final class Builder {
    private final SparseFixedBitSet deletedDocs;
    private final int maxDoc;
    private Integer deletedCount;

    private Builder(SparseFixedBitSet deletedDocs, int maxDoc) {
      this.deletedDocs = deletedDocs;
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
     * Builds the SparseLiveDocs instance.
     *
     * @return a new SparseLiveDocs instance
     * @throws IllegalArgumentException if deletedCount is outside valid range [0, maxDoc]
     */
    public SparseLiveDocs build() {
      int count = deletedCount != null ? deletedCount : deletedDocs.cardinality();

      if (count < 0 || count > maxDoc) {
        throw new IllegalArgumentException(
            "deletedCount=" + count + " is outside valid range [0, " + maxDoc + "]");
      }

      assert count == deletedDocs.cardinality()
          : "deletedCount="
              + count
              + " does not match deletedDocs.cardinality()="
              + deletedDocs.cardinality();

      return new SparseLiveDocs(deletedDocs, maxDoc, count);
    }
  }

  private SparseLiveDocs(final SparseFixedBitSet deletedDocs, int maxDoc, int deletedCount) {
    assert deletedDocs.length >= maxDoc;
    this.maxDoc = maxDoc;
    this.deletedDocs = deletedDocs;
    this.deletedCount = deletedCount;
  }

  @Override
  public boolean get(int index) {
    return !deletedDocs.get(index);
  }

  @Override
  public int length() {
    return maxDoc;
  }

  @Override
  public DocIdSetIterator liveDocsIterator() {
    return new FilteredDocIdSetIterator(
        maxDoc, maxDoc - deletedCount, doc -> !deletedDocs.get(doc));
  }

  @Override
  public DocIdSetIterator deletedDocsIterator() {
    return new BitSetIterator(deletedDocs, deletedCount);
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
    return deletedDocs.ramBytesUsed();
  }

  @Override
  public String toString() {
    return "SparseLiveDocs(maxDoc="
        + maxDoc
        + ", deleted="
        + deletedCount()
        + ", deletionRate="
        + String.format(Locale.ROOT, "%.2f%%", 100.0 * deletedCount() / maxDoc)
        + ")";
  }
}
