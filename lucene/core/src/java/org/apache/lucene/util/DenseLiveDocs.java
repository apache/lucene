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
 * by wrapping an existing {@link FixedBitSet} read from disk during segment loading.
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
   * Creates a new DenseLiveDocs with no deletions (all documents live).
   *
   * @param maxDoc the maximum document ID (exclusive)
   */
  public DenseLiveDocs(int maxDoc) {
    this.maxDoc = maxDoc;
    this.liveDocs = new FixedBitSet(maxDoc);
    liveDocs.set(0, maxDoc);
    this.deletedCount = 0;
  }

  /**
   * Creates a DenseLiveDocs wrapping an existing FixedBitSet of live documents.
   *
   * @param liveDocs bit set where set bits represent LIVE documents
   * @param maxDoc the maximum document ID (exclusive)
   */
  public DenseLiveDocs(final FixedBitSet liveDocs, int maxDoc) {
    assert liveDocs.length() >= maxDoc;
    this.maxDoc = maxDoc;
    this.liveDocs = liveDocs;
    this.deletedCount = maxDoc - liveDocs.cardinality();
  }

  @Override
  public boolean get(int index) {
    return liveDocs.get(index);
  }

  @Override
  public int length() {
    return maxDoc;
  }

  @Override
  public DocIdSetIterator liveDocsIterator() {
    return new BitSetIterator(liveDocs, liveDocs.cardinality());
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
