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

import java.io.IOException;
import org.apache.lucene.search.DocIdSetIterator;

/**
 * LiveDocs implementation optimized for dense deletions.
 *
 * <p>This implementation stores LIVE documents using {@link FixedBitSet}, which is the traditional
 * approach used by Lucene. This provides:
 *
 * <ul>
 *   <li>O(1) random access via {@link #get(int)}
 *   <li>Memory usage proportional to maxDoc, not deletion count
 *   <li>Efficient iteration over live documents
 * </ul>
 *
 * <p>This is most efficient when deletions are dense (typically &ge; 20% of documents). For sparser
 * deletions, {@link SparseLiveDocs} should be used instead.
 *
 * <p><b>Standard semantics:</b> Set bits represent LIVE documents, which is the traditional Lucene
 * approach:
 *
 * <ul>
 *   <li>{@link #get(int)} returns true if doc is LIVE (bit IS set in liveDocs)
 *   <li>{@link #deletedDocsIterator()} iterates documents where bit is NOT set in liveDocs
 * </ul>
 *
 * @lucene.experimental
 */
public class DenseLiveDocs implements LiveDocs {

  private final FixedBitSet liveDocs;
  private final int maxDoc;
  private int deletedCount;

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
    // For dense deletions, we can iterate efficiently over live docs
    return new BitSetIterator(liveDocs, liveDocs.cardinality());
  }

  @Override
  public DocIdSetIterator deletedDocsIterator() {
    // For dense deletions, we need to scan all docs and find clear bits
    // Return docs where bit is NOT set (deleted docs)
    return new FilteredDocIdSetIterator(liveDocs, maxDoc, deletedCount, doc -> !liveDocs.get(doc));
  }

  @Override
  public int deletedCount() {
    return deletedCount;
  }

  /**
   * Marks a document as deleted.
   *
   * @param docID the document ID to delete
   */
  public void delete(int docID) {
    if (liveDocs.get(docID)) {
      liveDocs.clear(docID);
      deletedCount++;
    }
  }

  /**
   * Returns the underlying fixed bit set of live documents.
   *
   * <p>Exposed for testing and codec serialization. Note that set bits represent LIVE documents.
   *
   * @return the fixed bit set of live documents
   */
  public FixedBitSet getLiveDocs() {
    return liveDocs;
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
        + String.format("%.2f%%", 100.0 * deletedCount() / maxDoc)
        + ")";
  }
}
