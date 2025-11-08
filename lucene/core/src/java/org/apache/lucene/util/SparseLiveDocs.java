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
import java.util.function.IntPredicate;
import org.apache.lucene.search.DocIdSetIterator;

/**
 * LiveDocs implementation optimized for sparse deletions.
 *
 * <p>This implementation stores DELETED documents using {@link SparseFixedBitSet}, which provides:
 *
 * <ul>
 *   <li>O(1) random access via {@link #get(int)}
 *   <li>O(deletedDocs) iteration via {@link #deletedDocsIterator()}
 *   <li>Memory usage proportional to number of deleted documents, not total documents
 * </ul>
 *
 * <p>This is efficient when deletions are sparse. For dense deletions, {@link DenseLiveDocs}
 * should be used instead.
 *
 * <p><b>Inverted semantics:</b> Unlike typical live docs that store which documents are live, this
 * stores which documents are DELETED. Therefore:
 *
 * <ul>
 *   <li>{@link #get(int)} returns true if doc is LIVE (bit is NOT set in deletedDocs)
 *   <li>{@link #deletedDocsIterator()} iterates documents where bit IS set in deletedDocs
 * </ul>
 *
 * @lucene.experimental
 */
public class SparseLiveDocs implements LiveDocs {

  private final SparseFixedBitSet deletedDocs;
  private final int maxDoc;

  /**
   * Creates a new SparseLiveDocs with no deletions.
   *
   * @param maxDoc the maximum document ID (exclusive)
   */
  public SparseLiveDocs(int maxDoc) {
    this.maxDoc = maxDoc;
    this.deletedDocs = new SparseFixedBitSet(maxDoc);
  }

  /**
   * Creates a SparseLiveDocs wrapping an existing SparseFixedBitSet of deleted documents.
   *
   * @param deletedDocs bit set where set bits represent DELETED documents
   * @param maxDoc the maximum document ID (exclusive)
   */
  public SparseLiveDocs(final SparseFixedBitSet deletedDocs, int maxDoc) {
    assert deletedDocs.length >= maxDoc;
    this.maxDoc = maxDoc;
    this.deletedDocs = deletedDocs;
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
    // For sparse deletions, we need to scan all docs and skip deleted ones
    // Return docs where bit is NOT set (live docs)
    return new FilteredDocIdSetIterator(deletedDocs, maxDoc, maxDoc - deletedDocs.cardinality(), doc -> !deletedDocs.get(doc));
  }

  @Override
  public DocIdSetIterator deletedDocsIterator() {
    return new BitSetIterator(deletedDocs, deletedDocs.approximateCardinality());
  }

  @Override
  public int deletedCount() {
    return deletedDocs.cardinality();
  }

  /**
   * Marks a document as deleted.
   *
   * @param docID the document ID to delete
   */
  public void delete(int docID) {
    deletedDocs.set(docID);
  }

  /**
   * Returns the underlying sparse bit set of deleted documents.
   *
   * <p>Exposed for testing and codec serialization. Note that set bits represent DELETED
   * documents.
   *
   * @return the sparse bit set of deleted documents
   */
  public SparseFixedBitSet getDeletedDocs() {
    return deletedDocs;
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
        + String.format("%.2f%%", 100.0 * deletedCount() / maxDoc)
        + ")";
  }
}
