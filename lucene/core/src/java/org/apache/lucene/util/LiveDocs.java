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

import org.apache.lucene.search.DocIdSetIterator;

/**
 * Extension of {@link Bits} that provides efficient iteration over deleted documents.
 *
 * <p>This interface enables efficient sequential access to deleted documents, which is beneficial
 * when deletions are sparse (typically for low deletion rates). For sparse deletions, iterating
 * deleted documents directly in O(deletedDocs) time is much more efficient than scanning all
 * documents in O(maxDoc) time.
 *
 * <p>Implementations should provide optimal iteration strategies based on deletion density:
 *
 * <ul>
 *   <li>Sparse deletions: Use {@link SparseFixedBitSet} to store deleted docs, enabling
 *       O(deletedDocs) iteration
 *   <li>Dense deletions: Use {@link FixedBitSet} to store live docs, maintaining current behavior
 * </ul>
 *
 * @lucene.experimental
 */
public interface LiveDocs extends Bits {

  /**
   * Returns an iterator over live document IDs.
   *
   * <p>The returned iterator provides sequential access to all live documents in ascending order.
   * The iteration complexity depends on the implementation:
   *
   * <ul>
   *   <li>For sparse deletions: O(maxDoc) - may need to scan all documents
   *   <li>For dense deletions: O(liveDocs) - only visits live documents
   * </ul>
   *
   * <p>Callers can use {@link DocIdSetIterator#cost()} to determine the expected number of live
   * documents.
   *
   * @return an iterator over live document IDs
   */
  DocIdSetIterator liveDocsIterator();

  /**
   * Returns an iterator over deleted document IDs.
   *
   * <p>The returned iterator provides sequential access to all deleted documents in ascending
   * order. The iteration complexity depends on the implementation:
   *
   * <ul>
   *   <li>For sparse deletions: O(deletedDocs) - only visits deleted documents
   *   <li>For dense deletions: O(maxDoc) - may need to scan all documents
   * </ul>
   *
   * <p>Callers can use {@link DocIdSetIterator#cost()} to determine if sparse iteration would be
   * beneficial for their use case.
   *
   * @return an iterator over deleted document IDs, or an empty iterator if no documents are deleted
   */
  DocIdSetIterator deletedDocsIterator();

  /**
   * Returns the number of deleted documents.
   *
   * <p>This can be used to determine deletion density and choose appropriate algorithms.
   *
   * @return the number of deleted documents in this segment
   */
  int deletedCount();
}
