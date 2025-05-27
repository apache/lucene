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
import org.apache.lucene.util.Bits;

/**
 * Expert: Common scoring functionality for different types of queries.
 *
 * <p>A <code>Scorer</code> exposes an {@link #iterator()} over documents matching a query in
 * increasing order of doc id.
 */
public abstract class Scorer extends Scorable {

  /** Returns the doc ID that is currently being scored. */
  public abstract int docID();

  /**
   * Return a {@link DocIdSetIterator} over matching documents.
   *
   * <p>The returned iterator will either be positioned on {@code -1} if no documents have been
   * scored yet, {@link DocIdSetIterator#NO_MORE_DOCS} if all documents have been scored already, or
   * the last document id that has been scored otherwise.
   *
   * <p>The returned iterator is a view: calling this method several times will return iterators
   * that have the same state.
   */
  public abstract DocIdSetIterator iterator();

  /**
   * Optional method: Return a {@link TwoPhaseIterator} view of this {@link Scorer}. A return value
   * of {@code null} indicates that two-phase iteration is not supported.
   *
   * <p>Note that the returned {@link TwoPhaseIterator}'s {@link TwoPhaseIterator#approximation()
   * approximation} must advance synchronously with the {@link #iterator()}: advancing the
   * approximation must advance the iterator and vice-versa.
   *
   * <p>Implementing this method is typically useful on {@link Scorer}s that have a high
   * per-document overhead in order to confirm matches.
   *
   * <p>The default implementation returns {@code null}.
   */
  public TwoPhaseIterator twoPhaseIterator() {
    return null;
  }

  /**
   * Advance to the block of documents that contains {@code target} in order to get scoring
   * information about this block. This method is implicitly called by {@link
   * DocIdSetIterator#advance(int)} and {@link DocIdSetIterator#nextDoc()} on the returned doc ID.
   * Calling this method doesn't modify the current {@link DocIdSetIterator#docID()}. It returns a
   * number that is greater than or equal to all documents contained in the current block, but less
   * than any doc IDS of the next block. {@code target} must be &gt;= {@link #docID()} as well as
   * all targets that have been passed to {@link #advanceShallow(int)} so far.
   */
  public int advanceShallow(int target) throws IOException {
    return DocIdSetIterator.NO_MORE_DOCS;
  }

  /**
   * Return the maximum score that documents between the last {@code target} that this iterator was
   * {@link #advanceShallow(int) shallow-advanced} to included and {@code upTo} included.
   */
  public abstract float getMaxScore(int upTo) throws IOException;

  /**
   * Return a new batch of doc IDs and scores, starting at the current doc ID, and ending before
   * {@code upTo}. Because it starts on the current doc ID, it is illegal to call this method if the
   * {@link #docID() current doc ID} is {@code -1}.
   *
   * <p>An empty return value indicates that there are no postings left between the current doc ID
   * and {@code upTo}.
   *
   * <p>Implementations should ideally fill the buffer with a number of entries comprised between 8
   * and a couple hundreds, to keep heap requirements contained, while still being large enough to
   * enable operations on the buffer to auto-vectorize efficiently.
   *
   * <p>The default implementation is provided below:
   *
   * <pre class="prettyprint">
   * int batchSize = 16; // arbitrary
   * buffer.growNoCopy(batchSize);
   * int size = 0;
   * DocIdSetIterator iterator = iterator();
   * for (int doc = docID(); doc &lt; upTo &amp;&amp; size &lt; batchSize; doc = iterator.nextDoc()) {
   *   if (liveDocs == null || liveDocs.get(doc)) {
   *     buffer.docs[size] = doc;
   *     buffer.scores[size] = score();
   *     ++size;
   *   }
   * }
   * buffer.size = size;
   * </pre>
   *
   * <p><b>NOTE</b>: The provided {@link DocAndScoreBuffer} should not hold references to internal
   * data structures.
   *
   * <p><b>NOTE</b>: In case this {@link Scorer} exposes a {@link #twoPhaseIterator()
   * TwoPhaseIterator}, it should be positioned on a matching document before this method is called.
   *
   * @lucene.internal
   */
  public void nextDocsAndScores(int upTo, Bits liveDocs, DocAndScoreBuffer buffer)
      throws IOException {
    int batchSize = 16; // arbitrary
    buffer.growNoCopy(batchSize);
    int size = 0;
    DocIdSetIterator iterator = iterator();
    for (int doc = docID(); doc < upTo && size < batchSize; doc = iterator.nextDoc()) {
      if (liveDocs == null || liveDocs.get(doc)) {
        buffer.docs[size] = doc;
        buffer.scores[size] = score();
        ++size;
      }
    }
    buffer.size = size;
  }
}
