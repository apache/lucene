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
import org.apache.lucene.util.FixedBitSet;

/**
 * This abstract class defines methods to iterate over a set of non-decreasing doc ids. Note that
 * this class assumes it iterates on doc Ids, and therefore {@link #NO_MORE_DOCS} is set to {@value
 * #NO_MORE_DOCS} in order to be used as a sentinel object. Implementations of this class are
 * expected to consider {@link Integer#MAX_VALUE} as an invalid value.
 */
public abstract class DocIdSetIterator {

  /** An empty {@code DocIdSetIterator} instance */
  public static DocIdSetIterator empty() {
    return new RangeDocIdSetIterator(0, 0);
  }

  /** A {@link DocIdSetIterator} that matches all documents up to {@code maxDoc - 1}. */
  public static DocIdSetIterator all(int maxDoc) {
    if (maxDoc < 0) {
      throw new IllegalArgumentException("maxDoc must be >= 0, but got maxDoc=" + maxDoc);
    }
    return new RangeDocIdSetIterator(0, maxDoc);
  }

  /**
   * A {@link DocIdSetIterator} that matches a range documents from minDocID (inclusive) to maxDocID
   * (exclusive).
   */
  public static DocIdSetIterator range(int minDoc, int maxDoc) {
    if (minDoc >= maxDoc) {
      throw new IllegalArgumentException(
          "minDoc must be < maxDoc but got minDoc=" + minDoc + " maxDoc=" + maxDoc);
    }
    if (minDoc < 0) {
      throw new IllegalArgumentException("minDoc must be >= 0 but got minDoc=" + minDoc);
    }
    return new RangeDocIdSetIterator(minDoc, maxDoc);
  }

  private static class RangeDocIdSetIterator extends AbstractDocIdSetIterator {

    private final int minDoc, maxDoc;

    RangeDocIdSetIterator(int minDoc, int maxDoc) {
      // advance relies on minDoc <= maxDoc for correctness
      assert minDoc <= maxDoc;
      this.minDoc = minDoc;
      this.maxDoc = maxDoc;
    }

    @Override
    public int nextDoc() {
      return advance(doc + 1);
    }

    @Override
    public int advance(int target) {
      if (target >= maxDoc) {
        doc = NO_MORE_DOCS;
      } else if (target < minDoc) {
        doc = minDoc;
      } else {
        doc = target;
      }
      return doc;
    }

    @Override
    public long cost() {
      return maxDoc - minDoc;
    }

    @Override
    public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) {
      assert offset <= doc;
      upTo = Math.min(upTo, maxDoc);
      if (upTo > doc) {
        bitSet.set(doc - offset, upTo - offset);
        advance(upTo);
      }
    }

    @Override
    public int docIDRunEnd() throws IOException {
      return maxDoc;
    }
  }

  /**
   * When returned by {@link #nextDoc()}, {@link #advance(int)} and {@link #docID()} it means there
   * are no more docs in the iterator.
   */
  public static final int NO_MORE_DOCS = Integer.MAX_VALUE;

  /**
   * Returns the following:
   *
   * <ul>
   *   <li><code>-1</code> if {@link #nextDoc()} or {@link #advance(int)} were not called yet.
   *   <li>{@link #NO_MORE_DOCS} if the iterator has exhausted.
   *   <li>Otherwise it should return the doc ID it is currently on.
   * </ul>
   *
   * @since 2.9
   */
  public abstract int docID();

  /**
   * Advances to the next document in the set and returns the doc it is currently on, or {@link
   * #NO_MORE_DOCS} if there are no more docs in the set.<br>
   * <b>NOTE:</b> after the iterator has exhausted you should not call this method, as it may result
   * in unpredicted behavior.
   *
   * @since 2.9
   */
  public abstract int nextDoc() throws IOException;

  /**
   * Advances to the first beyond the current whose document number is greater than or equal to
   * <i>target</i>, and returns the document number itself. Exhausts the iterator and returns {@link
   * #NO_MORE_DOCS} if <i>target</i> is greater than the highest document number in the set.
   *
   * <p>The behavior of this method is <b>undefined</b> when called with <code> target &le; current
   * </code>, or after the iterator has exhausted. Both cases may result in unpredicted behavior.
   *
   * <p>When <code> target &gt; current</code> it behaves as if written:
   *
   * <pre class="prettyprint">
   * int advance(int target) {
   *   int doc;
   *   while ((doc = nextDoc()) &lt; target) {
   *   }
   *   return doc;
   * }
   * </pre>
   *
   * Some implementations are considerably more efficient than that.
   *
   * <p><b>NOTE:</b> this method may be called with {@link #NO_MORE_DOCS} for efficiency by some
   * Scorers. If your implementation cannot efficiently determine that it should exhaust, it is
   * recommended that you check for that value in each call to this method.
   *
   * @since 2.9
   */
  public abstract int advance(int target) throws IOException;

  /**
   * Slow (linear) implementation of {@link #advance} relying on {@link #nextDoc()} to advance
   * beyond the target position.
   */
  protected final int slowAdvance(int target) throws IOException {
    assert docID() < target;
    int doc;
    do {
      doc = nextDoc();
    } while (doc < target);
    return doc;
  }

  /**
   * Returns the estimated cost of this {@link DocIdSetIterator}.
   *
   * <p>This is generally an upper bound of the number of documents this iterator might match, but
   * may be a rough heuristic, hardcoded value, or otherwise completely inaccurate.
   */
  public abstract long cost();

  /**
   * Load doc IDs into a {@link FixedBitSet}. This should behave exactly as if implemented as below,
   * which is the default implementation:
   *
   * <pre class="prettyprint">
   * for (int doc = docID(); doc &lt; upTo; doc = nextDoc()) {
   *   bitSet.set(doc - offset);
   * }
   * </pre>
   *
   * <p><b>Note</b>: {@code offset} must be less than or equal to the {@link #docID() current doc
   * ID}. Behaviour is undefined if this iterator is unpositioned.
   *
   * <p><b>Note</b>: It is important not to clear bits from {@code bitSet} that may be already set.
   *
   * <p><b>Note</b>: {@code offset} may be negative.
   *
   * @lucene.internal
   */
  public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
    assert offset <= docID();
    for (int doc = docID(); doc < upTo; doc = nextDoc()) {
      bitSet.set(doc - offset);
    }
  }

  /**
   * Returns the end of the run of consecutive doc IDs that match this {@link DocIdSetIterator} and
   * that contains the current {@link #docID()}, that is: one plus the last doc ID of the run.
   *
   * <ol>
   *   <li>The returned doc is greater than {@link #docID()}.
   *   <li>All docs in range {@code [docID(), docIDRunEnd())} match this iterator.
   *   <li>The current position of this iterator is not affected by calling {@link #docIDRunEnd()}.
   * </ol>
   *
   * <p><b>Note</b>: It is illegal to call this method when the iterator is exhausted or not
   * positioned.
   *
   * <p>The default implementation assumes runs of a single doc ID and returns {@link #docID()}) +
   * 1.
   */
  public int docIDRunEnd() throws IOException {
    return docID() + 1;
  }
}
