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
package org.apache.lucene.search.join;

import java.io.IOException;
import org.apache.lucene.index.QueryTimeout;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.HitQueue;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.util.BitSet;

/**
 * Iterates the accepted child documents one parent at a time, tracking the best scoring child of
 * each parent. Scoring is delegated to the given {@link VectorScorer}.
 *
 * @lucene.experimental
 */
class DiversifyingChildrenVectorScorer {
  private final VectorScorer vectorScorer;
  private final DocIdSetIterator vectorIterator;
  private final DocIdSetIterator acceptedChildrenIterator;
  private final BitSet parentBitSet;
  private int currentParent = -1;
  private int bestChild = -1;
  private float currentScore = Float.NEGATIVE_INFINITY;

  /**
   * @param acceptedChildrenIterator the child documents to score
   * @param parentBitSet the parent documents
   * @param vectorScorer scores a child document against the query vector
   */
  DiversifyingChildrenVectorScorer(
      DocIdSetIterator acceptedChildrenIterator, BitSet parentBitSet, VectorScorer vectorScorer) {
    this.acceptedChildrenIterator = acceptedChildrenIterator;
    this.vectorScorer = vectorScorer;
    this.vectorIterator = vectorScorer.iterator();
    this.parentBitSet = parentBitSet;
  }

  private int bestChild() {
    return bestChild;
  }

  private int nextParent() throws IOException {
    int nextChild = acceptedChildrenIterator.docID();
    if (nextChild == -1) {
      nextChild = acceptedChildrenIterator.nextDoc();
    }
    if (nextChild == DocIdSetIterator.NO_MORE_DOCS) {
      currentParent = DocIdSetIterator.NO_MORE_DOCS;
      return currentParent;
    }
    currentScore = Float.NEGATIVE_INFINITY;
    currentParent = parentBitSet.nextSetBit(nextChild);
    do {
      vectorIterator.advance(nextChild);
      float score = vectorScorer.score();
      if (score > currentScore) {
        bestChild = nextChild;
        currentScore = score;
      }
    } while ((nextChild = acceptedChildrenIterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS
        && nextChild < currentParent);
    return currentParent;
  }

  private float score() throws IOException {
    return currentScore;
  }

  /**
   * Returns the top {@code k} scoring children, at most one per parent document. The results are
   * marked as a lower bound if the given timeout is met before every parent has been visited. The
   * accepted children are consumed, so this may be called once per instance.
   *
   * @param k how many children to return
   * @param queryTimeout the timeout to honour, or null for no timeout
   */
  TopDocs collect(int k, QueryTimeout queryTimeout) throws IOException {
    final int queueSize = Math.min(k, Math.toIntExact(acceptedChildrenIterator.cost()));
    HitQueue queue = new HitQueue(queueSize, true);
    TotalHits.Relation relation = TotalHits.Relation.EQUAL_TO;
    ScoreDoc topDoc = queue.top();
    while (nextParent() != DocIdSetIterator.NO_MORE_DOCS) {
      // Mark results as partial if timeout is met
      if (queryTimeout != null && queryTimeout.shouldExit()) {
        relation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
        break;
      }

      float score = score();
      if (score > topDoc.score) {
        topDoc.score = score;
        topDoc.doc = bestChild();
        topDoc = queue.updateTop();
      }
    }

    // Remove any remaining sentinel values
    while (queue.size() > 0 && queue.top().score < 0) {
      queue.pop();
    }

    ScoreDoc[] topScoreDocs = queue.drainToArrayHighestFirst(ScoreDoc[]::new);

    TotalHits totalHits = new TotalHits(acceptedChildrenIterator.cost(), relation);
    return new TopDocs(totalHits, topScoreDocs);
  }
}
