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

package org.apache.lucene.util.hnsw;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;

/**
 * KnnResults is a collector for gathering kNN results and providing topDocs from the gathered
 * neighbors
 */
public abstract class KnnResults {

  /** KnnResults when exiting search early and returning empty top docs */
  static class EmptyKnnResults extends KnnResults {
    public EmptyKnnResults(int visitedCount, int visitLimit) {
      super(visitLimit);
      this.visitedCount = visitedCount;
    }

    @Override
    public void doClear() {}

    @Override
    public boolean collect(int vectorId, float similarity) {
      throw new IllegalArgumentException();
    }

    @Override
    public boolean isFull() {
      return true;
    }

    @Override
    public float minSimilarity() {
      return 0;
    }

    @Override
    public TopDocs topDocs() {
      TotalHits th = new TotalHits(visitedCount, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
      return new TopDocs(th, new ScoreDoc[0]);
    }
  }

  static class OrdinalTranslatedKnnResults extends KnnResults {
    private final KnnResults in;
    private final IntToIntFunction vectorOrdinalToDocId;

    OrdinalTranslatedKnnResults(KnnResults in, IntToIntFunction vectorOrdinalToDocId) {
      super(in.visitLimit);
      this.in = in;
      this.vectorOrdinalToDocId = vectorOrdinalToDocId;
    }

    @Override
    void doClear() {
      in.clear();
    }

    @Override
    boolean collect(int vectorId, float similarity) {
      return in.collect(vectorOrdinalToDocId.apply(vectorId), similarity);
    }

    @Override
    boolean isFull() {
      return in.isFull();
    }

    @Override
    float minSimilarity() {
      return in.minSimilarity();
    }

    @Override
    public TopDocs topDocs() {
      TopDocs td = in.topDocs();
      return new TopDocs(
          new TotalHits(
              visitedCount(),
              incomplete()
                  ? TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO
                  : TotalHits.Relation.EQUAL_TO),
          td.scoreDocs);
    }
  }

  protected int visitedCount;
  private final int visitLimit;

  protected KnnResults(int visitLimit) {
    this.visitLimit = visitLimit;
  }

  final void clear() {
    this.visitedCount = 0;
    doClear();
  }

  /** Clear the current results. */
  abstract void doClear();

  /**
   * @return is the current result set marked as incomplete?
   */
  final boolean incomplete() {
    return visitedCount >= visitLimit;
  }

  final void incVisitedCount(int count) {
    assert count > 0;
    this.visitedCount += count;
  }

  /**
   * @return the current visited count
   */
  final int visitedCount() {
    return visitedCount;
  }

  /**
   * Collect the provided vectorId and include in the result set.
   *
   * @param vectorId the vector to collect
   * @param similarity its calculated similarity
   * @return true if the vector is collected
   */
  abstract boolean collect(int vectorId, float similarity);

  /**
   * @return Is the current result set considered full
   */
  abstract boolean isFull();

  /**
   * @return the current minimum similarity in the collection
   */
  abstract float minSimilarity();

  /**
   * This drains the collected nearest kNN results and returns them in a new {@link TopDocs}
   * collection, ordered by score descending
   *
   * @return The collected top documents
   */
  public abstract TopDocs topDocs();
}
