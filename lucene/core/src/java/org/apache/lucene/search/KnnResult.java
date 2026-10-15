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

import org.apache.lucene.util.ArrayUtil;

/**
 * The result of a kNN search, carrying the collected {@link TopDocs} along with how they were
 * produced.
 *
 * @param strategy whether the results came from an approximate or exact search
 * @param topDocs the collected top documents
 * @param strongResultCount the number of leading results that were collected within the requested
 *     per-leaf k; any results beyond this were collected optimistically
 * @lucene.experimental
 */
public record KnnResult(Strategy strategy, TopDocs topDocs, int strongResultCount) {

  /** An empty result. */
  public static final KnnResult NO_RESULT =
      new KnnResult(Strategy.EXACT, TopDocsCollector.EMPTY_TOPDOCS);

  /** Creates a result where all collected documents are considered strong results. */
  public KnnResult(Strategy strategy, TopDocs topDocs) {
    this(strategy, topDocs, topDocs.scoreDocs.length);
  }

  /** Shifts all doc ids in {@link #topDocs()} by the given doc base, in place. */
  public void addDocBase(int docBase) {
    if (docBase > 0) {
      for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
        scoreDoc.doc += docBase;
      }
    }
  }

  /**
   * Returns true if this result was computed by an optimistic search. That is, we wagered that we
   * could find all the competitive hits from this segment without fully exhausting the search
   * budget.
   */
  public boolean isOptimistic() {
    return strategy == Strategy.APPROXIMATE && topDocs.scoreDocs.length > strongResultCount;
  }

  /**
   * Returns true if this result came from an approximate search that filled its strong results and
   * whose weakest strong result is still competitive with {@code minCompetitiveScore}, meaning that
   * re-searching this segment more deeply could find further competitive hits.
   *
   * @param perLeafK the number of strong results the search for this segment asked for
   * @param minCompetitiveScore the minimum score needed to make the global top hits
   */
  public boolean hasMoreCompetitiveScores(int perLeafK, float minCompetitiveScore) {
    // A search that collected fewer than perLeafK strong hits explored everything it could reach
    return strategy == Strategy.APPROXIMATE
        && strongResultCount > 0
        && strongResultCount >= perLeafK
        && topDocs.scoreDocs[strongResultCount - 1].score >= minCompetitiveScore;
  }

  /** Returns the strong results only, i.e. the first {@link #strongResultCount()} hits. */
  public TopDocs strongTopDocs() {
    if (strongResultCount >= topDocs.scoreDocs.length) {
      return topDocs;
    }
    return new TopDocs(
        topDocs.totalHits, ArrayUtil.copyOfSubArray(topDocs.scoreDocs, 0, strongResultCount));
  }

  /** How a kNN result was computed. */
  public enum Strategy {
    /** Results came from an approximate (e.g. graph) search. */
    APPROXIMATE,
    /** Results came from a brute-force exact search. */
    EXACT
  }
}
