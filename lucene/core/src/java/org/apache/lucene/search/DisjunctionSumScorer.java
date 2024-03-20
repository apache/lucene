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
import java.util.List;
import org.apache.lucene.util.MathUtil;

/** A Scorer for OR like queries, counterpart of <code>ConjunctionScorer</code>. */
final class DisjunctionSumScorer extends DisjunctionScorer {

  private final List<Scorer> scorers;

  /**
   * Construct a <code>DisjunctionScorer</code>.
   *
   * @param weight The weight to be used.
   * @param subScorers Array of at least two subscorers.
   */
  DisjunctionSumScorer(Weight weight, List<Scorer> subScorers, ScoreMode scoreMode)
      throws IOException {
    super(weight, subScorers, scoreMode);
    this.scorers = subScorers;
  }

  @Override
  protected float score(DisiWrapper topList) throws IOException {
    double score = 0;

    for (DisiWrapper w = topList; w != null; w = w.next) {
      score += w.scorer.score();
    }
    return (float) score;
  }

  @Override
  public int advanceShallow(int target) throws IOException {
    int min = DocIdSetIterator.NO_MORE_DOCS;
    for (Scorer scorer : scorers) {
      if (scorer.docID() <= target) {
        min = Math.min(min, scorer.advanceShallow(target));
      }
    }
    return min;
  }

  @Override
  public float getMaxScore(int upTo) throws IOException {
    double maxScore = 0;
    for (Scorer scorer : scorers) {
      if (scorer.docID() <= upTo) {
        maxScore += scorer.getMaxScore(upTo);
      }
    }
    return (float) MathUtil.sumUpperBound(maxScore, scorers.size());
  }
}
