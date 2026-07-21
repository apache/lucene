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
import java.util.Collection;

/**
 * Skips blocks whose fused score upper bound is not competitive, without pruning individual signals
 * and thereby turning negative matching evidence into a neutral non-match.
 */
final class BlockMaxLogOddsFusionScorer extends Scorer {
  private final Scorer in;
  private final DocIdSetIterator approximation;
  private final TwoPhaseIterator twoPhase;
  private final DocIdSetIterator iterator;
  private float minCompetitiveScore;

  BlockMaxLogOddsFusionScorer(Scorer in) {
    this.in = in;
    TwoPhaseIterator innerTwoPhase = in.twoPhaseIterator();
    DocIdSetIterator innerApproximation =
        innerTwoPhase == null ? in.iterator() : innerTwoPhase.approximation();
    this.approximation = blockSkippingIterator(innerApproximation);
    if (innerTwoPhase == null) {
      this.twoPhase = null;
      this.iterator = approximation;
    } else {
      this.twoPhase =
          new TwoPhaseIterator(approximation) {
            @Override
            public boolean matches() throws IOException {
              return innerTwoPhase.matches();
            }

            @Override
            public float matchCost() {
              return innerTwoPhase.matchCost();
            }
          };
      this.iterator = TwoPhaseIterator.asDocIdSetIterator(twoPhase);
    }
  }

  private DocIdSetIterator blockSkippingIterator(DocIdSetIterator innerApproximation) {
    return new FilterDocIdSetIterator(innerApproximation) {
      private int upTo = -1;
      private float maxScore = Float.POSITIVE_INFINITY;

      private void moveToNextBlock(int target) throws IOException {
        upTo = BlockMaxLogOddsFusionScorer.this.advanceShallow(target);
        maxScore = BlockMaxLogOddsFusionScorer.this.getMaxScore(upTo);
      }

      private int advanceTarget(int target) throws IOException {
        if (target == NO_MORE_DOCS || minCompetitiveScore == 0f) {
          return target;
        }
        if (target > upTo) {
          moveToNextBlock(target);
        }
        while (maxScore < minCompetitiveScore) {
          if (upTo == NO_MORE_DOCS) {
            return NO_MORE_DOCS;
          }
          target = upTo + 1;
          moveToNextBlock(target);
        }
        return target;
      }

      @Override
      public int nextDoc() throws IOException {
        return advance(docID() + 1);
      }

      @Override
      public int advance(int target) throws IOException {
        while (true) {
          target = advanceTarget(target);
          if (target == NO_MORE_DOCS) {
            return in.advance(target);
          }
          int doc = in.docID() < target ? in.advance(target) : in.docID();
          if (doc == NO_MORE_DOCS || minCompetitiveScore == 0f || doc <= upTo) {
            return doc;
          }
          target = doc;
        }
      }
    };
  }

  @Override
  public int docID() {
    return in.docID();
  }

  @Override
  public DocIdSetIterator iterator() {
    return iterator;
  }

  @Override
  public TwoPhaseIterator twoPhaseIterator() {
    return twoPhase;
  }

  @Override
  public float score() throws IOException {
    return in.score();
  }

  @Override
  public int advanceShallow(int target) throws IOException {
    return in.advanceShallow(target);
  }

  @Override
  public float getMaxScore(int upTo) throws IOException {
    return in.getMaxScore(upTo);
  }

  @Override
  public void setMinCompetitiveScore(float minScore) {
    assert minScore >= minCompetitiveScore;
    minCompetitiveScore = Math.max(minCompetitiveScore, minScore);
  }

  @Override
  public Collection<ChildScorable> getChildren() throws IOException {
    return in.getChildren();
  }
}
