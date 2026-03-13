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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.lucene.index.FreqAndNormBuffer;
import org.apache.lucene.index.Impacts;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.ImpactsSource;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.util.PriorityQueue;

/**
 * Expert: Find exact phrases
 *
 * @lucene.internal
 */
public final class ExactPhraseMatcher extends PhraseMatcher {

  private static class PostingsAndPosition {
    private final PostingsEnum postings;
    private final int offset;
    private int freq, upTo, pos;

    public PostingsAndPosition(PostingsEnum postings, int offset) {
      this.postings = postings;
      this.offset = offset;
    }
  }

  private final PostingsAndPosition[] postings;
  private final DocIdSetIterator approximation;
  private final ImpactsDISI impactsApproximation;

  /** Expert: Creates ExactPhraseMatcher instance */
  public ExactPhraseMatcher(
      PhraseQuery.PostingsAndFreq[] postings,
      ScoreMode scoreMode,
      SimScorer scorer,
      float matchCost) {
    super(matchCost);

    final DocIdSetIterator approximation =
        ConjunctionUtils.intersectIterators(Arrays.stream(postings).map(p -> p.postings).toList());
    final ImpactsSource impactsSource =
        mergeImpacts(Arrays.stream(postings).map(p -> p.impacts).toArray(ImpactsEnum[]::new));

    this.impactsApproximation =
        new ImpactsDISI(approximation, new MaxScoreCache(impactsSource, scorer));
    if (scoreMode == ScoreMode.TOP_SCORES) {
      // TODO: only do this when this is the top-level scoring clause
      // (ScorerSupplier#setTopLevelScoringClause) to save the overhead of wrapping with ImpactsDISI
      // when it would not help
      this.approximation = impactsApproximation;
    } else {
      this.approximation = approximation;
    }

    List<PostingsAndPosition> postingsAndPositions = new ArrayList<>();
    for (PhraseQuery.PostingsAndFreq posting : postings) {
      postingsAndPositions.add(new PostingsAndPosition(posting.postings, posting.position));
    }
    this.postings = postingsAndPositions.toArray(PostingsAndPosition[]::new);
  }

  @Override
  DocIdSetIterator approximation() {
    return approximation;
  }

  @Override
  ImpactsDISI impactsApproximation() {
    return impactsApproximation;
  }

  @Override
  float maxFreq() {
    int minFreq = postings[0].freq;
    for (int i = 1; i < postings.length; i++) {
      minFreq = Math.min(minFreq, postings[i].freq);
    }
    return minFreq;
  }

  /**
   * Advance the given pos enum to the first position on or after {@code target}. Return {@code
   * false} if the enum was exhausted before reaching {@code target} and {@code true} otherwise.
   */
  private static boolean advancePosition(PostingsAndPosition posting, int target)
      throws IOException {
    while (posting.pos < target) {
      if (posting.upTo == posting.freq) {
        return false;
      } else {
        posting.pos = posting.postings.nextPosition();
        posting.upTo += 1;
      }
    }
    return true;
  }

  @Override
  public void reset() throws IOException {
    for (PostingsAndPosition posting : postings) {
      posting.freq = posting.postings.freq();
      posting.pos = -1;
      posting.upTo = 0;
    }
  }

  @Override
  public boolean nextMatch() throws IOException {
    final PostingsAndPosition lead = postings[0];
    if (lead.upTo < lead.freq) {
      lead.pos = lead.postings.nextPosition();
      lead.upTo += 1;
    } else {
      return false;
    }
    advanceHead:
    while (true) {
      final int phrasePos = lead.pos - lead.offset;
      for (int j = 1; j < postings.length; ++j) {
        final PostingsAndPosition posting = postings[j];
        final int expectedPos = phrasePos + posting.offset;

        // advance up to the same position as the lead
        if (advancePosition(posting, expectedPos) == false) {
          break advanceHead;
        }

        if (posting.pos != expectedPos) { // we advanced too far
          if (advancePosition(lead, posting.pos - posting.offset + lead.offset)) {
            continue advanceHead;
          } else {
            break advanceHead;
          }
        }
      }
      return true;
    }
    return false;
  }

  @Override
  float sloppyWeight() {
    return 1;
  }

  @Override
  public int startPosition() {
    return postings[0].pos;
  }

  @Override
  public int endPosition() {
    return postings[postings.length - 1].pos;
  }

  @Override
  public int startOffset() throws IOException {
    return postings[0].postings.startOffset();
  }

  @Override
  public int endOffset() throws IOException {
    return postings[postings.length - 1].postings.endOffset();
  }

  /** Merge impacts for multiple terms of an exact phrase. */
  static ImpactsSource mergeImpacts(ImpactsEnum[] impactsEnums) {
    // Iteration of block boundaries uses the impacts enum with the lower cost.
    // This is consistent with BlockMaxConjunctionScorer.
    int tmpLeadIndex = -1;
    for (int i = 0; i < impactsEnums.length; ++i) {
      if (tmpLeadIndex == -1 || impactsEnums[i].cost() < impactsEnums[tmpLeadIndex].cost()) {
        tmpLeadIndex = i;
      }
    }
    final int leadIndex = tmpLeadIndex;

    return new ImpactsSource() {

      static class SubIterator {
        final FreqAndNormBuffer buffer;
        int index;
        int freq;
        long norm;
        boolean exhausted;

        SubIterator(FreqAndNormBuffer buffer) {
          this.buffer = buffer;
          this.index = 0;
          next();
        }

        void next() {
          if (index >= buffer.size) {
            exhausted = true;
          } else {
            freq = buffer.freqs[index];
            norm = buffer.norms[index];
            index++;
          }
        }
      }

      @Override
      public Impacts getImpacts() throws IOException {
        final Impacts[] impacts = new Impacts[impactsEnums.length];
        for (int i = 0; i < impactsEnums.length; ++i) {
          impacts[i] = impactsEnums[i].getImpacts();
        }
        final Impacts lead = impacts[leadIndex];
        return new Impacts() {

          private final FreqAndNormBuffer mergedImpacts = new FreqAndNormBuffer();

          {
            mergedImpacts.growNoCopy(1);
          }

          @Override
          public int numLevels() {
            // Delegate to the lead
            return lead.numLevels();
          }

          @Override
          public int getDocIdUpTo(int level) {
            // Delegate to the lead
            return lead.getDocIdUpTo(level);
          }

          /**
           * Return the minimum level whose impacts are valid up to {@code docIdUpTo}, or {@code -1}
           * if there is no such level.
           */
          private int getLevel(Impacts impacts, int docIdUpTo) {
            for (int level = 0, numLevels = impacts.numLevels(); level < numLevels; ++level) {
              if (impacts.getDocIdUpTo(level) >= docIdUpTo) {
                return level;
              }
            }
            return -1;
          }

          @Override
          public FreqAndNormBuffer getImpacts(int level) {
            final int docIdUpTo = getDocIdUpTo(level);

            PriorityQueue<SubIterator> pq =
                PriorityQueue.usingComparator(
                    impacts.length, Comparator.comparingInt(si -> si.freq));

            boolean hasImpacts = false;
            FreqAndNormBuffer onlyImpactList = null;
            List<SubIterator> subIterators = new ArrayList<>(impacts.length);
            for (int i = 0; i < impacts.length; ++i) {
              int impactsLevel = getLevel(impacts[i], docIdUpTo);
              if (impactsLevel == -1) {
                // This instance doesn't have useful impacts, ignore it: this is safe.
                continue;
              }

              FreqAndNormBuffer impactList = impacts[i].getImpacts(impactsLevel);
              if (impactList.freqs[0] == Integer.MAX_VALUE && impactList.norms[0] == 1L) {
                // Dummy impacts, ignore it too.
                continue;
              }

              SubIterator subIterator = new SubIterator(impactList);
              subIterators.add(subIterator);
              if (hasImpacts == false) {
                hasImpacts = true;
                onlyImpactList = impactList;
              } else {
                onlyImpactList = null; // there are multiple impacts
              }
            }

            if (hasImpacts == false) {
              mergedImpacts.freqs[0] = Integer.MAX_VALUE;
              mergedImpacts.norms[0] = 1L;
              mergedImpacts.size = 1;
              return mergedImpacts;
            } else if (onlyImpactList != null) {
              return onlyImpactList;
            }

            // Idea: merge impacts by freq. The tricky thing is that we need to
            // consider freq values that are not in the impacts too. For
            // instance if the list of impacts is [{freq=2,norm=10}, {freq=4,norm=12}],
            // there might well be a document that has a freq of 2 and a length of 11,
            // which was just not added to the list of impacts because {freq=2,norm=10}
            // is more competitive.
            // We walk impacts in parallel through a PQ ordered by freq. At any time,
            // the competitive impact consists of the lowest freq among all entries of
            // the PQ (the top) and the highest norm (tracked separately).
            pq.addAll(subIterators);
            mergedImpacts.size = 0;
            SubIterator top = pq.top();
            int currentFreq = top.freq;
            long currentNorm = 0;
            for (SubIterator it : pq) {
              if (Long.compareUnsigned(it.norm, currentNorm) > 0) {
                currentNorm = it.norm;
              }
            }

            outer:
            while (true) {

              if (mergedImpacts.size > 0
                  && mergedImpacts.norms[mergedImpacts.size - 1] == currentNorm) {
                mergedImpacts.freqs[mergedImpacts.size - 1] = currentFreq;
              } else {
                mergedImpacts.add(currentFreq, currentNorm);
              }

              do {
                top.next();
                if (top.exhausted) {
                  // At least one clause doesn't have any more documents below the current norm,
                  // so we can safely ignore further clauses. The only reason why they have more
                  // impacts is because they cover more documents that we are not interested in.
                  break outer;
                }
                if (Long.compareUnsigned(top.norm, currentNorm) > 0) {
                  currentNorm = top.norm;
                }
                top = pq.updateTop();
              } while (top.freq == currentFreq);

              currentFreq = top.freq;
            }

            return mergedImpacts;
          }
        };
      }

      @Override
      public void advanceShallow(int target) throws IOException {
        for (ImpactsEnum impactsEnum : impactsEnums) {
          impactsEnum.advanceShallow(target);
        }
      }
    };
  }
}
