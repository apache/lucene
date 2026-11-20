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
package org.apache.lucene.misc.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.index.Impact;
import org.apache.lucene.index.Impacts;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.util.Bits;

/** BulkScorer that prioritizes document ranges using impact information. */
public class ImpactRangeBulkScorer extends BulkScorer {

  private static final long MAX_RANGES = 10000;

  private final BulkScorer delegate;
  private final int rangeSize;
  private final int minDoc;
  private final int maxDoc;
  private final SimScorer simScorer;
  private final ImpactsEnum impactsEnum;

  public ImpactRangeBulkScorer(
      BulkScorer delegate,
      int rangeSize,
      int minDoc,
      int maxDoc,
      SimScorer simScorer,
      ImpactsEnum impactsEnum) {
    this.delegate = delegate;
    this.rangeSize = rangeSize;
    this.minDoc = minDoc;
    this.maxDoc = maxDoc;
    this.simScorer = simScorer;
    this.impactsEnum = impactsEnum;
  }

  @Override
  public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
    int actualMin = Math.max(min, minDoc);
    int actualMax = Math.min(max, maxDoc);

    if (actualMin >= actualMax) {
      return actualMax;
    }

    if (impactsEnum == null || simScorer == null) {
      return delegate.score(collector, acceptDocs, actualMin, actualMax);
    }

    List<Range> ranges;
    try {
      ranges = calculateRangePriorities(actualMin, actualMax);
    } catch (IOException e) {
      throw e;
    }

    if (ranges.isEmpty()) {
      return delegate.score(collector, acceptDocs, actualMin, actualMax);
    }

    Collections.sort(ranges, (a, b) -> Float.compare(b.priority, a.priority));

    EarlyTerminationWrapper wrapper = new EarlyTerminationWrapper(collector);

    int lastDoc = -1;
    for (Range range : ranges) {
      if (wrapper.minCompetitiveScore > 0 && range.priority < wrapper.minCompetitiveScore) {
        continue;
      }

      int rangeLastDoc = delegate.score(wrapper, acceptDocs, range.start, range.end);
      if (rangeLastDoc > lastDoc) {
        lastDoc = rangeLastDoc;
      }
    }

    return lastDoc == -1 ? actualMax : lastDoc;
  }

  private static class EarlyTerminationWrapper implements LeafCollector {
    private final LeafCollector delegate;
    private float minCompetitiveScore = 0;

    EarlyTerminationWrapper(LeafCollector delegate) {
      this.delegate = delegate;
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
      Scorable wrapper =
          new Scorable() {
            @Override
            public float score() throws IOException {
              return scorer.score();
            }

            @Override
            public void setMinCompetitiveScore(float minScore) throws IOException {
              minCompetitiveScore = minScore;
              scorer.setMinCompetitiveScore(minScore);
            }
          };
      delegate.setScorer(wrapper);
    }

    @Override
    public void collect(int doc) throws IOException {
      delegate.collect(doc);
    }
  }

  private List<Range> calculateRangePriorities(int min, int max) throws IOException {
    if (max <= min) {
      return new ArrayList<>();
    }
    int numRanges = (max - min + rangeSize - 1) / rangeSize;
    if (numRanges <= 0) {
      return new ArrayList<>();
    }
    if (numRanges > MAX_RANGES) {
      return new ArrayList<>();
    }
    List<Range> ranges = new ArrayList<>(numRanges);

    int lastShallowTarget = -1;

    for (int i = 0; i < numRanges; i++) {
      int rangeStart = min + i * rangeSize;
      int rangeEnd = Math.min(rangeStart + rangeSize, max);

      float priority = 0;
      try {
        if (rangeStart > lastShallowTarget) {
          impactsEnum.advanceShallow(rangeStart);
          lastShallowTarget = rangeStart;
        }

        Impacts impacts = impactsEnum.getImpacts();

        float maxScore = 0;
        for (int level = 0; level < impacts.numLevels(); level++) {
          int docUpTo = impacts.getDocIdUpTo(level);
          if (docUpTo >= rangeStart) {
            List<Impact> impactList = impacts.getImpacts(level);
            for (Impact impact : impactList) {
              float score = simScorer.score(impact.freq, impact.norm);
              maxScore = Math.max(maxScore, score);
            }
            if (docUpTo >= rangeEnd) {
              break;
            }
          }
        }
        priority = maxScore;
      } catch (IOException e) {
        throw e;
      }

      ranges.add(new Range(rangeStart, rangeEnd, priority));
    }

    return ranges;
  }

  private static class Range {
    final int start;
    final int end;
    final float priority;

    Range(int start, int end, float priority) {
      this.start = start;
      this.end = end;
      this.priority = priority;
    }
  }

  @Override
  public long cost() {
    return delegate.cost();
  }
}
