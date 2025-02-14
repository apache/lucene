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
import java.util.Collections;

/**
 * A {@link Scorer} which wraps another scorer and caches the score of the current document.
 * Successive calls to {@link #score()} will return the same result and will not invoke the wrapped
 * Scorer's score() method, unless the current document has changed.<br>
 * This class might be useful due to the changes done to the {@link Collector} interface, in which
 * the score is not computed for a document by default, only if the collector requests it. Some
 * collectors may need to use the score in several places, however all they have in hand is a {@link
 * Scorer} object, and might end up computing the score of a document more than once.
 */
public final class ScoreCachingWrappingScorer extends Scorable {

  private boolean scoreIsCached;
  private float curScore;
  private final Scorable in;

  /**
   * Wrap the provided {@link LeafCollector} so that scores are computed lazily and cached if
   * accessed multiple times.
   */
  public static LeafCollector wrap(LeafCollector collector) {
    if (collector instanceof ScoreCachingWrappingLeafCollector) {
      return collector;
    }
    return new ScoreCachingWrappingLeafCollector(collector);
  }

  private static class ScoreCachingWrappingLeafCollector extends FilterLeafCollector {

    ScoreCachingWrappingLeafCollector(LeafCollector in) {
      super(in);
    }

    private ScoreCachingWrappingScorer scorer;

    @Override
    public void setScorer(Scorable scorer) throws IOException {
      this.scorer = new ScoreCachingWrappingScorer(scorer);
      super.setScorer(this.scorer);
    }

    @Override
    public void collect(int doc) throws IOException {
      if (scorer != null) {
        // Invalidate cache when collecting a new doc
        scorer.scoreIsCached = false;
      }
      super.collect(doc);
    }

    @Override
    public DocIdSetIterator competitiveIterator() throws IOException {
      return in.competitiveIterator();
    }
  }

  /** Creates a new instance by wrapping the given scorer. */
  private ScoreCachingWrappingScorer(Scorable scorer) {
    this.in = scorer;
  }

  @Override
  public float score() throws IOException {
    if (scoreIsCached == false) {
      curScore = in.score();
      scoreIsCached = true;
    }

    return curScore;
  }

  @Override
  public void setMinCompetitiveScore(float minScore) throws IOException {
    in.setMinCompetitiveScore(minScore);
  }

  @Override
  public Collection<ChildScorable> getChildren() {
    return Collections.singleton(new ChildScorable(in, "CACHED"));
  }
}
