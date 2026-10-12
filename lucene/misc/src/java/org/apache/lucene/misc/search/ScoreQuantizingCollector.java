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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FilterCollector;
import org.apache.lucene.search.FilterLeafCollector;
import org.apache.lucene.search.FilterScorable;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.TopScoreDocCollector;

/**
 * A {@link FilterCollector} that quantizes scores of the scorer in order to compute top-k hits more
 * efficiently. This should generally be considered an unsafe approach to computing top-k hits, as
 * the top hits would be different compared to without wrapping with this collector. However it is
 * worth noting that top hits would be correct in quantized space.
 */
public final class ScoreQuantizingCollector extends FilterCollector {

  private final int mask;

  /**
   * Sole constructor. The number of accuracy bits configures a trade-off between performance and
   * how much accuracy is retained. Lower values retain less accuracy but make performance better.
   * It is recommended to avoid passing values greater than 5 to actually observe speedups.
   *
   * @param in The collector to wrap, most likely a {@link TopScoreDocCollector}.
   * @param accuracyBits How many bits of accuracy to retain, in [1,24). 24 is disallowed since this
   *     is the number of accuracy bits of single-precision floating point numbers, so this wrapper
   *     would not change scores.
   */
  public ScoreQuantizingCollector(Collector in, int accuracyBits) {
    super(in);
    if (accuracyBits < 1 || accuracyBits >= 24) {
      throw new IllegalArgumentException("accuracyBits must be in [1,24), got " + accuracyBits);
    }
    // floats have 23 mantissa bits
    // we do -1 on the number of accuracy bits to account for the implicit bit
    mask = ~0 << 23 - (accuracyBits - 1);
  }

  @Override
  public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
    return new FilterLeafCollector(in.getLeafCollector(context)) {
      @Override
      public void setScorer(Scorable scorer) throws IOException {
        in.setScorer(new QuantizingScorable(scorer));
      }
    };
  }

  private class QuantizingScorable extends FilterScorable {

    QuantizingScorable(Scorable scorer) {
      super(scorer);
    }

    @Override
    public float score() throws IOException {
      return roundDown(in.score());
    }

    @Override
    public void setMinCompetitiveScore(float minScore) throws IOException {
      in.setMinCompetitiveScore(roundUp(minScore));
    }
  }

  float roundDown(float score) {
    if (Float.isFinite(score) == false) {
      return score;
    }
    int scoreBits = Float.floatToIntBits(score);
    scoreBits &= mask;
    return Float.intBitsToFloat(scoreBits);
  }

  float roundUp(float score) {
    if (Float.isFinite(score) == false) {
      return score;
    }
    int scoreBits = Float.floatToIntBits(score);
    scoreBits = 1 + ((scoreBits - 1) | ~mask);
    return Float.intBitsToFloat(scoreBits);
  }
}
