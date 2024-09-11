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
package org.apache.lucene.search.similarities;

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.util.SmallFloat;

/** Similarity that returns the TF as score */
public class TFSimilarity extends Similarity {
  private final boolean discountOverlaps;

  /**
   * TFSimilarity with these default values:
   *
   * <ul>
   *   <li>{@code discountOverlaps = true}
   * </ul>
   */
  public TFSimilarity() {
    this(true);
  }

  /**
   * @param discountOverlaps True if overlap tokens (tokens with a position of increment of zero)
   *     are discounted from the document's length.
   */
  public TFSimilarity(boolean discountOverlaps) {
    this.discountOverlaps = discountOverlaps;
  }

  /** Returns true if overlap tokens are discounted from the document's length. */
  public boolean getDiscountOverlaps() {
    return discountOverlaps;
  }

  @Override
  public long computeNorm(FieldInvertState state) {
    final int numTerms;
    if (state.getIndexOptions() == IndexOptions.DOCS && state.getIndexCreatedVersionMajor() >= 8) {
      numTerms = state.getUniqueTermCount();
    } else if (discountOverlaps) {
      numTerms = state.getLength() - state.getNumOverlap();
    } else {
      numTerms = state.getLength();
    }
    return SmallFloat.intToByte4(numTerms);
  }

  @Override
  public SimScorer scorer(
      float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
    return new SimScorer() {
      @Override
      public float score(float freq, long norm) {
        return boost * freq;
      }
    };
  }
}
