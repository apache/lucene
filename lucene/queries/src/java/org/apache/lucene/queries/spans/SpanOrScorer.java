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
package org.apache.lucene.queries.spans;

import java.io.IOException;
import java.util.List;
import org.apache.lucene.index.NumericDocValues;

/**
 * Scorer for {@link SpanOrQuery} that sums scores from each matching sub-clause independently, so
 * each clause uses only its own terms' IDF rather than a combined IDF across all clauses.
 */
final class SpanOrScorer extends SpanScorer {

  private final List<SpanScorer> subScorers;

  SpanOrScorer(List<SpanScorer> subScorers, Spans mergedSpans, NumericDocValues norms)
      throws IOException {
    // Pass null simScorer: scoreCurrentDoc() is overridden to sum sub-scorer scores.
    super(mergedSpans, null, norms);
    this.subScorers = subScorers;
  }

  @Override
  protected float scoreCurrentDoc() throws IOException {
    int doc = docID();
    float sum = 0;
    for (SpanScorer sub : subScorers) {
      if (sub.docID() < doc) {
        sub.iterator().advance(doc);
      }
      if (sub.docID() == doc && sub.scorer != null) {
        sum += sub.score();
      }
    }
    return sum;
  }
}
