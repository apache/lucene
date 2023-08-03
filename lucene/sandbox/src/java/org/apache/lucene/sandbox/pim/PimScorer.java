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

package org.apache.lucene.sandbox.pim;

import java.io.IOException;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

/** Scorer for PIM */
public class PimScorer extends Scorer {

  private PimMatch current;
  private float minCompetitiveScore;
  private DpuResultsReader dpuResults;

  public PimScorer(Weight weight, DpuResultsReader dpuResults) {
    super(weight);
    this.dpuResults = dpuResults;
    current = PimMatch.UNSET;
  }

  @Override
  public float score() throws IOException {
    return current.score;
  }

  @Override
  public int docID() {
    return current.docId;
  }

  @Override
  public DocIdSetIterator iterator() {
    return new DocIdSetIterator() {
      @Override
      public int docID() {
        return current.docId;
      }

      @Override
      public int nextDoc() throws IOException {
        do {
          if (!dpuResults.next()) {
            current = PimMatch.NO_MORE_RESULTS;
            break;
          }
          current = dpuResults.match();
        } while (current.score < minCompetitiveScore);
        return current.docId;
      }

      @Override
      public int advance(int target) throws IOException {
        int docId;
        while ((docId = nextDoc()) < target) {}
        return docId;
      }

      @Override
      public long cost() {
        return 0;
      }
    };
  }

  @Override
  public void setMinCompetitiveScore(float minScore) {
    minCompetitiveScore = minScore;
  }

  @Override
  public float getMaxScore(int upTo) {
    throw new UnsupportedOperationException(); // TODO
  }
}
