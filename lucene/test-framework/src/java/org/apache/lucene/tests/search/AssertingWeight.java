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
package org.apache.lucene.tests.search;

import static org.apache.lucene.tests.util.LuceneTestCase.usually;

import java.io.IOException;
import java.util.Random;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.FilterWeight;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;

class AssertingWeight extends FilterWeight {

  final Random random;
  final ScoreMode scoreMode;

  AssertingWeight(Random random, Weight in, ScoreMode scoreMode) {
    super(in);
    this.random = random;
    this.scoreMode = scoreMode;
  }

  @Override
  public int count(LeafReaderContext context) throws IOException {
    final int count = in.count(context);
    if (count < -1 || count > context.reader().numDocs()) {
      throw new AssertionError("count=" + count + ", numDocs=" + context.reader().numDocs());
    }
    return count;
  }

  @Override
  public Matches matches(LeafReaderContext context, int doc) throws IOException {
    Matches matches = in.matches(context, doc);
    if (matches == null) return null;
    return new AssertingMatches(matches);
  }

  @Override
  public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
    final ScorerSupplier inScorerSupplier = in.scorerSupplier(context);
    if (inScorerSupplier == null) {
      return null;
    }
    return new ScorerSupplier() {
      private boolean getCalled = false;
      private boolean topLevelScoringClause = false;

      @Override
      public Scorer get(long leadCost) throws IOException {
        assert getCalled == false;
        getCalled = true;
        assert leadCost >= 0 : leadCost;
        return AssertingScorer.wrap(
            new Random(random.nextLong()),
            inScorerSupplier.get(leadCost),
            scoreMode,
            topLevelScoringClause);
      }

      @Override
      public BulkScorer bulkScorer() throws IOException {
        assert getCalled == false;

        BulkScorer inScorer;
        // We explicitly test both the delegate's bulk scorer, and also the normal scorer.
        // This ensures that normal scorers are sometimes tested with an asserting wrapper.
        if (usually(random)) {
          getCalled = true;
          inScorer = inScorerSupplier.bulkScorer();
        } else {
          // Don't set getCalled = true, since this calls #get under the hood
          inScorer = super.bulkScorer();
          assert getCalled;
        }

        return AssertingBulkScorer.wrap(
            new Random(random.nextLong()), inScorer, context.reader().maxDoc());
      }

      @Override
      public long cost() {
        final long cost = inScorerSupplier.cost();
        assert cost >= 0;
        return cost;
      }

      @Override
      public void setTopLevelScoringClause() {
        assert getCalled == false;
        topLevelScoringClause = true;
        inScorerSupplier.setTopLevelScoringClause();
      }
    };
  }
}
