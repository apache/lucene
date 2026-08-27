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

import java.io.IOException;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DocIdStream;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.IOIntConsumer;

public class TestAssertingLeafCollector extends LuceneTestCase {

  public void testScoreRejectedDuringDocIdStreamCollection() throws IOException {
    AssertingLeafCollector collector = new AssertingLeafCollector(scoreCallingCollector(), 0, 10);
    collector.setScorer(new ConstantScoreScorer(1f, ScoreMode.COMPLETE, DocIdSetIterator.all(10)));

    if (TEST_ASSERTS_ENABLED) {
      expectThrows(AssertionError.class, () -> collector.collect(singletonStream(1)));
    } else {
      collector.collect(singletonStream(1));
    }
  }

  public void testScoreRejectedDuringRangeCollection() throws IOException {
    AssertingLeafCollector collector = new AssertingLeafCollector(scoreCallingCollector(), 0, 10);
    collector.setScorer(new ConstantScoreScorer(1f, ScoreMode.COMPLETE, DocIdSetIterator.all(10)));

    if (TEST_ASSERTS_ENABLED) {
      expectThrows(AssertionError.class, () -> collector.collectRange(1, 3));
    } else {
      collector.collectRange(1, 3);
    }
  }

  public void testScoreAllowedDuringRangeCollectionWithNonScorer() throws IOException {
    AssertingLeafCollector collector = new AssertingLeafCollector(scoreCallingCollector(), 0, 10);
    collector.setScorer(scorable());

    collector.collectRange(1, 3);
  }

  private static Scorable scorable() {
    return new Scorable() {
      @Override
      public float score() {
        return 1f;
      }
    };
  }

  private static LeafCollector scoreCallingCollector() {
    return new LeafCollector() {
      private Scorable scorer;

      @Override
      public void setScorer(Scorable scorer) {
        this.scorer = scorer;
      }

      @Override
      public void collect(int doc) throws IOException {
        scorer.score();
      }

      @Override
      public void collect(DocIdStream stream) throws IOException {
        scorer.score();
      }

      @Override
      public void collectRange(int min, int max) throws IOException {
        scorer.score();
      }
    };
  }

  private static DocIdStream singletonStream(int doc) {
    return new DocIdStream() {
      private boolean consumed;

      @Override
      public void forEach(int upTo, IOIntConsumer consumer) throws IOException {
        if (consumed == false && doc < upTo) {
          consumed = true;
          consumer.accept(doc);
        }
      }

      @Override
      public int count(int upTo) {
        if (consumed || doc >= upTo) {
          return 0;
        }
        consumed = true;
        return 1;
      }

      @Override
      public int intoArray(int upTo, int[] array) {
        assert array.length > 0;
        if (consumed || doc >= upTo) {
          return 0;
        }
        consumed = true;
        array[0] = doc;
        return 1;
      }

      @Override
      public boolean mayHaveRemaining() {
        return consumed == false;
      }
    };
  }
}
