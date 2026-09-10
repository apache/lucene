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
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestAssertingScorer extends LuceneTestCase {

  /**
   * The wrapper must report the wrapped iterator's real {@link TwoPhaseIterator#docIDRunEnd()}, not
   * the conservative default — otherwise framework-wrapped searches never exercise the
   * bulk-collection fast paths that run ends enable.
   */
  public void testTwoPhaseDocIDRunEndDelegates() throws IOException {
    TwoPhaseIterator asserting =
        wrap(
            new AllMatchesTwoPhase(20) {
              @Override
              public int docIDRunEnd() {
                return 10;
              }
            });
    assertEquals(0, asserting.approximation().nextDoc());
    assertEquals(10, asserting.docIDRunEnd());
  }

  /** The conservative default ("no known run") must pass through unchanged too. */
  public void testTwoPhaseDocIDRunEndConservativeDefault() throws IOException {
    TwoPhaseIterator asserting = wrap(new AllMatchesTwoPhase(20));
    assertEquals(0, asserting.approximation().nextDoc());
    assertEquals(asserting.approximation().docID(), asserting.docIDRunEnd());
  }

  /** A run end below the approximation's current doc ID violates the contract. */
  public void testTwoPhaseDocIDRunEndBelowCurrentDocTrips() throws IOException {
    TwoPhaseIterator asserting =
        wrap(
            new AllMatchesTwoPhase(20) {
              @Override
              public int docIDRunEnd() {
                return approximation().docID() - 1;
              }
            });
    assertEquals(0, asserting.approximation().nextDoc());
    if (TEST_ASSERTS_ENABLED) {
      expectThrows(AssertionError.class, asserting::docIDRunEnd);
    } else {
      assertEquals(-1, asserting.docIDRunEnd());
    }
  }

  private static class AllMatchesTwoPhase extends TwoPhaseIterator {
    AllMatchesTwoPhase(int maxDoc) {
      super(DocIdSetIterator.all(maxDoc));
    }

    @Override
    public boolean matches() {
      return true;
    }

    @Override
    public float matchCost() {
      return 1f;
    }
  }

  private static TwoPhaseIterator wrap(TwoPhaseIterator tpi) {
    Scorer scorer = new ConstantScoreScorer(1f, ScoreMode.COMPLETE_NO_SCORES, tpi);
    return AssertingScorer.wrap(scorer, false, false).twoPhaseIterator();
  }
}
