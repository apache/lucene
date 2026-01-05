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
import java.util.Arrays;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class TestDisjunctionDISIApproximation extends LuceneTestCase {

  public void testDocIDRunEnd() throws IOException {
    DocIdSetIterator clause1 = DocIdSetIterator.range(10_000, 30_000);
    DocIdSetIterator clause2 = DocIdSetIterator.range(20_000, 50_000);
    DocIdSetIterator clause3 = DocIdSetIterator.range(60_000, 60_001);
    long leadCost = TestUtil.nextLong(random(), 1, 100_000);
    Scorer scorer1 = new ConstantScoreScorer(1f, ScoreMode.COMPLETE_NO_SCORES, clause1);
    Scorer scorer2 = new ConstantScoreScorer(1f, ScoreMode.COMPLETE_NO_SCORES, clause2);
    Scorer scorer3 = new ConstantScoreScorer(1f, ScoreMode.COMPLETE_NO_SCORES, clause3);
    DocIdSetIterator iterator =
        new DisjunctionDISIApproximation(
            Arrays.asList(
                new DisiWrapper(scorer1, false),
                new DisiWrapper(scorer2, false),
                new DisiWrapper(scorer3, false)),
            leadCost);
    assertEquals(10_000, iterator.nextDoc());
    assertEquals(30_000, iterator.docIDRunEnd());
    assertEquals(25_000, iterator.advance(25_000));
    assertEquals(50_000, iterator.docIDRunEnd());
    assertEquals(60_000, iterator.advance(50_000));
    assertEquals(60_001, iterator.docIDRunEnd());
  }
}
