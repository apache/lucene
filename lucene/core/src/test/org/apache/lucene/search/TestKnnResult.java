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

import org.apache.lucene.tests.util.LuceneTestCase;

public class TestKnnResult extends LuceneTestCase {

  private static TopDocs topDocs(ScoreDoc... scoreDocs) {
    return new TopDocs(new TotalHits(scoreDocs.length, TotalHits.Relation.EQUAL_TO), scoreDocs);
  }

  public void testTwoArgConstructorMakesAllResultsStrong() {
    TopDocs td = topDocs(new ScoreDoc(1, 3f), new ScoreDoc(2, 2f), new ScoreDoc(3, 1f));

    KnnResult result = new KnnResult(KnnResult.Method.APPROXIMATE, td);

    assertSame(td, result.topDocs());
    assertEquals(3, result.strongResultCount());
  }

  public void testNoResult() {
    KnnResult empty = KnnResult.NO_RESULT;

    assertEquals(KnnResult.Method.EXACT, empty.strategy());
    assertEquals(0, empty.topDocs().scoreDocs.length);
    assertEquals(0, empty.strongResultCount());
    assertFalse(empty.hasMoreCompetitiveScores(10, Float.NEGATIVE_INFINITY));
  }

  public void testAddDocBase() {
    KnnResult result =
        new KnnResult(KnnResult.Method.EXACT, topDocs(new ScoreDoc(1, 3f), new ScoreDoc(5, 2f)));
    result.addDocBase(0); // no-op
    assertEquals(1, result.topDocs().scoreDocs[0].doc);
    result.addDocBase(10);
    assertEquals(11, result.topDocs().scoreDocs[0].doc);
    assertEquals(15, result.topDocs().scoreDocs[1].doc);
    // scores untouched
    assertEquals(3f, result.topDocs().scoreDocs[0].score, 0f);
  }

  public void testHasMoreCompetitiveScores() {
    // exact results never have more to find
    assertFalse(
        new KnnResult(KnnResult.Method.EXACT, topDocs(new ScoreDoc(1, 3f)), 1)
            .hasMoreCompetitiveScores(10, 0f));
    assertFalse(
        new KnnResult(KnnResult.Method.APPROXIMATE, topDocs(new ScoreDoc(1, 3f)), 1)
            .hasMoreCompetitiveScores(10, 0f));
    // approximate with no strong results: nothing to compare against
    assertFalse(
        new KnnResult(KnnResult.Method.APPROXIMATE, topDocs(new ScoreDoc(1, 3f)), 0)
            .hasMoreCompetitiveScores(10, Float.NEGATIVE_INFINITY));
    // weakest strong result (score 2) is competitive with the global threshold: more to find
    KnnResult result =
        new KnnResult(
            KnnResult.Method.APPROXIMATE,
            topDocs(new ScoreDoc(1, 3f), new ScoreDoc(2, 2f), new ScoreDoc(3, 1f)),
            2);
    assertTrue(result.hasMoreCompetitiveScores(10, 2f));
    assertTrue(result.hasMoreCompetitiveScores(10, 1.5f));
    assertFalse(result.hasMoreCompetitiveScores(10, 2.5f));
  }

  public void testHasMoreCompetitiveScoresLeafFillsGlobalK() {
    KnnResult result =
        new KnnResult(
            KnnResult.Method.APPROXIMATE,
            topDocs(
                new ScoreDoc(1, 4f), new ScoreDoc(2, 3f), new ScoreDoc(3, 2f), new ScoreDoc(4, 1f)),
            3);
    assertFalse(result.hasMoreCompetitiveScores(3, 1f));
    assertFalse(result.hasMoreCompetitiveScores(3, Float.NEGATIVE_INFINITY));
    assertTrue(result.hasMoreCompetitiveScores(10, 2f));
    assertFalse(result.hasMoreCompetitiveScores(10, 2.5f));
  }

  public void testStrongTopDocs() {
    TopDocs td = topDocs(new ScoreDoc(1, 3f), new ScoreDoc(2, 2f), new ScoreDoc(3, 1f));
    // all strong: returns the original TopDocs
    KnnResult allStrong = new KnnResult(KnnResult.Method.APPROXIMATE, td, 3);
    assertSame(td, allStrong.strongTopDocs());
    // partial: leading slice, totalHits preserved
    KnnResult partial = new KnnResult(KnnResult.Method.APPROXIMATE, td, 2);
    TopDocs strong = partial.strongTopDocs();
    assertEquals(2, strong.scoreDocs.length);
    assertEquals(1, strong.scoreDocs[0].doc);
    assertEquals(2, strong.scoreDocs[1].doc);
    assertEquals(td.totalHits, strong.totalHits);
  }
}
