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

public class TestTopDocsRRF extends LuceneTestCase {

  public void testBasics() {
    TopDocs td1 =
        new TopDocs(
            new TotalHits(100, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),
            new ScoreDoc[] {new ScoreDoc(42, 10f), new ScoreDoc(10, 5f), new ScoreDoc(20, 3f)});
    TopDocs td2 =
        new TopDocs(
            new TotalHits(80, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),
            new ScoreDoc[] {new ScoreDoc(10, 10f), new ScoreDoc(20, 5f)});

    TopDocs rrfTd = TopDocs.rrf(3, 20, new TopDocs[] {td1, td2});
    assertEquals(new TotalHits(100, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), rrfTd.totalHits);

    ScoreDoc[] rrfScoreDocs = rrfTd.scoreDocs;
    assertEquals(3, rrfScoreDocs.length);

    assertEquals(10, rrfScoreDocs[0].doc);
    assertEquals(-1, rrfScoreDocs[0].shardIndex);
    assertEquals((float) (1d / (20 + 2) + 1d / (20 + 1)), rrfScoreDocs[0].score, 0f);

    assertEquals(20, rrfScoreDocs[1].doc);
    assertEquals(-1, rrfScoreDocs[1].shardIndex);
    assertEquals((float) (1d / (20 + 3) + 1d / (20 + 2)), rrfScoreDocs[1].score, 0f);

    assertEquals(42, rrfScoreDocs[2].doc);
    assertEquals(-1, rrfScoreDocs[2].shardIndex);
    assertEquals((float) (1d / (20 + 1)), rrfScoreDocs[2].score, 0f);
  }

  public void testShardIndex() {
    TopDocs td1 =
        new TopDocs(
            new TotalHits(100, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),
            new ScoreDoc[] {
              new ScoreDoc(42, 10f, 0), new ScoreDoc(10, 5f, 1), new ScoreDoc(20, 3f, 0)
            });
    TopDocs td2 =
        new TopDocs(
            new TotalHits(80, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),
            new ScoreDoc[] {new ScoreDoc(10, 10f, 1), new ScoreDoc(20, 5f, 1)});

    TopDocs rrfTd = TopDocs.rrf(3, 20, new TopDocs[] {td1, td2});
    assertEquals(new TotalHits(100, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), rrfTd.totalHits);

    ScoreDoc[] rrfScoreDocs = rrfTd.scoreDocs;
    assertEquals(3, rrfScoreDocs.length);

    assertEquals(10, rrfScoreDocs[0].doc);
    assertEquals(1, rrfScoreDocs[0].shardIndex);
    assertEquals((float) (1d / (20 + 2) + 1d / (20 + 1)), rrfScoreDocs[0].score, 0f);

    assertEquals(42, rrfScoreDocs[1].doc);
    assertEquals(0, rrfScoreDocs[1].shardIndex);
    assertEquals((float) (1d / (20 + 1)), rrfScoreDocs[1].score, 0f);

    assertEquals(20, rrfScoreDocs[2].doc);
    assertEquals(1, rrfScoreDocs[2].shardIndex);
    assertEquals((float) (1d / (20 + 2)), rrfScoreDocs[2].score, 0f);
  }

  public void testInconsistentShardIndex() {
    TopDocs td1 =
        new TopDocs(
            new TotalHits(100, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),
            new ScoreDoc[] {
              new ScoreDoc(42, 10f, 0), new ScoreDoc(10, 5f, 1), new ScoreDoc(20, 3f, 0)
            });
    TopDocs td2 =
        new TopDocs(
            new TotalHits(80, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),
            new ScoreDoc[] {new ScoreDoc(10, 10f, -1), new ScoreDoc(20, 5f, -1)});

    IllegalArgumentException e =
        expectThrows(
            IllegalArgumentException.class, () -> TopDocs.rrf(3, 20, new TopDocs[] {td1, td2}));
    assertTrue(e.getMessage().contains("shardIndex"));
  }

  public void testInvalidTopN() {
    TopDocs td1 =
        new TopDocs(
            new TotalHits(100, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), new ScoreDoc[0]);
    TopDocs td2 =
        new TopDocs(
            new TotalHits(80, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), new ScoreDoc[0]);

    IllegalArgumentException e =
        expectThrows(
            IllegalArgumentException.class, () -> TopDocs.rrf(0, 20, new TopDocs[] {td1, td2}));
    assertTrue(e.getMessage().contains("topN"));
  }

  public void testInvalidK() {
    TopDocs td1 =
        new TopDocs(
            new TotalHits(100, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), new ScoreDoc[0]);
    TopDocs td2 =
        new TopDocs(
            new TotalHits(80, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), new ScoreDoc[0]);

    IllegalArgumentException e =
        expectThrows(
            IllegalArgumentException.class, () -> TopDocs.rrf(3, 0, new TopDocs[] {td1, td2}));
    assertTrue(e.getMessage().contains("k"));
  }
}
