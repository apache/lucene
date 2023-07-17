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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;

// These basic tests are similar to some of the tests in TestWANDScorer, and may not need to be kept
public class TestMaxScoreBulkScorer extends LuceneTestCase {

  private void writeDocuments(Directory dir) throws IOException {
    try (IndexWriter w =
        new IndexWriter(dir, newIndexWriterConfig().setMergePolicy(newLogMergePolicy()))) {

      for (String[] values :
          Arrays.asList(
              new String[] {"A", "B"}, // 0
              new String[] {"A"}, // 1
              new String[] {}, // 2
              new String[] {"A", "B", "C"}, // 3
              new String[] {"B"}, // 4
              new String[] {"B", "C"} // 5
              )) {
        Document doc = new Document();
        for (String value : values) {
          doc.add(new StringField("foo", value, Field.Store.NO));
        }
        w.addDocument(doc);
        for (int i = 1; i < MaxScoreBulkScorer.INNER_WINDOW_SIZE; ++i) {
          w.addDocument(new Document());
        }
      }
      w.forceMerge(1);
    }
  }

  public void testBasicsWithTwoDisjunctionClauses() throws Exception {
    try (Directory dir = newDirectory()) {
      writeDocuments(dir);

      try (IndexReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = newSearcher(reader);

        Query query =
            new BooleanQuery.Builder()
                .add(
                    new BoostQuery(new ConstantScoreQuery(new TermQuery(new Term("foo", "A"))), 2),
                    BooleanClause.Occur.SHOULD)
                .add(
                    new ConstantScoreQuery(new TermQuery(new Term("foo", "B"))),
                    BooleanClause.Occur.SHOULD)
                .build();

        BulkScorer scorer =
            searcher
                .createWeight(searcher.rewrite(query), ScoreMode.TOP_SCORES, 1)
                .bulkScorer(searcher.getIndexReader().leaves().get(0));

        scorer.score(
            new LeafCollector() {

              private int i;
              private Scorable scorer;

              @Override
              public void setScorer(Scorable scorer) throws IOException {
                this.scorer = scorer;
              }

              @Override
              public void collect(int doc) throws IOException {
                switch (i++) {
                  case 0:
                    assertEquals(0, doc);
                    assertEquals(2 + 1, scorer.score(), 0);
                    break;
                  case 1:
                    assertEquals(2048, doc);
                    assertEquals(2, scorer.score(), 0);
                    break;
                  case 2:
                    assertEquals(6144, doc);
                    assertEquals(2 + 1, scorer.score(), 0);
                    break;
                  case 3:
                    assertEquals(8192, doc);
                    assertEquals(1, scorer.score(), 0);
                    break;
                  case 4:
                    assertEquals(10240, doc);
                    assertEquals(1, scorer.score(), 0);
                    break;
                  default:
                    fail();
                    break;
                }
              }
            },
            null);
      }
    }
  }

  public void testBasicsWithTwoDisjunctionClausesAndSkipping() throws Exception {
    try (Directory dir = newDirectory()) {
      writeDocuments(dir);

      try (IndexReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = newSearcher(reader);

        Query query =
            new BooleanQuery.Builder()
                .add(
                    new BoostQuery(new ConstantScoreQuery(new TermQuery(new Term("foo", "A"))), 2),
                    BooleanClause.Occur.SHOULD)
                .add(
                    new ConstantScoreQuery(new TermQuery(new Term("foo", "B"))),
                    BooleanClause.Occur.SHOULD)
                .build();

        BulkScorer scorer =
            searcher
                .createWeight(searcher.rewrite(query), ScoreMode.TOP_SCORES, 1)
                .bulkScorer(searcher.getIndexReader().leaves().get(0));

        scorer.score(
            new LeafCollector() {

              private int i;
              private Scorable scorer;

              @Override
              public void setScorer(Scorable scorer) throws IOException {
                this.scorer = scorer;
              }

              @Override
              public void collect(int doc) throws IOException {
                switch (i++) {
                  case 0:
                    assertEquals(0, doc);
                    assertEquals(2 + 1, scorer.score(), 0);
                    break;
                  case 1:
                    assertEquals(2048, doc);
                    assertEquals(2, scorer.score(), 0);
                    // simulate top-2 retrieval
                    scorer.setMinCompetitiveScore(Math.nextUp(2));
                    break;
                  case 2:
                    assertEquals(6144, doc);
                    assertEquals(2 + 1, scorer.score(), 0);
                    scorer.setMinCompetitiveScore(Math.nextUp(2 + 1));
                    break;
                  default:
                    fail();
                    break;
                }
              }
            },
            null);
      }
    }
  }

  public void testBasicsWithThreeDisjunctionClauses() throws Exception {
    try (Directory dir = newDirectory()) {
      writeDocuments(dir);

      try (IndexReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = newSearcher(reader);

        Query query =
            new BooleanQuery.Builder()
                .add(
                    new BoostQuery(new ConstantScoreQuery(new TermQuery(new Term("foo", "A"))), 2),
                    BooleanClause.Occur.SHOULD)
                .add(
                    new ConstantScoreQuery(new TermQuery(new Term("foo", "B"))),
                    BooleanClause.Occur.SHOULD)
                .add(
                    new BoostQuery(new ConstantScoreQuery(new TermQuery(new Term("foo", "C"))), 3),
                    BooleanClause.Occur.SHOULD)
                .build();

        BulkScorer scorer =
            searcher
                .createWeight(searcher.rewrite(query), ScoreMode.TOP_SCORES, 1)
                .bulkScorer(searcher.getIndexReader().leaves().get(0));

        scorer.score(
            new LeafCollector() {

              private int i;
              private Scorable scorer;

              @Override
              public void setScorer(Scorable scorer) throws IOException {
                this.scorer = scorer;
              }

              @Override
              public void collect(int doc) throws IOException {
                switch (i++) {
                  case 0:
                    assertEquals(0, doc);
                    assertEquals(2 + 1, scorer.score(), 0);
                    break;
                  case 1:
                    assertEquals(2048, doc);
                    assertEquals(2, scorer.score(), 0);
                    break;
                  case 2:
                    assertEquals(6144, doc);
                    assertEquals(2 + 1 + 3, scorer.score(), 0);
                    break;
                  case 3:
                    assertEquals(8192, doc);
                    assertEquals(1, scorer.score(), 0);
                    break;
                  case 4:
                    assertEquals(10240, doc);
                    assertEquals(1 + 3, scorer.score(), 0);
                    break;
                  default:
                    fail();
                    break;
                }
              }
            },
            null);
      }
    }
  }

  public void testBasicsWithThreeDisjunctionClausesAndSkipping() throws Exception {
    try (Directory dir = newDirectory()) {
      writeDocuments(dir);

      try (IndexReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = newSearcher(reader);

        Query query =
            new BooleanQuery.Builder()
                .add(
                    new BoostQuery(new ConstantScoreQuery(new TermQuery(new Term("foo", "A"))), 2),
                    BooleanClause.Occur.SHOULD)
                .add(
                    new ConstantScoreQuery(new TermQuery(new Term("foo", "B"))),
                    BooleanClause.Occur.SHOULD)
                .add(
                    new BoostQuery(new ConstantScoreQuery(new TermQuery(new Term("foo", "C"))), 3),
                    BooleanClause.Occur.SHOULD)
                .build();

        BulkScorer scorer =
            searcher
                .createWeight(searcher.rewrite(query), ScoreMode.TOP_SCORES, 1)
                .bulkScorer(searcher.getIndexReader().leaves().get(0));

        scorer.score(
            new LeafCollector() {

              private int i;
              private Scorable scorer;

              @Override
              public void setScorer(Scorable scorer) throws IOException {
                this.scorer = scorer;
              }

              @Override
              public void collect(int doc) throws IOException {
                switch (i++) {
                  case 0:
                    assertEquals(0, doc);
                    assertEquals(2 + 1, scorer.score(), 0);
                    break;
                  case 1:
                    assertEquals(2048, doc);
                    assertEquals(2, scorer.score(), 0);
                    // simulate top-2 retrieval
                    scorer.setMinCompetitiveScore(Math.nextUp(2));
                    break;
                  case 2:
                    assertEquals(6144, doc);
                    assertEquals(2 + 1 + 3, scorer.score(), 0);
                    scorer.setMinCompetitiveScore(Math.nextUp(2 + 1));
                    break;
                  case 3:
                    assertEquals(10240, doc);
                    assertEquals(1 + 3, scorer.score(), 0);
                    scorer.setMinCompetitiveScore(Math.nextUp(1 + 3));
                    break;
                  default:
                    fail();
                    break;
                }
              }
            },
            null);
      }
    }
  }
}
