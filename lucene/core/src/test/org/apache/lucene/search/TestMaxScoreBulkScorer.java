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
import java.util.Collections;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.Bits;

// These basic tests are similar to some of the tests in TestWANDScorer, and may not need to be kept
public class TestMaxScoreBulkScorer extends LuceneTestCase {

  private static class CapMaxScoreWindowAt2048Scorer extends FilterScorer {

    public CapMaxScoreWindowAt2048Scorer(Scorer in) {
      super(in);
    }

    @Override
    public int advanceShallow(int target) throws IOException {
      return Math.min(target | 0x7FF, in.advanceShallow(target));
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
      return in.getMaxScore(upTo);
    }
  }

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

        Query clause1 =
            new BoostQuery(new ConstantScoreQuery(new TermQuery(new Term("foo", "A"))), 2);
        Query clause2 = new ConstantScoreQuery(new TermQuery(new Term("foo", "B")));
        LeafReaderContext context = searcher.getIndexReader().leaves().get(0);
        Scorer scorer1 =
            searcher
                .createWeight(searcher.rewrite(clause1), ScoreMode.TOP_SCORES, 1f)
                .scorer(context);
        scorer1 = new CapMaxScoreWindowAt2048Scorer(scorer1);
        Scorer scorer2 =
            searcher
                .createWeight(searcher.rewrite(clause2), ScoreMode.TOP_SCORES, 1f)
                .scorer(context);
        scorer2 = new CapMaxScoreWindowAt2048Scorer(scorer2);

        BulkScorer scorer =
            new MaxScoreBulkScorer(context.reader().maxDoc(), Arrays.asList(scorer1, scorer2));

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

        Query clause1 =
            new BoostQuery(new ConstantScoreQuery(new TermQuery(new Term("foo", "A"))), 2);
        Query clause2 = new ConstantScoreQuery(new TermQuery(new Term("foo", "B")));
        LeafReaderContext context = searcher.getIndexReader().leaves().get(0);
        Scorer scorer1 =
            searcher
                .createWeight(searcher.rewrite(clause1), ScoreMode.TOP_SCORES, 1f)
                .scorer(context);
        scorer1 = new CapMaxScoreWindowAt2048Scorer(scorer1);
        Scorer scorer2 =
            searcher
                .createWeight(searcher.rewrite(clause2), ScoreMode.TOP_SCORES, 1f)
                .scorer(context);
        scorer2 = new CapMaxScoreWindowAt2048Scorer(scorer2);

        BulkScorer scorer =
            new MaxScoreBulkScorer(context.reader().maxDoc(), Arrays.asList(scorer1, scorer2));

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

        Query clause1 =
            new BoostQuery(new ConstantScoreQuery(new TermQuery(new Term("foo", "A"))), 2);
        Query clause2 = new ConstantScoreQuery(new TermQuery(new Term("foo", "B")));
        Query clause3 =
            new BoostQuery(new ConstantScoreQuery(new TermQuery(new Term("foo", "C"))), 3);
        LeafReaderContext context = searcher.getIndexReader().leaves().get(0);
        Scorer scorer1 =
            searcher
                .createWeight(searcher.rewrite(clause1), ScoreMode.TOP_SCORES, 1f)
                .scorer(context);
        scorer1 = new CapMaxScoreWindowAt2048Scorer(scorer1);
        Scorer scorer2 =
            searcher
                .createWeight(searcher.rewrite(clause2), ScoreMode.TOP_SCORES, 1f)
                .scorer(context);
        scorer2 = new CapMaxScoreWindowAt2048Scorer(scorer2);
        Scorer scorer3 =
            searcher
                .createWeight(searcher.rewrite(clause3), ScoreMode.TOP_SCORES, 1f)
                .scorer(context);
        scorer3 = new CapMaxScoreWindowAt2048Scorer(scorer3);

        BulkScorer scorer =
            new MaxScoreBulkScorer(
                context.reader().maxDoc(), Arrays.asList(scorer1, scorer2, scorer3));

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

        Query clause1 =
            new BoostQuery(new ConstantScoreQuery(new TermQuery(new Term("foo", "A"))), 2);
        Query clause2 = new ConstantScoreQuery(new TermQuery(new Term("foo", "B")));
        Query clause3 =
            new BoostQuery(new ConstantScoreQuery(new TermQuery(new Term("foo", "C"))), 3);
        LeafReaderContext context = searcher.getIndexReader().leaves().get(0);
        Scorer scorer1 =
            searcher
                .createWeight(searcher.rewrite(clause1), ScoreMode.TOP_SCORES, 1f)
                .scorer(context);
        scorer1 = new CapMaxScoreWindowAt2048Scorer(scorer1);
        Scorer scorer2 =
            searcher
                .createWeight(searcher.rewrite(clause2), ScoreMode.TOP_SCORES, 1f)
                .scorer(context);
        scorer2 = new CapMaxScoreWindowAt2048Scorer(scorer2);
        Scorer scorer3 =
            searcher
                .createWeight(searcher.rewrite(clause3), ScoreMode.TOP_SCORES, 1f)
                .scorer(context);
        scorer3 = new CapMaxScoreWindowAt2048Scorer(scorer3);

        BulkScorer scorer =
            new MaxScoreBulkScorer(
                context.reader().maxDoc(), Arrays.asList(scorer1, scorer2, scorer3));

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

  private static class FakeWeight extends Weight {

    protected FakeWeight() {
      super(null);
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return false;
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      throw new UnsupportedOperationException();
    }
  }

  private static class FakeScorer extends Scorer {

    final String toString;
    int docID = -1;
    int maxScoreUpTo = DocIdSetIterator.NO_MORE_DOCS;
    float maxScore = 1f;
    int cost = 10;

    protected FakeScorer(String toString) {
      super(new FakeWeight());
      this.toString = toString;
    }

    @Override
    public int docID() {
      return docID;
    }

    @Override
    public DocIdSetIterator iterator() {
      return DocIdSetIterator.all(cost); // just so that it exposes the right cost
    }

    @Override
    public int advanceShallow(int target) throws IOException {
      return maxScoreUpTo;
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
      return maxScore;
    }

    @Override
    public float score() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
      return toString;
    }
  }

  public void testDeletes() throws IOException {

    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig().setMergePolicy(newLogMergePolicy());
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc1 = new Document();
    doc1.add(new StringField("field", "foo", Store.NO));
    doc1.add(new StringField("field", "bar", Store.NO));
    doc1.add(new StringField("field", "quux", Store.NO));
    Document doc2 = new Document();
    Document doc3 = new Document();
    for (IndexableField field : doc1) {
      doc2.add(field);
      doc3.add(field);
    }
    doc1.add(new StringField("id", "1", Store.NO));
    doc2.add(new StringField("id", "2", Store.NO));
    doc3.add(new StringField("id", "3", Store.NO));
    w.addDocument(doc1);
    w.addDocument(doc2);
    w.addDocument(doc3);

    w.forceMerge(1);

    IndexReader reader = DirectoryReader.open(w);
    w.close();

    Query query =
        new BooleanQuery.Builder()
            .add(
                new BoostQuery(new ConstantScoreQuery(new TermQuery(new Term("field", "foo"))), 1f),
                Occur.SHOULD)
            .add(
                new BoostQuery(
                    new ConstantScoreQuery(new TermQuery(new Term("field", "bar"))), 1.5f),
                Occur.SHOULD)
            .add(
                new BoostQuery(
                    new ConstantScoreQuery(new TermQuery(new Term("field", "quux"))), 0.1f),
                Occur.SHOULD)
            .build();

    IndexSearcher searcher = newSearcher(reader);
    Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.TOP_SCORES, 1f);

    Bits liveDocs =
        new Bits() {
          @Override
          public boolean get(int index) {
            return index == 1;
          }

          @Override
          public int length() {
            return 3;
          }
        };

    // Test min competitive scores that exercise different execution modes
    for (float minCompetitiveScore :
        new float[] {
          0f, // 3 essential clauses
          1f, // 2 essential clauses
          1.2f, // 1 essential clause
          2f // two required clauses
        }) {
      BulkScorer scorer = weight.bulkScorer(searcher.getIndexReader().leaves().get(0));
      LeafCollector collector =
          new LeafCollector() {

            int i = 0;

            @Override
            public void setScorer(Scorable scorer) throws IOException {
              scorer.setMinCompetitiveScore(minCompetitiveScore);
            }

            @Override
            public void collect(int doc) throws IOException {
              assertEquals(1, doc);
              assertEquals(0, i++);
            }

            @Override
            public void finish() throws IOException {
              assertEquals(1, i);
            }
          };
      scorer.score(collector, liveDocs);
      collector.finish();
    }

    reader.close();
    dir.close();
  }

  // This test simulates what happens over time for the query `the quick fox` as collection
  // progresses and the minimum competitive score increases.
  public void testPartition() throws IOException {
    FakeScorer the = new FakeScorer("the");
    the.cost = 9_000;
    the.maxScore = 0.1f;
    FakeScorer quick = new FakeScorer("quick");
    quick.cost = 1_000;
    quick.maxScore = 1f;
    FakeScorer fox = new FakeScorer("fox");
    fox.cost = 900;
    fox.maxScore = 1.1f;

    MaxScoreBulkScorer scorer = new MaxScoreBulkScorer(10_000, Arrays.asList(the, quick, fox));
    the.docID = 4;
    the.maxScoreUpTo = 130;
    quick.docID = 4;
    quick.maxScoreUpTo = 999;
    fox.docID = 10;
    fox.maxScoreUpTo = 1_200;

    Collections.shuffle(Arrays.asList(scorer.allScorers), random());
    scorer.updateMaxWindowScores(4, 100);
    assertTrue(scorer.partitionScorers());
    assertEquals(0, scorer.firstEssentialScorer); // all clauses are essential
    assertEquals(3, scorer.firstRequiredScorer); // no required clauses

    // less than the minimum score of every clause
    scorer.minCompetitiveScore = 0.09f;
    Collections.shuffle(Arrays.asList(scorer.allScorers), random());
    scorer.updateMaxWindowScores(4, 100);
    assertTrue(scorer.partitionScorers());
    assertEquals(0, scorer.firstEssentialScorer); // all clauses are still essential
    assertEquals(3, scorer.firstRequiredScorer); // no required clauses

    // equal to the maximum score of `the`
    scorer.minCompetitiveScore = 0.1f;
    Collections.shuffle(Arrays.asList(scorer.allScorers), random());
    scorer.updateMaxWindowScores(4, 100);
    assertTrue(scorer.partitionScorers());
    assertEquals(0, scorer.firstEssentialScorer); // all clauses are still essential
    assertEquals(3, scorer.firstRequiredScorer); // no required clauses

    // gt than the minimum score of `the`
    scorer.minCompetitiveScore = 0.11f;
    Collections.shuffle(Arrays.asList(scorer.allScorers), random());
    scorer.updateMaxWindowScores(4, 100);
    assertTrue(scorer.partitionScorers());
    assertEquals(1, scorer.firstEssentialScorer); // the is non essential
    assertEquals(3, scorer.firstRequiredScorer); // no required clauses
    assertSame(the, scorer.allScorers[0].scorer);

    // equal to the sum of the max scores of the and quick
    scorer.minCompetitiveScore = 1.1f;
    Collections.shuffle(Arrays.asList(scorer.allScorers), random());
    scorer.updateMaxWindowScores(4, 100);
    assertTrue(scorer.partitionScorers());
    assertEquals(1, scorer.firstEssentialScorer); // the is non essential
    assertEquals(3, scorer.firstRequiredScorer); // no required clauses
    assertSame(the, scorer.allScorers[0].scorer);

    // greater than the sum of the max scores of the and quick
    scorer.minCompetitiveScore = 1.11f;
    Collections.shuffle(Arrays.asList(scorer.allScorers), random());
    scorer.updateMaxWindowScores(4, 100);
    assertTrue(scorer.partitionScorers());
    assertEquals(2, scorer.firstEssentialScorer); // the and quick are non essential
    assertEquals(2, scorer.firstRequiredScorer); // fox is required
    assertSame(the, scorer.allScorers[0].scorer);
    assertSame(quick, scorer.allScorers[1].scorer);
    assertSame(fox, scorer.allScorers[2].scorer);

    // equal to the sum of the max scores of the and fox
    scorer.minCompetitiveScore = 1.2f;
    Collections.shuffle(Arrays.asList(scorer.allScorers), random());
    scorer.updateMaxWindowScores(4, 100);
    assertTrue(scorer.partitionScorers());
    assertEquals(2, scorer.firstEssentialScorer); // the and quick are non essential
    assertEquals(2, scorer.firstRequiredScorer); // fox is required
    assertSame(the, scorer.allScorers[0].scorer);
    assertSame(quick, scorer.allScorers[1].scorer);
    assertSame(fox, scorer.allScorers[2].scorer);

    // greater than the sum of the max scores of the and fox
    scorer.minCompetitiveScore = 1.21f;
    Collections.shuffle(Arrays.asList(scorer.allScorers), random());
    scorer.updateMaxWindowScores(4, 100);
    assertTrue(scorer.partitionScorers());
    assertEquals(2, scorer.firstEssentialScorer); // the and quick are non essential
    assertEquals(1, scorer.firstRequiredScorer); // quick and fox are required
    assertSame(the, scorer.allScorers[0].scorer);
    assertSame(quick, scorer.allScorers[1].scorer);
    assertSame(fox, scorer.allScorers[2].scorer);

    // equal to the sum of the max scores of quick and fox
    scorer.minCompetitiveScore = 2.1f;
    Collections.shuffle(Arrays.asList(scorer.allScorers), random());
    scorer.updateMaxWindowScores(4, 100);
    assertTrue(scorer.partitionScorers());
    assertEquals(2, scorer.firstEssentialScorer); // the and quick are non essential
    assertEquals(1, scorer.firstRequiredScorer); // quick and fox are required
    assertSame(the, scorer.allScorers[0].scorer);
    assertSame(quick, scorer.allScorers[1].scorer);
    assertSame(fox, scorer.allScorers[2].scorer);

    // greater than the sum of the max scores of quick and fox
    scorer.minCompetitiveScore = 2.11f;
    Collections.shuffle(Arrays.asList(scorer.allScorers), random());
    scorer.updateMaxWindowScores(4, 100);
    assertTrue(scorer.partitionScorers());
    assertEquals(2, scorer.firstEssentialScorer); // the and quick are non essential
    assertEquals(0, scorer.firstRequiredScorer); // all terms are required
    assertSame(the, scorer.allScorers[0].scorer);
    assertSame(quick, scorer.allScorers[1].scorer);
    assertSame(fox, scorer.allScorers[2].scorer);

    // greater than the sum of the max scores of quick and fox
    scorer.minCompetitiveScore = 2.11f;
    Collections.shuffle(Arrays.asList(scorer.allScorers), random());
    scorer.updateMaxWindowScores(4, 100);
    assertTrue(scorer.partitionScorers());
    assertEquals(2, scorer.firstEssentialScorer); // the and quick are non essential
    assertEquals(0, scorer.firstRequiredScorer); // all terms are required
    assertSame(the, scorer.allScorers[0].scorer);
    assertSame(quick, scorer.allScorers[1].scorer);
    assertSame(fox, scorer.allScorers[2].scorer);

    // equal to the sum of the max scores of all terms
    scorer.minCompetitiveScore = 2.2f;
    Collections.shuffle(Arrays.asList(scorer.allScorers), random());
    scorer.updateMaxWindowScores(4, 100);
    assertTrue(scorer.partitionScorers());
    assertEquals(2, scorer.firstEssentialScorer); // the and quick are non essential
    assertEquals(0, scorer.firstRequiredScorer); // all terms are required
    assertSame(the, scorer.allScorers[0].scorer);
    assertSame(quick, scorer.allScorers[1].scorer);
    assertSame(fox, scorer.allScorers[2].scorer);

    // greater than the sum of the max scores of all terms
    scorer.minCompetitiveScore = 2.21f;
    Collections.shuffle(Arrays.asList(scorer.allScorers), random());
    scorer.updateMaxWindowScores(4, 100);
    assertFalse(scorer.partitionScorers()); // no possible match in this window
  }
}
