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
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.search.AssertingQuery;
import org.apache.lucene.tests.search.BlockScoreQueryWrapper;
import org.apache.lucene.tests.search.CheckHits;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class TestWANDScorer extends LuceneTestCase {

  public void testScalingFactor() {
    doTestScalingFactor(1);
    doTestScalingFactor(2);
    doTestScalingFactor(Math.nextDown(1f));
    doTestScalingFactor(Math.nextUp(1f));
    doTestScalingFactor(Float.MIN_VALUE);
    doTestScalingFactor(Math.nextUp(Float.MIN_VALUE));
    doTestScalingFactor(Float.MAX_VALUE);
    doTestScalingFactor(Math.nextDown(Float.MAX_VALUE));
    assertEquals(WANDScorer.scalingFactor(Float.MIN_VALUE) + 1, WANDScorer.scalingFactor(0));
    assertEquals(
        WANDScorer.scalingFactor(Float.MAX_VALUE) - 1,
        WANDScorer.scalingFactor(Float.POSITIVE_INFINITY));

    // Greater scores produce lower scaling factors
    assertTrue(WANDScorer.scalingFactor(1f) > WANDScorer.scalingFactor(10f));
    assertTrue(
        WANDScorer.scalingFactor(Float.MAX_VALUE)
            > WANDScorer.scalingFactor(Float.POSITIVE_INFINITY));
    assertTrue(WANDScorer.scalingFactor(0f) > WANDScorer.scalingFactor(Float.MIN_VALUE));
  }

  private void doTestScalingFactor(float f) {
    int scalingFactor = WANDScorer.scalingFactor(f);
    float scaled = Math.scalb(f, scalingFactor);
    assertTrue("" + scaled, scaled >= 1 << (WANDScorer.FLOAT_MANTISSA_BITS - 1));
    assertTrue("" + scaled, scaled < 1 << WANDScorer.FLOAT_MANTISSA_BITS);
  }

  public void testScaleMaxScore() {
    assertEquals(
        1 << (WANDScorer.FLOAT_MANTISSA_BITS - 1),
        WANDScorer.scaleMaxScore(32f, WANDScorer.scalingFactor(32f)));
    assertEquals(1, WANDScorer.scaleMaxScore(32f, WANDScorer.scalingFactor(Math.scalb(1f, 60))));
    assertEquals(
        1, WANDScorer.scaleMaxScore(32f, WANDScorer.scalingFactor(Float.POSITIVE_INFINITY)));
  }

  private Query maybeWrap(Query query) {
    if (random().nextBoolean()) {
      query = new BlockScoreQueryWrapper(query, TestUtil.nextInt(random(), 2, 8));
      query = new AssertingQuery(random(), query);
    }
    return query;
  }

  public void testBasics() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w =
        new IndexWriter(dir, newIndexWriterConfig().setMergePolicy(newLogMergePolicy()));
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
        doc.add(new StringField("foo", value, Store.NO));
      }
      w.addDocument(doc);
    }
    w.forceMerge(1);
    w.close();

    IndexReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = newSearcher(reader);

    Query query =
        new WANDScorerQuery(
            new BooleanQuery.Builder()
                .add(
                    new BoostQuery(new ConstantScoreQuery(new TermQuery(new Term("foo", "A"))), 2),
                    Occur.SHOULD)
                .add(new ConstantScoreQuery(new TermQuery(new Term("foo", "B"))), Occur.SHOULD)
                .add(
                    new BoostQuery(new ConstantScoreQuery(new TermQuery(new Term("foo", "C"))), 3),
                    Occur.SHOULD)
                .build());

    Scorer scorer =
        searcher
            .createWeight(searcher.rewrite(query), ScoreMode.TOP_SCORES, 1)
            .scorer(searcher.getIndexReader().leaves().get(0));

    assertEquals(0, scorer.iterator().nextDoc());
    assertEquals(2 + 1, scorer.score(), 0);

    assertEquals(1, scorer.iterator().nextDoc());
    assertEquals(2, scorer.score(), 0);

    assertEquals(3, scorer.iterator().nextDoc());
    assertEquals(2 + 1 + 3, scorer.score(), 0);

    assertEquals(4, scorer.iterator().nextDoc());
    assertEquals(1, scorer.score(), 0);

    assertEquals(5, scorer.iterator().nextDoc());
    assertEquals(1 + 3, scorer.score(), 0);

    assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());

    scorer =
        searcher
            .createWeight(searcher.rewrite(query), ScoreMode.TOP_SCORES, 1)
            .scorer(searcher.getIndexReader().leaves().get(0));
    scorer.setMinCompetitiveScore(4);

    assertEquals(3, scorer.iterator().nextDoc());
    assertEquals(2 + 1 + 3, scorer.score(), 0);

    assertEquals(5, scorer.iterator().nextDoc());
    assertEquals(1 + 3, scorer.score(), 0);

    assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());

    scorer =
        searcher
            .createWeight(searcher.rewrite(query), ScoreMode.TOP_SCORES, 1)
            .scorer(searcher.getIndexReader().leaves().get(0));

    assertEquals(0, scorer.iterator().nextDoc());
    assertEquals(2 + 1, scorer.score(), 0);

    scorer.setMinCompetitiveScore(10);

    assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());

    //  test a filtered disjunction
    query =
        new BooleanQuery.Builder()
            .add(
                new WANDScorerQuery(
                    new BooleanQuery.Builder()
                        .add(
                            new BoostQuery(
                                new ConstantScoreQuery(new TermQuery(new Term("foo", "A"))), 2),
                            Occur.SHOULD)
                        .add(
                            new ConstantScoreQuery(new TermQuery(new Term("foo", "B"))),
                            Occur.SHOULD)
                        .build()),
                Occur.MUST)
            .add(new TermQuery(new Term("foo", "C")), Occur.FILTER)
            .build();

    scorer =
        searcher
            .createWeight(searcher.rewrite(query), ScoreMode.TOP_SCORES, 1)
            .scorer(searcher.getIndexReader().leaves().get(0));

    assertEquals(3, scorer.iterator().nextDoc());
    assertEquals(2 + 1, scorer.score(), 0);

    assertEquals(5, scorer.iterator().nextDoc());
    assertEquals(1, scorer.score(), 0);

    assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());

    scorer =
        searcher
            .createWeight(searcher.rewrite(query), ScoreMode.TOP_SCORES, 1)
            .scorer(searcher.getIndexReader().leaves().get(0));

    scorer.setMinCompetitiveScore(2);

    assertEquals(3, scorer.iterator().nextDoc());
    assertEquals(2 + 1, scorer.score(), 0);

    assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());

    // Now test a filtered disjunction with a MUST_NOT
    query =
        new BooleanQuery.Builder()
            .add(
                new WANDScorerQuery(
                    new BooleanQuery.Builder()
                        .add(
                            new BoostQuery(
                                new ConstantScoreQuery(new TermQuery(new Term("foo", "A"))), 2),
                            Occur.SHOULD)
                        .add(
                            new ConstantScoreQuery(new TermQuery(new Term("foo", "B"))),
                            Occur.SHOULD)
                        .build()),
                Occur.MUST)
            .add(new TermQuery(new Term("foo", "C")), Occur.MUST_NOT)
            .build();

    scorer =
        searcher
            .createWeight(searcher.rewrite(query), ScoreMode.TOP_SCORES, 1)
            .scorer(searcher.getIndexReader().leaves().get(0));

    assertEquals(0, scorer.iterator().nextDoc());
    assertEquals(2 + 1, scorer.score(), 0);

    assertEquals(1, scorer.iterator().nextDoc());
    assertEquals(2, scorer.score(), 0);

    assertEquals(4, scorer.iterator().nextDoc());
    assertEquals(1, scorer.score(), 0);

    assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());

    scorer =
        searcher
            .createWeight(searcher.rewrite(query), ScoreMode.TOP_SCORES, 1)
            .scorer(searcher.getIndexReader().leaves().get(0));

    scorer.setMinCompetitiveScore(3);

    assertEquals(0, scorer.iterator().nextDoc());
    assertEquals(2 + 1, scorer.score(), 0);

    assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());

    reader.close();
    dir.close();
  }

  public void testBasicsWithDisjunctionAndMinShouldMatch() throws Exception {
    try (Directory dir = newDirectory()) {
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
            doc.add(new StringField("foo", value, Store.NO));
          }
          w.addDocument(doc);
        }

        w.forceMerge(1);
      }

      try (IndexReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = newSearcher(reader);

        Query query =
            new WANDScorerQuery(
                new BooleanQuery.Builder()
                    .add(
                        new BoostQuery(
                            new ConstantScoreQuery(new TermQuery(new Term("foo", "A"))), 2),
                        Occur.SHOULD)
                    .add(new ConstantScoreQuery(new TermQuery(new Term("foo", "B"))), Occur.SHOULD)
                    .add(
                        new BoostQuery(
                            new ConstantScoreQuery(new TermQuery(new Term("foo", "C"))), 3),
                        Occur.SHOULD)
                    .setMinimumNumberShouldMatch(2)
                    .build());

        Scorer scorer =
            searcher
                .createWeight(searcher.rewrite(query), ScoreMode.TOP_SCORES, 1)
                .scorer(searcher.getIndexReader().leaves().get(0));

        assertEquals(0, scorer.iterator().nextDoc());
        assertEquals(2 + 1, scorer.score(), 0);

        assertEquals(3, scorer.iterator().nextDoc());
        assertEquals(2 + 1 + 3, scorer.score(), 0);

        assertEquals(5, scorer.iterator().nextDoc());
        assertEquals(1 + 3, scorer.score(), 0);

        assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());

        scorer =
            searcher
                .createWeight(searcher.rewrite(query), ScoreMode.TOP_SCORES, 1)
                .scorer(searcher.getIndexReader().leaves().get(0));
        scorer.setMinCompetitiveScore(4);

        assertEquals(3, scorer.iterator().nextDoc());
        assertEquals(2 + 1 + 3, scorer.score(), 0);

        assertEquals(5, scorer.iterator().nextDoc());
        assertEquals(1 + 3, scorer.score(), 0);

        assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());

        scorer =
            searcher
                .createWeight(searcher.rewrite(query), ScoreMode.TOP_SCORES, 1)
                .scorer(searcher.getIndexReader().leaves().get(0));

        assertEquals(0, scorer.iterator().nextDoc());
        assertEquals(2 + 1, scorer.score(), 0);

        scorer.setMinCompetitiveScore(10);

        assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());
      }
    }
  }

  public void testBasicsWithDisjunctionAndMinShouldMatchAndTailSizeCondition() throws Exception {
    try (Directory dir = newDirectory()) {
      try (IndexWriter w =
          new IndexWriter(dir, newIndexWriterConfig().setMergePolicy(newLogMergePolicy()))) {
        for (String[] values :
            Arrays.asList(
                new String[] {"A", "B"}, // 0
                new String[] {"A"}, // 1
                new String[] {}, // 2
                new String[] {"A", "B", "C"}, // 3
                // 2 "B"s here and the non constant score term query below forces the
                // tailMaxScore >= minCompetitiveScore && tailSize < minShouldMatch condition
                new String[] {"B", "B"}, // 4
                new String[] {"B", "C"} // 5
                )) {
          Document doc = new Document();
          for (String value : values) {
            doc.add(new StringField("foo", value, Store.NO));
          }
          w.addDocument(doc);
        }

        w.forceMerge(1);
      }

      try (IndexReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = newSearcher(reader);

        Query query =
            new WANDScorerQuery(
                new BooleanQuery.Builder()
                    .add(new TermQuery(new Term("foo", "A")), Occur.SHOULD)
                    .add(new TermQuery(new Term("foo", "B")), Occur.SHOULD)
                    .add(new TermQuery(new Term("foo", "C")), Occur.SHOULD)
                    .setMinimumNumberShouldMatch(2)
                    .build());

        Scorer scorer =
            searcher
                .createWeight(searcher.rewrite(query), ScoreMode.TOP_SCORES, 1)
                .scorer(searcher.getIndexReader().leaves().get(0));

        assertEquals(0, scorer.iterator().nextDoc());
        scorer.setMinCompetitiveScore(scorer.score());

        assertEquals(3, scorer.iterator().nextDoc());
      }
    }
  }

  public void testBasicsWithDisjunctionAndMinShouldMatchAndNonScoringMode() throws Exception {
    try (Directory dir = newDirectory()) {
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
            doc.add(new StringField("foo", value, Store.NO));
          }
          w.addDocument(doc);
        }

        w.forceMerge(1);
      }

      try (IndexReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = newSearcher(reader);

        Query query =
            new WANDScorerQuery(
                new BooleanQuery.Builder()
                    .add(
                        new BoostQuery(
                            new ConstantScoreQuery(new TermQuery(new Term("foo", "A"))), 2),
                        Occur.SHOULD)
                    .add(new ConstantScoreQuery(new TermQuery(new Term("foo", "B"))), Occur.SHOULD)
                    .add(
                        new BoostQuery(
                            new ConstantScoreQuery(new TermQuery(new Term("foo", "C"))), 3),
                        Occur.SHOULD)
                    .setMinimumNumberShouldMatch(2)
                    .build());

        Scorer scorer =
            searcher
                .createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1)
                .scorer(searcher.getIndexReader().leaves().get(0));

        assertEquals(0, scorer.iterator().nextDoc());
        assertEquals(3, scorer.iterator().nextDoc());
        assertEquals(5, scorer.iterator().nextDoc());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());
      }
    }
  }

  public void testBasicsWithFilteredDisjunctionAndMinShouldMatch() throws Exception {
    try (Directory dir = newDirectory()) {
      try (IndexWriter w =
          new IndexWriter(dir, newIndexWriterConfig().setMergePolicy(newLogMergePolicy()))) {
        for (String[] values :
            Arrays.asList(
                new String[] {"A", "B"}, // 0
                new String[] {"A", "C", "D"}, // 1
                new String[] {}, // 2
                new String[] {"A", "B", "C", "D"}, // 3
                new String[] {"B"}, // 4
                new String[] {"C", "D"} // 5
                )) {
          Document doc = new Document();
          for (String value : values) {
            doc.add(new StringField("foo", value, Store.NO));
          }
          w.addDocument(doc);
        }

        w.forceMerge(1);
      }

      try (IndexReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = newSearcher(reader);

        Query query =
            new BooleanQuery.Builder()
                .add(
                    new WANDScorerQuery(
                        new BooleanQuery.Builder()
                            .add(
                                new BoostQuery(
                                    new ConstantScoreQuery(new TermQuery(new Term("foo", "A"))), 2),
                                Occur.SHOULD)
                            .add(
                                new ConstantScoreQuery(new TermQuery(new Term("foo", "B"))),
                                Occur.SHOULD)
                            .add(
                                new BoostQuery(
                                    new ConstantScoreQuery(new TermQuery(new Term("foo", "D"))), 4),
                                Occur.SHOULD)
                            .setMinimumNumberShouldMatch(2)
                            .build()),
                    Occur.MUST)
                .add(new TermQuery(new Term("foo", "C")), Occur.FILTER)
                .build();

        Scorer scorer =
            searcher
                .createWeight(searcher.rewrite(query), ScoreMode.TOP_SCORES, 1)
                .scorer(searcher.getIndexReader().leaves().get(0));

        assertEquals(1, scorer.iterator().nextDoc());
        assertEquals(2 + 4, scorer.score(), 0);

        assertEquals(3, scorer.iterator().nextDoc());
        assertEquals(2 + 1 + 4, scorer.score(), 0);

        assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());

        scorer =
            searcher
                .createWeight(searcher.rewrite(query), ScoreMode.TOP_SCORES, 1)
                .scorer(searcher.getIndexReader().leaves().get(0));

        scorer.setMinCompetitiveScore(2 + 1 + 4);

        assertEquals(3, scorer.iterator().nextDoc());
        assertEquals(2 + 1 + 4, scorer.score(), 0);

        assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());
      }
    }
  }

  public void testBasicsWithFilteredDisjunctionAndMinShouldMatchAndNonScoringMode()
      throws Exception {
    try (Directory dir = newDirectory()) {
      try (IndexWriter w =
          new IndexWriter(dir, newIndexWriterConfig().setMergePolicy(newLogMergePolicy()))) {
        for (String[] values :
            Arrays.asList(
                new String[] {"A", "B"}, // 0
                new String[] {"A", "C", "D"}, // 1
                new String[] {}, // 2
                new String[] {"A", "B", "C", "D"}, // 3
                new String[] {"B"}, // 4
                new String[] {"C", "D"} // 5
                )) {
          Document doc = new Document();
          for (String value : values) {
            doc.add(new StringField("foo", value, Store.NO));
          }
          w.addDocument(doc);
        }

        w.forceMerge(1);
      }

      try (IndexReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = newSearcher(reader);

        Query query =
            new BooleanQuery.Builder()
                .add(
                    new WANDScorerQuery(
                        new BooleanQuery.Builder()
                            .add(
                                new BoostQuery(
                                    new ConstantScoreQuery(new TermQuery(new Term("foo", "A"))), 2),
                                Occur.SHOULD)
                            .add(
                                new ConstantScoreQuery(new TermQuery(new Term("foo", "B"))),
                                Occur.SHOULD)
                            .add(
                                new BoostQuery(
                                    new ConstantScoreQuery(new TermQuery(new Term("foo", "D"))), 4),
                                Occur.SHOULD)
                            .setMinimumNumberShouldMatch(2)
                            .build()),
                    Occur.MUST)
                .add(new TermQuery(new Term("foo", "C")), Occur.FILTER)
                .build();

        Scorer scorer =
            searcher
                .createWeight(searcher.rewrite(query), ScoreMode.TOP_DOCS, 1)
                .scorer(searcher.getIndexReader().leaves().get(0));

        assertEquals(1, scorer.iterator().nextDoc());
        assertEquals(3, scorer.iterator().nextDoc());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());
      }
    }
  }

  public void testBasicsWithFilteredDisjunctionAndMustNotAndMinShouldMatch() throws Exception {
    try (Directory dir = newDirectory()) {
      try (IndexWriter w =
          new IndexWriter(dir, newIndexWriterConfig().setMergePolicy(newLogMergePolicy()))) {
        for (String[] values :
            Arrays.asList(
                new String[] {"A", "B"}, // 0
                new String[] {"A", "C", "D"}, // 1
                new String[] {}, // 2
                new String[] {"A", "B", "C", "D"}, // 3
                new String[] {"B", "D"}, // 4
                new String[] {"C", "D"} // 5
                )) {
          Document doc = new Document();
          for (String value : values) {
            doc.add(new StringField("foo", value, Store.NO));
          }
          w.addDocument(doc);
        }

        w.forceMerge(1);
      }

      try (IndexReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = newSearcher(reader);

        Query query =
            new BooleanQuery.Builder()
                .add(
                    new WANDScorerQuery(
                        new BooleanQuery.Builder()
                            .add(
                                new BoostQuery(
                                    new ConstantScoreQuery(new TermQuery(new Term("foo", "A"))), 2),
                                Occur.SHOULD)
                            .add(
                                new ConstantScoreQuery(new TermQuery(new Term("foo", "B"))),
                                Occur.SHOULD)
                            .add(
                                new BoostQuery(
                                    new ConstantScoreQuery(new TermQuery(new Term("foo", "D"))), 4),
                                Occur.SHOULD)
                            .setMinimumNumberShouldMatch(2)
                            .build()),
                    Occur.MUST)
                .add(new TermQuery(new Term("foo", "C")), Occur.MUST_NOT)
                .build();

        Scorer scorer =
            searcher
                .createWeight(searcher.rewrite(query), ScoreMode.TOP_SCORES, 1)
                .scorer(searcher.getIndexReader().leaves().get(0));

        assertEquals(0, scorer.iterator().nextDoc());
        assertEquals(2 + 1, scorer.score(), 0);

        assertEquals(4, scorer.iterator().nextDoc());
        assertEquals(1 + 4, scorer.score(), 0);

        assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());

        scorer =
            searcher
                .createWeight(searcher.rewrite(query), ScoreMode.TOP_SCORES, 1)
                .scorer(searcher.getIndexReader().leaves().get(0));

        scorer.setMinCompetitiveScore(4);

        assertEquals(4, scorer.iterator().nextDoc());
        assertEquals(1 + 4, scorer.score(), 0);

        assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());
      }
    }
  }

  public void testBasicsWithFilteredDisjunctionAndMustNotAndMinShouldMatchAndNonScoringMode()
      throws Exception {
    try (Directory dir = newDirectory()) {
      try (IndexWriter w =
          new IndexWriter(dir, newIndexWriterConfig().setMergePolicy(newLogMergePolicy()))) {
        for (String[] values :
            Arrays.asList(
                new String[] {"A", "B"}, // 0
                new String[] {"A", "C", "D"}, // 1
                new String[] {}, // 2
                new String[] {"A", "B", "C", "D"}, // 3
                new String[] {"B", "D"}, // 4
                new String[] {"C", "D"} // 5
                )) {
          Document doc = new Document();
          for (String value : values) {
            doc.add(new StringField("foo", value, Store.NO));
          }
          w.addDocument(doc);
        }

        w.forceMerge(1);
      }

      try (IndexReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = newSearcher(reader);

        Query query =
            new BooleanQuery.Builder()
                .add(
                    new WANDScorerQuery(
                        new BooleanQuery.Builder()
                            .add(
                                new BoostQuery(
                                    new ConstantScoreQuery(new TermQuery(new Term("foo", "A"))), 2),
                                Occur.SHOULD)
                            .add(
                                new ConstantScoreQuery(new TermQuery(new Term("foo", "B"))),
                                Occur.SHOULD)
                            .add(
                                new BoostQuery(
                                    new ConstantScoreQuery(new TermQuery(new Term("foo", "D"))), 4),
                                Occur.SHOULD)
                            .setMinimumNumberShouldMatch(2)
                            .build()),
                    Occur.MUST)
                .add(new TermQuery(new Term("foo", "C")), Occur.MUST_NOT)
                .build();

        Scorer scorer =
            searcher
                .createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1)
                .scorer(searcher.getIndexReader().leaves().get(0));

        assertEquals(0, scorer.iterator().nextDoc());
        assertEquals(4, scorer.iterator().nextDoc());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());
      }
    }
  }

  public void testRandom() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    int numDocs = atLeast(1000);
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      int numValues = random().nextInt(1 << random().nextInt(5));
      int start = random().nextInt(10);
      for (int j = 0; j < numValues; ++j) {
        doc.add(new StringField("foo", Integer.toString(start + j), Store.NO));
      }
      w.addDocument(doc);
    }
    IndexReader reader = DirectoryReader.open(w);
    w.close();
    // turn off concurrent search to avoid Random object used across threads resulting into
    // RuntimeException, as WANDScorerQuery#createWeight has reference to this searcher,
    // but will be called during searching
    IndexSearcher searcher = newSearcher(reader, true, true, false);

    for (int iter = 0; iter < 100; ++iter) {
      int start = random().nextInt(10);
      int numClauses = random().nextInt(1 << random().nextInt(5));
      BooleanQuery.Builder builder = new BooleanQuery.Builder();
      for (int i = 0; i < numClauses; ++i) {
        builder.add(
            maybeWrap(new TermQuery(new Term("foo", Integer.toString(start + i)))), Occur.SHOULD);
      }
      Query query = new WANDScorerQuery(builder.build());

      CheckHits.checkTopScores(random(), query, searcher);

      int filterTerm = random().nextInt(30);
      Query filteredQuery =
          new BooleanQuery.Builder()
              .add(query, Occur.MUST)
              .add(new TermQuery(new Term("foo", Integer.toString(filterTerm))), Occur.FILTER)
              .build();

      CheckHits.checkTopScores(random(), filteredQuery, searcher);
    }
    reader.close();
    dir.close();
  }

  /** Degenerate case: all clauses produce a score of 0. */
  public void testRandomWithZeroScores() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    int numDocs = atLeast(1000);
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      int numValues = random().nextInt(1 << random().nextInt(5));
      int start = random().nextInt(10);
      for (int j = 0; j < numValues; ++j) {
        doc.add(new StringField("foo", Integer.toString(start + j), Store.NO));
      }
      w.addDocument(doc);
    }
    IndexReader reader = DirectoryReader.open(w);
    w.close();
    // turn off concurrent search to avoid Random object used across threads resulting into
    // RuntimeException, as WANDScorerQuery#createWeight has reference to this searcher,
    // but will be called during searching
    IndexSearcher searcher = newSearcher(reader, true, true, false);

    for (int iter = 0; iter < 100; ++iter) {
      int start = random().nextInt(10);
      int numClauses = random().nextInt(1 << random().nextInt(5));
      BooleanQuery.Builder builder = new BooleanQuery.Builder();
      for (int i = 0; i < numClauses; ++i) {
        builder.add(
            maybeWrap(
                new BoostQuery(
                    new ConstantScoreQuery(
                        new TermQuery(new Term("foo", Integer.toString(start + i)))),
                    0f)),
            Occur.SHOULD);
      }
      Query query = new WANDScorerQuery(builder.build());

      CheckHits.checkTopScores(random(), query, searcher);

      int filterTerm = random().nextInt(30);
      Query filteredQuery =
          new BooleanQuery.Builder()
              .add(query, Occur.MUST)
              .add(new TermQuery(new Term("foo", Integer.toString(filterTerm))), Occur.FILTER)
              .build();

      CheckHits.checkTopScores(random(), filteredQuery, searcher);
    }
    reader.close();
    dir.close();
  }

  /** Test the case when some clauses produce infinite max scores. */
  public void testRandomWithInfiniteMaxScore() throws IOException {
    doTestRandomSpecialMaxScore(Float.POSITIVE_INFINITY);
  }

  /** Test the case when some clauses produce finite max scores, but their sum overflows. */
  public void testRandomWithMaxScoreOverflow() throws IOException {
    doTestRandomSpecialMaxScore(Float.MAX_VALUE);
  }

  private void doTestRandomSpecialMaxScore(float maxScore) throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    int numDocs = atLeast(1000);
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      int numValues = random().nextInt(1 << random().nextInt(5));
      int start = random().nextInt(10);
      for (int j = 0; j < numValues; ++j) {
        doc.add(new StringField("foo", Integer.toString(start + j), Store.NO));
      }
      w.addDocument(doc);
    }
    IndexReader reader = DirectoryReader.open(w);
    w.close();
    // turn off concurrent search to avoid Random object used across threads resulting into
    // RuntimeException, as WANDScorerQuery#createWeight has reference to this searcher,
    // but will be called during searching
    IndexSearcher searcher = newSearcher(reader, true, true, false);

    for (int iter = 0; iter < 100; ++iter) {
      int start = random().nextInt(10);
      int numClauses = random().nextInt(1 << random().nextInt(5));
      BooleanQuery.Builder builder = new BooleanQuery.Builder();
      for (int i = 0; i < numClauses; ++i) {
        Query query = new TermQuery(new Term("foo", Integer.toString(start + i)));
        if (random().nextBoolean()) {
          query =
              new MaxScoreWrapperQuery(
                  query, numDocs / TestUtil.nextInt(random(), 1, 100), maxScore);
        }
        builder.add(query, Occur.SHOULD);
      }
      Query query = new WANDScorerQuery(builder.build());

      CheckHits.checkTopScores(random(), query, searcher);

      int filterTerm = random().nextInt(30);
      Query filteredQuery =
          new BooleanQuery.Builder()
              .add(query, Occur.MUST)
              .add(new TermQuery(new Term("foo", Integer.toString(filterTerm))), Occur.FILTER)
              .build();

      CheckHits.checkTopScores(random(), filteredQuery, searcher);
    }
    reader.close();
    dir.close();
  }

  private static class MaxScoreWrapperScorer extends FilterScorer {

    private final int maxRange;
    private final float maxScore;
    private int lastShallowTarget = -1;

    MaxScoreWrapperScorer(Scorer scorer, int maxRange, float maxScore) {
      super(scorer);
      this.maxRange = maxRange;
      this.maxScore = maxScore;
    }

    @Override
    public int advanceShallow(int target) throws IOException {
      lastShallowTarget = target;
      return in.advanceShallow(target);
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
      if (upTo - Math.max(docID(), lastShallowTarget) >= maxRange) {
        return maxScore;
      }
      return in.getMaxScore(upTo);
    }
  }

  private static class MaxScoreWrapperQuery extends Query {

    private final Query query;
    private final int maxRange;
    private final float maxScore;

    /**
     * If asked for the maximum score over a range of doc IDs that is greater than or equal to
     * maxRange, this query will return the provided maxScore.
     */
    MaxScoreWrapperQuery(Query query, int maxRange, float maxScore) {
      this.query = query;
      this.maxRange = maxRange;
      this.maxScore = maxScore;
    }

    @Override
    public String toString(String field) {
      return query.toString(field);
    }

    @Override
    public boolean equals(Object obj) {
      if (sameClassAs(obj) == false) {
        return false;
      }
      MaxScoreWrapperQuery that = (MaxScoreWrapperQuery) obj;
      return query.equals(that.query) && maxRange == that.maxRange && maxScore == that.maxScore;
    }

    @Override
    public int hashCode() {
      int hash = classHash();
      hash = 31 * hash + query.hashCode();
      hash = 31 * hash + Integer.hashCode(maxRange);
      hash = 31 * hash + Float.hashCode(maxScore);
      return hash;
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
      Query rewritten = query.rewrite(indexSearcher);
      if (rewritten != query) {
        return new MaxScoreWrapperQuery(rewritten, maxRange, maxScore);
      }
      return super.rewrite(indexSearcher);
    }

    @Override
    public void visit(QueryVisitor visitor) {}

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
        throws IOException {
      return new FilterWeight(query.createWeight(searcher, scoreMode, boost)) {
        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
          Scorer scorer = super.scorer(context);
          if (scorer == null) {
            return null;
          } else {
            return new MaxScoreWrapperScorer(scorer, maxRange, maxScore);
          }
        }

        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
          ScorerSupplier supplier = super.scorerSupplier(context);
          if (supplier == null) {
            return null;
          } else {
            return new ScorerSupplier() {

              @Override
              public Scorer get(long leadCost) throws IOException {
                return new MaxScoreWrapperScorer(supplier.get(leadCost), maxRange, maxScore);
              }

              @Override
              public long cost() {
                return supplier.cost();
              }
            };
          }
        }
      };
    }
  }

  private static class WANDScorerQuery extends Query {
    private final BooleanQuery query;

    private WANDScorerQuery(BooleanQuery query) {
      assert query.clauses().size() == query.getClauses(Occur.SHOULD).size()
          : "This test utility query is only used to create WANDScorer for disjunctions.";
      this.query = query;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
        throws IOException {
      return new Weight(query) {

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
          // no-ops
          return null;
        }

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
          BooleanWeight weight = (BooleanWeight) query.createWeight(searcher, scoreMode, boost);
          List<Scorer> optionalScorers =
              weight.weightedClauses.stream()
                  .map(wc -> wc.weight)
                  .map(
                      w -> {
                        try {
                          return w.scorerSupplier(context);
                        } catch (IOException e) {
                          throw new AssertionError(e);
                        }
                      })
                  .filter(Objects::nonNull)
                  .map(
                      ss -> {
                        try {
                          return ss.get(Long.MAX_VALUE);
                        } catch (IOException e) {
                          throw new AssertionError(e);
                        }
                      })
                  .collect(Collectors.toList());

          if (optionalScorers.size() > 0) {
            return new WANDScorer(
                weight, optionalScorers, query.getMinimumNumberShouldMatch(), scoreMode);
          } else {
            return weight.scorer(context);
          }
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
          return false;
        }
      };
    }

    @Override
    public String toString(String field) {
      return "WANDScorerQuery";
    }

    @Override
    public void visit(QueryVisitor visitor) {
      // no-ops
    }

    @Override
    public boolean equals(Object other) {
      return sameClassAs(other) && query.equals(((TestWANDScorer.WANDScorerQuery) other).query);
    }

    @Override
    public int hashCode() {
      return 31 * classHash() + query.hashCode();
    }
  }
}
