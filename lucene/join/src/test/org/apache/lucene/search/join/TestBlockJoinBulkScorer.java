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
package org.apache.lucene.search.join;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class TestBlockJoinBulkScorer extends LuceneTestCase {
  private static final String TYPE_FIELD_NAME = "type";
  private static final String VALUE_FIELD_NAME = "value";
  private static final String PARENT_FILTER_VALUE = "parent";
  private static final String CHILD_FILTER_VALUE = "child";

  private enum MatchValue {
    MATCH_A("A", 1),
    MATCH_B("B", 2),
    MATCH_C("C", 3),
    MATCH_D("D", 4);

    private static final List<MatchValue> VALUES = List.of(values());

    private final String text;
    private final int score;

    MatchValue(String text, int score) {
      this.text = text;
      this.score = score;
    }

    public String getText() {
      return text;
    }

    public int getScore() {
      return score;
    }

    @Override
    public String toString() {
      return text;
    }

    public static MatchValue random() {
      return RandomPicks.randomFrom(LuceneTestCase.random(), VALUES);
    }
  }

  private record ChildDocMatch(int docId, List<MatchValue> matches) {
    public ChildDocMatch(int docId, List<MatchValue> matches) {
      this.docId = docId;
      this.matches = Collections.unmodifiableList(matches);
    }
  }

  private static Map<Integer, List<ChildDocMatch>> populateRandomIndex(
      RandomIndexWriter writer, int maxParentDocCount, int maxChildDocCount, int maxChildDocMatches)
      throws IOException {
    Map<Integer, List<ChildDocMatch>> expectedMatches = new HashMap<>();

    final int parentDocCount = random().nextInt(1, maxParentDocCount + 1);
    int currentDocId = 0;
    for (int i = 0; i < parentDocCount; i++) {
      final int childDocCount = random().nextInt(maxChildDocCount + 1);
      List<Document> docs = new ArrayList<>(childDocCount);
      List<ChildDocMatch> childDocMatches = new ArrayList<>(childDocCount);

      for (int j = 0; j < childDocCount; j++) {
        // Build a child doc
        Document childDoc = new Document();
        childDoc.add(newStringField(TYPE_FIELD_NAME, CHILD_FILTER_VALUE, Field.Store.NO));

        final int matchCount = random().nextInt(maxChildDocMatches + 1);
        List<MatchValue> matchValues = new ArrayList<>(matchCount);
        for (int k = 0; k < matchCount; k++) {
          // Add a match to the child doc
          MatchValue matchValue = MatchValue.random();
          matchValues.add(matchValue);
          childDoc.add(newStringField(VALUE_FIELD_NAME, matchValue.getText(), Field.Store.NO));
        }

        docs.add(childDoc);
        childDocMatches.add(new ChildDocMatch(currentDocId++, matchValues));
      }

      // Build a parent doc
      Document parentDoc = new Document();
      parentDoc.add(newStringField(TYPE_FIELD_NAME, PARENT_FILTER_VALUE, Field.Store.NO));
      docs.add(parentDoc);

      // Don't add parent docs with no children to expectedMatches
      if (childDocCount > 0) {
        expectedMatches.put(currentDocId, childDocMatches);
      }
      currentDocId++;

      writer.addDocuments(docs);
    }

    return expectedMatches;
  }

  private static void populateStaticIndex(RandomIndexWriter writer) throws IOException {
    // Use these vars to improve readability when defining the docs
    final String A = MatchValue.MATCH_A.getText();
    final String B = MatchValue.MATCH_B.getText();
    final String C = MatchValue.MATCH_C.getText();
    final String D = MatchValue.MATCH_D.getText();

    for (String[][] values :
        Arrays.asList(
            new String[][] {{A, B}, {A, B, C}},
            new String[][] {{A}, {B}},
            new String[][] {{}},
            new String[][] {{A, B, C}, {A, B, C, D}},
            new String[][] {{B}},
            new String[][] {{B, C}, {A, B}, {A, C}})) {

      List<Document> docs = new ArrayList<>();
      for (String[] value : values) {
        Document childDoc = new Document();
        childDoc.add(newStringField(TYPE_FIELD_NAME, CHILD_FILTER_VALUE, Field.Store.NO));
        for (String v : value) {
          childDoc.add(newStringField(VALUE_FIELD_NAME, v, Field.Store.NO));
        }
        docs.add(childDoc);
      }

      Document parentDoc = new Document();
      parentDoc.add(newStringField(TYPE_FIELD_NAME, PARENT_FILTER_VALUE, Field.Store.NO));
      docs.add(parentDoc);

      writer.addDocuments(docs);
    }
  }

  private static Map<Integer, Float> computeExpectedScores(
      Map<Integer, List<ChildDocMatch>> expectedMatches,
      ScoreMode joinScoreMode,
      org.apache.lucene.search.ScoreMode searchScoreMode) {
    Map<Integer, Float> expectedScores = new HashMap<>();
    for (var entry : expectedMatches.entrySet()) {
      // Filter out child docs with no matches since those will never contribute to the score
      List<ChildDocMatch> childDocMatches =
          entry.getValue().stream().filter(m -> !m.matches().isEmpty()).toList();
      if (childDocMatches.isEmpty()) {
        continue;
      }

      double expectedScore = 0;
      if (searchScoreMode.needsScores()) {
        boolean firstScore = true;
        for (ChildDocMatch childDocMatch : childDocMatches) {
          float expectedChildDocScore = computeExpectedScore(childDocMatch);
          switch (joinScoreMode) {
            case Total:
            case Avg:
              expectedScore += expectedChildDocScore;
              break;
            case Min:
              expectedScore =
                  firstScore
                      ? expectedChildDocScore
                      : Math.min(expectedScore, expectedChildDocScore);
              break;
            case Max:
              expectedScore = Math.max(expectedScore, expectedChildDocScore);
              break;
            case None:
              break;
            default:
              throw new AssertionError();
          }

          firstScore = false;
        }

        if (joinScoreMode == ScoreMode.Avg) {
          expectedScore /= childDocMatches.size();
        }
      }

      expectedScores.put(entry.getKey(), (float) expectedScore);
    }

    return expectedScores;
  }

  private static float computeExpectedScore(ChildDocMatch childDocMatch) {
    float expectedScore = 0.0f;
    Set<MatchValue> matchValueSet = new HashSet<>(childDocMatch.matches());
    for (MatchValue matchValue : matchValueSet) {
      expectedScore += matchValue.getScore();
    }

    return expectedScore;
  }

  private static ToParentBlockJoinQuery buildQuery(ScoreMode scoreMode) {
    BooleanQuery.Builder childQueryBuilder = new BooleanQuery.Builder();
    for (MatchValue matchValue : MatchValue.VALUES) {
      childQueryBuilder.add(
          new BoostQuery(
              new ConstantScoreQuery(
                  new TermQuery(new Term(VALUE_FIELD_NAME, matchValue.getText()))),
              matchValue.getScore()),
          BooleanClause.Occur.SHOULD);
    }
    BitSetProducer parentsFilter =
        new QueryBitSetProducer(new TermQuery(new Term(TYPE_FIELD_NAME, PARENT_FILTER_VALUE)));
    return new ToParentBlockJoinQuery(childQueryBuilder.build(), parentsFilter, scoreMode);
  }

  private static void assertScores(
      BulkScorer bulkScorer,
      org.apache.lucene.search.ScoreMode scoreMode,
      Float minScore,
      Map<Integer, Float> expectedScores)
      throws IOException {
    assertScores(bulkScorer, scoreMode, minScore, List.of(expectedScores));
  }

  private static void assertScores(
      BulkScorer bulkScorer,
      org.apache.lucene.search.ScoreMode scoreMode,
      Float minScore,
      List<Map<Integer, Float>> expectedScoresList)
      throws IOException {
    Map<Integer, Float> actualScores = new HashMap<>();
    bulkScorer.score(
        new LeafCollector() {
          private Scorable scorer;

          @Override
          public void setScorer(Scorable scorer) throws IOException {
            assertNotNull(scorer);
            this.scorer = scorer;
            if (minScore != null) {
              this.scorer.setMinCompetitiveScore(minScore);
            }
          }

          @Override
          public void collect(int doc) throws IOException {
            assertNotNull(scorer);
            actualScores.put(doc, scoreMode.needsScores() ? scorer.score() : 0);
          }
        },
        null,
        0,
        NO_MORE_DOCS);

    if (expectedScoresList.size() == 1) {
      assertEquals(expectedScoresList.getFirst(), actualScores);
    } else {
      assertEqualsToOneOf(expectedScoresList, actualScores);
    }
  }

  private static void assertEqualsToOneOf(List<?> expectedList, Object actual) {
    boolean foundMatch = false;
    for (Object expected : expectedList) {
      if (Objects.equals(expected, actual)) {
        foundMatch = true;
        break;
      }
    }

    if (!foundMatch) {
      throw new AssertionError("expected one of: " + expectedList + " but was: " + actual);
    }
  }

  // TODO: incredibly slow
  @Nightly
  public void testScoreRandomIndices() throws IOException {
    for (int i = 0; i < 200 * RANDOM_MULTIPLIER; i++) {
      try (Directory dir = newDirectory()) {
        Map<Integer, List<ChildDocMatch>> expectedMatches;
        try (RandomIndexWriter w =
            new RandomIndexWriter(
                random(),
                dir,
                newIndexWriterConfig()
                    .setMergePolicy(
                        // retain doc id order
                        newLogMergePolicy()))) {
          w.w
              .getConfig()
              .getCodec()
              .compoundFormat()
              .setShouldUseCompoundFile(random().nextBoolean());

          expectedMatches =
              populateRandomIndex(
                  w,
                  TestUtil.nextInt(random(), 10 * RANDOM_MULTIPLIER, 30 * RANDOM_MULTIPLIER),
                  20,
                  3);
          w.forceMerge(1);
        }

        try (IndexReader reader = DirectoryReader.open(dir)) {
          final IndexSearcher searcher = newSearcher(reader);
          final ScoreMode joinScoreMode =
              RandomPicks.randomFrom(LuceneTestCase.random(), ScoreMode.values());
          final org.apache.lucene.search.ScoreMode searchScoreMode =
              RandomPicks.randomFrom(
                  LuceneTestCase.random(), org.apache.lucene.search.ScoreMode.values());
          final Map<Integer, Float> expectedScores =
              computeExpectedScores(expectedMatches, joinScoreMode, searchScoreMode);

          ToParentBlockJoinQuery query = buildQuery(joinScoreMode);
          Weight weight = searcher.createWeight(searcher.rewrite(query), searchScoreMode, 1);
          ScorerSupplier ss = weight.scorerSupplier(searcher.getIndexReader().leaves().get(0));
          if (ss == null) {
            // Score supplier will be null when there are no matches
            assertTrue(expectedScores.isEmpty());
            continue;
          }

          assertScores(ss.bulkScorer(), searchScoreMode, null, expectedScores);
        }
      }
    }
  }

  public void testSetMinCompetitiveScoreWithScoreModeMax() throws IOException {
    try (Directory dir = newDirectory()) {
      try (RandomIndexWriter w =
          new RandomIndexWriter(
              random(),
              dir,
              newIndexWriterConfig()
                  .setMergePolicy(
                      // retain doc id order
                      newLogMergePolicy()))) {
        w.w
            .getConfig()
            .getCodec()
            .compoundFormat()
            .setShouldUseCompoundFile(random().nextBoolean());
        populateStaticIndex(w);
        w.forceMerge(1);
      }

      try (IndexReader reader = DirectoryReader.open(dir)) {
        final IndexSearcher searcher = newSearcher(reader);
        final ToParentBlockJoinQuery query = buildQuery(ScoreMode.Max);
        final org.apache.lucene.search.ScoreMode scoreMode =
            org.apache.lucene.search.ScoreMode.TOP_SCORES;
        final Weight weight = searcher.createWeight(searcher.rewrite(query), scoreMode, 1);

        {
          Map<Integer, Float> expectedScores =
              Map.of(
                  2, 6.0f,
                  5, 2.0f,
                  10, 10.0f,
                  12, 2.0f,
                  16, 5.0f);

          ScorerSupplier ss = weight.scorerSupplier(searcher.getIndexReader().leaves().get(0));
          ss.setTopLevelScoringClause();
          assertScores(ss.bulkScorer(), scoreMode, null, expectedScores);
        }

        {
          // This test case has two potential results.
          // If all docs are scored in the same batch, then doc 16 is collected because
          // MaxScoreBulkScorer scores assuming A will match in doc 13, leading to a potential max
          // score of 6.
          // If the scoring is split across two or more batches, then doc 16 is not collected
          // because MaxScoreBulkScorer does not assume A will match in doc 13, leading to a
          // potential max score of 5.
          Map<Integer, Float> expectedScores1 =
              Map.of(
                  2, 6.0f,
                  10, 10.0f);
          Map<Integer, Float> expectedScores2 =
              Map.of(
                  2, 6.0f,
                  10, 10.0f,
                  16, 5.0f);

          ScorerSupplier ss = weight.scorerSupplier(searcher.getIndexReader().leaves().get(0));
          ss.setTopLevelScoringClause();
          assertScores(ss.bulkScorer(), scoreMode, 6.0f, List.of(expectedScores1, expectedScores2));
        }

        {
          Map<Integer, Float> expectedScores = Map.of();

          ScorerSupplier ss = weight.scorerSupplier(searcher.getIndexReader().leaves().get(0));
          ss.setTopLevelScoringClause();
          assertScores(ss.bulkScorer(), scoreMode, 11.0f, expectedScores);
        }
      }
    }
  }

  public void testSetMinCompetitiveScoreWithScoreModeNone() throws IOException {
    try (Directory dir = newDirectory()) {
      try (RandomIndexWriter w =
          new RandomIndexWriter(
              random(),
              dir,
              newIndexWriterConfig()
                  .setMergePolicy(
                      // retain doc id order
                      newLogMergePolicy()))) {
        w.w
            .getConfig()
            .getCodec()
            .compoundFormat()
            .setShouldUseCompoundFile(random().nextBoolean());
        populateStaticIndex(w);
        w.forceMerge(1);
      }

      try (IndexReader reader = DirectoryReader.open(dir)) {
        final IndexSearcher searcher = newSearcher(reader);
        final ToParentBlockJoinQuery query = buildQuery(ScoreMode.None);
        final org.apache.lucene.search.ScoreMode scoreMode =
            org.apache.lucene.search.ScoreMode.TOP_SCORES;
        final Weight weight = searcher.createWeight(searcher.rewrite(query), scoreMode, 1);

        {
          Map<Integer, Float> expectedScores =
              Map.of(
                  2, 0.0f,
                  5, 0.0f,
                  10, 0.0f,
                  12, 0.0f,
                  16, 0.0f);

          ScorerSupplier ss = weight.scorerSupplier(searcher.getIndexReader().leaves().get(0));
          ss.setTopLevelScoringClause();
          assertScores(ss.bulkScorer(), scoreMode, null, expectedScores);
        }

        {
          Map<Integer, Float> expectedScores =
              Map.of(
                  2, 0.0f,
                  5, 0.0f,
                  10, 0.0f,
                  12, 0.0f,
                  16, 0.0f);

          ScorerSupplier ss = weight.scorerSupplier(searcher.getIndexReader().leaves().get(0));
          ss.setTopLevelScoringClause();
          assertScores(ss.bulkScorer(), scoreMode, 0.0f, expectedScores);
        }

        {
          Map<Integer, Float> expectedScores = Map.of();

          ScorerSupplier ss = weight.scorerSupplier(searcher.getIndexReader().leaves().get(0));
          ss.setTopLevelScoringClause();
          assertScores(ss.bulkScorer(), scoreMode, Math.nextUp(0f), expectedScores);
        }
      }
    }
  }
}
