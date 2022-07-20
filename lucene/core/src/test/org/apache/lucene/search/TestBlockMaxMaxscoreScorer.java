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
import java.util.stream.Collectors;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;

// These basic tests are similar to some of the tests in TestWANDScorer, and may not need to be kept
public class TestBlockMaxMaxscoreScorer extends LuceneTestCase {
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
            new BlockMaxMaxscoreQuery(
                new BooleanQuery.Builder()
                    .add(
                        new BoostQuery(
                            new ConstantScoreQuery(new TermQuery(new Term("foo", "A"))), 2),
                        BooleanClause.Occur.SHOULD)
                    .add(
                        new ConstantScoreQuery(new TermQuery(new Term("foo", "B"))),
                        BooleanClause.Occur.SHOULD)
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
        assertEquals(2 + 1, scorer.score(), 0);

        assertEquals(4, scorer.iterator().nextDoc());
        assertEquals(1, scorer.score(), 0);

        assertEquals(5, scorer.iterator().nextDoc());
        assertEquals(1, scorer.score(), 0);

        assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());
      }
    }
  }

  public void testBasicsWithThreeDisjunctionClauses() throws Exception {
    try (Directory dir = newDirectory()) {
      writeDocuments(dir);

      try (IndexReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = newSearcher(reader);

        Query query =
            new BlockMaxMaxscoreQuery(
                new BooleanQuery.Builder()
                    .add(
                        new BoostQuery(
                            new ConstantScoreQuery(new TermQuery(new Term("foo", "A"))), 2),
                        BooleanClause.Occur.SHOULD)
                    .add(
                        new ConstantScoreQuery(new TermQuery(new Term("foo", "B"))),
                        BooleanClause.Occur.SHOULD)
                    .add(
                        new BoostQuery(
                            new ConstantScoreQuery(new TermQuery(new Term("foo", "C"))), 3),
                        BooleanClause.Occur.SHOULD)
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
      }
    }
  }

  public void testBasicsWithFilteredDisjunction() throws Exception {
    try (Directory dir = newDirectory()) {
      writeDocuments(dir);

      try (IndexReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = newSearcher(reader);

        Query query =
            new BooleanQuery.Builder()
                .add(
                    new BlockMaxMaxscoreQuery(
                        new BooleanQuery.Builder()
                            .add(
                                new BoostQuery(
                                    new ConstantScoreQuery(new TermQuery(new Term("foo", "A"))), 2),
                                BooleanClause.Occur.SHOULD)
                            .add(
                                new ConstantScoreQuery(new TermQuery(new Term("foo", "B"))),
                                BooleanClause.Occur.SHOULD)
                            .build()),
                    BooleanClause.Occur.MUST)
                .add(new TermQuery(new Term("foo", "C")), BooleanClause.Occur.FILTER)
                .build();

        Scorer scorer =
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
      }
    }
  }

  public void testBasicsWithExclusion() throws Exception {
    try (Directory dir = newDirectory()) {
      writeDocuments(dir);

      try (IndexReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = newSearcher(reader);

        Query query =
            new BooleanQuery.Builder()
                .add(
                    new BlockMaxMaxscoreQuery(
                        new BooleanQuery.Builder()
                            .add(
                                new BoostQuery(
                                    new ConstantScoreQuery(new TermQuery(new Term("foo", "A"))), 2),
                                BooleanClause.Occur.SHOULD)
                            .add(
                                new ConstantScoreQuery(new TermQuery(new Term("foo", "B"))),
                                BooleanClause.Occur.SHOULD)
                            .build()),
                    BooleanClause.Occur.MUST)
                .add(new TermQuery(new Term("foo", "C")), BooleanClause.Occur.MUST_NOT)
                .build();

        Scorer scorer =
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
      }
    }
  }

  private static class BlockMaxMaxscoreQuery extends Query {
    private final BooleanQuery query;

    private BlockMaxMaxscoreQuery(BooleanQuery query) {
      assert query.isPureDisjunction()
          : "This test utility query is only used to create BlockMaxMaxscoreScorer for disjunctions.";
      assert query.clauses().size() >= 2
          : "There must be at least two optional clauses to use this test utility query.";
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
                  .map(
                      ss -> {
                        try {
                          return ss.get(Long.MAX_VALUE);
                        } catch (IOException e) {
                          throw new AssertionError(e);
                        }
                      })
                  .collect(Collectors.toList());

          return new BlockMaxMaxscoreScorer(weight, optionalScorers);
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
          return false;
        }
      };
    }

    @Override
    public String toString(String field) {
      return "BlockMaxMaxscoreQuery";
    }

    @Override
    public void visit(QueryVisitor visitor) {
      // no-ops
    }

    @Override
    public boolean equals(Object other) {
      return sameClassAs(other) && query.equals(((BlockMaxMaxscoreQuery) other).query);
    }

    @Override
    public int hashCode() {
      return 31 * classHash() + query.hashCode();
    }
  }
}
