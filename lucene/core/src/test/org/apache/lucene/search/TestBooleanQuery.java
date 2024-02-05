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

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomBoolean;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.search.DummyTotalHitCountCollector;
import org.apache.lucene.tests.search.FixedBitSetCollector;
import org.apache.lucene.tests.search.QueryUtils;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.automaton.Operations;

public class TestBooleanQuery extends LuceneTestCase {

  public void testEquality() throws Exception {
    BooleanQuery.Builder bq1 = new BooleanQuery.Builder();
    bq1.add(new TermQuery(new Term("field", "value1")), BooleanClause.Occur.SHOULD);
    bq1.add(new TermQuery(new Term("field", "value2")), BooleanClause.Occur.SHOULD);
    BooleanQuery.Builder nested1 = new BooleanQuery.Builder();
    nested1.add(new TermQuery(new Term("field", "nestedvalue1")), BooleanClause.Occur.SHOULD);
    nested1.add(new TermQuery(new Term("field", "nestedvalue2")), BooleanClause.Occur.SHOULD);
    bq1.add(nested1.build(), BooleanClause.Occur.SHOULD);

    BooleanQuery.Builder bq2 = new BooleanQuery.Builder();
    bq2.add(new TermQuery(new Term("field", "value1")), BooleanClause.Occur.SHOULD);
    bq2.add(new TermQuery(new Term("field", "value2")), BooleanClause.Occur.SHOULD);
    BooleanQuery.Builder nested2 = new BooleanQuery.Builder();
    nested2.add(new TermQuery(new Term("field", "nestedvalue1")), BooleanClause.Occur.SHOULD);
    nested2.add(new TermQuery(new Term("field", "nestedvalue2")), BooleanClause.Occur.SHOULD);
    bq2.add(nested2.build(), BooleanClause.Occur.SHOULD);

    assertEquals(bq1.build(), bq2.build());
  }

  public void testEqualityDoesNotDependOnOrder() {
    TermQuery[] queries =
        new TermQuery[] {
          new TermQuery(new Term("foo", "bar")), new TermQuery(new Term("foo", "baz"))
        };
    for (int iter = 0; iter < 10; ++iter) {
      List<BooleanClause> clauses = new ArrayList<>();
      final int numClauses = random().nextInt(20);
      for (int i = 0; i < numClauses; ++i) {
        Query query = RandomPicks.randomFrom(random(), queries);
        if (random().nextBoolean()) {
          query = new BoostQuery(query, random().nextFloat());
        }
        Occur occur = RandomPicks.randomFrom(random(), Occur.values());
        clauses.add(new BooleanClause(query, occur));
      }

      final int minShouldMatch = random().nextInt(5);
      BooleanQuery.Builder bq1Builder = new BooleanQuery.Builder();
      bq1Builder.setMinimumNumberShouldMatch(minShouldMatch);
      for (BooleanClause clause : clauses) {
        bq1Builder.add(clause);
      }
      final BooleanQuery bq1 = bq1Builder.build();

      Collections.shuffle(clauses, random());
      BooleanQuery.Builder bq2Builder = new BooleanQuery.Builder();
      bq2Builder.setMinimumNumberShouldMatch(minShouldMatch);
      for (BooleanClause clause : clauses) {
        bq2Builder.add(clause);
      }
      final BooleanQuery bq2 = bq2Builder.build();

      QueryUtils.checkEqual(bq1, bq2);
    }
  }

  public void testEqualityOnDuplicateShouldClauses() {
    BooleanQuery bq1 =
        new BooleanQuery.Builder()
            .setMinimumNumberShouldMatch(random().nextInt(2))
            .add(new TermQuery(new Term("foo", "bar")), Occur.SHOULD)
            .build();
    BooleanQuery bq2 =
        new BooleanQuery.Builder()
            .setMinimumNumberShouldMatch(bq1.getMinimumNumberShouldMatch())
            .add(new TermQuery(new Term("foo", "bar")), Occur.SHOULD)
            .add(new TermQuery(new Term("foo", "bar")), Occur.SHOULD)
            .build();
    QueryUtils.checkUnequal(bq1, bq2);
  }

  public void testEqualityOnDuplicateMustClauses() {
    BooleanQuery bq1 =
        new BooleanQuery.Builder()
            .setMinimumNumberShouldMatch(random().nextInt(2))
            .add(new TermQuery(new Term("foo", "bar")), Occur.MUST)
            .build();
    BooleanQuery bq2 =
        new BooleanQuery.Builder()
            .setMinimumNumberShouldMatch(bq1.getMinimumNumberShouldMatch())
            .add(new TermQuery(new Term("foo", "bar")), Occur.MUST)
            .add(new TermQuery(new Term("foo", "bar")), Occur.MUST)
            .build();
    QueryUtils.checkUnequal(bq1, bq2);
  }

  public void testEqualityOnDuplicateFilterClauses() {
    BooleanQuery bq1 =
        new BooleanQuery.Builder()
            .setMinimumNumberShouldMatch(random().nextInt(2))
            .add(new TermQuery(new Term("foo", "bar")), Occur.FILTER)
            .build();
    BooleanQuery bq2 =
        new BooleanQuery.Builder()
            .setMinimumNumberShouldMatch(bq1.getMinimumNumberShouldMatch())
            .add(new TermQuery(new Term("foo", "bar")), Occur.FILTER)
            .add(new TermQuery(new Term("foo", "bar")), Occur.FILTER)
            .build();
    QueryUtils.checkEqual(bq1, bq2);
  }

  public void testEqualityOnDuplicateMustNotClauses() {
    BooleanQuery bq1 =
        new BooleanQuery.Builder()
            .setMinimumNumberShouldMatch(random().nextInt(2))
            .add(new MatchAllDocsQuery(), Occur.MUST)
            .add(new TermQuery(new Term("foo", "bar")), Occur.FILTER)
            .build();
    BooleanQuery bq2 =
        new BooleanQuery.Builder()
            .setMinimumNumberShouldMatch(bq1.getMinimumNumberShouldMatch())
            .add(new MatchAllDocsQuery(), Occur.MUST)
            .add(new TermQuery(new Term("foo", "bar")), Occur.FILTER)
            .add(new TermQuery(new Term("foo", "bar")), Occur.FILTER)
            .build();
    QueryUtils.checkEqual(bq1, bq2);
  }

  public void testHashCodeIsStable() {
    BooleanQuery bq =
        new BooleanQuery.Builder()
            .add(
                new TermQuery(new Term("foo", TestUtil.randomSimpleString(random()))), Occur.SHOULD)
            .add(
                new TermQuery(new Term("foo", TestUtil.randomSimpleString(random()))), Occur.SHOULD)
            .build();
    final int hashCode = bq.hashCode();
    assertEquals(hashCode, bq.hashCode());
  }

  public void testTooManyClauses() {
    // Bad code (such as in a Query.rewrite() impl) should be prevented from creating a BooleanQuery
    // that directly exceeds the maxClauseCount (prior to needing IndexSearcher.rewrite() to do a
    // full walk of the final result)
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    for (int i = 0; i < IndexSearcher.getMaxClauseCount(); i++) {
      bq.add(new TermQuery(new Term("foo", "bar-" + i)), Occur.SHOULD);
    }
    expectThrows(
        IndexSearcher.TooManyClauses.class,
        () -> {
          bq.add(new TermQuery(new Term("foo", "bar-MAX")), Occur.SHOULD);
        });
  }

  // LUCENE-1630
  public void testNullOrSubScorer() throws Throwable {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newTextField("field", "a b c d", Field.Store.NO));
    w.addDocument(doc);

    IndexReader r = w.getReader();
    IndexSearcher s = newSearcher(r);
    // this test relies upon coord being the default implementation,
    // otherwise scores are different!
    s.setSimilarity(new ClassicSimilarity());

    BooleanQuery.Builder q = new BooleanQuery.Builder();
    q.add(new TermQuery(new Term("field", "a")), BooleanClause.Occur.SHOULD);

    // PhraseQuery w/ no terms added returns a null scorer
    PhraseQuery pq = new PhraseQuery("field", new String[0]);
    q.add(pq, BooleanClause.Occur.SHOULD);
    assertEquals(1, s.search(q.build(), 10).totalHits.value);

    // A required clause which returns null scorer should return null scorer to
    // IndexSearcher.
    q = new BooleanQuery.Builder();
    pq = new PhraseQuery("field", new String[0]);
    q.add(new TermQuery(new Term("field", "a")), BooleanClause.Occur.SHOULD);
    q.add(pq, BooleanClause.Occur.MUST);
    assertEquals(0, s.search(q.build(), 10).totalHits.value);

    DisjunctionMaxQuery dmq =
        new DisjunctionMaxQuery(Arrays.asList(new TermQuery(new Term("field", "a")), pq), 1.0f);
    assertEquals(1, s.search(dmq, 10).totalHits.value);

    r.close();
    w.close();
    dir.close();
  }

  public void testDeMorgan() throws Exception {
    Directory dir1 = newDirectory();
    RandomIndexWriter iw1 = new RandomIndexWriter(random(), dir1);
    Document doc1 = new Document();
    doc1.add(newTextField("field", "foo bar", Field.Store.NO));
    iw1.addDocument(doc1);
    IndexReader reader1 = iw1.getReader();
    iw1.close();

    Directory dir2 = newDirectory();
    RandomIndexWriter iw2 = new RandomIndexWriter(random(), dir2);
    Document doc2 = new Document();
    doc2.add(newTextField("field", "foo baz", Field.Store.NO));
    iw2.addDocument(doc2);
    IndexReader reader2 = iw2.getReader();
    iw2.close();

    BooleanQuery.Builder query = new BooleanQuery.Builder(); // Query: +foo -ba*
    query.add(new TermQuery(new Term("field", "foo")), BooleanClause.Occur.MUST);
    WildcardQuery wildcardQuery =
        new WildcardQuery(
            new Term("field", "ba*"),
            Operations.DEFAULT_DETERMINIZE_WORK_LIMIT,
            MultiTermQuery.SCORING_BOOLEAN_REWRITE);
    query.add(wildcardQuery, BooleanClause.Occur.MUST_NOT);

    MultiReader multireader = new MultiReader(reader1, reader2);
    IndexSearcher searcher = newSearcher(multireader);
    assertEquals(0, searcher.search(query.build(), 10).totalHits.value);

    final ExecutorService es =
        Executors.newCachedThreadPool(new NamedThreadFactory("NRT search threads"));
    searcher = new IndexSearcher(multireader, es);
    if (VERBOSE) System.out.println("rewritten form: " + searcher.rewrite(query.build()));
    assertEquals(0, searcher.search(query.build(), 10).totalHits.value);
    es.shutdown();
    es.awaitTermination(1, TimeUnit.SECONDS);

    multireader.close();
    reader1.close();
    reader2.close();
    dir1.close();
    dir2.close();
  }

  public void testBS2DisjunctionNextVsAdvance() throws Exception {
    final Directory d = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), d);
    final int numDocs = atLeast(300);
    for (int docUpto = 0; docUpto < numDocs; docUpto++) {
      String contents = "a";
      if (random().nextInt(20) <= 16) {
        contents += " b";
      }
      if (random().nextInt(20) <= 8) {
        contents += " c";
      }
      if (random().nextInt(20) <= 4) {
        contents += " d";
      }
      if (random().nextInt(20) <= 2) {
        contents += " e";
      }
      if (random().nextInt(20) <= 1) {
        contents += " f";
      }
      Document doc = new Document();
      doc.add(new TextField("field", contents, Field.Store.NO));
      w.addDocument(doc);
    }
    w.forceMerge(1);
    final IndexReader r = w.getReader();
    final IndexSearcher s = newSearcher(r);
    w.close();

    for (int iter = 0; iter < 10 * RANDOM_MULTIPLIER; iter++) {
      if (VERBOSE) {
        System.out.println("iter=" + iter);
      }
      final List<String> terms = new ArrayList<>(Arrays.asList("a", "b", "c", "d", "e", "f"));
      final int numTerms = TestUtil.nextInt(random(), 1, terms.size());
      while (terms.size() > numTerms) {
        terms.remove(random().nextInt(terms.size()));
      }

      if (VERBOSE) {
        System.out.println("  terms=" + terms);
      }

      final BooleanQuery.Builder q = new BooleanQuery.Builder();
      for (String term : terms) {
        q.add(
            new BooleanClause(new TermQuery(new Term("field", term)), BooleanClause.Occur.SHOULD));
      }

      Weight weight = s.createWeight(s.rewrite(q.build()), ScoreMode.COMPLETE, 1);

      Scorer scorer = weight.scorer(s.leafContexts.get(0));

      // First pass: just use .nextDoc() to gather all hits
      final List<ScoreDoc> hits = new ArrayList<>();
      while (scorer.iterator().nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
        hits.add(new ScoreDoc(scorer.docID(), scorer.score()));
      }

      if (VERBOSE) {
        System.out.println("  " + hits.size() + " hits");
      }

      // Now, randomly next/advance through the list and
      // verify exact match:
      for (int iter2 = 0; iter2 < 10; iter2++) {

        weight = s.createWeight(s.rewrite(q.build()), ScoreMode.COMPLETE, 1);
        scorer = weight.scorer(s.leafContexts.get(0));

        if (VERBOSE) {
          System.out.println("  iter2=" + iter2);
        }

        int upto = -1;
        while (upto < hits.size()) {
          final int nextUpto;
          final int nextDoc;
          final int left = hits.size() - upto;
          if (left == 1 || random().nextBoolean()) {
            // next
            nextUpto = 1 + upto;
            nextDoc = scorer.iterator().nextDoc();
          } else {
            // advance
            int inc = TestUtil.nextInt(random(), 1, left - 1);
            nextUpto = inc + upto;
            nextDoc = scorer.iterator().advance(hits.get(nextUpto).doc);
          }

          if (nextUpto == hits.size()) {
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, nextDoc);
          } else {
            final ScoreDoc hit = hits.get(nextUpto);
            assertEquals(hit.doc, nextDoc);
            // Test for precise float equality:
            assertTrue(
                "doc "
                    + hit.doc
                    + " has wrong score: expected="
                    + hit.score
                    + " actual="
                    + scorer.score(),
                hit.score == scorer.score());
          }
          upto = nextUpto;
        }
      }
    }

    r.close();
    d.close();
  }

  public void testMinShouldMatchLeniency() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    Document doc = new Document();
    doc.add(newTextField("field", "a b c d", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = DirectoryReader.open(w);
    IndexSearcher s = newSearcher(r);
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(new TermQuery(new Term("field", "a")), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(new Term("field", "b")), BooleanClause.Occur.SHOULD);

    // No doc can match: BQ has only 2 clauses and we are asking for minShouldMatch=4
    bq.setMinimumNumberShouldMatch(4);
    assertEquals(0, s.search(bq.build(), 1).totalHits.value);
    r.close();
    w.close();
    dir.close();
  }

  private static FixedBitSet getMatches(IndexSearcher searcher, Query query) throws IOException {
    return searcher.search(query, FixedBitSetCollector.createManager(searcher.reader.maxDoc()));
  }

  public void testFILTERClauseBehavesLikeMUST() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    Field f = newTextField("field", "a b c d", Field.Store.NO);
    doc.add(f);
    w.addDocument(doc);
    f.setStringValue("b d");
    w.addDocument(doc);
    f.setStringValue("d");
    w.addDocument(doc);
    w.commit();

    DirectoryReader reader = w.getReader();
    final IndexSearcher searcher = new IndexSearcher(reader);

    for (List<String> requiredTerms :
        Arrays.<List<String>>asList(
            Arrays.asList("a", "d"),
            Arrays.asList("a", "b", "d"),
            Arrays.asList("d"),
            Arrays.asList("e"),
            Arrays.asList())) {
      final BooleanQuery.Builder bq1 = new BooleanQuery.Builder();
      final BooleanQuery.Builder bq2 = new BooleanQuery.Builder();
      for (String term : requiredTerms) {
        final Query q = new TermQuery(new Term("field", term));
        bq1.add(q, Occur.MUST);
        bq2.add(q, Occur.FILTER);
      }

      final FixedBitSet matches1 = getMatches(searcher, bq1.build());
      final FixedBitSet matches2 = getMatches(searcher, bq2.build());
      assertEquals(matches1, matches2);
    }

    reader.close();
    w.close();
    dir.close();
  }

  private void assertSameScoresWithoutFilters(IndexSearcher searcher, BooleanQuery bq)
      throws IOException {
    final BooleanQuery.Builder bq2Builder = new BooleanQuery.Builder();
    for (BooleanClause c : bq) {
      if (c.getOccur() != Occur.FILTER) {
        bq2Builder.add(c);
      }
    }
    bq2Builder.setMinimumNumberShouldMatch(bq.getMinimumNumberShouldMatch());
    BooleanQuery bq2 = bq2Builder.build();

    final AtomicBoolean matched = new AtomicBoolean();
    searcher.search(
        bq,
        new CollectorManager<SimpleCollector, Void>() {
          @Override
          public SimpleCollector newCollector() {
            return new SimpleCollector() {
              int docBase;
              Scorable scorer;

              @Override
              protected void doSetNextReader(LeafReaderContext context) throws IOException {
                super.doSetNextReader(context);
                docBase = context.docBase;
              }

              @Override
              public ScoreMode scoreMode() {
                return ScoreMode.COMPLETE;
              }

              @Override
              public void setScorer(Scorable scorer) {
                this.scorer = scorer;
              }

              @Override
              public void collect(int doc) throws IOException {
                final float actualScore = scorer.score();
                final float expectedScore =
                    searcher.explain(bq2, docBase + doc).getValue().floatValue();
                assertEquals(expectedScore, actualScore, 10e-5);
                matched.set(true);
              }
            };
          }

          @Override
          public Void reduce(Collection<SimpleCollector> collectors) {
            return null;
          }
        });

    assertTrue(matched.get());
  }

  public void testFilterClauseDoesNotImpactScore() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    Field f = newTextField("field", "a b c d", Field.Store.NO);
    doc.add(f);
    w.addDocument(doc);
    f.setStringValue("b d");
    w.addDocument(doc);
    f.setStringValue("a d");
    w.addDocument(doc);
    w.commit();

    DirectoryReader reader = w.getReader();
    final IndexSearcher searcher = newSearcher(reader);

    BooleanQuery.Builder qBuilder = new BooleanQuery.Builder();
    qBuilder.add(new TermQuery(new Term("field", "a")), Occur.FILTER);

    // With a single clause, we will rewrite to the underlying
    // query. Make sure that it returns null scores
    assertSameScoresWithoutFilters(searcher, qBuilder.build());

    // Now with two clauses, we will get a conjunction scorer
    // Make sure it returns null scores
    qBuilder.add(new TermQuery(new Term("field", "b")), Occur.FILTER);
    BooleanQuery q = qBuilder.build();
    assertSameScoresWithoutFilters(searcher, q);

    // Now with a scoring clause, we need to make sure that
    // the boolean scores are the same as those from the term
    // query
    qBuilder.add(new TermQuery(new Term("field", "c")), Occur.SHOULD);
    q = qBuilder.build();
    assertSameScoresWithoutFilters(searcher, q);

    // FILTER and empty SHOULD
    qBuilder = new BooleanQuery.Builder();
    qBuilder.add(new TermQuery(new Term("field", "a")), Occur.FILTER);
    qBuilder.add(new TermQuery(new Term("field", "e")), Occur.SHOULD);
    q = qBuilder.build();
    assertSameScoresWithoutFilters(searcher, q);

    // mix of FILTER and MUST
    qBuilder = new BooleanQuery.Builder();
    qBuilder.add(new TermQuery(new Term("field", "a")), Occur.FILTER);
    qBuilder.add(new TermQuery(new Term("field", "d")), Occur.MUST);
    q = qBuilder.build();
    assertSameScoresWithoutFilters(searcher, q);

    // FILTER + minShouldMatch
    qBuilder = new BooleanQuery.Builder();
    qBuilder.add(new TermQuery(new Term("field", "b")), Occur.FILTER);
    qBuilder.add(new TermQuery(new Term("field", "a")), Occur.SHOULD);
    qBuilder.add(new TermQuery(new Term("field", "d")), Occur.SHOULD);
    qBuilder.setMinimumNumberShouldMatch(1);
    q = qBuilder.build();
    assertSameScoresWithoutFilters(searcher, q);

    reader.close();
    w.close();
    dir.close();
  }

  public void testConjunctionPropagatesApproximations() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    Field f = newTextField("field", "a b c", Field.Store.NO);
    doc.add(f);
    w.addDocument(doc);
    w.commit();

    DirectoryReader reader = w.getReader();
    // not LuceneTestCase.newSearcher to not have the asserting wrappers
    // and do instanceof checks
    final IndexSearcher searcher = new IndexSearcher(reader);
    searcher.setQueryCache(null); // to still have approximations

    PhraseQuery pq = new PhraseQuery("field", "a", "b");

    BooleanQuery.Builder q = new BooleanQuery.Builder();
    q.add(pq, Occur.MUST);
    q.add(new TermQuery(new Term("field", "c")), Occur.FILTER);

    final Weight weight = searcher.createWeight(searcher.rewrite(q.build()), ScoreMode.COMPLETE, 1);
    final Scorer scorer = weight.scorer(searcher.getIndexReader().leaves().get(0));
    assertTrue(scorer instanceof ConjunctionScorer);
    assertNotNull(scorer.twoPhaseIterator());

    reader.close();
    w.close();
    dir.close();
  }

  public void testDisjunctionPropagatesApproximations() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    Field f = newTextField("field", "a b c", Field.Store.NO);
    doc.add(f);
    w.addDocument(doc);
    w.commit();

    DirectoryReader reader = w.getReader();
    final IndexSearcher searcher = new IndexSearcher(reader);
    searcher.setQueryCache(null); // to still have approximations

    PhraseQuery pq = new PhraseQuery("field", "a", "b");

    BooleanQuery.Builder q = new BooleanQuery.Builder();
    q.add(pq, Occur.SHOULD);
    q.add(new TermQuery(new Term("field", "c")), Occur.SHOULD);

    final Weight weight = searcher.createWeight(searcher.rewrite(q.build()), ScoreMode.COMPLETE, 1);
    final Scorer scorer = weight.scorer(reader.leaves().get(0));
    assertTrue(scorer instanceof DisjunctionScorer);
    assertNotNull(scorer.twoPhaseIterator());

    reader.close();
    w.close();
    dir.close();
  }

  public void testBoostedScorerPropagatesApproximations() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    Field f = newTextField("field", "a b c", Field.Store.NO);
    doc.add(f);
    w.addDocument(doc);
    w.commit();

    DirectoryReader reader = w.getReader();
    // not LuceneTestCase.newSearcher to not have the asserting wrappers
    // and do instanceof checks
    final IndexSearcher searcher = new IndexSearcher(reader);
    searcher.setQueryCache(null); // to still have approximations

    PhraseQuery pq = new PhraseQuery("field", "a", "b");

    BooleanQuery.Builder q = new BooleanQuery.Builder();
    q.add(pq, Occur.SHOULD);
    q.add(new TermQuery(new Term("field", "d")), Occur.SHOULD);

    final Weight weight = searcher.createWeight(searcher.rewrite(q.build()), ScoreMode.COMPLETE, 1);
    final Scorer scorer = weight.scorer(searcher.getIndexReader().leaves().get(0));
    assertTrue(scorer instanceof PhraseScorer);
    assertNotNull(scorer.twoPhaseIterator());

    reader.close();
    w.close();
    dir.close();
  }

  public void testExclusionPropagatesApproximations() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    Field f = newTextField("field", "a b c", Field.Store.NO);
    doc.add(f);
    w.addDocument(doc);
    w.commit();

    DirectoryReader reader = w.getReader();
    final IndexSearcher searcher = new IndexSearcher(reader);
    searcher.setQueryCache(null); // to still have approximations

    PhraseQuery pq = new PhraseQuery("field", "a", "b");

    BooleanQuery.Builder q = new BooleanQuery.Builder();
    q.add(pq, Occur.SHOULD);
    q.add(new TermQuery(new Term("field", "c")), Occur.MUST_NOT);

    final Weight weight = searcher.createWeight(searcher.rewrite(q.build()), ScoreMode.COMPLETE, 1);
    final Scorer scorer = weight.scorer(reader.leaves().get(0));
    assertTrue(scorer instanceof ReqExclScorer);
    assertNotNull(scorer.twoPhaseIterator());

    reader.close();
    w.close();
    dir.close();
  }

  public void testReqOptPropagatesApproximations() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    Field f = newTextField("field", "a b c", Field.Store.NO);
    doc.add(f);
    w.addDocument(doc);
    w.commit();

    DirectoryReader reader = w.getReader();
    final IndexSearcher searcher = new IndexSearcher(reader);
    searcher.setQueryCache(null); // to still have approximations

    PhraseQuery pq = new PhraseQuery("field", "a", "b");

    BooleanQuery.Builder q = new BooleanQuery.Builder();
    q.add(pq, Occur.MUST);
    q.add(new TermQuery(new Term("field", "c")), Occur.SHOULD);

    final Weight weight = searcher.createWeight(searcher.rewrite(q.build()), ScoreMode.COMPLETE, 1);
    final Scorer scorer = weight.scorer(reader.leaves().get(0));
    assertTrue(scorer instanceof ReqOptSumScorer);
    assertNotNull(scorer.twoPhaseIterator());

    reader.close();
    w.close();
    dir.close();
  }

  // LUCENE-9620 Add Weight#count(LeafReaderContext)
  public void testQueryMatchesCount() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    int randomNumDocs = TestUtil.nextInt(random(), 10, 100);
    int numMatchingDocs = 0;

    for (int i = 0; i < randomNumDocs; i++) {
      Document doc = new Document();
      Field f;
      if (random().nextBoolean()) {
        f = newTextField("field", "a b c " + random().nextInt(), Field.Store.NO);
        numMatchingDocs++;
      } else {
        f = newTextField("field", String.valueOf(random().nextInt()), Field.Store.NO);
      }
      doc.add(f);
      w.addDocument(doc);
    }
    w.commit();

    DirectoryReader reader = w.getReader();
    final IndexSearcher searcher = new IndexSearcher(reader);

    BooleanQuery.Builder q = new BooleanQuery.Builder();
    q.add(new PhraseQuery("field", "a", "b"), Occur.SHOULD);
    q.add(new TermQuery(new Term("field", "c")), Occur.SHOULD);

    Query builtQuery = q.build();

    assertEquals(searcher.count(builtQuery), numMatchingDocs);

    IOUtils.close(reader, w, dir);
  }

  public void testConjunctionMatchesCount() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
    Document doc = new Document();
    LongPoint longPoint = new LongPoint("long", 3L);
    doc.add(longPoint);
    StringField stringField = new StringField("string", "abc", Store.NO);
    doc.add(stringField);
    writer.addDocument(doc);
    longPoint.setLongValue(10);
    stringField.setStringValue("xyz");
    writer.addDocument(doc);
    IndexReader reader = DirectoryReader.open(writer);
    writer.close();
    IndexSearcher searcher = new IndexSearcher(reader);

    Query query =
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term("string", "abc")), Occur.MUST)
            .add(LongPoint.newExactQuery("long", 3L), Occur.FILTER)
            .build();
    Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1f);
    // Both queries match a single doc, BooleanWeight can't figure out the count of the conjunction
    assertEquals(-1, weight.count(reader.leaves().get(0)));

    query =
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term("string", "missing")), Occur.MUST)
            .add(LongPoint.newExactQuery("long", 3L), Occur.FILTER)
            .build();
    weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1f);
    // One query has a count of 0, the conjunction has a count of 0 too
    assertEquals(0, weight.count(reader.leaves().get(0)));

    query =
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term("string", "abc")), Occur.MUST)
            .add(LongPoint.newExactQuery("long", 5L), Occur.FILTER)
            .build();
    weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1f);
    // One query has a count of 0, the conjunction has a count of 0 too
    assertEquals(0, weight.count(reader.leaves().get(0)));

    query =
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term("string", "abc")), Occur.MUST)
            .add(LongPoint.newRangeQuery("long", 0L, 10L), Occur.FILTER)
            .build();
    // One query matches all docs, the count of the conjunction is the count of the other query
    weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1f);
    assertEquals(1, weight.count(reader.leaves().get(0)));

    query =
        new BooleanQuery.Builder()
            .add(new MatchAllDocsQuery(), Occur.MUST)
            .add(LongPoint.newRangeQuery("long", 1L, 5L), Occur.FILTER)
            .build();
    // One query matches all docs, the count of the conjunction is the count of the other query
    weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1f);
    assertEquals(1, weight.count(reader.leaves().get(0)));

    reader.close();
    dir.close();
  }

  public void testDisjunctionMatchesCount() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
    Document doc = new Document();
    LongPoint longPoint = new LongPoint("long", 3L);
    LongPoint longPoint3dim = new LongPoint("long3dim", 3L, 4L, 5L);
    doc.add(longPoint);
    doc.add(longPoint3dim);
    StringField stringField = new StringField("string", "abc", Store.NO);
    doc.add(stringField);
    writer.addDocument(doc);
    longPoint.setLongValue(10);
    longPoint3dim.setLongValues(10L, 11L, 12L);
    stringField.setStringValue("xyz");
    writer.addDocument(doc);
    IndexReader reader = DirectoryReader.open(writer);
    writer.close();
    IndexSearcher searcher = new IndexSearcher(reader);

    Query query =
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term("string", "abc")), Occur.SHOULD)
            .add(LongPoint.newExactQuery("long", 3L), Occur.SHOULD)
            .build();
    Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1f);
    // Both queries match a single doc, BooleanWeight can't figure out the count of the disjunction
    assertEquals(-1, weight.count(reader.leaves().get(0)));

    query =
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term("string", "missing")), Occur.SHOULD)
            .add(LongPoint.newExactQuery("long", 3L), Occur.SHOULD)
            .build();
    weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1f);
    // One query has a count of 0, the disjunction count is the other count
    assertEquals(1, weight.count(reader.leaves().get(0)));

    query =
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term("string", "abc")), Occur.SHOULD)
            .add(LongPoint.newExactQuery("long", 5L), Occur.SHOULD)
            .build();
    weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1f);
    // One query has a count of 0, the disjunction count is the other count
    assertEquals(1, weight.count(reader.leaves().get(0)));

    query =
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term("string", "abc")), Occur.SHOULD)
            .add(LongPoint.newRangeQuery("long", 0L, 10L), Occur.SHOULD)
            .build();
    // One query matches all docs, the count of the disjunction is the number of docs
    weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1f);
    assertEquals(2, weight.count(reader.leaves().get(0)));

    query =
        new BooleanQuery.Builder()
            .add(new MatchAllDocsQuery(), Occur.SHOULD)
            .add(LongPoint.newRangeQuery("long", 1L, 5L), Occur.SHOULD)
            .build();
    // One query matches all docs, the count of the disjunction is the number of docs
    weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1f);
    assertEquals(2, weight.count(reader.leaves().get(0)));

    long[] lower = new long[] {4L, 5L, 6L};
    long[] upper = new long[] {9L, 10L, 11L};
    Query unknownCountQuery = LongPoint.newRangeQuery("long3dim", lower, upper);
    assert reader.leaves().size() == 1;
    assert searcher
            .createWeight(unknownCountQuery, ScoreMode.COMPLETE, 1f)
            .count(reader.leaves().get(0))
        == -1;

    query =
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term("string", "xyz")), Occur.MUST)
            .add(unknownCountQuery, Occur.MUST_NOT)
            .add(new MatchAllDocsQuery(), Occur.MUST_NOT)
            .build();
    weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1f);
    // count of the first MUST_NOT clause is unknown, but the second MUST_NOT clause matches all
    // docs
    assertEquals(0, weight.count(reader.leaves().get(0)));

    query =
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term("string", "xyz")), Occur.MUST)
            .add(unknownCountQuery, Occur.MUST_NOT)
            .add(new TermQuery(new Term("string", "abc")), Occur.MUST_NOT)
            .build();
    weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1f);
    // count of the first MUST_NOT clause is unknown, though the second MUST_NOT clause matche one
    // doc, we can't figure out the number of
    // docs
    assertEquals(-1, weight.count(reader.leaves().get(0)));

    // test pure disjunction
    query =
        new BooleanQuery.Builder()
            .add(unknownCountQuery, Occur.SHOULD)
            .add(new MatchAllDocsQuery(), Occur.SHOULD)
            .build();
    weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1f);
    // count of the first SHOULD clause is unknown, but the second SHOULD clause matches all docs
    assertEquals(2, weight.count(reader.leaves().get(0)));

    query =
        new BooleanQuery.Builder()
            .add(unknownCountQuery, Occur.SHOULD)
            .add(new TermQuery(new Term("string", "abc")), Occur.SHOULD)
            .build();
    weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1f);
    // count of the first SHOULD clause is unknown, though the second SHOULD clause matche one doc,
    // we can't figure out the number of
    // docs
    assertEquals(-1, weight.count(reader.leaves().get(0)));

    reader.close();
    dir.close();
  }

  public void testTwoClauseTermDisjunctionCountOptimization() throws Exception {
    int largerTermCount = RandomNumbers.randomIntBetween(random(), 11, 100);
    int smallerTermCount = RandomNumbers.randomIntBetween(random(), 1, (largerTermCount - 1) / 10);

    List<String[]> docContent = new ArrayList<>(largerTermCount + smallerTermCount);

    for (int i = 0; i < largerTermCount; i++) {
      docContent.add(new String[] {"large"});
    }

    for (int i = 0; i < smallerTermCount; i++) {
      docContent.add(new String[] {"small", "also small"});
    }

    try (Directory dir = newDirectory()) {
      try (IndexWriter w =
          new IndexWriter(dir, newIndexWriterConfig().setMergePolicy(newLogMergePolicy()))) {

        for (String[] values : docContent) {
          Document doc = new Document();
          for (String value : values) {
            doc.add(new StringField("foo", value, Field.Store.NO));
          }
          w.addDocument(doc);
        }
        w.forceMerge(1);
      }

      try (IndexReader reader = DirectoryReader.open(dir)) {
        final int[] countInvocations = new int[] {0};
        IndexSearcher countingIndexSearcher =
            new IndexSearcher(reader) {
              @Override
              public int count(Query query) throws IOException {
                countInvocations[0]++;
                return super.count(query);
              }
            };

        {
          // Test no matches in either term
          countInvocations[0] = 0;
          BooleanQuery query =
              new BooleanQuery.Builder()
                  .add(new TermQuery(new Term("foo", "no match")), BooleanClause.Occur.SHOULD)
                  .add(new TermQuery(new Term("foo", "also no match")), BooleanClause.Occur.SHOULD)
                  .build();

          assertEquals(0, countingIndexSearcher.count(query));
          assertEquals(3, countInvocations[0]);
        }
        {
          // Test match no match in first term
          countInvocations[0] = 0;
          BooleanQuery query =
              new BooleanQuery.Builder()
                  .add(new TermQuery(new Term("foo", "no match")), BooleanClause.Occur.SHOULD)
                  .add(new TermQuery(new Term("foo", "small")), BooleanClause.Occur.SHOULD)
                  .build();

          assertEquals(smallerTermCount, countingIndexSearcher.count(query));
          assertEquals(3, countInvocations[0]);
        }
        {
          // Test match no match in second term
          countInvocations[0] = 0;
          BooleanQuery query =
              new BooleanQuery.Builder()
                  .add(new TermQuery(new Term("foo", "small")), BooleanClause.Occur.SHOULD)
                  .add(new TermQuery(new Term("foo", "no match")), BooleanClause.Occur.SHOULD)
                  .build();

          assertEquals(smallerTermCount, countingIndexSearcher.count(query));
          assertEquals(3, countInvocations[0]);
        }
        {
          // Test match in both terms that hits optimization threshold with small term first
          countInvocations[0] = 0;

          BooleanQuery query =
              new BooleanQuery.Builder()
                  .add(new TermQuery(new Term("foo", "small")), BooleanClause.Occur.SHOULD)
                  .add(new TermQuery(new Term("foo", "large")), BooleanClause.Occur.SHOULD)
                  .build();

          int count = countingIndexSearcher.count(query);

          assertEquals(largerTermCount + smallerTermCount, count);
          assertEquals(4, countInvocations[0]);

          assertTrue(query.isTwoClausePureDisjunctionWithTerms());
          Query[] queries =
              query.rewriteTwoClauseDisjunctionWithTermsForCount(countingIndexSearcher);
          assertEquals(queries.length, 3);
          assertEquals(smallerTermCount, countingIndexSearcher.count(queries[0]));
          assertEquals(largerTermCount, countingIndexSearcher.count(queries[1]));
        }
        {
          // Test match in both terms that hits optimization threshold with large term first
          countInvocations[0] = 0;

          BooleanQuery query =
              new BooleanQuery.Builder()
                  .add(new TermQuery(new Term("foo", "large")), BooleanClause.Occur.SHOULD)
                  .add(new TermQuery(new Term("foo", "small")), BooleanClause.Occur.SHOULD)
                  .build();

          int count = countingIndexSearcher.count(query);

          assertEquals(largerTermCount + smallerTermCount, count);
          assertEquals(4, countInvocations[0]);

          assertTrue(query.isTwoClausePureDisjunctionWithTerms());
          Query[] queries =
              query.rewriteTwoClauseDisjunctionWithTermsForCount(countingIndexSearcher);
          assertEquals(queries.length, 3);
          assertEquals(largerTermCount, countingIndexSearcher.count(queries[0]));
          assertEquals(smallerTermCount, countingIndexSearcher.count(queries[1]));
        }
        {
          // Test match in both terms that doesn't hit optimization threshold
          countInvocations[0] = 0;
          BooleanQuery query =
              new BooleanQuery.Builder()
                  .add(new TermQuery(new Term("foo", "small")), BooleanClause.Occur.SHOULD)
                  .add(new TermQuery(new Term("foo", "also small")), BooleanClause.Occur.SHOULD)
                  .build();

          int count = countingIndexSearcher.count(query);

          assertEquals(smallerTermCount, count);
          assertEquals(3, countInvocations[0]);
        }
      }
    }
  }

  // test BlockMaxMaxscoreScorer
  public void testDisjunctionTwoClausesMatchesCountAndScore() throws Exception {
    List<String[]> docContent =
        Arrays.asList(
            new String[] {"A", "B"}, // 0
            new String[] {"A"}, // 1
            new String[] {}, // 2
            new String[] {"A", "B", "C"}, // 3
            new String[] {"B"}, // 4
            new String[] {"B", "C"} // 5
            );

    // result sorted by score
    int[][] matchDocScore = {
      {0, 2 + 1},
      {3, 2 + 1},
      {1, 2},
      {4, 1},
      {5, 1}
    };

    try (Directory dir = newDirectory()) {
      try (IndexWriter w =
          new IndexWriter(dir, newIndexWriterConfig().setMergePolicy(newLogMergePolicy()))) {

        for (String[] values : docContent) {
          Document doc = new Document();
          for (String value : values) {
            doc.add(new StringField("foo", value, Field.Store.NO));
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
                    new BoostQuery(new ConstantScoreQuery(new TermQuery(new Term("foo", "A"))), 2),
                    BooleanClause.Occur.SHOULD)
                .add(
                    new ConstantScoreQuery(new TermQuery(new Term("foo", "B"))),
                    BooleanClause.Occur.SHOULD)
                .build();

        TopDocs topDocs = searcher.search(query, 10);

        for (int i = 0; i < topDocs.scoreDocs.length; i++) {
          ScoreDoc scoreDoc = topDocs.scoreDocs[i];
          assertEquals(matchDocScore[i][0], scoreDoc.doc);
          assertEquals(matchDocScore[i][1], scoreDoc.score, 0);
        }
      }
    }
  }

  public void testDisjunctionRandomClausesMatchesCount() throws Exception {
    int numFieldValue = RandomNumbers.randomIntBetween(random(), 1, 10);
    int[] numDocsPerFieldValue = new int[numFieldValue];
    int allDocsCount = 0;

    for (int i = 0; i < numDocsPerFieldValue.length; i++) {
      int numDocs = RandomNumbers.randomIntBetween(random(), 10, 50);
      numDocsPerFieldValue[i] = numDocs;
      allDocsCount += numDocs;
    }

    try (Directory dir = newDirectory()) {
      try (IndexWriter w =
          new IndexWriter(dir, newIndexWriterConfig().setMergePolicy(newLogMergePolicy()))) {

        for (int i = 0; i < numFieldValue; i++) {
          for (int j = 0; j < numDocsPerFieldValue[i]; j++) {
            Document doc = new Document();
            doc.add(new StringField("field", String.valueOf(i), Field.Store.NO));
            w.addDocument(doc);
          }
        }

        w.forceMerge(1);
      }

      int matchedDocsCount = 0;
      try (IndexReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = newSearcher(reader);

        BooleanQuery.Builder builder = new BooleanQuery.Builder();

        for (int i = 0; i < numFieldValue; i++) {
          if (randomBoolean()) {
            matchedDocsCount += numDocsPerFieldValue[i];
            builder.add(
                new TermQuery(new Term("field", String.valueOf(i))), BooleanClause.Occur.SHOULD);
          }
        }

        Query query = builder.build();

        TopDocs topDocs = searcher.search(query, allDocsCount);
        assertEquals(matchedDocsCount, topDocs.scoreDocs.length);
      }
    }
  }

  public void testProhibitedMatchesCount() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
    Document doc = new Document();
    LongPoint longPoint = new LongPoint("long", 3L);
    doc.add(longPoint);
    StringField stringField = new StringField("string", "abc", Store.NO);
    doc.add(stringField);
    writer.addDocument(doc);
    longPoint.setLongValue(10);
    stringField.setStringValue("xyz");
    writer.addDocument(doc);
    IndexReader reader = DirectoryReader.open(writer);
    writer.close();
    IndexSearcher searcher = new IndexSearcher(reader);

    Query query =
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term("string", "abc")), Occur.MUST)
            .add(LongPoint.newExactQuery("long", 3L), Occur.MUST_NOT)
            .build();
    Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1f);
    // Both queries match a single doc, BooleanWeight can't figure out the count of the query
    assertEquals(-1, weight.count(reader.leaves().get(0)));

    query =
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term("string", "missing")), Occur.MUST)
            .add(LongPoint.newExactQuery("long", 3L), Occur.MUST_NOT)
            .build();
    weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1f);
    // the positive clause doesn't match any docs, so the overall query doesn't either
    assertEquals(0, weight.count(reader.leaves().get(0)));

    query =
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term("string", "abc")), Occur.MUST)
            .add(LongPoint.newExactQuery("long", 5L), Occur.MUST_NOT)
            .build();
    weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1f);
    // the negative clause doesn't match any docs, so the overall count is the count of the positive
    // clause
    assertEquals(1, weight.count(reader.leaves().get(0)));

    query =
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term("string", "abc")), Occur.MUST)
            .add(LongPoint.newRangeQuery("long", 0L, 10L), Occur.MUST_NOT)
            .build();
    // the negative clause matches all docs, so the query doesn't match any docs
    weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1f);
    assertEquals(0, weight.count(reader.leaves().get(0)));

    query =
        new BooleanQuery.Builder()
            .add(LongPoint.newRangeQuery("long", 0L, 10L), Occur.MUST)
            .add(new TermQuery(new Term("string", "abc")), Occur.MUST_NOT)
            .build();
    // The positive clause matches all docs, so we can subtract the number of matches of the
    // negative clause
    weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1f);
    assertEquals(1, weight.count(reader.leaves().get(0)));

    reader.close();
    dir.close();
  }

  public void testRandomBooleanQueryMatchesCount() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
    Document doc = new Document();
    LongPoint longPoint = new LongPoint("long", 3L);
    doc.add(longPoint);
    StringField stringField = new StringField("string", "abc", Store.NO);
    doc.add(stringField);
    writer.addDocument(doc);
    longPoint.setLongValue(10);
    stringField.setStringValue("xyz");
    writer.addDocument(doc);
    IndexReader reader = DirectoryReader.open(writer);
    writer.close();
    IndexSearcher searcher = new IndexSearcher(reader);
    for (int iter = 0; iter < 1000; ++iter) {
      final int numClauses = TestUtil.nextInt(random(), 2, 5);
      BooleanQuery.Builder builder = new BooleanQuery.Builder();
      int numShouldClauses = 0;
      for (int i = 0; i < numClauses; ++i) {
        Query query;
        switch (random().nextInt(6)) {
          case 0:
            query = new TermQuery(new Term("string", "abc"));
            break;
          case 1:
            query = LongPoint.newExactQuery("long", 3L);
            break;
          case 2:
            query = new TermQuery(new Term("string", "missing"));
            break;
          case 3:
            query = LongPoint.newExactQuery("long", 5L);
            break;
          case 4:
            query = new MatchAllDocsQuery();
            break;
          default:
            query = LongPoint.newRangeQuery("long", 0L, 10L);
            break;
        }
        Occur occur = RandomPicks.randomFrom(random(), Occur.values());
        if (occur == Occur.SHOULD) {
          numShouldClauses++;
        }
        builder.add(query, occur);
      }
      builder.setMinimumNumberShouldMatch(TestUtil.nextInt(random(), 0, numShouldClauses));
      Query booleanQuery = builder.build();
      assertEquals(
          (int) searcher.search(booleanQuery, DummyTotalHitCountCollector.createManager()),
          searcher.count(booleanQuery));
    }
    reader.close();
    dir.close();
  }

  public void testToString() {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(new TermQuery(new Term("field", "a")), Occur.SHOULD);
    bq.add(new TermQuery(new Term("field", "b")), Occur.MUST);
    bq.add(new TermQuery(new Term("field", "c")), Occur.MUST_NOT);
    bq.add(new TermQuery(new Term("field", "d")), Occur.FILTER);
    assertEquals("a +b -c #d", bq.build().toString("field"));
  }

  public void testQueryVisitor() throws IOException {
    Term a = new Term("f", "a");
    Term b = new Term("f", "b");
    Term c = new Term("f", "c");
    Term d = new Term("f", "d");
    BooleanQuery.Builder bqBuilder = new BooleanQuery.Builder();
    bqBuilder.add(new TermQuery(a), Occur.SHOULD);
    bqBuilder.add(new TermQuery(b), Occur.MUST);
    bqBuilder.add(new TermQuery(c), Occur.FILTER);
    bqBuilder.add(new TermQuery(d), Occur.MUST_NOT);
    BooleanQuery bq = bqBuilder.build();

    bq.visit(
        new QueryVisitor() {

          Term expected;

          @Override
          public QueryVisitor getSubVisitor(Occur occur, Query parent) {
            switch (occur) {
              case SHOULD:
                expected = a;
                break;
              case MUST:
                expected = b;
                break;
              case FILTER:
                expected = c;
                break;
              case MUST_NOT:
                expected = d;
                break;
              default:
                throw new IllegalStateException();
            }
            return this;
          }

          @Override
          public void consumeTerms(Query query, Term... terms) {
            assertEquals(expected, terms[0]);
          }
        });
  }
}
