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

package org.apache.lucene.monitor;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Supplier;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.ByteRunnable;

/** Presearcher tests with NotSlots enabled, in addition to all of {@link PresearcherTestBase} */
public abstract class NotSlotPresearcherTestBase extends PresearcherTestBase {

  protected abstract Presearcher createPresearcher(
      int maxNotSlots, List<CustomQueryHandler> handlers);

  @Override
  protected Presearcher createPresearcher() {
    return createPresearcher(1, List.of());
  }

  static Document doc(String... fieldValues) {
    Document doc = new Document();
    for (int i = 0; i < fieldValues.length; i += 2) {
      doc.add(new TextField(fieldValues[i], fieldValues[i + 1], Field.Store.NO));
    }
    return doc;
  }

  /** Registers {@code query} and returns {@code {queriesRun, matchCount}} for {@code doc} */
  int[] run(Presearcher presearcher, Query query, Document doc) throws IOException {
    try (Monitor monitor = new Monitor(WHITESPACE, presearcher)) {
      monitor.register(new MonitorQuery("q", query));
      MatchingQueries<QueryMatch> matches = monitor.match(doc, QueryMatch.SIMPLE_MATCHER);
      return new int[] {matches.getQueriesRun(), matches.getMatchCount()};
    }
  }

  int[] run(Presearcher presearcher, String query, Document doc) throws IOException {
    return run(presearcher, parse(query), doc);
  }

  int[] run(String query, Document doc) throws IOException {
    return run(createPresearcher(), query, doc);
  }

  static void assertRunAndMatched(int queriesRun, int matches, int[] actual) {
    assertEquals("queries run", queriesRun, actual[0]);
    assertEquals("matches", matches, actual[1]);
  }

  public void testMatchAllMinusTermExcludesDocumentsWithTheTerm() throws IOException {
    assertRunAndMatched(0, 0, run("*:* -f:t", doc("f", "t")));
    assertRunAndMatched(1, 1, run("*:* -f:t", doc("f", "x")));
    assertRunAndMatched(1, 1, run("*:* -f:t", doc("g", "y")));
  }

  /** Wraps {@code query} so that the Monitor's QueryDecomposer leaves it whole */
  static Query undecomposed(String query) {
    return new ConstantScoreQuery(parse(query));
  }

  public void testPureNegativeQuery() throws IOException {
    // The QueryDecomposer drops top-level pure negative queries, as Lucene matches nothing for them
    assertRunAndMatched(0, 0, run("-f:t", doc("f", "x")));
    // nested, it is a NotSlot
    assertRunAndMatched(0, 0, run(createPresearcher(), undecomposed("-f:t"), doc("f", "t")));
    assertRunAndMatched(1, 0, run(createPresearcher(), undecomposed("-f:t"), doc("f", "x")));
  }

  public void testNegatedDisjunction() throws IOException {
    assertRunAndMatched(0, 0, run("*:* -(y:b z:c)", doc("y", "b")));
    assertRunAndMatched(0, 0, run("*:* -(y:b z:c)", doc("z", "c")));
    assertRunAndMatched(1, 1, run("*:* -(y:b z:c)", doc("y", "q")));
  }

  public void testNegatedConjunctionNeedsTwoNotSlots() throws IOException {
    String query = "*:* -(+y:b +z:c)";
    Document both = doc("y", "b", "z", "c");
    // one NotSlot: the pass overflows and accepts every document
    assertRunAndMatched(1, 0, run(createPresearcher(1, List.of()), query, both));
    assertRunAndMatched(0, 0, run(createPresearcher(2, List.of()), query, both));
    assertRunAndMatched(1, 1, run(createPresearcher(2, List.of()), query, doc("y", "b")));
  }

  public void testDisjunctionOfNegations() throws IOException {
    // decomposed, each disjunct is its own query with one NotSlot
    assertRunAndMatched(0, 0, run("(*:* -a:x) (*:* -b:y)", doc("a", "x", "b", "y")));

    Query query = undecomposed("(*:* -a:x) (*:* -b:y)");
    Document both = doc("a", "x", "b", "y");
    assertRunAndMatched(1, 0, run(createPresearcher(1, List.of()), query, both));
    assertRunAndMatched(0, 0, run(createPresearcher(2, List.of()), query, both));
    assertRunAndMatched(1, 1, run(createPresearcher(2, List.of()), query, doc("a", "x")));
  }

  public void testInexactNegationsNeverExclude() throws IOException {
    assertRunAndMatched(1, 1, run("*:* -f:\"w1 w2\"", doc("f", "w2 w1")));
    assertRunAndMatched(1, 0, run("*:* -f:\"w1 w2\"", doc("f", "w1 w2")));
    assertRunAndMatched(1, 0, run("*:* -f:b*", doc("f", "bb")));
    assertRunAndMatched(1, 0, run("*:* -f:[a TO c]", doc("f", "b")));
  }

  public void testBatchesOnlyExcludeOnTermsEveryDocumentContains() throws IOException {
    try (Monitor monitor = newMonitor()) {
      monitor.register(new MonitorQuery("q", parse("*:* -f:t")));

      MultiMatchingQueries<QueryMatch> matches =
          monitor.match(new Document[] {doc("f", "t"), doc("f", "x")}, QueryMatch.SIMPLE_MATCHER);
      assertEquals(1, matches.getQueriesRun());
      assertEquals(0, matches.getMatchCount(0));
      assertEquals(1, matches.getMatchCount(1));

      matches =
          monitor.match(
              new Document[] {doc("f", "t", "x", "1"), doc("f", "t")}, QueryMatch.SIMPLE_MATCHER);
      assertEquals(0, matches.getQueriesRun());
    }
  }

  public void testCustomQueryHandlerNegation() throws IOException {
    Query query =
        new BooleanQuery.Builder()
            .add(MatchAllDocsQuery.INSTANCE, BooleanClause.Occur.SHOULD)
            .add(
                new TermInSetQuery("f", List.of(new BytesRef("x"), new BytesRef("y"))),
                BooleanClause.Occur.MUST_NOT)
            .build();
    Presearcher negatable = createPresearcher(1, List.of(new TermInSetQueryHandler(true)));
    assertRunAndMatched(0, 0, run(negatable, query, doc("f", "y")));
    assertRunAndMatched(1, 1, run(negatable, query, doc("f", "z")));

    Presearcher notNegatable = createPresearcher(1, List.of(new TermInSetQueryHandler(false)));
    assertRunAndMatched(1, 0, run(notNegatable, query, doc("f", "y")));
  }

  public void testFieldsNamedLikeNotSlotFieldsAreSafe() throws IOException {
    // A query with a positive term on a user field named like NotSlot 0 of field x (single pass,
    // and pass 0 of a multipass presearcher) must not be excluded by the document's x:t. The
    // helper query puts x:t in the term filter, so that the document's x:t can exclude queries.
    // If the NotSlot field naming changes, the guard in TermFilteredPresearcher.addPass must still
    // cover it; testNotSlotFieldNamesAreGuarded checks that.
    for (String field :
        new String[] {
          TermFilteredPresearcher.notSlotField("x", 0),
          TermFilteredPresearcher.notSlotField("x_0", 0)
        }) {
      try (Monitor monitor = newMonitor()) {
        monitor.register(
            new MonitorQuery("target", undecomposed(field + ":t (*:* -y:z)")),
            new MonitorQuery("helper", parse("*:* -x:t")));
        MatchingQueries<QueryMatch> matches =
            monitor.match(doc("x", "t"), QueryMatch.SIMPLE_MATCHER);
        assertNotNull(field, matches.matches("target"));
        assertEquals(field, 1, matches.getQueriesRun());
      }
    }
  }

  /** The terms of the document-side MUST_NOT clauses, by field */
  private static Map<String, Set<String>> negatedDocumentTerms(Query query) {
    Map<String, Set<String>> terms = new TreeMap<>();
    collectNegated(query, false, terms);
    return terms;
  }

  private static void collectNegated(Query query, boolean negated, Map<String, Set<String>> out) {
    if (query instanceof BooleanQuery bq) {
      for (BooleanClause clause : bq.clauses()) {
        collectNegated(clause.query(), clause.occur() == BooleanClause.Occur.MUST_NOT, out);
      }
    } else if (negated && query instanceof TermInSetQuery tisq) {
      tisq.visit(
          new QueryVisitor() {
            @Override
            public void consumeTerms(Query q, Term... ts) {
              for (Term t : ts) {
                out.computeIfAbsent(t.field(), _ -> new TreeSet<>()).add(t.text());
              }
            }

            @Override
            public void consumeTermsMatching(
                Query q, String field, Supplier<ByteRunnable> automaton) {
              out.computeIfAbsent(field, _ -> new TreeSet<>()).add(tisq.getTermsCount() + " terms");
            }
          });
    }
  }

  public void testTermFilterAppliesToNegatedDocumentTerms() throws IOException {
    Presearcher presearcher = createPresearcher();
    Directory dir = new ByteBuffersDirectory();
    try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(WHITESPACE))) {
      writer.addDocument(presearcher.indexQuery(parse("*:* -f:t"), Map.of()));
      writer.addDocument(presearcher.indexQuery(parse("+g:x -f:u -h:v"), Map.of()));
    }
    StringBuilder text = new StringBuilder("t");
    for (int i = 0; i < 1000; i++) {
      text.append(" w").append(i);
    }
    Document doc = doc("f", text.toString(), "g", "x y", "h", "w1");
    try (DirectoryReader reader = DirectoryReader.open(dir);
        DocumentBatch batch = DocumentBatch.of(WHITESPACE, doc)) {
      Query documentQuery =
          presearcher.buildQuery(batch.get(), new QueryIndex.QueryTermFilter(reader));
      // Of the document's 1,000+ terms, only f:t is negated by a query: u and v are not in the
      // document, and no query negates the others
      Map<String, Set<String>> negated = negatedDocumentTerms(documentQuery);
      assertFalse(negated.toString(), negated.isEmpty());
      for (Map.Entry<String, Set<String>> e : negated.entrySet()) {
        assertTrue(e.toString(), e.getKey().startsWith("f_") || e.getKey().startsWith("f__not_"));
        assertEquals(e.toString(), Set.of("t"), e.getValue());
      }
    }
    dir.close();
  }

  public void testOnlyRealDocumentTermsExclude() throws IOException {
    // The regexp handler turns the document's apple into tokens such as apXX; only real terms of
    // the document may exclude queries, so the query negating the literal term apXX still matches
    Presearcher presearcher = createPresearcher(1, List.of(new RegexpQueryHandler()));
    assertRunAndMatched(1, 1, run(presearcher, "*:* -f:apXX", doc("f", "apple")));
    // real terms passed through the handler still exclude
    assertRunAndMatched(0, 0, run(presearcher, "*:* -f:apple", doc("f", "apple")));

    // A handler rewriting tokens in place: the document contains Apple, not apple
    CustomQueryHandler lowercasing =
        new CustomQueryHandler() {
          @Override
          public QueryTree handleQuery(Query query, TermWeightor termWeightor) {
            return null;
          }

          @Override
          public TokenStream wrapTermStream(String field, TokenStream in) {
            return new LowerCaseFilter(in);
          }
        };
    Query query =
        new BooleanQuery.Builder()
            .add(MatchAllDocsQuery.INSTANCE, BooleanClause.Occur.SHOULD)
            .add(new TermQuery(new Term("f", "apple")), BooleanClause.Occur.MUST_NOT)
            .build();
    assertRunAndMatched(
        1, 1, run(createPresearcher(1, List.of(lowercasing)), query, doc("f", "Apple")));
  }
}
