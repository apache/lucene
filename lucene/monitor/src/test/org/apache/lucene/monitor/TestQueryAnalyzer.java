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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

public class TestQueryAnalyzer extends LuceneTestCase {

  public static final QueryAnalyzer analyzer = new QueryAnalyzer();

  private Set<Term> collectTerms(QueryTree tree) {
    Set<Term> terms = new HashSet<>();
    tree.collectTerms((f, b) -> terms.add(new Term(f, b)));
    return terms;
  }

  private Set<Term> collectTerms(Query query) {
    return collectTerms(analyzer.buildTree(query, TermWeightor.DEFAULT));
  }

  public void testAdvancesCollectDifferentTerms() {

    Query q = MonitorTestBase.parse("field:(+hello +goodbye)");
    QueryTree querytree = analyzer.buildTree(q, TermWeightor.DEFAULT);

    Set<Term> expected = Collections.singleton(new Term("field", "goodbye"));
    assertEquals(expected, collectTerms(querytree));

    assertTrue(querytree.advancePhase(0));
    expected = Collections.singleton(new Term("field", "hello"));
    assertEquals(expected, collectTerms(querytree));

    assertFalse(querytree.advancePhase(0));

    assertEquals(expected, collectTerms(querytree));
  }

  public void testDisjunctionsWithAnyClausesOnlyReturnANYTOKEN() {

    // disjunction containing a pure negative - we can't narrow this down
    Query q = MonitorTestBase.parse("hello goodbye (*:* -term)");

    Set<Term> terms = collectTerms(q);
    assertEquals(1, terms.size());
    assertEquals(TermFilteredPresearcher.ANYTOKEN_FIELD, terms.iterator().next().field());
  }

  public void testConjunctionsDoNotAdvanceOverANYTOKENs() {

    Query q = MonitorTestBase.parse("+hello +howdyedo +(goodbye (*:* -whatever))");
    QueryTree tree = analyzer.buildTree(q, TermWeightor.DEFAULT);

    Set<Term> expected = Collections.singleton(new Term("field", "howdyedo"));
    assertEquals(expected, collectTerms(tree));

    assertTrue(tree.advancePhase(0));
    expected = Collections.singleton(new Term("field", "hello"));
    assertEquals(expected, collectTerms(tree));

    assertFalse(tree.advancePhase(0));
    assertEquals(expected, collectTerms(tree));
  }

  public void testConjunctionsCannotAdvanceOverMinWeightedTokens() {

    TermWeightor weightor =
        TermWeightor.combine(
            TermWeightor.termWeightor(0.1, new BytesRef("startterm")),
            TermWeightor.lengthWeightor(1, 1));

    QueryAnalyzer analyzer = new QueryAnalyzer();

    Query q = MonitorTestBase.parse("+startterm +hello +goodbye");
    QueryTree tree = analyzer.buildTree(q, weightor);

    Set<Term> expected = Collections.singleton(new Term("field", "goodbye"));
    assertEquals(expected, collectTerms(tree));

    assertTrue(tree.advancePhase(0.5));
    expected = Collections.singleton(new Term("field", "hello"));
    assertEquals(expected, collectTerms(tree));

    assertFalse(tree.advancePhase(0.5));
  }

  public void testNestedConjunctions() {

    Query q = MonitorTestBase.parse("+(+(+(+aaaa +cc) +(+d +bbb)))");
    QueryTree tree = analyzer.buildTree(q, TermWeightor.DEFAULT);

    Set<Term> expected = Collections.singleton(new Term("field", "aaaa"));
    assertEquals(expected, collectTerms(tree));
    assertTrue(tree.advancePhase(0));

    expected = Collections.singleton(new Term("field", "bbb"));
    assertEquals(expected, collectTerms(tree));
    assertTrue(tree.advancePhase(0));

    expected = Collections.singleton(new Term("field", "cc"));
    assertEquals(expected, collectTerms(tree));
    assertTrue(tree.advancePhase(0));

    expected = Collections.singleton(new Term("field", "d"));
    assertEquals(expected, collectTerms(tree));
    assertFalse(tree.advancePhase(0));
  }

  public void testNestedDisjunctions() {

    Query q = MonitorTestBase.parse("+(+((+aaaa +cc) (+dd +bbb +f)))");
    QueryTree tree = analyzer.buildTree(q, TermWeightor.DEFAULT);

    Set<Term> expected =
        new HashSet<>(Arrays.asList(new Term("field", "aaaa"), new Term("field", "bbb")));
    assertEquals(expected, collectTerms(tree));
    assertTrue(tree.advancePhase(0));

    expected = new HashSet<>(Arrays.asList(new Term("field", "cc"), new Term("field", "dd")));
    assertEquals(expected, collectTerms(tree));
    assertTrue(tree.advancePhase(0));

    expected = new HashSet<>(Arrays.asList(new Term("field", "cc"), new Term("field", "f")));
    assertEquals(expected, collectTerms(tree));
    assertFalse(tree.advancePhase(0));
  }

  public void testShouldClausesBesideFilterClausesAreIgnored() {
    Query query =
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term("field", "a")), BooleanClause.Occur.FILTER)
            .add(new TermQuery(new Term("field", "b")), BooleanClause.Occur.SHOULD)
            .build();
    assertEquals(Set.of(new Term("field", "a")), collectTerms(query));
  }

  public void testMinWeightAdvances() {
    QueryTree tree =
        QueryTree.disjunction(
            QueryTree.conjunction(
                QueryTree.term(new Term("field", "term1"), 1),
                QueryTree.term(new Term("field", "term2"), 0.1),
                QueryTree.anyTerm("*:*")),
            QueryTree.conjunction(
                QueryTree.disjunction(
                    QueryTree.term(new Term("field", "term4"), 0.2),
                    QueryTree.term(new Term("field", "term5"), 1)),
                QueryTree.term(new Term("field", "term3"), 0.5)));

    Set<Term> expected =
        new HashSet<>(Arrays.asList(new Term("field", "term1"), new Term("field", "term3")));
    assertEquals(expected, collectTerms(tree));
    assertTrue(tree.advancePhase(0.1f));

    expected =
        new HashSet<>(
            Arrays.asList(
                new Term("field", "term1"),
                new Term("field", "term4"),
                new Term("field", "term5")));
    assertEquals(expected, collectTerms(tree));
    assertFalse(tree.advancePhase(0.1f));
  }

  // NotSlots (negation optimized)

  private static final QueryAnalyzer notSlotAnalyzer = new QueryAnalyzer(List.of(), true);

  private static Term t(String text) {
    return new Term("field", text);
  }

  private static final Set<Term> ANY =
      Set.of(new Term(TermFilteredPresearcher.ANYTOKEN_FIELD, TermFilteredPresearcher.ANYTOKEN));

  /** The terms and NotSlots of the current path */
  private record Pass(Set<Term> terms, List<Set<Term>> notSlots) {}

  private static Pass collect(QueryTree tree) {
    Set<Term> terms = new HashSet<>();
    List<Set<Term>> notSlots = new ArrayList<>();
    tree.collectTerms(
        new QueryTree.TermCollector() {
          @Override
          public void accept(String field, BytesRef term) {
            terms.add(new Term(field, BytesRef.deepCopyOf(term)));
          }

          @Override
          public void notSlot(Set<Term> slotTerms) {
            notSlots.add(slotTerms);
          }
        });
    return new Pass(terms, notSlots);
  }

  private static QueryTree notSlotTree(Query query) {
    return notSlotAnalyzer.buildTree(query, TermWeightor.DEFAULT);
  }

  private static Pass notSlotPass(String query) {
    return collect(notSlotTree(MonitorTestBase.parse(query)));
  }

  public void testNegatedTermBecomesANotSlotAfterThePositives() {
    QueryTree tree = notSlotTree(MonitorTestBase.parse("+a -b"));
    assertEquals(new Pass(Set.of(t("a")), List.of()), collect(tree));
    assertTrue(tree.advancePhase(0));
    assertEquals(new Pass(Set.of(), List.of(Set.of(t("b")))), collect(tree));
    assertFalse(tree.advancePhase(0));
  }

  public void testNegatedTermsOfAConjunctionShareANotSlot() {
    assertEquals(new Pass(Set.of(), List.of(Set.of(t("b"), t("c")))), notSlotPass("*:* -b -c"));
    assertEquals(new Pass(Set.of(), List.of(Set.of(t("b"), t("c")))), notSlotPass("-b -c"));
    // De Morgan: NOT(b OR c) = NOT b AND NOT c
    assertEquals(new Pass(Set.of(), List.of(Set.of(t("b"), t("c")))), notSlotPass("*:* -(b c)"));
  }

  public void testNegatedConjunctionIsADisjunctionOfNotSlots() {
    Pass pass = notSlotPass("*:* -(+b +c)");
    assertEquals(Set.of(), pass.terms());
    assertEquals(Set.of(Set.of(t("b")), Set.of(t("c"))), new HashSet<>(pass.notSlots()));
  }

  public void testNegatedOneTermPhraseIsANotSlot() {
    Query query =
        new BooleanQuery.Builder()
            .add(MatchAllDocsQuery.INSTANCE, BooleanClause.Occur.SHOULD)
            .add(new PhraseQuery("field", "b"), BooleanClause.Occur.MUST_NOT)
            .build();
    assertEquals(new Pass(Set.of(), List.of(Set.of(t("b")))), collect(notSlotTree(query)));
  }

  private static Query matchAllExcept(Query negated) {
    return new BooleanQuery.Builder()
        .add(MatchAllDocsQuery.INSTANCE, BooleanClause.Occur.SHOULD)
        .add(negated, BooleanClause.Occur.MUST_NOT)
        .build();
  }

  public void testNegationThroughOtherQueryTypes() {
    TermQuery b = new TermQuery(t("b"));
    List<Set<Term>> notB = List.of(Set.of(t("b")));
    assertEquals(
        new Pass(Set.of(), notB), collect(notSlotTree(matchAllExcept(new BoostQuery(b, 2)))));
    assertEquals(
        new Pass(Set.of(), notB), collect(notSlotTree(matchAllExcept(new ConstantScoreQuery(b)))));
    Query oneTermSet = new TermInSetQuery("field", List.of(new BytesRef("b")));
    assertEquals(new Pass(Set.of(), notB), collect(notSlotTree(matchAllExcept(oneTermSet))));
    Query synonyms = new SynonymQuery.Builder("field").addTerm(t("b")).addTerm(t("c")).build();
    assertEquals(
        new Pass(Set.of(), List.of(Set.of(t("b"), t("c")))),
        collect(notSlotTree(matchAllExcept(synonyms))));

    // not negated exactly: a disjunction-max wrapper, an empty boolean query (which matches
    // nothing), and a query whose handler does not implement handleNegatedQuery
    Query dismax = new DisjunctionMaxQuery(List.of(b, new TermQuery(t("c"))), 0);
    assertEquals(new Pass(ANY, List.of()), collect(notSlotTree(matchAllExcept(dismax))));
    Query empty = new BooleanQuery.Builder().build();
    assertEquals(new Pass(ANY, List.of()), collect(notSlotTree(matchAllExcept(empty))));
    QueryAnalyzer regexp = new QueryAnalyzer(List.of(new RegexpQueryHandler()), true);
    Query negatedRegexp = matchAllExcept(new RegexpQuery(t("ab.*")));
    assertEquals(
        new Pass(ANY, List.of()), collect(regexp.buildTree(negatedRegexp, TermWeightor.DEFAULT)));
  }

  public void testDoubleNegationIsPositive() {
    // NOT(a AND NOT b) = NOT a OR b
    assertEquals(new Pass(Set.of(t("b")), List.of(Set.of(t("a")))), notSlotPass("*:* -(+a -b)"));
  }

  public void testInexactNegationsAreAny() {
    for (String query :
        new String[] {
          "*:* -\"a b\"", "*:* -b*", "*:* -field:[a TO c]", "*:* -/ab.*/", "*:* -(-b)"
        }) {
      assertEquals(query, new Pass(ANY, List.of()), notSlotPass(query));
    }
    BooleanQuery msm =
        new BooleanQuery.Builder()
            .add(new TermQuery(t("b")), BooleanClause.Occur.SHOULD)
            .add(new TermQuery(t("c")), BooleanClause.Occur.SHOULD)
            .setMinimumNumberShouldMatch(1)
            .build();
    Query query =
        new BooleanQuery.Builder()
            .add(MatchAllDocsQuery.INSTANCE, BooleanClause.Occur.SHOULD)
            .add(msm, BooleanClause.Occur.MUST_NOT)
            .build();
    assertEquals(new Pass(ANY, List.of()), collect(notSlotTree(query)));

    // a query that passes its terms straight to the visitor, but needs all of them to match
    Query allTerms =
        new Query() {
          @Override
          public void visit(QueryVisitor visitor) {
            visitor.consumeTerms(this, t("b"), t("c"));
          }

          @Override
          public String toString(String field) {
            return "allTerms";
          }

          @Override
          public boolean equals(Object obj) {
            return obj == this;
          }

          @Override
          public int hashCode() {
            return System.identityHashCode(this);
          }
        };
    query =
        new BooleanQuery.Builder()
            .add(MatchAllDocsQuery.INSTANCE, BooleanClause.Occur.SHOULD)
            .add(allTerms, BooleanClause.Occur.MUST_NOT)
            .build();
    assertEquals(new Pass(ANY, List.of()), collect(notSlotTree(query)));
  }

  public void testCustomQueryHandlersUnderNegation() {
    Query query =
        new BooleanQuery.Builder()
            .add(MatchAllDocsQuery.INSTANCE, BooleanClause.Occur.SHOULD)
            .add(
                new TermInSetQuery("field", List.of(new BytesRef("x"), new BytesRef("y"))),
                BooleanClause.Occur.MUST_NOT)
            .build();

    // the handler's positive tree is never negated
    QueryAnalyzer notNegatable = new QueryAnalyzer(List.of(new TermInSetQueryHandler(false)), true);
    assertEquals(
        new Pass(ANY, List.of()), collect(notNegatable.buildTree(query, TermWeightor.DEFAULT)));

    QueryAnalyzer negatable =
        new QueryAnalyzer(
            List.of(new TermInSetQueryHandler(false), new TermInSetQueryHandler(true)), true);
    assertEquals(
        new Pass(Set.of(), List.of(Set.of(t("x"), t("y")))),
        collect(negatable.buildTree(query, TermWeightor.DEFAULT)));

    // with negation off, handleNegatedQuery is never called
    CustomQueryHandler throwing =
        new CustomQueryHandler() {
          @Override
          public QueryTree handleQuery(Query q, TermWeightor w) {
            return null;
          }

          @Override
          public QueryTree handleNegatedQuery(Query q, TermWeightor w) {
            throw new AssertionError("called with negation off");
          }
        };
    QueryAnalyzer off = new QueryAnalyzer(List.of(throwing), false);
    assertEquals(
        new Pass(ANY, List.of()),
        collect(off.buildTree(MonitorTestBase.parse("*:* -/ab.*/"), TermWeightor.DEFAULT)));
  }
}
