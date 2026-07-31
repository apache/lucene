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
package org.apache.lucene.queryparser.classic;

import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.QueryParser.Operator;
import org.apache.lucene.search.Query;
import org.apache.lucene.tests.util.LuceneTestCase;

/**
 * Reproduces and guards against a bug GITHUB#16441 in {@link QueryParserBase#addMultiTermClauses}: when a
 * MultiFieldQueryParser query is analyzed to a single term via the whitespace "MultiTerm" grammar
 * path (see QueryParser.jj), and a field leaf other than a bare TermQuery (a boosted term, a
 * PrefixQuery, etc.) is produced for one of the fields, the resulting query incorrectly forces
 * every field clause to Occur.MUST under the AND default operator instead of preserving the
 * intended Occur.SHOULD disjunction across fields.
 *
 * <p>This is a variant of the issue tracked as https://github.com/apache/lucene/issues/8648. The
 * "allNestedTermQueries" heuristic in {@code addMultiTermClauses} originally only recognized bare
 * {@code TermQuery} clauses; the fix widens it to recognize any leaf clause that is not itself a
 * nested {@code BooleanQuery}, since the real distinguishing signal is whether q's direct clauses
 * represent several term positions to be joined by the default operator (nested BooleanQuery
 * clauses), or field alternatives for a single term position (any other leaf type), which must be
 * passed through unchanged regardless of the default operator.
 *
 * <p>This test class also guards against two regressions introduced by intermediate, incorrect
 * fix attempts: one that only special-cased BoostQuery (missed PrefixQuery and similar leaves),
 * and one that stopped flattening nested BooleanQuery clauses altogether (broke the genuine
 * multi-term AND case, since QueryParser.jj's Query() production bypasses addMultiTermClauses'
 * output entirely whenever it ends up adding a single clause).
 */
public class TestMultiFieldQueryParserBoostAndOperator extends LuceneTestCase {

  private MultiFieldQueryParser boostedParser() {
    return new MultiFieldQueryParser(new String[] {"field1", "field2"}, new StandardAnalyzer(), Map.of("field1", 2.0f));
  }

  private MultiFieldQueryParser plainParser() {
    return new MultiFieldQueryParser(new String[] {"field1", "field2"}, new StandardAnalyzer());
  }

  /** The original bug: a single analyzed term, boosted on one field, under AND. */
  public void testSingleTermMultiTermPathWithBoostKeepsShouldClauses() throws Exception {
    MultiFieldQueryParser parser = boostedParser();
    String queryText = QueryParser.escape("hello !");

    parser.setDefaultOperator(Operator.OR);
    assertEquals("(field1:hello)^2.0 field2:hello", parser.parse(queryText).toString());

    parser.setDefaultOperator(Operator.AND);
    assertEquals("(field1:hello)^2.0 field2:hello", parser.parse(queryText).toString());
  }

  /** Same scenario without a boost map: must remain unaffected. */
  public void testSingleTermMultiTermPathWithoutBoostIsUnaffected() throws Exception {
    MultiFieldQueryParser parser = plainParser();
    String queryText = QueryParser.escape("hello !");

    parser.setDefaultOperator(Operator.AND);
    assertEquals("field1:hello field2:hello", parser.parse(queryText).toString());
  }

  /**
   * Regression guard: genuine multi-term input under AND must still get the per-position "+"
   * joining. This is the case that an earlier, overly broad fix attempt (never flatten) broke.
   */
  public void testMultiTermStillJoinedWithDefaultOperator() throws Exception {
    MultiFieldQueryParser parser = plainParser();

    parser.setDefaultOperator(Operator.OR);
    assertEquals(
        "(field1:firstterm field2:firstterm) (field1:secondterm field2:secondterm)",
        parser.parse("firstterm secondterm").toString());

    parser.setDefaultOperator(Operator.AND);
    assertEquals(
        "+(field1:firstterm field2:firstterm) +(field1:secondterm field2:secondterm)",
        parser.parse("firstterm secondterm").toString());
  }

  /** Same multi-term case with boosts, to make sure the per-position groups keep their boost. */
  public void testMultiTermWithBoostStillJoinedWithDefaultOperator() throws Exception {
    MultiFieldQueryParser parser = boostedParser();

    parser.setDefaultOperator(Operator.AND);
    assertEquals(
        "+((field1:firstterm)^2.0 field2:firstterm) +((field1:secondterm)^2.0 field2:secondterm)",
        parser.parse("firstterm secondterm").toString());
  }

  /**
   * Regression guard for the gap in the first (rejected) fix attempt, which only special-cased
   * BoostQuery: a per-field PrefixQuery leaf must be treated the same way as a boosted TermQuery,
   * i.e. passed through with its original SHOULD occur rather than forced to MUST.
   */
  public void testSingleTermMultiTermPathWithPrefixQueryKeepsShouldClauses() throws Exception {
    MultiFieldQueryParser parser = plainParser();
    // Two escaped, whitespace-separated tokens that the analyzer reduces to a single analyzed
    // prefix term, forcing the MultiTerm grammar path.
    String queryText = QueryParser.escape("hel*") + " " + QueryParser.escape("!");

    parser.setDefaultOperator(Operator.AND);
    Query q = parser.parse(queryText);

    // Whatever the exact PrefixQuery rendering, the two fields must remain an optional
    // disjunction (no leading "+" on either field clause).
    String s = q.toString();
    assertFalse("expected no forced MUST on field clauses: " + s, s.startsWith("+"));
  }
}
