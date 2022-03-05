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
package org.apache.lucene.queryparser.flexible.standard;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import java.io.StringReader;
import java.util.Locale;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.charstream.FastCharStream;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler;
import org.apache.lucene.queryparser.flexible.standard.nodes.IntervalQueryNode;
import org.apache.lucene.queryparser.flexible.standard.parser.StandardSyntaxParser;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Test;

/** Test interval sub-query support in {@link StandardQueryParser}. */
public class TestStandardQPEnhancements extends LuceneTestCase {
  protected static final String FLD_DEFAULT = "defaultField";
  protected static final String FLD_WHITESPACE = "whitespaceField";

  final StandardQueryParser getQueryParser() {
    var analyzer =
        new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            return new TokenStreamComponents(new MockTokenizer(MockTokenizer.WHITESPACE, true));
          }
        };

    var qp = new StandardQueryParser(analyzer);
    qp.setDefaultOperator(StandardQueryConfigHandler.Operator.AND);
    qp.setMultiFields(new String[] {});
    return qp;
  }

  @Test
  public void testMinShouldMatchOperator() throws Exception {
    Query parsed =
        parsedQuery(
            String.format(
                Locale.ROOT,
                "(%s:foo OR %s:bar OR %s:baz)@2",
                FLD_WHITESPACE,
                FLD_WHITESPACE,
                FLD_WHITESPACE));

    MatcherAssert.assertThat(
        ((BooleanQuery) parsed).getMinimumNumberShouldMatch(), Matchers.equalTo(2));
  }

  @Test
  public void testAtLeast() throws Exception {
    checkIntervalQueryNode("fn:atleast(3 FOO BAR baz)");
  }

  @Test
  public void testMaxWidth() throws Exception {
    checkIntervalQueryNode("fn:maxwidth(3 fn:atleast(2 foo bar baz))");
  }

  @Test
  public void testQuotedTerm() throws Exception {
    checkIntervalQueryNode("fn:atleast(2 \"foo\" \"BAR baz\")");
  }

  @Test
  public void testMaxGaps() throws Exception {
    checkIntervalQueryNode("fn:maxgaps(2 fn:unordered(foo BAR baz))");
  }

  @Test
  public void testOrdered() throws Exception {
    checkIntervalQueryNode("fn:ordered(foo BAR baz)");
  }

  @Test
  public void testUnordered() throws Exception {
    checkIntervalQueryNode("fn:unordered(foo BAR baz)");
  }

  @Test
  public void testOr() throws Exception {
    checkIntervalQueryNode("fn:or(foo baz)");
  }

  @Test
  public void testWildcard() throws Exception {
    checkIntervalQueryNode("fn:wildcard(foo*)");

    // Explicit maxExpansions.
    checkIntervalQueryNode("fn:wildcard(foo* 128)");
  }

  @Test
  public void testPhrase() throws Exception {
    checkIntervalQueryNode("fn:phrase(abc def fn:or(baz boo))");
  }

  @Test
  public void testBefore() throws Exception {
    checkIntervalQueryNode("fn:before(abc fn:ordered(foo bar))");
  }

  @Test
  public void testAfter() throws Exception {
    checkIntervalQueryNode("fn:after(abc fn:ordered(foo bar))");
  }

  @Test
  public void testContaining() throws Exception {
    checkIntervalQueryNode("fn:containing(big small)");
  }

  @Test
  public void testContainedBy() throws Exception {
    checkIntervalQueryNode("fn:containedBy(small big)");
  }

  @Test
  public void testNotContaining() throws Exception {
    checkIntervalQueryNode("fn:notContaining(minuend subtrahend)");
  }

  @Test
  public void testNotContainedBy() throws Exception {
    checkIntervalQueryNode("fn:notContainedBy(small big)");
  }

  @Test
  public void testWithin() throws Exception {
    checkIntervalQueryNode("fn:within(small 2 fn:ordered(big foo))");
  }

  @Test
  public void testNotWithin() throws Exception {
    checkIntervalQueryNode("fn:notWithin(small 2 fn:ordered(big foo))");
  }

  @Test
  public void testOverlapping() throws Exception {
    checkIntervalQueryNode("fn:overlapping(fn:ordered(big foo) small)");
  }

  @Test
  public void testNonOverlapping() throws Exception {
    checkIntervalQueryNode("fn:nonOverlapping(fn:ordered(big foo) small)");
  }

  @Test
  public void testUnorderedNoOverlaps() throws Exception {
    checkIntervalQueryNode("fn:unorderedNoOverlaps(fn:ordered(big foo) small)");
  }

  @Test
  public void testExtend() throws Exception {
    checkIntervalQueryNode("fn:extend(fn:ordered(big foo) 2 5)");
  }

  @Test
  public void testFuzzy() throws Exception {
    checkIntervalQueryNode("fn:fuzzyTerm(dfe)");
    // Explicit maxEdits
    checkIntervalQueryNode("fn:fuzzyTerm(dfe 2)");
    // Explicit maxExpansions
    checkIntervalQueryNode("fn:fuzzyTerm(dfe 2 128)");
  }

  protected void checkIntervalQueryNode(String query) throws Exception {
    // Check raw parser first.
    var syntaxParser = new StandardSyntaxParser(new FastCharStream(new StringReader(query)));
    QueryNode queryNode = syntaxParser.TopLevelQuery(FLD_DEFAULT);
    MatcherAssert.assertThat(queryNode, Matchers.instanceOf(IntervalQueryNode.class));

    var queryParser = getQueryParser();
    Query parsedQuery;
    if (RandomizedTest.randomBoolean()) {
      queryParser.setMultiFields(new String[] {FLD_DEFAULT});
      parsedQuery = queryParser.parse(query, null);
    } else {
      parsedQuery = queryParser.parse(query, FLD_DEFAULT);
    }
    MatcherAssert.assertThat(parsedQuery, Matchers.notNullValue());

    // Emit toString() for visual diagnostics.
    IntervalQueryNode intervalQueryNode = (IntervalQueryNode) queryNode;
    intervalQueryNode.setAnalyzer(queryParser.getAnalyzer());
    System.out.printf(
        Locale.ROOT, "query: %s%n  node: %s%n  query: %s%n", query, queryNode, parsedQuery);
  }

  protected String parsed(String query) throws Exception {
    return parsedQuery(query).toString("<no-default>");
  }

  protected Query parsedQuery(String query) throws Exception {
    return getQueryParser().parse(query, /* no default field. */ null);
  }
}
