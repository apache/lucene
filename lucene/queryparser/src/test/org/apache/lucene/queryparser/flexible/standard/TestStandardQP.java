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

import java.util.Locale;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.DateTools.Resolution;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.nodes.BooleanQueryNode;
import org.apache.lucene.queryparser.flexible.standard.builders.BooleanQueryNodeBuilder;
import org.apache.lucene.queryparser.flexible.standard.builders.StandardQueryTreeBuilder;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.Operator;
import org.apache.lucene.queryparser.util.QueryParserTestBase;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.analysis.MockTokenizer;

/** Tests QueryParser. */
public class TestStandardQP extends QueryParserTestBase {

  public StandardQueryParser getParser(Analyzer a) throws Exception {
    if (a == null) a = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);
    StandardQueryParser qp = new StandardQueryParser(a);
    qp.setDefaultOperator(Operator.OR);

    return qp;
  }

  public Query parse(String query, StandardQueryParser qp) throws Exception {
    return qp.parse(query, getDefaultField());
  }

  @Override
  public CommonQueryParserConfiguration getParserConfig(Analyzer a) throws Exception {
    return getParser(a);
  }

  @Override
  public Query getQuery(String query, CommonQueryParserConfiguration cqpC) throws Exception {
    assert cqpC != null : "Parameter must not be null";
    assert (cqpC instanceof StandardQueryParser)
        : "Parameter must be instance of StandardQueryParser";
    StandardQueryParser qp = (StandardQueryParser) cqpC;
    return parse(query, qp);
  }

  @Override
  public Query getQuery(String query, Analyzer a) throws Exception {
    return parse(query, getParser(a));
  }

  @Override
  public boolean isQueryParserException(Exception exception) {
    return exception instanceof QueryNodeException;
  }

  @Override
  public void setDefaultOperatorOR(CommonQueryParserConfiguration cqpC) {
    assert (cqpC instanceof StandardQueryParser);
    StandardQueryParser qp = (StandardQueryParser) cqpC;
    qp.setDefaultOperator(Operator.OR);
  }

  @Override
  public void setDefaultOperatorAND(CommonQueryParserConfiguration cqpC) {
    assert (cqpC instanceof StandardQueryParser);
    StandardQueryParser qp = (StandardQueryParser) cqpC;
    qp.setDefaultOperator(Operator.AND);
  }

  @Override
  public void setAutoGeneratePhraseQueries(CommonQueryParserConfiguration cqpC, boolean value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setDateResolution(
      CommonQueryParserConfiguration cqpC, CharSequence field, Resolution value) {
    assert (cqpC instanceof StandardQueryParser);
    StandardQueryParser qp = (StandardQueryParser) cqpC;
    qp.getDateResolutionMap().put(field, value);
  }

  @Override
  public void testOperatorVsWhitespace() throws Exception {
    // LUCENE-2566 is not implemented for StandardQueryParser
    // TODO implement LUCENE-2566 and remove this (override)method
    Analyzer a =
        new Analyzer() {
          @Override
          public TokenStreamComponents createComponents(String fieldName) {
            return new TokenStreamComponents(new MockTokenizer(MockTokenizer.WHITESPACE, false));
          }
        };
    assertQueryEquals("a - b", a, "a -b");
    assertQueryEquals("a + b", a, "a +b");
    assertQueryEquals("a ! b", a, "a -b");
  }

  @Override
  public void testRangeWithPhrase() throws Exception {
    // StandardSyntaxParser does not differentiate between a term and a
    // one-term-phrase in a range query.
    // Is this an issue? Should StandardSyntaxParser mark the text as
    // wasEscaped=true ?
    assertQueryEquals("[\\* TO \"*\"]", null, "[\\* TO *]");
  }

  @Override
  public void testEscapedVsQuestionMarkAsWildcard() throws Exception {
    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    assertQueryEquals("a:b\\-?c", a, "a:b-?c");
    assertQueryEquals("a:b\\+?c", a, "a:b+?c");
    assertQueryEquals("a:b\\:?c", a, "a:b:?c");

    assertQueryEquals("a:b\\\\?c", a, "a:b\\?c");
  }

  @Override
  public void testEscapedWildcard() throws Exception {
    CommonQueryParserConfiguration qp =
        getParserConfig(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false));
    WildcardQuery q = new WildcardQuery(new Term("field", "foo?ba?r")); // TODO not correct!!
    assertEquals(q, getQuery("foo\\?ba?r", qp));
  }

  @Override
  public void testAutoGeneratePhraseQueriesOn() throws Exception {
    expectThrows(
        UnsupportedOperationException.class,
        () -> {
          setAutoGeneratePhraseQueries(getParser(null), true);
          super.testAutoGeneratePhraseQueriesOn();
        });
  }

  @Override
  public void testStarParsing() throws Exception {}

  @Override
  public void testDefaultOperator() throws Exception {
    StandardQueryParser qp = getParser(new MockAnalyzer(random()));
    // make sure OR is the default:
    assertEquals(StandardQueryConfigHandler.Operator.OR, qp.getDefaultOperator());
    setDefaultOperatorAND(qp);
    assertEquals(StandardQueryConfigHandler.Operator.AND, qp.getDefaultOperator());
    setDefaultOperatorOR(qp);
    assertEquals(StandardQueryConfigHandler.Operator.OR, qp.getDefaultOperator());
  }

  @Override
  public void testNewFieldQuery() throws Exception {
    /* ordinary behavior, synonyms form uncoordinated boolean query */
    StandardQueryParser dumb = getParser(new Analyzer1());
    BooleanQuery.Builder expanded = new BooleanQuery.Builder();
    expanded.add(new TermQuery(new Term("field", "dogs")), BooleanClause.Occur.SHOULD);
    expanded.add(new TermQuery(new Term("field", "dog")), BooleanClause.Occur.SHOULD);
    assertEquals(expanded.build(), dumb.parse("\"dogs\"", "field"));
    /* even with the phrase operator the behavior is the same */
    assertEquals(expanded.build(), dumb.parse("dogs", "field"));

    /* custom behavior, the synonyms are expanded, unless you use quote operator */
    // TODO test something like "SmartQueryParser()"
  }

  // TODO: Remove this specialization once the flexible standard parser gets multi-word synonym
  // support
  @Override
  public void testQPA() throws Exception {
    super.testQPA();

    assertQueryEquals("term phrase term", qpAnalyzer, "term (phrase1 phrase2) term");

    CommonQueryParserConfiguration cqpc = getParserConfig(qpAnalyzer);
    setDefaultOperatorAND(cqpc);
    assertQueryEquals(cqpc, "field", "term phrase term", "+term +(+phrase1 +phrase2) +term");
  }

  public void testBooleanQueryExceedingMaxClauses() throws Exception {
    int prior = IndexSearcher.getMaxClauseCount();
    try {
      // Lower the max clause count to a very small value.
      IndexSearcher.setMaxClauseCount(3);

      // Check the defaults.
      {
        StandardQueryParser qp = new StandardQueryParser();
        qp.setAnalyzer(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false));

        // We can't optimize conjunction clauses so this should fail.
        qp.setDefaultOperator(StandardQueryConfigHandler.Operator.AND);
        expectThrows(
            QueryNodeException.class,
            () -> {
              qp.parse("t000 t001 t002 t004", "field");
            });

        // We can optimize disjunction clauses but this is an opt-in option
        // since it changes scoring. By default, it should also fail.
        qp.setDefaultOperator(StandardQueryConfigHandler.Operator.OR);
        expectThrows(
            QueryNodeException.class,
            () -> {
              qp.parse("t000 t001 t002 t003", "field");
            });
      }

      // Check non-default options.
      {
        StandardQueryParser qp = new StandardQueryParser();
        StandardQueryTreeBuilder customQueryBuilder = new StandardQueryTreeBuilder();
        BooleanQueryNodeBuilder booleanQueryNodeBuilder = new BooleanQueryNodeBuilder();
        customQueryBuilder.setBuilder(BooleanQueryNode.class, booleanQueryNodeBuilder);
        qp.setQueryBuilder(customQueryBuilder);
        qp.setAnalyzer(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false));

        booleanQueryNodeBuilder.setMinClauseCountForDisjunctionOptimization(
            IndexSearcher.getMaxClauseCount());

        // We do agree to optimize disjunction clauses now, even sacrificing the scoring function.
        qp.setDefaultOperator(StandardQueryConfigHandler.Operator.OR);
        qp.parse("t000 t001 t002 t003", "field");

        // Run some searches and verify everything works.
        try (Directory dir = newDirectory();
            IndexWriter w =
                new IndexWriter(
                    dir,
                    newIndexWriterConfig(
                        new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)))) {
          for (int i = 0; i < 100; i++) {
            Document d = new Document();
            d.add(new StringField("field", String.format(Locale.ROOT, "t%03d", i), Field.Store.NO));
            w.addDocument(d);
          }
          w.commit();

          try (var liveReader = DirectoryReader.open(w)) {
            IndexSearcher searcher = new IndexSearcher(liveReader);

            qp.setDefaultOperator(StandardQueryConfigHandler.Operator.OR);
            TopDocs topDocs =
                searcher.search(qp.parse("t000 t001 t002 t003 t004 t005", "field"), 10);
            assertEquals(6, topDocs.scoreDocs.length);

            // Check if fields are split/ merged properly when the optimization is applied.
            IndexSearcher.setMaxClauseCount(4);
            topDocs =
                searcher.search(
                    qp.parse(
                        "field:(t000 t001 t002 t003 t004) OR otherField:(t005 t006) OR field:t007",
                        "missingField"),
                    10);
            assertEquals(6, topDocs.scoreDocs.length);
          }
        }
      }
    } finally {
      IndexSearcher.setMaxClauseCount(prior);
    }
  }
}
