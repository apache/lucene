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
package org.apache.lucene.queryparser.indri;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.IndriAndQuery;
import org.apache.lucene.search.IndriOrQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;

/** Tests for {@link IndriQueryParser} */
public class TestIndriQueryParser extends LuceneTestCase {

  /**
   * helper to parse a query with whitespace+lowercase analyzer across "field", with default
   * operator of MUST
   */
  private Query parse(String text) throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());
    IndriQueryParser parser = new IndriQueryParser(analyzer, "field");
    return parser.parseQuery(text);
  }

  /** test the Indri AND */
  public void testIndriAND() throws Exception {
    List<BooleanClause> clauses = new ArrayList<>();
    Query query1 = new TermQuery(new Term("field", "foo"));
    clauses.add(new BooleanClause(query1, Occur.SHOULD));
    Query query2 = new TermQuery(new Term("field", "bar"));
    clauses.add(new BooleanClause(query2, Occur.SHOULD));

    IndriAndQuery expected = new IndriAndQuery(clauses);

    assertEquals(expected, parse("#and(foo bar)"));
    assertEquals(expected, parse("foo bar"));
  }

  /** test the Indri OR */
  public void testIndriOR() throws Exception {
    List<BooleanClause> clauses = new ArrayList<>();
    Query query1 = new TermQuery(new Term("field", "foo"));
    clauses.add(new BooleanClause(query1, Occur.SHOULD));
    Query query2 = new TermQuery(new Term("field", "bar"));
    clauses.add(new BooleanClause(query2, Occur.SHOULD));

    IndriOrQuery expected = new IndriOrQuery(clauses);

    assertEquals(expected, parse("#or(foo bar)"));
  }
}
