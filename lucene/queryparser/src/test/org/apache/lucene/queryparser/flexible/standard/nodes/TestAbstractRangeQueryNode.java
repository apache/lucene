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
package org.apache.lucene.queryparser.flexible.standard.nodes;

import static org.hamcrest.Matchers.is;

import java.text.NumberFormat;
import java.util.Locale;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.queryparser.flexible.standard.config.PointsConfig;
import org.apache.lucene.queryparser.flexible.standard.parser.EscapeQuerySyntaxImpl;
import org.apache.lucene.search.Query;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestAbstractRangeQueryNode extends LuceneTestCase {
  private static final EscapeQuerySyntax escaper = new EscapeQuerySyntaxImpl();
  private static final StandardQueryParser parser = new StandardQueryParser();

  /* GITHUB#7865 bug in toQueryString() */
  public void testTermRangeQueryNode() throws Exception {
    FieldQueryNode lower = new FieldQueryNode("FIELD", "aaa", 0, 0);
    FieldQueryNode upper = new FieldQueryNode("FIELD", "zzz", 0, 0);
    TermRangeQueryNode origNode = new TermRangeQueryNode(lower, upper, true, true);
    CharSequence queryString = origNode.toQueryString(escaper);

    new FieldQueryNode("FIELD", "(literal parens)", 0, 0).getTermEscaped(escaper);

    // query string should have expected format and parse into a valid query
    assertThat(queryString, is("FIELD:[aaa TO zzz]"));
    Query parsedQuery = parser.parse(queryString.toString(), "");
    assertThat(parsedQuery.toString(), is(queryString));
  }

  /* GITHUB#7865 bug in toQueryString() */
  public void testPointRangeQueryNode() throws Exception {
    NumberFormat format = NumberFormat.getIntegerInstance(Locale.ROOT);
    PointQueryNode lower = new PointQueryNode("FIELD", 1, format);
    PointQueryNode upper = new PointQueryNode("FIELD", 999, format);

    PointRangeQueryNode origNode =
        new PointRangeQueryNode(lower, upper, true, true, new PointsConfig(format, Integer.class));
    CharSequence queryString = origNode.toQueryString(escaper);

    // query string should have expected format and parse into a valid query
    assertThat(queryString, is("FIELD:[1 TO 999]"));
    Query parsedQuery = parser.parse(queryString.toString(), "");
    assertThat(parsedQuery.toString(), is(queryString));
  }
}
