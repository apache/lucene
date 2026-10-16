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
package org.apache.lucene.queryparser.flexible.standard.parser;

import java.util.Locale;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QuotedFieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestEscapeQuerySyntaxImpl extends LuceneTestCase {
  private static final EscapeQuerySyntax escaper = new EscapeQuerySyntaxImpl();

  // U+0130 lower-cases to two chars ("i" + U+0307) in all but Turkic locales, so matching on a
  // lower-cased copy of the text puts the escape char at the wrong position.
  public void testEscapeWhenLowerCasingChangesLength() {
    assertEquals(
        "İ\\:x", escaper.escape("İ:x", Locale.ENGLISH, EscapeQuerySyntax.Type.NORMAL).toString());
    assertEquals(
        "İ\\:", escaper.escape("İ:", Locale.ENGLISH, EscapeQuerySyntax.Type.NORMAL).toString());
    assertEquals(
        "İ\\\" OR secret:\\\"x",
        escaper
            .escape("İ\" OR secret:\"x", Locale.ENGLISH, EscapeQuerySyntax.Type.STRING)
            .toString());
  }

  public void testEscapedTermsParseBack() throws Exception {
    String term = "İ:x";
    QueryNode node =
        new StandardSyntaxParser()
            .parse(
                "body:" + escaper.escape(term, Locale.ENGLISH, EscapeQuerySyntax.Type.NORMAL), "f");
    assertTrue(node instanceof FieldQueryNode);
    assertEquals("body", ((FieldQueryNode) node).getFieldAsString());
    assertEquals(term, ((FieldQueryNode) node).getTextAsString());

    String phrase = "İ\" OR secret:\"x";
    node =
        new StandardSyntaxParser()
            .parse(
                "body:\""
                    + escaper.escape(phrase, Locale.ENGLISH, EscapeQuerySyntax.Type.STRING)
                    + "\"",
                "f");
    assertTrue(node instanceof QuotedFieldQueryNode);
    assertEquals("body", ((QuotedFieldQueryNode) node).getFieldAsString());
    assertEquals(phrase, ((QuotedFieldQueryNode) node).getTextAsString());
  }
}
