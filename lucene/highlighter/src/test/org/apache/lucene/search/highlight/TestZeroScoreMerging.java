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
package org.apache.lucene.search.highlight;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.apache.lucene.tests.util.LuceneTestCase;

/** Regression test for merging zero-scored fragments into scored fragments (#15333). */
public class TestZeroScoreMerging extends LuceneTestCase {

  public void testZeroScoredFragmentsAreNotMergedIntoHighlights() throws Exception {
    // Build a text with large zero-matching regions around a single matching token "credit".
    String prefix = repeat('a', 130); // ensures at least one 100-char fragment with score 0
    String match = " credit ";
    String suffix = repeat('b', 130);
    String text = prefix + match + suffix;

    MockAnalyzer analyzer = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);

    TermQuery query = new TermQuery(new Term("f", "credit"));
    QueryScorer scorer = new QueryScorer(query, "f");
    Highlighter highlighter = new Highlighter(new SimpleHTMLFormatter(), scorer);
    highlighter.setTextFragmenter(new SimpleSpanFragmenter(scorer, 100));

    TokenStream ts = analyzer.tokenStream("f", text);
    String[] best = highlighter.getBestFragments(ts, text, 3);

    // We expect only the fragment containing the match to be returned, not merged with neighbors
    assertEquals("Only the scored fragment should be returned", 1, best.length);
    assertTrue(
        "Returned fragment must contain the highlighted match",
        best[0].contains("<B>credit</B>") || best[0].contains("<b>credit</b>"));
    // And it should not be overly long (i.e., not a merge of 3x100-size fragments)
    assertTrue("Fragment should be near the configured size", best[0].length() <= 160);
  }

  private static String repeat(char c, int count) {
    StringBuilder sb = new StringBuilder(count);
    for (int i = 0; i < count; i++) {
      sb.append(c);
    }
    return sb.toString();
  }
}
