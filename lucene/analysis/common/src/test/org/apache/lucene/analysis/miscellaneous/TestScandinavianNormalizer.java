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
package org.apache.lucene.analysis.miscellaneous;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.miscellaneous.ScandinavianNormalizer.Foldings;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.tests.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.tests.analysis.MockTokenizer;

/** Tests low level the normalizer functionality */
public class TestScandinavianNormalizer extends BaseTokenStreamTestCase {
  public void testNoFoldings() throws Exception {
    Analyzer analyzer = createAnalyzer(Collections.emptySet());
    checkOneTerm(analyzer, "aa", "aa");
    checkOneTerm(analyzer, "ao", "ao");
    checkOneTerm(analyzer, "ae", "ae");
    checkOneTerm(analyzer, "oo", "oo");
    checkOneTerm(analyzer, "oe", "oe");
    analyzer.close();
  }

  public void testAeFolding() throws Exception {
    Analyzer analyzer = createAnalyzer(Set.of(Foldings.AE));
    checkOneTerm(analyzer, "aa", "aa");
    checkOneTerm(analyzer, "ao", "ao");
    checkOneTerm(analyzer, "ae", "æ");
    checkOneTerm(analyzer, "aE", "æ");
    checkOneTerm(analyzer, "Ae", "Æ");
    checkOneTerm(analyzer, "AE", "Æ");
    checkOneTerm(analyzer, "oo", "oo");
    checkOneTerm(analyzer, "oe", "oe");
    analyzer.close();
  }

  public void testAaFolding() throws Exception {
    Analyzer analyzer = createAnalyzer(Set.of(Foldings.AA));
    checkOneTerm(analyzer, "aa", "å");
    checkOneTerm(analyzer, "aA", "å");
    checkOneTerm(analyzer, "Aa", "Å");
    checkOneTerm(analyzer, "AA", "Å");
    checkOneTerm(analyzer, "ao", "ao");
    checkOneTerm(analyzer, "ae", "ae");
    checkOneTerm(analyzer, "oo", "oo");
    checkOneTerm(analyzer, "oe", "oe");
    analyzer.close();
  }

  public void testOeFolding() throws Exception {
    Analyzer analyzer = createAnalyzer(Set.of(Foldings.OE));
    checkOneTerm(analyzer, "aa", "aa");
    checkOneTerm(analyzer, "ao", "ao");
    checkOneTerm(analyzer, "ae", "ae");
    checkOneTerm(analyzer, "oo", "oo");
    checkOneTerm(analyzer, "oe", "ø");
    checkOneTerm(analyzer, "oE", "ø");
    checkOneTerm(analyzer, "Oe", "Ø");
    checkOneTerm(analyzer, "OE", "Ø");
    analyzer.close();
  }

  public void testOoFolding() throws Exception {
    Analyzer analyzer = createAnalyzer(Set.of(Foldings.OO));
    checkOneTerm(analyzer, "aa", "aa");
    checkOneTerm(analyzer, "ao", "ao");
    checkOneTerm(analyzer, "ae", "ae");
    checkOneTerm(analyzer, "oo", "ø");
    checkOneTerm(analyzer, "oO", "ø");
    checkOneTerm(analyzer, "Oo", "Ø");
    checkOneTerm(analyzer, "OO", "Ø");
    checkOneTerm(analyzer, "oe", "oe");
    analyzer.close();
  }

  public void testAoFolding() throws Exception {
    Analyzer analyzer = createAnalyzer(Set.of(Foldings.AO));
    checkOneTerm(analyzer, "aa", "aa");
    checkOneTerm(analyzer, "ao", "å");
    checkOneTerm(analyzer, "aO", "å");
    checkOneTerm(analyzer, "Ao", "Å");
    checkOneTerm(analyzer, "AO", "Å");
    checkOneTerm(analyzer, "ae", "ae");
    checkOneTerm(analyzer, "oo", "oo");
    checkOneTerm(analyzer, "oe", "oe");
    analyzer.close();
  }

  private Analyzer createAnalyzer(Set<Foldings> foldings) {
    return new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String field) {
        final Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        final TokenStream stream =
            new TokenFilter(tokenizer) {
              private final CharTermAttribute charTermAttribute =
                  addAttribute(CharTermAttribute.class);
              private final ScandinavianNormalizer normalizer =
                  new ScandinavianNormalizer(foldings);

              @Override
              public boolean incrementToken() throws IOException {
                if (!input.incrementToken()) {
                  return false;
                }
                charTermAttribute.setLength(
                    normalizer.processToken(
                        charTermAttribute.buffer(), charTermAttribute.length()));
                return true;
              }
            };
        return new TokenStreamComponents(tokenizer, stream);
      }
    };
  }
}
