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
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.tests.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.tests.analysis.CannedTokenStream;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.apache.lucene.tests.analysis.Token;

/** */
public class TestTrimFilter extends BaseTokenStreamTestCase {

  public void testTrim() throws Exception {
    TokenStream ts =
        new CannedTokenStream(
            new Token(" a ", 1, 5),
            new Token("b   ", 6, 10),
            new Token("cCc", 11, 15),
            new Token("   ", 16, 20),
            new Token("", 21, 21));
    ts = new TrimFilter(ts);

    assertTokenStreamContents(ts, new String[] {"a", "b", "cCc", "", ""});
  }

  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    Analyzer a =
        new Analyzer() {

          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            Tokenizer tokenizer = new MockTokenizer(MockTokenizer.KEYWORD, false);
            return new TokenStreamComponents(tokenizer, new TrimFilter(tokenizer));
          }
        };
    checkRandomData(random(), a, 200 * RANDOM_MULTIPLIER);
    a.close();
  }

  public void testEmptyTerm() throws IOException {
    Analyzer a =
        new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            Tokenizer tokenizer = new KeywordTokenizer();
            return new TokenStreamComponents(tokenizer, new TrimFilter(tokenizer));
          }
        };
    checkOneTerm(a, "", "");
    a.close();
  }
}
