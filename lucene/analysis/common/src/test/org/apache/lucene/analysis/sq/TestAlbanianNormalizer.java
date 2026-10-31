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
package org.apache.lucene.analysis.sq;

import java.io.IOException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.tests.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.tests.analysis.MockTokenizer;

/** Test {@link AlbanianNormalizer} */
public class TestAlbanianNormalizer extends BaseTokenStreamTestCase {

  public void testDiacritics() throws IOException {
    check("është", "eshte");
    check("çfarë", "cfare");
    check("mëshirëbërës", "meshireberes");
    check("ÇËSHTJE", "CESHTJE");
  }

  /**
   * ë and ç written as a base letter plus a combining mark, as text copied from PDFs often is, fold
   * the same way as the precomposed letters.
   */
  public void testCombiningMarks() throws IOException {
    check("është", "eshte");
    check("çfarë", "cfare");
    check("âsht", "asht");
  }

  /** Gheg writing marks nasal vowels with a circumflex (âsht, nânë, Malcís). */
  public void testGhegVowels() throws IOException {
    check("âsht", "asht");
    check("nânë", "nane");
    check("shqypnîs", "shqypnis");
    check("kênë", "kene");
    check("hûni", "huni");
    check("ÂSHT", "ASHT");
    // acute accents mark long vowels in the same tradition (Lahuta e Malcís)
    check("malcís", "malcis");
    check("lirís", "liris");
    check("kángë", "kange");
  }

  public void testApostrophes() throws IOException {
    check("kur'an", "kuran");
    check("kur’an", "kuran");
    check("kurʼan", "kuran");
    check("pa'pa'pa", "papapa");
  }

  /**
   * A one-letter clitic elided with an apostrophe is dropped, so that the word it was attached to
   * can still be recognized (ç'është → është, which is a stop word).
   */
  public void testElidedClitics() throws IOException {
    check("ç'është", "eshte");
    check("s'ka", "ka");
    check("t'i", "i");
    check("m'u", "u");
    check("q'u", "u");
    check("n'atë", "ate");
    check("s'a", "a");
    // nothing after the apostrophe: just drop it
    check("s'", "s");
  }

  public void testUnchanged() throws IOException {
    check("durim", "durim");
    check("shqiperi", "shqiperi");
  }

  private void check(final String input, final String expected) throws IOException {
    Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    tokenizer.setReader(new java.io.StringReader(input));
    TokenFilter tf = new AlbanianNormalizationFilter(tokenizer);
    assertTokenStreamContents(tf, new String[] {expected});
  }

  public void testEmptyTerm() throws IOException {
    Analyzer a =
        new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            Tokenizer tokenizer = new KeywordTokenizer();
            return new TokenStreamComponents(tokenizer, new AlbanianNormalizationFilter(tokenizer));
          }
        };
    checkOneTerm(a, "", "");
    a.close();
  }
}
