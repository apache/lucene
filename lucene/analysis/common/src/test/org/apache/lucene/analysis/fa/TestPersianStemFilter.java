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
package org.apache.lucene.analysis.fa;

import java.io.IOException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;
import org.apache.lucene.tests.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.tests.analysis.MockTokenizer;

/** Test the Persian Normalization Filter */
public class TestPersianStemFilter extends BaseTokenStreamTestCase {

  public void testAnSuffix() throws IOException {
    check("دوستان", "دوست");
  }

  public void testHaSuffix() throws IOException {
    check("کتابها", "کتاب");
  }

  public void testAtSuffix() throws IOException {
    check("جامدات", "جامد");
  }

  public void testYeeSuffix() throws IOException {
    check("عليرضايي", "عليرضا");
  }

  public void testYeSuffix() throws IOException {
    check("شادماني", "شادمان");
  }

  public void testTarSuffix() throws IOException {
    check("باحالتر", "باحال");
  }

  public void testTarinSuffix() throws IOException {
    check("خوبترين", "خوب");
  }

  public void testShouldntStem() throws IOException {
    check("کباب", "کباب");
  }

  public void testNonArabic() throws IOException {
    check("English", "English");
  }

  public void testWithKeywordAttribute() throws IOException {
    CharArraySet set = new CharArraySet(1, true);
    set.add("ساهدهات");
    MockTokenizer tokenStream = whitespaceMockTokenizer("ساهدهات");

    PersianStemFilter filter = new PersianStemFilter(new SetKeywordMarkerFilter(tokenStream, set));
    assertTokenStreamContents(filter, new String[] {"ساهدهات"});
  }

  private void check(final String input, final String expected) throws IOException {
    MockTokenizer tokenStream = whitespaceMockTokenizer(input);
    PersianStemFilter filter = new PersianStemFilter(tokenStream);
    assertTokenStreamContents(filter, new String[] {expected});
  }

  public void testEmptyTerm() throws IOException {
    Analyzer a =
        new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            Tokenizer tokenizer = new KeywordTokenizer();
            return new TokenStreamComponents(tokenizer, new PersianStemFilter(tokenizer));
          }
        };
    checkOneTerm(a, "", "");
    a.close();
  }
}
