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
  Analyzer a;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    a =
        new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            final Tokenizer source = new MockTokenizer();
            return new TokenStreamComponents(source, new PersianStemFilter(source));
          }
        };
  }

  @Override
  public void tearDown() throws Exception {
    a.close();
    super.tearDown();
  }

  public void testAnSuffix() throws IOException {
    checkOneTerm(a, "دوستان", "دوست");
  }

  public void testHaSuffix() throws IOException {
    checkOneTerm(a, "كتابها", "كتاب");
  }

  public void testAtSuffix() throws IOException {
    checkOneTerm(a, "جامدات", "جامد");
  }

  public void testYeeSuffix() throws IOException {
    checkOneTerm(a, "عليرضايي", "عليرضا");
  }

  public void testYeSuffix() throws IOException {
    checkOneTerm(a, "شادماني", "شادمان");
  }

  public void testTarSuffix() throws IOException {
    checkOneTerm(a, "باحالتر", "باحال");
  }

  public void testTarinSuffix() throws IOException {
    checkOneTerm(a, "خوبترين", "خوب");
  }

  public void testShouldntStem() throws IOException {
    checkOneTerm(a, "كباب", "كباب");
  }

  public void testNonArabic() throws IOException {
    checkOneTerm(a, "English", "english");
  }

  public void testWithKeywordAttribute() throws IOException {
    CharArraySet set = new CharArraySet(1, true);
    set.add("ساهدهات");
    MockTokenizer tokenStream = whitespaceMockTokenizer("ساهدهات");

    PersianStemFilter filter = new PersianStemFilter(new SetKeywordMarkerFilter(tokenStream, set));
    assertTokenStreamContents(filter, new String[] {"ساهدهات"});
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
