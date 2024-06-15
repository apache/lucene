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
package org.apache.lucene.analysis.ja;

import java.io.IOException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.tests.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.tests.analysis.MockTokenizer;

/** Tests for {@link JapaneseKatakanaUppercaseFilter} */
public class TestJapaneseKatakanaUppercaseFilter extends BaseTokenStreamTestCase {
  private Analyzer keywordAnalyzer, japaneseAnalyzer;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    keywordAnalyzer =
        new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
            return new TokenStreamComponents(
                tokenizer, new JapaneseKatakanaUppercaseFilter(tokenizer));
          }
        };
    japaneseAnalyzer =
        new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            Tokenizer tokenizer =
                new JapaneseTokenizer(
                    newAttributeFactory(), null, false, JapaneseTokenizer.Mode.SEARCH);
            return new TokenStreamComponents(
                tokenizer, new JapaneseKatakanaUppercaseFilter(tokenizer));
          }
        };
  }

  @Override
  public void tearDown() throws Exception {
    keywordAnalyzer.close();
    japaneseAnalyzer.close();
    super.tearDown();
  }

  public void testKanaUppercase() throws IOException {
    assertAnalyzesTo(
        keywordAnalyzer,
        "ァィゥェォヵㇰヶㇱㇲッㇳㇴㇵㇶㇷㇷ゚ㇸㇹㇺャュョㇻㇼㇽㇾㇿヮ",
        new String[] {"アイウエオカクケシスツトヌハヒフプヘホムヤユヨラリルレロワ"});
    assertAnalyzesTo(keywordAnalyzer, "ストップウォッチ", new String[] {"ストツプウオツチ"});
    assertAnalyzesTo(keywordAnalyzer, "サラニㇷ゚ カムイチェㇷ゚ ㇷ゚ㇷ゚", new String[] {"サラニプ", "カムイチエプ", "ププ"});
    assertAnalyzesTo(keywordAnalyzer, "カムイチェㇷ゚カムイチェ", new String[] {"カムイチエプカムイチエ"});
  }

  public void testKanaUppercaseWithSurrogatePair() throws IOException {
    // 𠀋 : \uD840\uDC0B
    assertAnalyzesTo(
        keywordAnalyzer,
        "\uD840\uDC0Bストップウォッチ ストップ\uD840\uDC0Bウォッチ ストップウォッチ\uD840\uDC0B",
        new String[] {"\uD840\uDC0Bストツプウオツチ", "ストツプ\uD840\uDC0Bウオツチ", "ストツプウオツチ\uD840\uDC0B"});
  }

  public void testKanaUppercaseWithJapaneseTokenizer() throws IOException {
    assertAnalyzesTo(
        japaneseAnalyzer, "時間をストップウォッチで測る", new String[] {"時間", "を", "ストツプウオツチ", "で", "測る"});
  }

  public void testUnsupportedHalfWidthVariants() throws IOException {
    // The below result is expected since only full-width katakana is supported
    assertAnalyzesTo(keywordAnalyzer, "ｽﾄｯﾌﾟｳｫｯﾁ", new String[] {"ｽﾄｯﾌﾟｳｫｯﾁ"});
  }

  public void testRandomData() throws IOException {
    checkRandomData(random(), keywordAnalyzer, 200 * RANDOM_MULTIPLIER);
  }

  public void testEmptyTerm() throws IOException {
    assertAnalyzesTo(keywordAnalyzer, "", new String[] {});
  }
}
