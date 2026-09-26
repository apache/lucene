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
package org.apache.lucene.analysis.th;

import java.io.IOException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.tests.analysis.BaseTokenStreamTestCase;

/** Test case for {@link ThaiNormalizer} and {@link ThaiNormalizationFilter}. */
public class TestThaiNormalizer extends BaseTokenStreamTestCase {

  public void testDoubleSaraE() throws IOException {
    // Two Sara E (U+0E40 U+0E40) should become Sara Ae (U+0E41)
    check("\u0E40\u0E40\u0E1B\u0E25\u0E01", "\u0E41\u0E1B\u0E25\u0E01"); // เเปลก -> แปลก
  }

  public void testSaraAmDecomposition() throws IOException {
    // Nikhahit (U+0E4D) + Sara Aa (U+0E32) -> Sara Am (U+0E33)
    check("\u0E17\u0E4D\u0E32\u0E07\u0E32\u0E19", "\u0E17\u0E33\u0E07\u0E32\u0E19"); // ทํางาน -> ทำงาน

    // Nikhahit + Tone (U+0E49) + Sara Aa -> Tone + Sara Am
    check("\u0E19\u0E4D\u0E49\u0E32", "\u0E19\u0E49\u0E33"); // นํ้า -> น้ำ

    // Tone + Nikhahit + Sara Aa -> Tone + Sara Am
    check("\u0E19\u0E49\u0E4D\u0E32", "\u0E19\u0E49\u0E33"); // น้ำ
  }

  public void testSaraAmWithFollowedTone() throws IOException {
    // Sara Am followed by Tone mark -> Tone mark + Sara Am
    check("\u0E19\u0E33\u0E49", "\u0E19\u0E49\u0E33");
  }

  public void testReorderToneAndAboveVowel() throws IOException {
    // Tone mark before Above Vowel (Mai Tho before Sara I) -> Sara I before Mai Tho
    check("\u0E01\u0E49\u0E34", "\u0E01\u0E34\u0E49"); // ก้ิ -> กิ้
  }

  public void testDuplicateVowelsAndTones() throws IOException {
    // Duplicate Sara Ii
    check("\u0E14\u0E35\u0E35", "\u0E14\u0E35"); // ดีี -> ดี
    // Duplicate Mai Tho
    check("\u0E41\u0E21\u0E49\u0E49", "\u0E41\u0E21\u0E49"); // แม้้ -> แม้
    // Duplicate Sara Aa
    check("\u0E19\u0E32\u0E32", "\u0E19\u0E32"); // นาา -> นา
  }

  public void testConsecutiveDifferentTones() throws IOException {
    // When multiple different tone marks are typed, keep the last one
    check("\u0E44\u0E21\u0E48\u0E49", "\u0E44\u0E21\u0E49"); // ไม่้ -> ไม้
  }

  public void testZeroWidthCharacters() throws IOException {
    check("\u0E20\u0E32\u0E29\u0E32\u200B\u0E44\u0E17\u0E22", "\u0E20\u0E32\u0E29\u0E32\u0E44\u0E17\u0E22");
    check("\u0E20\u0E32\u0E29\u0E32\u200C\u0E44\u0E17\u0E22", "\u0E20\u0E32\u0E29\u0E32\u0E44\u0E17\u0E22");
  }

  public void testLakkhangyao() throws IOException {
    // Preceded by Ru: preserve
    check("\u0E24\u0E45", "\u0E24\u0E45");
    // Not preceded by Ru or Lu: normalize to Sara Aa
    check("\u0E01\u0E45", "\u0E01\u0E32");
  }

  public void testDanglingMark() throws IOException {
    // Leading dangling tone mark without consonant
    check("\u0E49\u0E01\u0E32", "\u0E01\u0E32");
  }

  public void testEmptyTerm() throws IOException {
    Analyzer a =
        new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            Tokenizer tokenizer = new KeywordTokenizer();
            return new TokenStreamComponents(tokenizer, new ThaiNormalizationFilter(tokenizer));
          }
        };
    checkOneTerm(a, "", "");
    a.close();
  }

  private void check(String input, String output) throws IOException {
    Tokenizer tokenizer = whitespaceMockTokenizer(input);
    TokenFilter tf = new ThaiNormalizationFilter(tokenizer);
    assertTokenStreamContents(tf, new String[] {output});
  }
}
