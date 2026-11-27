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
import java.io.Reader;
import java.io.StringReader;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.tests.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.tests.analysis.MockTokenizer;

/** Test case for {@link ThaiCharFilter}. */
public class TestThaiCharFilter extends BaseTokenStreamTestCase {

  /** Double Sara E (เ + เ) normalized to Sara Ae (แ) with offset correction */
  public void testDoubleSaraE() throws IOException {
    CharFilter reader = new ThaiCharFilter(new StringReader("เเมว"));
    TokenStream ts = whitespaceMockTokenizer(reader);
    assertTokenStreamContents(ts, new String[] {"แมว"}, new int[] {0}, new int[] {4}, 4);
  }

  /** Sara Am recomposition (ํ + า -> ำ, and ํ + tone + า -> tone + ำ) */
  public void testSaraAmRecomposition() throws IOException {
    // น + ํ + ้ + า (4 chars) -> น้ำ (3 chars)
    CharFilter reader = new ThaiCharFilter(new StringReader("น\u0E4D\u0E49\u0E32"));
    TokenStream ts = whitespaceMockTokenizer(reader);
    assertTokenStreamContents(ts, new String[] {"น้ำ"}, new int[] {0}, new int[] {4}, 4);

    // ท + ํ + า (3 chars) -> ทำ (2 chars)
    reader = new ThaiCharFilter(new StringReader("ท\u0E4D\u0E32"));
    ts = whitespaceMockTokenizer(reader);
    assertTokenStreamContents(ts, new String[] {"ทำ"}, new int[] {0}, new int[] {3}, 3);
  }

  /** Consecutive repeated diacritics / tone marks deduplication */
  public void testDuplicateDiacritics() throws IOException {
    // ด + ี + ี (3 chars) -> ดี (2 chars)
    CharFilter reader = new ThaiCharFilter(new StringReader("ดีี"));
    TokenStream ts = whitespaceMockTokenizer(reader);
    assertTokenStreamContents(ts, new String[] {"ดี"}, new int[] {0}, new int[] {3}, 3);

    // ไ + ม + ้ + ้ (4 chars) -> ไม้ (3 chars)
    reader = new ThaiCharFilter(new StringReader("ไม้้"));
    ts = whitespaceMockTokenizer(reader);
    assertTokenStreamContents(ts, new String[] {"ไม้"}, new int[] {0}, new int[] {4}, 4);
  }

  /** Zero-width space and joiner removal */
  public void testZeroWidth() throws IOException {
    // สวัส + \u200B + ดี (7 chars in input) -> สวัสดี (6 chars in output, endOffset 7)
    CharFilter reader = new ThaiCharFilter(new StringReader("สวัส\u200Bดี"));
    TokenStream ts = whitespaceMockTokenizer(reader);
    assertTokenStreamContents(ts, new String[] {"สวัสดี"}, new int[] {0}, new int[] {7}, 7);
  }

  /** Misplaced tone mark before above/below vowel swapped to canonical order */
  public void testToneReordering() throws IOException {
    // บ + ่ + ั + น -> บ + ั + ่ + น (บั่น)
    CharFilter reader = new ThaiCharFilter(new StringReader("บ\u0E48\u0E31น"));
    TokenStream ts = whitespaceMockTokenizer(reader);
    assertTokenStreamContents(ts, new String[] {"บั่น"}, new int[] {0}, new int[] {4}, 4);
  }

  /** Lakkhangyao (ๅ) converted to Sara Aa (า) except after Ru (ฤ) or Lu (ฦ) */
  public void testLakkhangyao() throws IOException {
    CharFilter reader = new ThaiCharFilter(new StringReader("ฤๅ ฦๅ กๅ"));
    TokenStream ts = whitespaceMockTokenizer(reader);
    assertTokenStreamContents(
        ts, new String[] {"ฤๅ", "ฦๅ", "กา"}, new int[] {0, 3, 6}, new int[] {2, 5, 8}, 8);
  }

  /**
   * Demonstrates that pre-tokenization char filtering allows BreakIterator to segment words
   * correctly that would otherwise be mistakenly merged into one huge token.
   */
  public void testBreakIteratorSegmentationWithCharFilter() throws IOException {
    assumeTrue(
        "JRE does not support Thai dictionary-based BreakIterator", ThaiTokenizer.DBBI_AVAILABLE);
    // "ฉันรักเเมวมาก" (contains double Sara E in เเมว)
    // Without ThaiCharFilter, BreakIterator lumps "รักเเมวมาก" into 1 token.
    // With ThaiCharFilter, it normalizes to "ฉันรักแมวมาก" and segments all 4 words properly.
    Reader reader = new ThaiCharFilter(new StringReader("ฉันรักเเมวมาก"));
    Tokenizer tokenizer = new ThaiTokenizer();
    tokenizer.setReader(reader);
    assertTokenStreamContents(
        tokenizer,
        new String[] {"ฉัน", "รัก", "แมว", "มาก"},
        new int[] {0, 3, 6, 10},
        new int[] {3, 6, 10, 13},
        13);
  }

  public void testEmpty() throws IOException {
    CharFilter reader = new ThaiCharFilter(new StringReader(""));
    TokenStream ts = whitespaceMockTokenizer(reader);
    assertTokenStreamContents(ts, new String[] {});
  }

  public void testRandom() throws IOException {
    Analyzer analyzer =
        new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
            return new TokenStreamComponents(tokenizer, tokenizer);
          }

          @Override
          protected Reader initReader(String fieldName, Reader reader) {
            return new ThaiCharFilter(reader);
          }
        };
    checkRandomData(random(), analyzer, 200 * RANDOM_MULTIPLIER);
    analyzer.close();
  }
}
