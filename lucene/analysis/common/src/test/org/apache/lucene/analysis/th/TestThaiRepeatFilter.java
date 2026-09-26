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
import java.io.StringReader;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.tests.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.tests.analysis.MockTokenizer;

/** Test case for {@link ThaiRepeatFilter}. */
public class TestThaiRepeatFilter extends BaseTokenStreamTestCase {

  public void testStandaloneMaiyamok() throws IOException {
    // Standalone token "ๆ" after "เร็ว"
    TokenStream ts = whitespaceMockTokenizer("เร็ว ๆ");
    ts = new ThaiRepeatFilter(ts);
    assertTokenStreamContents(ts, new String[] {"เร็ว", "เร็ว"}, new int[] {0, 5}, new int[] {4, 6});
  }

  public void testAttachedMaiyamok() throws IOException {
    // Attached "ๆ" at the end of word "เร็วๆ"
    TokenStream ts = whitespaceMockTokenizer("เร็วๆ");
    ts = new ThaiRepeatFilter(ts);
    assertTokenStreamContents(ts, new String[] {"เร็ว", "เร็ว"}, new int[] {0, 4}, new int[] {4, 5});
  }

  public void testMultipleMaiyamok() throws IOException {
    // Double Maiyamok "มากๆๆ"
    TokenStream ts = whitespaceMockTokenizer("มากๆๆ");
    ts = new ThaiRepeatFilter(ts);
    assertTokenStreamContents(
        ts, new String[] {"มาก", "มาก", "มาก"}, new int[] {0, 3, 4}, new int[] {3, 4, 5});

    // Separate tokens "มาก ๆ ๆ"
    ts = whitespaceMockTokenizer("มาก ๆ ๆ");
    ts = new ThaiRepeatFilter(ts);
    assertTokenStreamContents(
        ts, new String[] {"มาก", "มาก", "มาก"}, new int[] {0, 4, 6}, new int[] {3, 5, 7});
  }

  public void testInSentence() throws IOException {
    TokenStream ts = whitespaceMockTokenizer("เด็ก ๆ กำลัง วิ่ง เล่น");
    ts = new ThaiRepeatFilter(ts);
    assertTokenStreamContents(
        ts,
        new String[] {"เด็ก", "เด็ก", "กำลัง", "วิ่ง", "เล่น"},
        new int[] {0, 5, 7, 13, 18},
        new int[] {4, 6, 12, 17, 22});
  }

  public void testLeadingDanglingMaiyamok() throws IOException {
    // Maiyamok at stream start with no preceding token
    TokenStream ts = whitespaceMockTokenizer("ๆ ครับ");
    ts = new ThaiRepeatFilter(ts);
    assertTokenStreamContents(ts, new String[] {"ครับ"});
  }

  public void testEmpty() throws IOException {
    TokenStream ts = whitespaceMockTokenizer("");
    ts = new ThaiRepeatFilter(ts);
    assertTokenStreamContents(ts, new String[] {});
  }

  public void testWithThaiTokenizer() throws IOException {
    assumeTrue(
        "JRE does not support Thai dictionary-based BreakIterator", ThaiTokenizer.DBBI_AVAILABLE);
    Tokenizer tokenizer = new ThaiTokenizer();
    tokenizer.setReader(new StringReader("วิ่งเร็วๆ"));
    TokenStream ts = new ThaiRepeatFilter(tokenizer);
    assertTokenStreamContents(
        ts, new String[] {"วิ่ง", "เร็ว", "เร็ว"}, new int[] {0, 4, 8}, new int[] {4, 8, 9});
  }

  public void testRandomStrings() throws IOException {
    Analyzer a =
        new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
            return new TokenStreamComponents(tokenizer, new ThaiRepeatFilter(tokenizer));
          }
        };
    checkRandomData(random(), a, 200 * RANDOM_MULTIPLIER);
    a.close();
  }
}
