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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.tests.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.apache.lucene.tests.util.TestUtil;
import org.junit.Test;

/** Test the truncate token filter. */
public class TestTruncateTokenFilter extends BaseTokenStreamTestCase {

  public void testLegacyTruncating() throws Exception {
    TokenStream stream =
        whitespaceMockTokenizer("abcdefg 1234567 ABCDEFG abcde abc 12345 123 1234567 1234😃5");
    stream = TruncateTokenFilter.truncateAfterChars(stream, 5);
    assertTokenStreamContents(
        stream,
        new String[] {
          "abcde",
          "12345",
          "ABCDE",
          "abcde",
          "abc",
          "12345",
          "123",
          "12345",
          "1234" + "😃".charAt(0)
        });
  }

  public void testCodePointTruncating() throws Exception {
    TokenStream stream =
        whitespaceMockTokenizer(
            "abcdefg 1234567 ABCDEFG abcde abc 12345 123 1234😃5 1 😃 😃12345 😃😃 😃😃😃 😃😃😃😃 😃😃😃😃😃 😃😃😃😃😃😃");
    stream = TruncateTokenFilter.truncateAfterCodePoints(stream, 5);
    assertTokenStreamContents(
        stream,
        new String[] {
          "abcde",
          "12345",
          "ABCDE",
          "abcde",
          "abc",
          "12345",
          "123",
          "1234😃",
          "1",
          "😃",
          "😃1234",
          "😃😃",
          "😃😃😃",
          "😃😃😃😃",
          "😃😃😃😃😃",
          "😃😃😃😃😃"
        });
  }

  public void testRandom() throws Exception {
    var rnd = random();
    for (int i = 0; i < 50 * RANDOM_MULTIPLIER; i++) {
      var truncateLength = rnd.nextInt(5) + 1;
      String text = TestUtil.randomAnalysisString(rnd, 200, false);

      TokenStream ts1 = whitespaceMockTokenizer(text);
      CharTermAttribute termAtt1 = ts1.addAttribute(CharTermAttribute.class);
      TokenStream ts2 =
          TruncateTokenFilter.truncateAfterCodePoints(
              whitespaceMockTokenizer(text), truncateLength);
      CharTermAttribute termAtt2 = ts2.addAttribute(CharTermAttribute.class);

      ts1.reset();
      ts2.reset();
      while (ts2.incrementToken()) {
        assertTrue(ts1.incrementToken());
        int len1 = Character.codePointCount(termAtt1, 0, termAtt1.length());
        int len2 = Character.codePointCount(termAtt2, 0, termAtt2.length());
        if (len1 <= truncateLength) {
          assertEquals(len1, len2);
          assertEquals(termAtt1.toString(), termAtt2.toString());
        } else {
          assertEquals(truncateLength, len2);
          assertTrue(termAtt1.toString().startsWith(termAtt2.toString()));
        }
      }
      assertFalse(ts1.incrementToken());
      ts1.end();
      ts2.end();
      ts1.close();
      ts2.close();
    }
  }

  public void testStressRandom() throws Exception {
    var rnd = random();
    var truncateLength = rnd.nextInt(5) + 1;
    Analyzer a =
        new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
            return new TokenStreamComponents(
                tokenizer, TruncateTokenFilter.truncateAfterCodePoints(tokenizer, truncateLength));
          }
        };
    checkRandomData(rnd, a, 20 * RANDOM_MULTIPLIER, truncateLength * 2);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testLegacyNonPositiveLength() throws Exception {
    TruncateTokenFilter.truncateAfterChars(
        whitespaceMockTokenizer("param must be a positive number"), -48);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNonPositiveLength() throws Exception {
    TruncateTokenFilter.truncateAfterCodePoints(
        whitespaceMockTokenizer("param must be a positive number"), -48);
  }
}
