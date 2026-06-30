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
package org.apache.lucene.analysis.ko;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.text.Normalizer;
import java.text.Normalizer.Form;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ko.KoreanTokenizer.DecompoundMode;
import org.apache.lucene.analysis.ko.tokenattributes.PartOfSpeechAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.tests.analysis.BaseTokenStreamFactoryTestCase;
import org.apache.lucene.tests.analysis.MockTokenizer;

public class TestHangulCompositionCharFilter extends BaseTokenStreamFactoryTestCase {

  public void testComposeModernConjoiningJamo() throws Exception {
    assertEquals(
        "가 각 한글",
        readFully(
            new HangulCompositionCharFilter(
                new StringReader(Normalizer.normalize("가 각 한글", Form.NFD)))));
  }

  public void testLeavesNonModernSequencesUnchanged() throws Exception {
    String input = "ㄱㅏ ᄀᄀ ᅡ 가ᅡ";
    assertEquals("ㄱㅏ ᄀᄀ ᅡ 가ᅡ", readFully(new HangulCompositionCharFilter(new StringReader(input))));
  }

  public void testNoOpInputs() throws Exception {
    assertEquals("", readFully(new HangulCompositionCharFilter(new StringReader(""))));
    assertEquals("ᄀ", readFully(new HangulCompositionCharFilter(new StringReader("ᄀ"))));
    assertEquals(
        "가나다 ABC", readFully(new HangulCompositionCharFilter(new StringReader("가나다 ABC"))));
  }

  public void testCorrectOffsets() throws Exception {
    CharFilter reader =
        new HangulCompositionCharFilter(
            new StringReader(Normalizer.normalize("가 난", Normalizer.Form.NFD)));
    assertEquals("가 난", readFully(reader));
    assertEquals(0, reader.correctOffset(0));
    assertEquals(2, reader.correctOffset(1));
    assertEquals(3, reader.correctOffset(2));
    assertEquals(6, reader.correctOffset(3));
  }

  public void testLeavesPrecomposedLvPlusTrailingJamoUnchanged() throws Exception {
    // Precomposed LV plus trailing jamo is outside the full conjoining-jamo sequence scope.
    String input = "하\u11AB";
    assertEquals(input, readFully(new HangulCompositionCharFilter(new StringReader(input))));
  }

  public void testNoriTermsMatchNfcForNfdInput() throws Exception {
    String nfc = "한국어 형태소를 분석합니다";
    String nfd = Normalizer.normalize(nfc, Form.NFD);

    try (Analyzer analyzer = koreanAnalyzer(false);
        Analyzer analyzerWithCharFilter = koreanAnalyzer(true)) {
      assertNotEquals(collectTermData(analyzer, nfc), collectTermData(analyzer, nfd));
      assertEquals(collectTermData(analyzer, nfc), collectTermData(analyzerWithCharFilter, nfd));
    }
  }

  public void testNoriOffsetsReferToOriginalNfdInput() throws Exception {
    String nfd = Normalizer.normalize("한국어 형태소를 분석합니다", Form.NFD);

    try (Analyzer analyzer = koreanAnalyzer(true)) {
      assertAnalyzesTo(
          analyzer,
          nfd,
          new String[] {"한국어", "한국", "어", "형태소", "형태", "소", "를", "분석", "합니다", "하", "ᄇ니다"},
          new int[] {0, 0, 6, 9, 9, 14, 16, 20, 26, 26, 26},
          new int[] {8, 6, 8, 16, 14, 16, 19, 26, 33, 33, 33});
    }
  }

  public void testFactory() throws Exception {
    Reader reader =
        charFilterFactory("hangulComposition")
            .create(new StringReader(Normalizer.normalize("한국어 처리합니다", Form.NFD)));
    TokenStream stream = whitespaceMockTokenizer(reader);
    assertTokenStreamContents(stream, new String[] {"한국어", "처리합니다"});
  }

  public void testBogusFactoryArguments() {
    IllegalArgumentException expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> charFilterFactory("hangulComposition", "bogusArg", "bogusValue"));
    assertTrue(expected.getMessage().contains("Unknown parameters"));
  }

  public void testRandomModernHangulCompositionMatchesNfc() throws Exception {
    int iterations = atLeast(100);
    for (int i = 0; i < iterations; i++) {
      String text = randomModernHangulText();
      assertEquals(
          text,
          readFully(
              new HangulCompositionCharFilter(
                  new StringReader(Normalizer.normalize(text, Form.NFD)))));
    }
  }

  public void testRandomData() throws Exception {
    Analyzer analyzer =
        new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
            return new TokenStreamComponents(tokenizer, tokenizer);
          }

          @Override
          protected Reader initReader(String fieldName, Reader reader) {
            return new HangulCompositionCharFilter(reader);
          }
        };
    checkRandomData(random(), analyzer, RANDOM_MULTIPLIER * 1000);
    analyzer.close();
  }

  private Analyzer koreanAnalyzer(boolean useHangulCompositionCharFilter) {
    return new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer =
            new KoreanTokenizer(newAttributeFactory(), null, DecompoundMode.MIXED, false);
        return new TokenStreamComponents(tokenizer);
      }

      @Override
      protected Reader initReader(String fieldName, Reader reader) {
        if (useHangulCompositionCharFilter) {
          return new HangulCompositionCharFilter(reader);
        }
        return reader;
      }
    };
  }

  private List<String> collectTermData(Analyzer analyzer, String input) throws IOException {
    List<String> result = new ArrayList<>();
    try (TokenStream ts = analyzer.tokenStream("ignored", input)) {
      CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
      PartOfSpeechAttribute partOfSpeechAtt = ts.addAttribute(PartOfSpeechAttribute.class);
      ts.reset();
      while (ts.incrementToken()) {
        result.add(
            termAtt
                + "/"
                + partOfSpeechAtt.getPOSType()
                + "/"
                + partOfSpeechAtt.getLeftPOS()
                + "/"
                + partOfSpeechAtt.getRightPOS());
      }
      ts.end();
    }
    return result;
  }

  private String randomModernHangulText() {
    StringBuilder builder = new StringBuilder();
    int length = random().nextInt(20);
    for (int i = 0; i < length; i++) {
      int choice = random().nextInt(8);
      if (choice == 0) {
        builder.append(' ');
      } else if (choice == 1) {
        builder.append((char) ('a' + random().nextInt(26)));
      } else {
        builder.append((char) (0xAC00 + random().nextInt(0xD7A4 - 0xAC00)));
      }
    }
    return builder.toString();
  }

  private static String readFully(Reader reader) throws IOException {
    StringBuilder builder = new StringBuilder();
    char[] buffer = new char[1024];
    int count;
    while ((count = reader.read(buffer)) != -1) {
      builder.append(buffer, 0, count);
    }
    return builder.toString();
  }
}
