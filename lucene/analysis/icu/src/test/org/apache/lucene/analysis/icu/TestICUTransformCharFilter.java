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
package org.apache.lucene.analysis.icu;

import static org.apache.lucene.analysis.icu.ICUTransformCharFilterFactory.FAIL_ON_ROLLBACK_BUFFER_OVERFLOW_ARGNAME;
import static org.apache.lucene.analysis.icu.ICUTransformCharFilterFactory.MAX_ROLLBACK_BUFFER_CAPACITY_ARGNAME;
import static org.apache.lucene.analysis.icu.ICUTransformCharFilterFactory.SUPPRESS_UNICODE_NORMALIZATION_EXTERNALIZATION_ARGNAME;

import com.ibm.icu.text.Transliterator;
import com.ibm.icu.text.UnicodeSet;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CharFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;

/** Test the ICUTransformCharFilter with some basic examples. */
public class TestICUTransformCharFilter extends BaseTokenStreamTestCase {

  public void testBasicFunctionality() throws Exception {
    checkToken("Traditional-Simplified", "簡化字", "简化字");
    checkToken("Katakana-Hiragana", "ヒラガナ", "ひらがな");
    checkToken("Fullwidth-Halfwidth", "アルアノリウ", "ｱﾙｱﾉﾘｳ");
    checkToken("Any-Latin", "Αλφαβητικός Κατάλογος", "Alphabētikós Katálogos");
    checkToken(
        "NFD; [:Nonspacing Mark:] Remove", "Alphabētikós Katálogos", "Alphabetikos Katalogos");
    checkToken("Han-Latin", "中国", "zhōng guó");
  }

  public void testRollbackBuffer() throws Exception {
    checkToken("Cyrillic-Latin", "яяяяя", "âââââ", false); // final NFC transform applied
    checkToken(
        "Cyrillic-Latin",
        0,
        false,
        "яяяяя",
        "a\u0302a\u0302a\u0302a\u0302a\u0302",
        true); // final NFC transform never applied
    checkToken(
        "Cyrillic-Latin",
        0,
        false,
        "яяяяя",
        "âââââ",
        false); // final NFC transform disabled internally, applied externally
    checkToken("Cyrillic-Latin", 2, false, "яяяяя", "ââa\u0302a\u0302a\u0302", true);
    checkToken(
        "Cyrillic-Latin",
        4,
        false,
        "яяяяяяяяяя",
        "ââa\u0302a\u0302a\u0302a\u0302a\u0302a\u0302a\u0302a\u0302",
        true);
    checkToken(
        "Cyrillic-Latin",
        8,
        false,
        "яяяяяяяяяяяяяяяяяяяя",
        "ââââââa\u0302ââââa\u0302ââââa\u0302âââ",
        true);
    try {
      checkToken(
          "Cyrillic-Latin",
          8,
          true,
          "яяяяяяяяяяяяяяяяяяяя",
          "ââââââa\u0302ââââa\u0302ââââa\u0302âââ",
          true);
      fail("with failOnRollbackBufferOverflow=true, we expect to throw a RuntimeException");
    } catch (RuntimeException ex) {
      // this is expected.
    }
  }

  public void testCustomFunctionality() throws Exception {
    String rules = "a > b; b > c;"; // convert a's to b's and b's to c's
    checkToken(
        Transliterator.createFromRules("test", rules, Transliterator.FORWARD),
        new StringReader("abacadaba"),
        "bcbcbdbcb");
  }

  public void testCustomFunctionality2() throws Exception {
    String rules = "c { a > b; a > d;"; // convert a's to b's and b's to c's
    checkToken(
        Transliterator.createFromRules("test", rules, Transliterator.FORWARD),
        new StringReader("caa"),
        "cbd");
  }

  public void testOptimizer() throws Exception {
    String rules = "a > b; b > c;"; // convert a's to b's and b's to c's
    Transliterator custom = Transliterator.createFromRules("test", rules, Transliterator.FORWARD);
    assertTrue(custom.getFilter() == null);
    new ICUTransformCharFilter(new StringReader(""), custom);
    assertTrue(custom.getFilter().equals(new UnicodeSet("[ab]")));
  }

  public void testOptimizer2() throws Exception {
    checkToken("Traditional-Simplified; CaseFold", "ABCDE", "abcde");
  }

  public void testOptimizerSurrogate() throws Exception {
    String rules = "\\U00020087 > x;"; // convert CJK UNIFIED IDEOGRAPH-20087 to an x
    Transliterator custom = Transliterator.createFromRules("test", rules, Transliterator.FORWARD);
    assertTrue(custom.getFilter() == null);
    new ICUTransformCharFilter(new StringReader(""), custom);
    assertTrue(custom.getFilter().equals(new UnicodeSet("[\\U00020087]")));
  }

  private void checkToken(Transliterator transliterator, Reader input, String expected)
      throws IOException {
    input =
        new ICUTransformCharFilter(
            input,
            transliterator,
            ICUTransformCharFilter.DEFAULT_MAX_ROLLBACK_BUFFER_CAPACITY,
            false);
    final KeywordTokenizer input1 = new KeywordTokenizer();
    input1.setReader(input);
    assertTokenStreamContents(input1, new String[] {expected});
  }

  private void checkToken(String id, String input, String expected) throws IOException {
    checkToken(
        id, ICUTransformCharFilter.DEFAULT_MAX_ROLLBACK_BUFFER_CAPACITY, false, input, expected);
  }

  private void checkToken(
      String id, String input, String expected, boolean suppressUnicodeNormExternalization)
      throws IOException {
    checkToken(
        id,
        ICUTransformCharFilter.DEFAULT_MAX_ROLLBACK_BUFFER_CAPACITY,
        false,
        input,
        expected,
        suppressUnicodeNormExternalization);
  }

  private void checkToken(
      String id,
      int maxRollbackBufferCapacity,
      boolean failOnRollbackBufferOverflow,
      String input,
      String expected)
      throws IOException {
    checkToken(
        getTransliteratingFilter(
            id, new StringReader(input), maxRollbackBufferCapacity, failOnRollbackBufferOverflow),
        expected);
  }

  private void checkToken(
      String id,
      int maxRollbackBufferCapacity,
      boolean failOnRollbackBufferOverflow,
      String input,
      String expected,
      boolean suppressUnicodeNormalizationExternalization)
      throws IOException {
    checkToken(
        getTransliteratingFilter(
            id,
            new StringReader(input),
            suppressUnicodeNormalizationExternalization,
            maxRollbackBufferCapacity,
            failOnRollbackBufferOverflow),
        expected);
  }

  private void checkToken(CharFilter input, String expected) throws IOException {
    final KeywordTokenizer input1 = new KeywordTokenizer();
    input1.setReader(input);
    assertTokenStreamContents(input1, new String[] {expected});
  }

  public void testRandomStringsLatinToKatakana() throws Exception {
    // this Transliterator often decreases character length wrt input
    testRandomStrings("Latin-Katakana");
  }

  public void testRandomStringsAnyToLatin() throws Exception {
    // this Transliterator often increases character length wrt input
    testRandomStrings("Any-Latin");
  }

  private static Analyzer getAnalyzer(String id, boolean suppressExternalize) {
    return new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        return new TokenStreamComponents(tokenizer, tokenizer);
      }

      @Override
      protected Reader initReader(String fieldName, Reader reader) {
        return super.initReader(
            fieldName, getTransliteratingFilter(id, reader, suppressExternalize));
      }

      @Override
      protected Reader initReaderForNormalization(String fieldName, Reader reader) {
        return super.initReaderForNormalization(
            fieldName, getTransliteratingFilter(id, reader, suppressExternalize));
      }
    };
  }

  /** blast some random strings through the analyzer */
  private void testRandomStrings(final String id) throws Exception {
    final boolean firstSuppressExternalize = random().nextBoolean();
    Analyzer a = getAnalyzer(id, firstSuppressExternalize);
    Analyzer b = getAnalyzer(id, !firstSuppressExternalize);
    checkRandomData(random(), a, b, 1000 * RANDOM_MULTIPLIER);
    a.close();
    b.close();
  }

  public void testEmptyTerm() throws IOException {
    Analyzer a =
        new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            Tokenizer tokenizer = new KeywordTokenizer();
            return new TokenStreamComponents(tokenizer, tokenizer);
          }

          @Override
          protected Reader initReader(String fieldName, Reader reader) {
            return super.initReader(fieldName, getTransliteratingFilter("Any-Latin", reader));
          }

          @Override
          protected Reader initReaderForNormalization(String fieldName, Reader reader) {
            return super.initReaderForNormalization(
                fieldName, getTransliteratingFilter("Any-Latin", reader));
          }
        };
    checkOneTerm(a, "", "");
    a.close();
  }

  private static CharFilter getTransliteratingFilter(String id, Reader input) {
    return getTransliteratingFilter(id, input, random().nextBoolean());
  }

  private static CharFilter getTransliteratingFilter(
      String id, Reader r, boolean suppressUnicodeNormalizationExternalization) {
    return getTransliteratingFilter(
        id,
        r,
        suppressUnicodeNormalizationExternalization,
        ICUTransformCharFilter.DEFAULT_MAX_ROLLBACK_BUFFER_CAPACITY,
        ICUTransformCharFilter.DEFAULT_FAIL_ON_ROLLBACK_BUFFER_OVERFLOW);
  }

  private static CharFilter getTransliteratingFilter(
      String id, Reader r, int maxRollbackBufferCapacity, boolean failOnRollbackBufferOverflow) {
    return getTransliteratingFilter(
        id, r, random().nextBoolean(), maxRollbackBufferCapacity, failOnRollbackBufferOverflow);
  }

  @SuppressWarnings("resource")
  private static CharFilter getTransliteratingFilter(
      String id,
      Reader r,
      boolean suppressUnicodeNormalizationExternalization,
      int maxRollbackBufferCapacity,
      boolean failOnRollbackBufferOverflow) {
    Map<String, String> args = new HashMap<>();
    args.put("id", id);
    args.put(MAX_ROLLBACK_BUFFER_CAPACITY_ARGNAME, Integer.toString(maxRollbackBufferCapacity));
    args.put(
        FAIL_ON_ROLLBACK_BUFFER_OVERFLOW_ARGNAME, Boolean.toString(failOnRollbackBufferOverflow));
    args.put(
        SUPPRESS_UNICODE_NORMALIZATION_EXTERNALIZATION_ARGNAME,
        Boolean.toString(suppressUnicodeNormalizationExternalization));
    return (CharFilter) (new ICUTransformCharFilterFactory(args)).create(r);
  }
}
