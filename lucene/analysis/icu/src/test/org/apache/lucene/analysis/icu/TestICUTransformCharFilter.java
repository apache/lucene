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

import com.ibm.icu.text.Transliterator;
import com.ibm.icu.text.UnicodeSet;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
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

  /**
   * Sanity check all top-level prepackaged Transliterators to make sure that no trivial errors are thrown on
   * instantiation. We're not really checking anything in particular here, but under the hood this will at least
   * make a cursory check for consistency between the "stock" Transliterator, and any potential "optimized" version
   * with externalized unicode normalization.
   */
  public void testNormalizationOptimizationOnAvailableIDs() throws Exception {
    Enumeration<String> ids = Transliterator.getAvailableIDs();
    List<String> idsWithNestedNormalization = new ArrayList<>();
    while (ids.hasMoreElements()) {
      String id = ids.nextElement();
      boolean hasNestedUnicodeNormalization = accumulateNestedUnicodeNormalization(id, idsWithNestedNormalization);
      try {
        // set `iterationsDefault=0` because in this test we simply want to ignore the non-optimized case.
        // set `iterationsOptimized=1` because `testRandomStrings` can be slow; we want this as a sanity check, but
        //  as a matter of course we should not need many iterations.
        boolean optimized = testRandomStrings(id, 0, 1);

        // We can't really _do_ much with `hasNestedUnicodeNormalization` or `optimized`, in terms of validation.
        // They are completely independent (i.e. not mutually exclusive). We leave these checks stubbed out here
        // because we're initially optimizing normalization that can be easily detected at the top level, but we
        // want some plumbing in place to be more transparent about what we're optimizing and what we're not (and
        // to some extent _why_).
        // TODO: We're aware that this is probably leaving "on the table" a bunch of potential optimization of the
        //  "_nested_ unicode normalization" case. It may be worth optimizing these nested cases (and addressing
        //  any additional complexity specific to that case) as a separate, follow-up issue.
      } catch (Exception ex) {
        // wrap the exception so that we can report the offending `id`
        throw new RuntimeException("problem for id: "+id, ex);
      }
    }
  }

  /**
   * It is possible that leading and trailing (or singleton) Transliterators might apply nested
   * Unicode normalization, thus acting in the capacity of a Normalizer, without qualifying as a
   * top-level Normalizer as currently defined in {@link
   * ICUTransformCharFilterFactory#unicodeNormalizationType(String)}. For now, simply detect these
   * cases (to facilitate more nuanced handling in the future, if necessary).
   *
   * @param topLevelId top level Transliterator parent id.
   * @param ids list to which the topLevelId will be added if nested unicode normalization is detected
   */
  private static boolean accumulateNestedUnicodeNormalization(String topLevelId, List<String> ids) {
    Transliterator levelOne = Transliterator.getInstance(topLevelId);
    Transliterator[] topLevelElements = levelOne.getElements();
    if (topLevelElements.length == 1 && levelOne == topLevelElements[0]) {
      // A leaf Transliterator; shortcircuit
      return false;
    }
    ArrayDeque<Transliterator> elements = new ArrayDeque<>(topLevelElements.length << 2); // oversize
    elements.addAll(Arrays.asList(topLevelElements));
    do {
      final Transliterator t = elements.removeFirst();
      if (ICUTransformCharFilterFactory.unicodeNormalizationType(t.getID()) != null) {
        ids.add(topLevelId);
        return true;
      }
      Transliterator[] subElements = t.getElements();
      if (subElements.length > 1 || t != subElements[0]) {
        for (Transliterator sub : subElements) {
          elements.addFirst(sub);
        }
      }
    } while (!elements.isEmpty());
    return false;
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
            null,
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
    // we _don't_ expect unicode norm externalization optimization in practice
    testRandomStrings("Latin-Katakana", 1000, -1);
  }

  public void testRandomStringsAnyToLatin() throws Exception {
    // this Transliterator often increases character length wrt input
    // we _don't_ expect unicode norm externalization optimization in practice
    testRandomStrings("Any-Latin", 1000, -1);
  }

  public void testRandomStringsKatakanaToHiragana() throws Exception {
    // this Transliterator often increases character length wrt input
    // we _do_ expect unicode norm externalization optimization in practice
    testRandomStrings("Katakana-Hiragana", -1, 1000);
  }

  private static Analyzer getAnalyzer(String id, boolean suppressExternalize, boolean[] optimized) {
    if (optimized != null && !suppressExternalize) {
      getTransliteratingFilter(optimized, id, new StringReader("dummy"), suppressExternalize);
    }
    return new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        return new TokenStreamComponents(tokenizer, tokenizer);
      }

      @Override
      protected Reader initReader(String fieldName, Reader reader) {
        return super.initReader(
            fieldName, getTransliteratingFilter(null, id, reader, suppressExternalize));
      }

      @Override
      protected Reader initReaderForNormalization(String fieldName, Reader reader) {
        return super.initReaderForNormalization(
            fieldName, getTransliteratingFilter(null, id, reader, suppressExternalize));
      }
    };
  }

  /** blast some random strings through the analyzer */
  private boolean testRandomStrings(final String id, int iterationsDefault, int iterationsOptimized) throws Exception {
    final boolean firstSuppressExternalize = random().nextBoolean();
    boolean[] optimized = new boolean[] { false };
    Analyzer a = getAnalyzer(id, firstSuppressExternalize, optimized);
    Analyzer b = getAnalyzer(id, !firstSuppressExternalize, optimized);
    final int iterations;
    if (optimized[0]) {
      iterations = iterationsOptimized;
    } else {
      iterations = iterationsDefault;
      // there's no optimization; it doesn't matter which analyzer we use.
      b.close();
      b = null;
    }
    assertTrue("implicitly expected optimized="+optimized[0]+"; requested iterations="+iterations,
        iterations >= 0);
    checkRandomData(random(), a, b, iterations * RANDOM_MULTIPLIER);
    a.close();
    if (b != null) {
      b.close();
    }
    return optimized[0];
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
    return getTransliteratingFilter(null, id, input, random().nextBoolean());
  }

  private static CharFilter getTransliteratingFilter(
      boolean[] optimized, String id, Reader r, boolean suppressUnicodeNormalizationExternalization) {
    return getTransliteratingFilter(
        optimized,
        id,
        r,
        suppressUnicodeNormalizationExternalization,
        ICUTransformCharFilter.DEFAULT_MAX_ROLLBACK_BUFFER_CAPACITY,
        ICUTransformCharFilter.DEFAULT_FAIL_ON_ROLLBACK_BUFFER_OVERFLOW);
  }

  private static CharFilter getTransliteratingFilter(
      String id, Reader r, int maxRollbackBufferCapacity, boolean failOnRollbackBufferOverflow) {
    return getTransliteratingFilter(
        null, id, r, random().nextBoolean(), maxRollbackBufferCapacity, failOnRollbackBufferOverflow);
  }

  @SuppressWarnings("resource")
  private static CharFilter getTransliteratingFilter(
      boolean[] optimized,
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
    ICUTransformCharFilterFactory factory = new ICUTransformCharFilterFactory(args,
        suppressUnicodeNormalizationExternalization);
    if (optimized != null) {
      optimized[0] |= factory.externalizedUnicodeNormalization();
    }
    return (CharFilter) factory.create(r);
  }
}
