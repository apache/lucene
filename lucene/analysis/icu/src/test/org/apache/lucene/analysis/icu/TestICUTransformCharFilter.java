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

import com.ibm.icu.text.Replaceable;
import com.ibm.icu.text.ReplaceableString;
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
import java.util.Random;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CharFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.util.AttributeFactory;
import org.apache.lucene.util.TestUtil;

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
   * Sanity check all top-level prepackaged Transliterators to make sure that no trivial errors are
   * thrown on instantiation. We're not really checking anything in particular here, but under the
   * hood this will at least make a cursory check for consistency between the "stock"
   * Transliterator, and any potential "optimized" version with externalized unicode normalization.
   */
  @Nightly
  public void testNormalizationOptimizationOnAvailableIDs() throws Exception {
    Enumeration<String> ids = Transliterator.getAvailableIDs();
    List<String> idsWithNestedNormalization = new ArrayList<>();
    while (ids.hasMoreElements()) {
      String id = ids.nextElement();
      @SuppressWarnings("unused")
      boolean hasNestedUnicodeNormalization =
          accumulateNestedUnicodeNormalization(id, idsWithNestedNormalization);
      try {
        // set `iterationsDefault=0` because in this test we simply want to ignore the non-optimized
        //  case.
        // set `iterationsOptimized=1` because `testRandomStrings` can be slow; we want this as a
        //  sanity check, but as a matter of course we should not need many iterations.
        @SuppressWarnings("unused")
        boolean optimized = testRandomStrings(id, 0, 1);

        // We can't really _do_ much with `hasNestedUnicodeNormalization` or `optimized`, in terms
        // of validation.
        // They are completely independent (i.e. not mutually exclusive). We leave these checks
        // stubbed out here because we're initially optimizing normalization that can be easily
        // detected at the top level, but we want some plumbing in place to be more transparent
        // about what we're optimizing and what we're not (and to some extent _why_).
        // TODO: We're aware that this is probably leaving "on the table" a bunch of potential
        //  optimization of the "_nested_ unicode normalization" case. It may be worth optimizing
        //  these nested cases (and addressing any additional complexity specific to that case)
        //  as a separate, follow-up issue.
      } catch (Throwable ex) {
        // wrap the exception so that we can report the offending `id`
        throw new RuntimeException("problem for id: " + id, ex);
      }
    }
  }

  public void testBespoke1() throws Exception {
    multiCheck("Latin-Hiragana", "pxo \u6722\u5473\uf9d5 qihwrblz ", "ぷくそ 朢味崙 きいうるぶるず ");
  }

  public void testBespoke2() throws Exception {
    // before `suffixBoundaryCorrect()` first-token endOffset for non-optimized was `2`, but
    // optimized was `5` (actually correct)
    // This was due to the fact that filtered "optimized" instances have certain boundaries "fed"
    // into them externally by the ICUBypassCharFilter, leading to better alignment of offset
    // adjustment boundaries. `suffixBoundaryCorrect()` was introduced to achieve better (and
    // consistent) behavior.
    multiCheck("Latn-Hang", "zudtq c", "숟틐 크");
  }

  public void testBespoke3() throws Exception {
    // this requires extra leading context
    multiCheck("Hang-Latn", "\uc980\ud3fb\uce39\ub46d\ubd5f\uc3e5\ud2e0 yxyn ds", "jyusspolhcheutdwogboechssolt-tuils yxyn ds");
  }

  public void testBespoke4() throws Exception {
    // output \u13a1 is the result of NFD(\u0117 => \u0065\u0307), TRANSLIT(\u0065 => \u13a1)
    // so in fact, "optimized" endOffset `12` is correct for token \u13a1, and `8` (from
    // "non-optimized") is incorrect. This is a perfect example of the improved offset
    // granularity achieved by separating CompositeTransliterators

    // Composite (stock) top-level Transliterator (und_FONIPA-chr) has `position.start`
    // advance blocked looking internally for combining diacritics, so does:
    //   2> "\u0117" => "\u0117" // rollback
    //   2> "\u0117 " => "\u13a1 " // rollback
    //   2> "\u0117 \u0006" => "\u13a1 "
    // Note that the presence of the final \u0006 defeats `suffixBoundaryCorrect()`

    // separated/optimized (und_FONIPA-chr/X_NO_NORM_IO) gets pre-decomposed input and
    // doesn't have to "black-box-NFC" its output, so does:
    //   2> "e" => "e" // rollback
    //   2> "e\u0307" => "\u13a1"
    //   2> " " => " " // rollback
    //   2> " \u0006" => " "

    try {
      multiCheck("und_FONIPA-chr", "agt \ufcc5 0\u177f\uee73\u0003\u0417\u0117 \u0006", "\u13a0\u13a9\u13d8  \u13a1 ");
      assertTrue("expected an exception to be thrown related to endOffsets",false);
    } catch (AssertionError er) {
      assertTrue("expected:<8> but was:<12>".equals(er.getMessage()));
    }
  }

  public void testBespoke5() throws Exception {
    // this requires extra leading context
    multiCheck("Latin-Gurmukhi",
        "\u076c'\u03b3\u0000  \ueb8d\uee65\u76de\u0013\uf095yE\u0734\ud8e3\ude6c\ud36c ",
        "\u076c\u03b3\u0000  \ueb8d\uee65\u76de\u0013\uf095\u0a2f\u0a47\u0734\ud8e3\ude6c\ud36c ");
  }

  private void multiCheck(String id, String text, String expected) throws Exception {
    // first, verify "gold standard" result (raw, non-incremental Transliterator)
    Transliterator t = Transliterator.getInstance(id);
    Replaceable in = new ReplaceableString(text);
    Transliterator.Position p = new Transliterator.Position(0, text.length(), 0);
    t.filteredTransliterate(in, p, false);
    assertEquals("found '" + escape(in.toString()) + "'", expected, in.toString());

    boolean optimized;
    optimized = checkToken(id, text, expected, null, true);
    assertFalse(optimized);
    optimized = checkToken(id, text, expected, null, false);
    assertTrue(optimized);

    AttributeFactory attributeFactory = BaseTokenStreamTestCase.newAttributeFactory();
    Analyzer a = getAnalyzer(id, true, null, attributeFactory);
    Analyzer b = getAnalyzer(id, false, null, attributeFactory);
    long seed = random().nextLong();
    AnalysisResult resA = checkAnalysisConsistency(new Random(seed), a, false, text);
    AnalysisResult resB = checkAnalysisConsistency(new Random(seed), b, false, text);
    assertEquals(resA, resB);
  }

  /**
   * It is possible that leading and trailing (or singleton) Transliterators might apply nested
   * Unicode normalization, thus acting in the capacity of a Normalizer, without qualifying as a
   * top-level Normalizer as currently defined in {@link
   * ICUTransformCharFilterFactory#unicodeNormalizationType(String)}. For now, simply detect these
   * cases (to facilitate more nuanced handling in the future, if necessary).
   *
   * @param topLevelId top level Transliterator parent id.
   * @param ids list to which the topLevelId will be added if nested unicode normalization is
   *     detected
   */
  private static boolean accumulateNestedUnicodeNormalization(String topLevelId, List<String> ids) {
    Transliterator levelOne = Transliterator.getInstance(topLevelId);
    Transliterator[] topLevelElements = levelOne.getElements();
    if (topLevelElements.length == 1 && levelOne == topLevelElements[0]) {
      // A leaf Transliterator; shortcircuit
      return false;
    }
    ArrayDeque<Transliterator> elements =
        new ArrayDeque<>(topLevelElements.length << 2); // oversize
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
    checkToken(id, input, expected, null, suppressUnicodeNormExternalization);
  }

  private boolean checkToken(
      String id, String input, String expected, String rules, boolean suppressUnicodeNormExternalization)
      throws IOException {
    return checkToken(
        id,
        ICUTransformCharFilter.DEFAULT_MAX_ROLLBACK_BUFFER_CAPACITY,
        false,
        input,
        expected,
        rules,
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
    checkToken(id, maxRollbackBufferCapacity, failOnRollbackBufferOverflow, input,
        expected, null, suppressUnicodeNormalizationExternalization);
  }
  /**
   * returns true if unicode norm externalization optimization has been applied
   */
  private boolean checkToken(
      String id,
      int maxRollbackBufferCapacity,
      boolean failOnRollbackBufferOverflow,
      String input,
      String expected,
      String rules,
      boolean suppressUnicodeNormalizationExternalization)
      throws IOException {
    boolean[] optimized = new boolean[]{false};
    checkToken(
        getTransliteratingFilter(
            optimized,
            id,
            new StringReader(input),
            rules,
            suppressUnicodeNormalizationExternalization,
            maxRollbackBufferCapacity,
            failOnRollbackBufferOverflow),
        expected);
    return optimized[0];
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
    return getAnalyzer(id, suppressExternalize, optimized, BaseTokenStreamTestCase.newAttributeFactory());
  }

  private static Analyzer getAnalyzer(String id, boolean suppressExternalize,
      boolean[] optimized, AttributeFactory attributeFactory) {
    if (optimized != null && !suppressExternalize) {
      getTransliteratingFilter(optimized, id, new StringReader("dummy"), suppressExternalize);
    }
    return new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(attributeFactory, MockTokenizer.WHITESPACE,
            false, MockTokenizer.DEFAULT_MAX_TOKEN_LENGTH);
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
  private boolean testRandomStrings(final String id, int iterationsDefault, int iterationsOptimized)
      throws Exception {
    final boolean firstSuppressExternalize = random().nextBoolean();
    boolean[] optimized = new boolean[] {false};
    AttributeFactory attributeFactory = BaseTokenStreamTestCase.newAttributeFactory();
    Analyzer a = getAnalyzer(id, firstSuppressExternalize, optimized, attributeFactory);
    Analyzer b = getAnalyzer(id, !firstSuppressExternalize, optimized, attributeFactory);
    final int iterations;
    if (optimized[0]) {
      iterations = iterationsOptimized;
    } else {
      iterations = iterationsDefault;
      // there's no optimization; it doesn't matter which analyzer we use.
      b.close();
      b = null;
    }
    assertTrue(
        "implicitly expected optimized=" + !optimized[0] + "; found optimized="
            + optimized[0] + "; requested iterations=" + iterations,
        iterations >= 0);
    // 20 is the default maxWordLength
    checkRandomData(random(), a, iterations * RANDOM_MULTIPLIER, 20);
    if (b != null) {
      long seed = random().nextLong();
      Random randomA = new Random(seed);
      Random randomB = new Random(seed);
      for (int i = iterations * RANDOM_MULTIPLIER; i >= 0; i--) {
        String text = TestUtil.randomAnalysisString(random(), 20, false);
        AnalysisResult resA = checkAnalysisConsistency(randomA, a, false, text);
        AnalysisResult resB = checkAnalysisConsistency(randomB, b, false, text);
        assertEquals(resA, resB);
      }
      b.close();
    }
    a.close();
    return optimized[0];
  }

  public void testParityWithFilter1() throws Exception {
    String id = "Hang-Latn";
    String text = "\u02b7\u02f2\u02da\u02ce\u02d7\u02c3\u02b7\u02dd mb";
    AttributeFactory attributeFactory = BaseTokenStreamTestCase.newAttributeFactory();
    Analyzer a = getAnalyzer(id, true, null, attributeFactory);
    Analyzer b = getAnalyzer(id, false, null, attributeFactory);
    long seed = random().nextLong();
    AnalysisResult resA = checkAnalysisConsistency(new Random(seed), a, false, text);
    AnalysisResult resB = checkAnalysisConsistency(new Random(seed), b, false, text);
    assertEquals(resA, resB);
  }

  public void testParityWithFilter2() throws Exception {
    String id = "si-si_Latn";
    String text = "\uf171\u0279\u00af \udbb5\udee0\ua991 zr \u1739\u1735\u1734\u1738\u173d";
    AttributeFactory attributeFactory = BaseTokenStreamTestCase.newAttributeFactory();
    Analyzer a = getAnalyzer(id, false, null, attributeFactory);
    Analyzer b = getAnalyzer(id, true, null, attributeFactory);
    long seed = random().nextLong();
    AnalysisResult resA = checkAnalysisConsistency(new Random(seed), a, false, text);
    AnalysisResult resB = checkAnalysisConsistency(new Random(seed), b, false, text);
    assertEquals(resA, resB);
  }

  /**
   * This test verifies that top-level filters are applied (or not!) properly to _all_ component
   * Transliterators. The three user-level input characters here are each handled differently
   * in illustrative ways:
   *  1. composed input "â" matches the top-level filter, is decomposed, its ascii "a" is mapped
   *     to ascii "i", and "i\u0302" (the decomposed product of composed input entirely matched
   *     by top-level filter) is composed into "î"
   *  2. for decomposed input "a\u0302", only ascii "a" matches the top-level filter; it is
   *     mapped, but is _not_ composed with input "\u0302", which did _not_ match the top-level
   *     filter
   *  3. decomposed input "i\u0302" is completely ignored (no part matches top-level filter)
   */
  public void testParityWithFilter3() throws Exception {
    final String id = "X_SIMPLE";
    final String rules = "::[âa]; ::NFD; a > i; ::NFC;";
    final String text = "âa\u0302i\u0302";
    final String expected = "îi\u0302i\u0302";

    // first, sanity-check against raw, non-incremental Transliterator
    Transliterator t = Transliterator.createFromRules(id, rules, Transliterator.FORWARD);
    Replaceable in = new ReplaceableString(text);
    Transliterator.Position p = new Transliterator.Position(0, text.length(), 0);
    t.filteredTransliterate(in, p, false);
    assertEquals(expected, in.toString());

    boolean optimized;
    optimized = checkToken(id, text, expected, rules, true);
    assertFalse(optimized);
    optimized = checkToken(id, text, expected, rules, false);
    assertTrue(optimized);
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
      boolean[] optimized,
      String id,
      Reader r,
      boolean suppressUnicodeNormalizationExternalization) {
    return getTransliteratingFilter(
        optimized,
        id,
        r,
        null,
        suppressUnicodeNormalizationExternalization,
        ICUTransformCharFilter.DEFAULT_MAX_ROLLBACK_BUFFER_CAPACITY,
        ICUTransformCharFilter.DEFAULT_FAIL_ON_ROLLBACK_BUFFER_OVERFLOW);
  }

  private static CharFilter getTransliteratingFilter(
      String id, Reader r, int maxRollbackBufferCapacity, boolean failOnRollbackBufferOverflow) {
    return getTransliteratingFilter(
        null,
        id,
        r,
        null,
        random().nextBoolean(),
        maxRollbackBufferCapacity,
        failOnRollbackBufferOverflow);
  }

  @SuppressWarnings("resource")
  private static CharFilter getTransliteratingFilter(
      boolean[] optimized,
      String id,
      Reader r,
      String rules,
      boolean suppressUnicodeNormalizationExternalization,
      int maxRollbackBufferCapacity,
      boolean failOnRollbackBufferOverflow) {
    Map<String, String> args = new HashMap<>();
    args.put("id", id);
    args.put(MAX_ROLLBACK_BUFFER_CAPACITY_ARGNAME, Integer.toString(maxRollbackBufferCapacity));
    args.put(
        FAIL_ON_ROLLBACK_BUFFER_OVERFLOW_ARGNAME, Boolean.toString(failOnRollbackBufferOverflow));
    ICUTransformCharFilterFactory factory =
        new ICUTransformCharFilterFactory(args, suppressUnicodeNormalizationExternalization, rules);
    if (optimized != null) {
      optimized[0] |= factory.externalizedUnicodeNormalization();
    }
    return (CharFilter) factory.create(r);
  }
}
