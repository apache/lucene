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

import com.ibm.icu.text.Replaceable;
import com.ibm.icu.text.ReplaceableString;
import com.ibm.icu.text.Transliterator;
import com.ibm.icu.text.UnicodeSet;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CharFilter;
import org.apache.lucene.analysis.CharFilterFactory;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.AttributeFactory;
import org.apache.lucene.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ComparisonFailure;

/** Test the ICUTransform2CharFilter with some basic examples. */
public class TestICUTransform2CharFilter extends BaseTokenStreamTestCase {

  public void testBasicFunctionality() throws Exception {
    System.err.println("BEGIN testBasicFunctionality");
    checkToken2("Traditional-Simplified", "簡化字", "简化字");
    checkToken2("Katakana-Hiragana", "ヒラガナ", "ひらがな");
    checkToken2("Fullwidth-Halfwidth", "アルアノリウ", "ｱﾙｱﾉﾘｳ");
    // TODO: the AnyTranslits need to have NFC internally post-applied!
    // TODO: at least for AnyTranslit, maxContextLength is not propagated! Introduce this.
    checkToken2("Any-Latin; NFC", "Αλφαβητικός Κατάλογος", "Alphabētikós Katálogos");
    checkToken2(
        "NFD; [:Nonspacing Mark:] Remove", "Alphabētikós Katálogos", "Alphabetikos Katalogos");
    checkToken2("Han-Latin", "中国", "zhōng guó");
    checkToken2("Cyrillic-Latin", "яяяяяяяяяяяяяяяяяяяяяяяяя", "âââââââââââââââââââââââââ");
  }

  private void checkToken2(String id, String input, String expected) throws Exception {
    Transliterator t = Transliterator.getInstance(id);
    checkToken2(t, new StringReader(input), expected);
  }

  private static final boolean INSPECT_OFFSET_DISCREPANCIES = false;
  /**
   * Sanity check all top-level prepackaged Transliterators to make sure that no trivial errors are
   * thrown on instantiation. We're not really checking anything in particular here, but under the
   * hood this will at least make a cursory check for consistency between the "stock"
   * Transliterator, and any potential "optimized" version with externalized unicode normalization.
   */
  public void testNormalizationOptimizationOnAvailableIDs() throws Exception {
    System.err.println("BEGIN testNormalizationOptimizationOnAvailableIDs");
    Enumeration<String> ids = Transliterator.getAvailableIDs();
    final int iterations = 1;
    List<Map.Entry<String, List<Map.Entry<String, String>>>> problems =
        INSPECT_OFFSET_DISCREPANCIES ? new ArrayList<>() : null;
    List<Map.Entry<String, String>> offsetDiscrepancies = new ArrayList<>(iterations);
    while (ids.hasMoreElements()) {
      String id = ids.nextElement();
      if ("AnyTransliterator".equals(Transliterator.getInstance(id).getClass().getSimpleName())) {
        // TODO: add support for AnyTransliterators!
        continue;
      }
      try {
        // we'll handle offset discrepancies ourselves
        testRandomStrings(id, iterations, offsetDiscrepancies);
        if (!offsetDiscrepancies.isEmpty()) {
          if (!INSPECT_OFFSET_DISCREPANCIES) {
            offsetDiscrepancies.clear();
          } else {
            problems.add(new AbstractMap.SimpleImmutableEntry<>(id, offsetDiscrepancies));
            offsetDiscrepancies = new ArrayList<>(iterations);
          }
        }
      } catch (Throwable ex) {
        // print the offending `id` before re-throwing
        String msg = ex.getMessage();
        if (msg != null
            && msg.contains("is less than the last recorded offset")
            && ICUTransformCharFilter.class
                .getCanonicalName()
                .equals(ex.getStackTrace()[1].getClassName())) {
          System.err.println("ignoring offset bug in legacy reference implementation");
        } else {
          System.err.println("unignored error for id \"" + id + "\"");
          throw ex;
        }
      }
    }
    if (INSPECT_OFFSET_DISCREPANCIES && !problems.isEmpty()) {
      System.err.println("offset discrepancies for `getAvailableIDs`:");
      for (Map.Entry<String, List<Map.Entry<String, String>>> e : problems) {
        checkOffsetDiscrepancies(e.getValue(), "id: " + e.getKey(), false);
      }
      if (FAIL_ON_OFFSET_DISCREPANCIES) {
        assertTrue(FOUND_OFFSET_DISCREPANCIES, false);
      }
    }
  }

  private static final boolean FAIL_ON_OFFSET_DISCREPANCIES = false;

  public void testBespoke1() throws Exception {
    System.err.println("BEGIN testBespoke1");
    multiCheck("Latin-Hiragana", "pxo \u6722\u5473\uf9d5 qihwrblz ", "ぷくそ 朢味崙 きいうるぶるず ");
  }

  public void testBespoke2() throws Exception {
    System.err.println("BEGIN testBespoke2");
    // before `suffixBoundaryCorrect()` first-token endOffset for non-optimized was `2`, but
    // optimized was `5` (actually correct)
    // This was due to the fact that filtered "optimized" instances have certain boundaries "fed"
    // into them externally by the ICUBypassCharFilter, leading to better alignment of offset
    // adjustment boundaries. `suffixBoundaryCorrect()` was introduced to achieve better (and
    // consistent) behavior.
    multiCheck("Latn-Hang", "zudtq c", "숟틐 크");
  }

  public void testBespoke3() throws Exception {
    System.err.println("BEGIN testBespoke3");
    // this requires extra leading context
    multiCheck(
        "Hang-Latn",
        "\uc980\ud3fb\uce39\ub46d\ubd5f\uc3e5\ud2e0 yxyn ds",
        "jyusspolhcheutdwogboechssolt-tuils yxyn ds");
  }

  public void testBespoke4() throws Exception {
    System.err.println("BEGIN testBespoke4");
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

    // decomposed Transliterator with integrated offset tracking in `CircularReplaceable`
    // does even better. Optimized (above) gets the right _endOffset_, but still has a
    // misleading startOffset because it only has an external "black box" view of the
    // _internal_ Transliterators.

    try {
      multiCheck(
          "und_FONIPA-chr",
          "agt \ufcc5 0\u177f\uee73\u0003\u0417\u0117 \u0006",
          "\u13a0\u13a9\u13d8  \u13a1 ");
      assertTrue("expected an exception to be thrown related to startOffsets", false);
    } catch (AssertionError er) {
      String expected =
          "offsetDiscrepancyStart input='agt \\ufcc5 0\\u177f\\uee73\\u0003\\u0417\\u0117 \\u0006'\n"
              + "\tt='\\u13a0\\u13a9\\u13d8'; expected=[0, 3]; actual=[0, 3]\n"
              + "\tt='\\u13a1'; expected=[7, 8]; actual=[11, 12] expected:<[0, 7]> but was:<[0, 11]>";
      assertEquals(expected, er.getMessage());
    }
  }

  public void testBespoke5() throws Exception {
    System.err.println("BEGIN testBespoke5");
    // this requires extra leading context
    multiCheck(
        "Latin-Gurmukhi",
        "\u076c'\u03b3\u0000  \ueb8d\uee65\u76de\u0013\uf095yE\u0734\ud8e3\ude6c\ud36c ",
        "\u076c\u03b3\u0000  \ueb8d\uee65\u76de\u0013\uf095\u0a2f\u0a47\u0734\ud8e3\ude6c\ud36c ");
  }

  public void testBespoke6() throws Exception {
    System.err.println("BEGIN testBespoke6");
    multiCheck(
        "Katakana-Hiragana",
        "r \udb40\udd9d \u2c76\u2c6f\u2c71\u2c71\u2c68\u2c7f\u2c74 oyt",
        "r \udb40\udd9d \u2c76\u2c6f\u2c71\u2c71\u2c68\u2c7f\u2c74 oyt");
  }

  public void testBespoke7() throws Exception {
    System.err.println("BEGIN testBespoke7");
    // check that final headDiff is accurately reported in offset correction
    multiCheck("Latin-Katakana", "vbco", "ヴブコ");
  }

  public void testBespoke8() throws Exception {
    System.err.println(
        "BEGIN testBespoke8 \"\u7417\u5dc0\u02acx\uda5e\udc53\ue95e \ud834\ude08 \"");
    Analyzer a =
        getAnalyzer("Katakana-Hiragana", false, TokenStream.DEFAULT_TOKEN_ATTRIBUTE_FACTORY);
    a.normalize("dummy", "\u7417\u5dc0\u02acx\uda5e\udc53\ue95e \ud834\ude08 ");
    // no check here; just make sure no error thrown on `normalize`
  }

  public void testBespoke9() throws Exception {
    System.err.println("BEGIN testBespoke9");
    multiCheck("Latin-Katakana", "dswxkvdj  \u62aa", "デスウクスクヴヂ  抪");
  }

  public void testBespoke10() throws Exception {
    System.err.println("BEGIN testBespoke10");
    // print(Transliterator.getInstance("cy-cy_FONIPA"), "");
    multiCheck("cy-cy_FONIPA", "vhguj", "vhˈɡɨ̞d͡ʒ");
    multiCheck(
        "cy-cy_FONIPA",
        "\u20f8\u20df\u20fd\u20f6\u20fe\u20e3 otfr \ufb9a vhguj",
        "⃸⃟⃽⃶⃾⃣ ɔtvr ﮚ vhɡɨ̞d͡ʒ");
  }

  public void testBespoke11() throws Exception {
    System.err.println("BEGIN testBespoke11");
    // This one is a problem. The space is removed by t[4] before t[3] converts the first
    // 'g', so t[3] doesn't have the space as context to know to simply _remove_ the g, and
    // t[3] _replaces_ it instead (which is wrong).
    // I think the fix here is to block advancing t idx beyond upstream transliterator's
    // _context_; we already block advancing t idx beyond upstream transliterator's
    // `committedTo`, so this shouldn't too challenging.
    // print(Transliterator.getInstance("es-zh"), "");
    String input = "f gnfg";
    input = "f gnfg";
    Replaceable r = new ReplaceableString(input);
    Transliterator.getInstance("es-es_FONIPA")
        .finishTransliteration(r, new Transliterator.Position(0, r.length(), 0));
    // System.err.println("XXX1 '"+r.toString()+"'");
    Transliterator.getInstance("es_FONIPA-zh")
        .finishTransliteration(r, new Transliterator.Position(0, r.length(), 0));
    // System.err.println("XXX2 '"+r.toString()+"'");
    multiCheck("es-zh", input, "弗恩弗格"); // 弗格恩弗格
  }

  public void testBespoke12() throws Exception {
    System.err.println("BEGIN testBespoke12");
    // we break up `multiCheck` here because we want to demonstrate with more granularity how
    // the "blame" for the discrepancy lies with the non-decomposed "reference" impl

    String id = "Hani-Latn";
    String text = "vbbujqy \ue71c\u9af1\u014f ";
    String expected = "vbbujqy bào ŏ ";

    // first, verify "gold standard" result (raw, non-incremental Transliterator)
    Transliterator t = Transliterator.getInstance(id);
    Replaceable in = new ReplaceableString(text);
    Transliterator.Position p = new Transliterator.Position(0, text.length(), 0);
    t.filteredTransliterate(in, p, false);
    assertEquals("found '" + escape(in.toString()) + "'", expected, in.toString());

    try {
      checkToken(id, text, expected, null, true);
      assertTrue("we expected the \"reference\" impl to throw an exception", false);
    } catch (AssertionError er) {
      // it might seem silly to quote the expected error message in full here, but we do it
      // to be transparent about documenting the nature of the inconsistency
      String expectErrorMsg = "term 0 expected:<vbbujqy bào[ ]ŏ > but was:<vbbujqy bào[]ŏ >";
      assertEquals(expectErrorMsg, er.getMessage());
    }
    checkToken(id, text, expected, null, false);

    // there's no point to checking the non-decomposed "reference" impl here, since we've already
    // established that its behavior is suspect
    AttributeFactory attributeFactory = BaseTokenStreamTestCase.newAttributeFactory();
    Analyzer b = getAnalyzer(id, false, attributeFactory);
    long seed = random().nextLong();
    checkAnalysisConsistency(new Random(seed), b, false, text);
  }

  public void testBespoke13() throws Exception {
    System.err.println("BEGIN testBespoke13");
    // print(Transliterator.getInstance("eo-chr"), "");
    try {
      multiCheck("eo-chr", "\ucec6A\udae9\udc82\u0362\u0170 <!--< \u0a53", "ᎠᎤ̋  ");
    } catch (AssertionError er) {
      // this example throws a spurious "offset discrepancy" error; ignore it.
      String msg = er.getMessage();
      if (msg == null || !msg.startsWith(OFFSET_DISCREPANCY_MSG_PREFIX)) {
        throw er;
      }
    }
  }

  public void testBespoke14() throws Exception {
    System.err.println("BEGIN testBespoke14");
    // print(Transliterator.getInstance("Latn-Hebr"), "");
    multiCheck(
        "Latn-Hebr",
        "lj N\u7b04\u0010\u0518\ueb99\ufdf0\u808d",
        "\u05dc\u05d6 \u05e0\u7b04\u0010\u0518\ueb99\ufdf0\u808d");
  }

  public void testBespoke14_2() throws Exception {
    System.err.println("BEGIN testBespoke14_2");
    // print(Transliterator.getInstance("Latn-Hebr"), "");
    multiCheck(
        "Latn-Hebr",
        "qqfzrsksg \u00ea\ueeb5\u0a51\u0145\u0767\u0114",
        "\u05e7\u05e7\u05e4\u05d6\u05e8\u05e1\u05db\u05e1\u05d2 \u05b6\u05c2\ueeb5\u0a51\u05e0\u0327\u0767\u05b0");
  }

  public void testBespoke15() throws Exception {
    System.err.println("BEGIN testBespoke15");
    // fails for maxContextLength=4; bump ICUTransform2CharFilterFactory.MAX_CONTEXT_LENGTH_FLOOR to
    // `5`
    // reducing to `3` also works, but this appears to be a different issue than `testBespoke16`
    multiCheck("cy-cy_FONIPA", "\u2407 JWOb", "\u2407 d\u0361\u0292w\u0254b");
  }

  public void testBespoke16() throws Exception {
    System.err.println("BEGIN testBespoke16");
    // don't split surrogate pairs at preContext boundary.
    // (this was causing an infinite loop in ICU code when pre-context of length 3 or 5
    // was passed in!)
    String input = "dcxnwpr \udb40\udd11\udb40\udd34\udb40\udde7\udb40\uddab\udb40\udda3";
    multiCheck(
        "Han-Latin/Names",
        input,
        "dcxnwpr \udb40\udd11\udb40\udd34\udb40\udde7\udb40\uddab\udb40\udda3");
  }

  public void testBespoke17() throws Exception {
    System.err.println("BEGIN testBespoke17");
    // fails for maxContextLength `2` and `5`
    // bump ICUTransform2CharFilterFactory.MAX_CONTEXT_LENGTH_FLOOR to `6`
    // this is a very similar issue to `testBespoke15`, but don't really understand
    // what's going on here
    multiCheck("cy-cy_FONIPA", "n\u3013IN\\ jmbinc", "n\u3013\u026an d\u0361\u0292mb\u026ank");
  }

  public void testBespoke18() throws Exception {
    System.err.println("BEGIN testBespoke18");
    try {
      multiCheck(
          "ch-chr",
          "\u2321 \ud860\udecc gtyey zmibo",
          "  \u13a9\u13d8\u13e4\u13e5 \u13cd\u13bb\u13c9");
    } catch (AssertionError er) {
      // this example throws a spurious "offset discrepancy" error; ignore it.
      String msg = er.getMessage();
      if (msg == null || !msg.startsWith(OFFSET_DISCREPANCY_MSG_PREFIX)) {
        throw er;
      }
    }
  }

  public void testBespoke19() throws Exception {
    System.err.println("BEGIN testBespoke19");
    // first pass assumes "complex" RuleBased translit, involving lots of copying around within the
    // Replaceable;
    // subsequent passes use straight "replace". Per the spec for Replaceable API, we trim common
    // prefix and
    // suffix from replacement and replacee ... but only for `replace` ... `copy` does no such extra
    // work.
    // This should be reconciled one way or another -- either make `copy` trim common prefixes and
    // suffixes,
    // or cause `replace` to _stop_ doing so. Initially resolved by disabling trim on replace
    // (easy). Yet to
    // be determined whether that's the "right" decision. It could make sense to do trimming of
    // prefix and
    // suffix separately, or conditionally?
    String id = "es_419-chr";
    String input = "gnanmtzkob quxtsh";
    // expected doesn't really figure in this problem
    @SuppressWarnings("unused")
    // TODO: what is this?
    String expected = "\u13be\u13c2\u13bb\u13d8\u13cd\u13aa\u13eb\u13ab\u13cd\u13d8\u13cd";
    Analyzer b = getAnalyzer(id, false, BaseTokenStreamTestCase.newAttributeFactory());
    long seed = random().nextLong();
    final boolean consistentTrimBehavior = true;
    try {
      checkAnalysisConsistency(new Random(seed), b, false, input);
      assertTrue("an exception should be thrown the first time", consistentTrimBehavior);
    } catch (AssertionError er) {
      assertFalse(consistentTrimBehavior);
      assertEquals("startOffset 0 term=ᎾᏂᎻᏘᏍᎪᏫᎫᏍᏘᏍ expected:<0> but was:<1>", er.getMessage());
      // NOTE: we need a new analyzer b/c close() not called on TokenStreamComponents from last one
      b = getAnalyzer(id, false, BaseTokenStreamTestCase.newAttributeFactory());
    }
    // no exception should be thrown the second time, regardless of trim behavior consistency
    checkAnalysisConsistency(new Random(seed), b, false, input);
  }

  public void testBespoke20() throws Exception {
    System.err.println("BEGIN testBespoke20");
    // bump ICUTransform2CharFilterFactory.MAX_CONTEXT_LENGTH_FLOOR to `7` (fails for `6`)
    // don't really understand what's going on here
    multiCheck("cy-cy_FONIPA", "\uaa63\uaa6d jbfbu", "\uaa63\uaa6d d\u0361\u0292bvb\u0268\u031e");
  }

  public void testBespoke21() throws Exception {
    System.err.println("BEGIN testBespoke21");
    String id = "Hiragana-Latin";
    String text =
        " \u4dfe\u0545\u0005\uda12\udd2e\u034f\udb80\udd50 \u30fe\u30e9\u30fc\u30ad"
            + "\u30e5\u30cd\u30b9\u30e2\u30e8 zwwpmct rqgshqijwurx ";
    String expected =
        " \u4dfe\u0545\u0005\uda12\udd2e\u034f\udb80\udd50 \udd50 \u30e9\u0304\u30ad"
            + "\u30e5\u30cd\u30b9\u30e2\u30e8 zwwpmct rqgshqijwurx ";
    // first, verify "gold standard" result (raw, non-incremental Transliterator)
    Transliterator t = Transliterator.getInstance(id);
    Replaceable in = new ReplaceableString(text);
    Transliterator.Position p = new Transliterator.Position(0, text.length(), 0);
    t.filteredTransliterate(in, p, false);
    assertEquals("found '" + escape(in.toString()) + "'", escape(expected), escape(in.toString()));

    checkToken(id, text, expected, null, true);

    CircularReplaceable.FIX_INTRODUCED_UNPAIRED_SURROGATES = random().nextBoolean();
    try {
      if (CircularReplaceable.FIX_INTRODUCED_UNPAIRED_SURROGATES) {
        expected = expected.replace(" \udd50 ", " \ufffd ");
      }
      checkToken(id, text, expected, null, false);

      AttributeFactory attributeFactory = BaseTokenStreamTestCase.newAttributeFactory();
      Analyzer b = getAnalyzer(id, false, attributeFactory);
      try {
        checkAnalysisConsistency(random(), b, false, text);
        assertTrue(CircularReplaceable.FIX_INTRODUCED_UNPAIRED_SURROGATES);
      } catch (IllegalStateException ex) {
        assertFalse(CircularReplaceable.FIX_INTRODUCED_UNPAIRED_SURROGATES);
        assertEquals("unpaired low surrogate: dd50", ex.getMessage());
      }
    } finally {
      CircularReplaceable.FIX_INTRODUCED_UNPAIRED_SURROGATES = true;
    }
  }

  public void testBespoke22() throws Exception {
    System.err.println("BEGIN testBespoke22");
    // This test provides a window into a bug where ICU BreakTransliterator can send
    // `position.start`
    // back to zero for runs containing no boundaries. ICUTransform2CharFilter should indeed detect
    // and correct this situation; but it consequently conflicts with the (incorrect)
    // transliteration
    // from ICUTransformCharFilter (and even bare/raw Transliterator), which have no such
    // mitigation.

    // This test is not perfect, but it affords us a baseline assertion that crosschecks
    // fail _differently_ depending on how the BreakTransliterator bug is handled. If the bug
    // is fixed upstream, we would expect this test to "fail" by not throwing the expected
    // exceptions, in which case this test (and the detection/mitigation in
    // ICUTransform2CharFilter!) should simply be removed.
    final int restore = ICUTransform2CharFilter.BATCH_BUFFER_SIZE;
    ICUTransform2CharFilter.BATCH_BUFFER_SIZE = 0;
    try {
      multiCheck("Thai-Latin", "\u0e49\u0e1a\u0e45\u0e21", "\u0302 b\u0268m");
      assertTrue("should throw ComparisonFailure", false);
    } catch (ComparisonFailure f) {
      ICUTransform2CharFilter.BATCH_BUFFER_SIZE = restore;
      assertEquals("term 0 expected:<̂ bɨ[]m> but was:<̂ bɨ[ ]m>", f.getMessage());
    } finally {
      ICUTransform2CharFilter.BATCH_BUFFER_SIZE = restore;
    }
    ICUTransform2CharFilter.BATCH_BUFFER_SIZE = 0;
    ICUTransform2CharFilter.MITIGATE_BREAK_TRANSLITERATOR_POSITION_BUG =
        false; // temporarily disable fixing
    try {
      multiCheck("Thai-Latin", "\u0e49\u0e1a\u0e45\u0e21", "\u0302 b\u0268m");
      assertTrue("should throw AssertionError", false);
    } catch (AssertionError er) {
      ICUTransform2CharFilter.BATCH_BUFFER_SIZE = restore;
      assertEquals(
          "offsetDiscrepancyEnd input='\\u0e49\\u0e1a\\u0e45\\u0e21'\n"
              + "\tt='\\u0302'; expected=[0, 0]; actual=[0, 1]\n"
              + "\tt='b\\u0268m'; expected=[1, 4]; actual=[1, 4] expected:<[0, 4]> but was:<[1, 4]>",
          er.getMessage());
    } finally {
      ICUTransform2CharFilter.BATCH_BUFFER_SIZE = restore;
      ICUTransform2CharFilter.MITIGATE_BREAK_TRANSLITERATOR_POSITION_BUG =
          true; // restore default value
    }
  }

  public void testBespoke23() throws Exception {
    System.err.println("BEGIN testBespoke23");
    multiCheck(
        "Hex-Any",
        "illegal codepoint &#xdb71f7; test",
        "Illegal codepoint",
        IllegalArgumentException.class);
    multiCheck("Hex-Any", "illegal codepoint &#xfffc; test", "illegal codepoint \ufffc test");
  }

  private void multiCheck(String id, String text, String expected) throws Exception {
    multiCheck(id, text, expected, null);
  }

  private void multiCheck(
      String id, String text, String expected, Class<? extends Throwable> expectThrows)
      throws Exception {
    // first, verify "gold standard" result (raw, non-incremental Transliterator)
    Transliterator t = Transliterator.getInstance(id);
    Replaceable in = new ReplaceableString(text);
    Transliterator.Position p = new Transliterator.Position(0, text.length(), 0);
    try {
      t.filteredTransliterate(in, p, false);
      assertNull(expectThrows);
      assertEquals("found '" + escape(in.toString()) + "'", expected, in.toString());
    } catch (Throwable ex) {
      if (expectThrows == null) {
        throw ex;
      }
      assertEquals(expectThrows, ex.getClass());
      assertEquals(expected, ex.getMessage());
    }

    try {
      checkToken(id, text, expected, null, false);
      assertNull(expectThrows);
    } catch (IllegalArgumentException ex) {
      if (expectThrows == null) {
        throw ex;
      }
      assertEquals(expectThrows, ex.getClass());
      assertEquals(expected, ex.getMessage());
      return; // we're satisfied this throws an error; don't bother with the rest
    }
    checkToken(id, text, expected, null, true);

    AttributeFactory attributeFactory = BaseTokenStreamTestCase.newAttributeFactory();
    Analyzer a = getAnalyzer(id, true, attributeFactory);
    Analyzer b = getAnalyzer(id, false, attributeFactory);
    long seed = random().nextLong();
    AnalysisResult resA = checkAnalysisConsistency(new Random(seed), a, false, text);
    AnalysisResult resB = checkAnalysisConsistency(new Random(seed), b, false, text);
    assertEquals(resA, resB, false);
  }

  public void testRtx() throws Exception {
    System.err.println("BEGIN testRtx");
    Transliterator t =
        Transliterator.createFromRules(
            "X_ROUND_TRIP", "a > bc; ::Null; bc > a;", Transliterator.FORWARD);
    String in = "a a a a a ";
    @SuppressWarnings("unused")
    // TODO: what is this
    int[] expectedOffsets = new int[0];
    checkToken2(t, new StringReader(in), in);
  }

  public void testExpand() throws Exception {
    System.err.println("BEGIN testExpand");
    Transliterator t =
        Transliterator.createFromRules("X_EXPAND", "a > bc;", Transliterator.FORWARD);
    String in = "a a a a a ";
    String expected = "bc bc bc bc bc ";
    @SuppressWarnings("unused")
    // TODO: what is this
    int[] expectedOffsets = new int[] {1, -1, 4, -2, 7, -3, 10, -4, 13, -5};
    checkToken2(t, new StringReader(in), expected);
  }

  public void testCustomFunctionality() throws Exception {
    System.err.println("BEGIN testCustomFunctionality");
    String rules = "a > b; b > c;"; // convert a's to b's and b's to c's
    checkToken2(
        Transliterator.createFromRules("test", rules, Transliterator.FORWARD),
        new StringReader("abacadaba"),
        "bcbcbdbcb");
  }

  public void testCustomFunctionality2() throws Exception {
    System.err.println("BEGIN testCustomFunctionality2");
    String rules = "c { a > b; a > d;"; // convert a's to b's and b's to c's
    checkToken(
        Transliterator.createFromRules("test", rules, Transliterator.FORWARD),
        new StringReader("caa"),
        "cbd");
  }

  public void testCustomBypass() throws Exception {
    System.err.println("BEGIN testCustomBypass");
    Transliterator inner =
        Transliterator.createFromRules("x_inner", "a > b; ::Null; b > c;", Transliterator.FORWARD);
    Transliterator.registerInstance(inner);
    String rules = "::Null; ::[a] x_inner; b > d"; // convert a's to b's and b's to c's
    Transliterator t = Transliterator.createFromRules("test", rules, Transliterator.FORWARD);
    // print(t, "");
    Replaceable r = new ReplaceableString("abab");
    t.finishTransliteration(r, new Transliterator.Position(0, r.length(), 0));
    assertEquals("cdcd", r.toString());
    checkToken2(t, new StringReader("abab"), "cdcd");
  }

  @SuppressWarnings("unused")
  // TODO: what is this?
  private void print(Transliterator t, String indent) {
    System.err.println(
        indent
            + t.getID()
            + ", "
            + t.getClass().getSimpleName()
            + ", f="
            + t.getFilter()
            + ", mcl="
            + t.getMaximumContextLength());
    Transliterator[] e = t.getElements();
    if (e.length == 1 && e[0] == t) {
      return;
    }
    indent = "\t".concat(indent);
    for (Transliterator sub : e) {
      print(sub, indent);
    }
  }

  public void testOptimizer() throws Exception {
    System.err.println("BEGIN testOptimizer");
    String rules = "a > b; b > c;"; // convert a's to b's and b's to c's
    Transliterator custom = Transliterator.createFromRules("test", rules, Transliterator.FORWARD);
    assertTrue(custom.getFilter() == null);
    new ICUTransformCharFilter(new StringReader(""), custom);
    assertTrue(custom.getFilter().equals(new UnicodeSet("[ab]")));
  }

  public void testOptimizer2() throws Exception {
    System.err.println("BEGIN testOptimizer2");
    checkToken("Traditional-Simplified; CaseFold", "ABCDE", "abcde");
  }

  public void testOptimizerSurrogate() throws Exception {
    System.err.println("BEGIN testOptimizerSurrogate");
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

  private void checkToken2(Transliterator transliterator, Reader input, String expected)
      throws IOException {
    input = ICUTransform2CharFilterFactory.wrap(input, transliterator);
    final KeywordTokenizer input1 = new KeywordTokenizer();
    input1.setReader(input);
    assertTokenStreamContents(input1, new String[] {expected});
  }

  @SuppressWarnings("unused")
  // TODO: what is this?
  private void checkToken(
      String id, String input, String expected, boolean suppressUnicodeNormExternalization)
      throws IOException {
    checkToken(id, input, expected, null, suppressUnicodeNormExternalization);
  }

  private void checkToken(String id, String input, String expected) throws IOException {
    checkToken(getTransliteratingFilter(id, new StringReader(input)), expected);
  }

  /** returns true if unicode norm externalization optimization has been applied */
  private void checkToken(
      String id,
      String input,
      String expected,
      String rules,
      boolean suppressUnicodeNormalizationExternalization)
      throws IOException {
    checkToken(
        getTransliteratingFilter(
            id, new StringReader(input), rules, suppressUnicodeNormalizationExternalization),
        expected);
  }

  private void checkToken(CharFilter input, String expected) throws IOException {
    final KeywordTokenizer input1 = new KeywordTokenizer();
    input1.setReader(input);
    assertTokenStreamContents(input1, new String[] {expected});
  }

  public void testRandomStringsLatinToKatakana() throws Exception {
    System.err.println("BEGIN testRandomStringsLatinToKatakana");
    // this Transliterator often decreases character length wrt input
    // we _don't_ expect unicode norm externalization optimization in practice
    List<Map.Entry<String, String>> offsetDiscrepancies = new ArrayList<>();
    testRandomStrings("Latin-Katakana", 1000, offsetDiscrepancies);
    checkOffsetDiscrepancies(offsetDiscrepancies, "offset discrepancies for Latin-Katakana", false);
  }

  // NOTE: this is temporarily disabled pending support for `AnyTransliterator`
  @Weekly
  public void testRandomStringsAnyToLatin() throws Exception {
    System.err.println("BEGIN testRandomStringsAnyToLatin");
    // this Transliterator often increases character length wrt input
    // we _don't_ expect unicode norm externalization optimization in practice
    testRandomStrings("Any-Latin", 1000, null);
  }

  public void testRandomStringsKatakanaToHiragana() throws Exception {
    System.err.println("BEGIN testRandomStringsKatakanaToHiragana");
    // this Transliterator often increases character length wrt input
    // we _do_ expect unicode norm externalization optimization in practice
    List<Map.Entry<String, String>> offsetDiscrepancies = new ArrayList<>();
    testRandomStrings("Katakana-Hiragana", 1000, offsetDiscrepancies);
    checkOffsetDiscrepancies(
        offsetDiscrepancies, "offset discrepancies for Katakana-Hiragana", true);
  }

  public void testRandomStringsThaiToLatin() throws Exception {
    System.err.println("BEGIN testRandomStringsThaiToLatin");
    // this internally uses BreakTransliterator, which, esp. when operating on small
    // incremental windows, has a bug that sends `position.start` back to `0`
    // To prevent spurious errors that would result from this bug (see `testBespoke22`
    // for a specific example), `testRandomStrings(...)` goes out of its way to check
    // for this case and re-try with the BreakIterator bug mitigation disabled
    List<Map.Entry<String, String>> offsetDiscrepancies = new ArrayList<>();
    testRandomStrings("Thai-Latin", 1000, offsetDiscrepancies);
    checkOffsetDiscrepancies(offsetDiscrepancies, "offset discrepancies for Thai-Latin", false);
  }

  @Nightly
  public void testPerfDecomposed() throws Exception {
    final int restore = ICUTransform2CharFilter.BATCH_BUFFER_SIZE;
    ICUTransform2CharFilter.BATCH_BUFFER_SIZE = ICUTransform2CharFilter.DEFAULT_BATCH_BUFFER_SIZE;
    try {
      perfTestLoop(Type.DECOMPOSED);
    } finally {
      ICUTransform2CharFilter.BATCH_BUFFER_SIZE = restore;
    }
  }

  @Nightly
  public void testPerfRollback() throws Exception {
    perfTestLoop(Type.ROLLBACK);
  }

  @Nightly
  public void testPerfTokenFilter() throws Exception {
    perfTestLoop(Type.TOKEN_FILTER);
  }

  @Nightly
  public void testPerfBareTranslit() throws Exception {
    perfTestLoop(Type.BARE_TRANSLIT);
  }

  private void perfTestLoop(Type type) throws Exception {
    Random loopRandom = new Random(testPerfSeed);
    long seed = loopRandom.nextLong();
    long start = System.currentTimeMillis();
    for (int i = 0; i < PERF_TEST_LOOP_ITERATIONS; i++) {
      final long runSeed = seed;
      naivePerformanceTest(new Random(runSeed), type);
      final long post = System.currentTimeMillis();
      System.err.println(
          type + " round " + i + " (" + (post - start) + "ms) -- seed=" + Long.toHexString(seed));
      start = post;
      if (!PERF_TEST_SAME_SEED) {
        seed = loopRandom.nextLong();
      }
    }
  }

  private static final boolean PERF_TEST_SAME_SEED = false;
  private static final int PERF_TEST_ITERATIONS = 20;
  private static final int PERF_TEST_MAX_WORD_LENGTH = 1000;
  private static int PERF_TEST_LOOP_ITERATIONS = 5;

  private void naivePerformanceTest(Random textGenRandom, Type type) throws Exception {
    Enumeration<String> ids = Transliterator.getAvailableIDs();
    int size = 0;
    while (ids.hasMoreElements()) {
      String id = ids.nextElement();
      if (!"AnyTransliterator".equals(Transliterator.getInstance(id).getClass().getSimpleName())) {
        try {
          size +=
              naivePerformanceTest(
                  textGenRandom, id, type, PERF_TEST_ITERATIONS, PERF_TEST_MAX_WORD_LENGTH);
        } catch (AssertionError er) {
          if (type == Type.ROLLBACK) {
            // swallow this; we know there are occasional issues here
          } else {
            throw er;
          }
        }
      }
    }
    assertTrue(size > 0);
  }

  private enum Type {
    DECOMPOSED,
    ROLLBACK,
    TOKEN_FILTER,
    BARE_TRANSLIT
  }

  private int naivePerformanceTest(
      Random textGenRandom, String id, Type type, int iterations, int maxWordLength)
      throws Exception {
    Transliterator t = null;
    Analyzer a;
    switch (type) {
      case DECOMPOSED:
        t = Transliterator.getInstance(id);
        a = analyzerFromCharFilterFactory(new ICUTransform2CharFilterFactory(t));
        break;
      case ROLLBACK:
        Map<String, String> args = new HashMap<>();
        args.put("id", id);
        CharFilterFactory f = new ICUTransformCharFilterFactory(args, true, null);
        a = analyzerFromCharFilterFactory(f);
        break;
      case TOKEN_FILTER:
        a =
            new Analyzer() {
              @Override
              protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer tokenizer =
                    new MockTokenizer(
                        TokenStream.DEFAULT_TOKEN_ATTRIBUTE_FACTORY,
                        MockTokenizer.WHITESPACE,
                        false,
                        MockTokenizer.DEFAULT_MAX_TOKEN_LENGTH);
                return new TokenStreamComponents(
                    tokenizer, new ICUTransformFilter(tokenizer, Transliterator.getInstance(id)));
              }
            };
        break;
      case BARE_TRANSLIT:
        return runBareTranslit(Transliterator.getInstance(id), iterations, maxWordLength);
      default:
        throw new IllegalArgumentException();
    }
    CircularReplaceable.introducedUnpairedSurrogate(); // clear any leftover status
    int size = 0;
    try {
      for (int i = iterations; i > 0; i--) {
        String text = TestUtil.randomAnalysisString(textGenRandom, maxWordLength, false);
        CircularReplaceable.FIX_INTRODUCED_UNPAIRED_SURROGATES =
            false; // first ensure that expected exception is thrown
        boolean done = false;
        do {
          TokenStream ts = a.tokenStream("dummy", new StringReader(text));
          CharTermAttribute termAtt = ts.getAttribute(CharTermAttribute.class);
          ts.reset();
          try {
            while (ts.incrementToken()) {
              size += termAtt.length();
            }
            ts.end();
            ts.close();
          } catch (IllegalStateException ex) {
            String exMsg = ex.getMessage();
            if (exMsg == null || !exMsg.startsWith("unpaired ")) {
              throw ex;
            }
            if (type != Type.DECOMPOSED) {
              System.err.println(ex);
              // swallow this; only DECOMPOSED is capable of mitigating introduced unpaired
              // surrogates
            }
            String msg = CircularReplaceable.introducedUnpairedSurrogate();
            assertNotNull(msg);
            // recover
            System.err.println("recover id " + id + ", " + msg + ", text='" + escape(text) + "'");
            a = analyzerFromCharFilterFactory(new ICUTransform2CharFilterFactory(t));
            CircularReplaceable.FIX_INTRODUCED_UNPAIRED_SURROGATES =
                true; // ensure that auto-fix has intended effect
            continue;
          } catch (Exception ex) {
            if (type == Type.ROLLBACK) {
              System.err.println(ex);
              // swallow this; we _know_ there are problems with ROLLBACK, and we're not going to
              // fix them.
            }
            throw new RuntimeException("id=" + id + ", text='" + escape(text) + "'", ex);
          }
          String msg = CircularReplaceable.introducedUnpairedSurrogate();
          if (msg != null) {
            System.err.println(
                "caught id "
                    + id
                    + ", fix="
                    + CircularReplaceable.FIX_INTRODUCED_UNPAIRED_SURROGATES
                    + ", "
                    + msg
                    + ", text='"
                    + escape(text)
                    + "'");
            assertTrue(CircularReplaceable.FIX_INTRODUCED_UNPAIRED_SURROGATES);
          }
          done = true;
        } while (!done);
      }
    } finally {
      CircularReplaceable.FIX_INTRODUCED_UNPAIRED_SURROGATES = true;
    }
    return size;
  }

  private static int runBareTranslit(Transliterator t, int iterations, int maxWordLength) {
    Random r = random();
    Transliterator.Position p = new Transliterator.Position();
    int size = 0;
    for (int i = iterations; i > 0; i--) {
      String text = TestUtil.randomAnalysisString(r, maxWordLength, false);
      Replaceable replaceable = new ReplaceableString(text);
      p.contextStart = p.start = 0;
      p.limit = p.contextLimit = text.length();
      t.filteredTransliterate(replaceable, p, false);
      size += replaceable.length();
    }
    return size;
  }

  private static Analyzer analyzerFromCharFilterFactory(CharFilterFactory f) {
    return new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer =
            new MockTokenizer(
                TokenStream.DEFAULT_TOKEN_ATTRIBUTE_FACTORY,
                MockTokenizer.WHITESPACE,
                false,
                MockTokenizer.DEFAULT_MAX_TOKEN_LENGTH);
        return new TokenStreamComponents(tokenizer, tokenizer);
      }

      @Override
      protected Reader initReader(String fieldName, Reader reader) {
        return super.initReader(fieldName, f.create(reader));
      }

      @Override
      protected Reader initReaderForNormalization(String fieldName, Reader reader) {
        return super.initReaderForNormalization(fieldName, f.create(reader));
      }
    };
  }

  private static long testPerfSeed;
  private static int restoreBatchBufferSize;

  @BeforeClass
  public static void beforeClass() throws Exception {
    testPerfSeed = random().nextLong();
    restoreBatchBufferSize = ICUTransform2CharFilter.BATCH_BUFFER_SIZE;
    ICUTransform2CharFilter.BATCH_BUFFER_SIZE = random().nextInt(33);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    ICUTransform2CharFilter.BATCH_BUFFER_SIZE = restoreBatchBufferSize;
  }

  private static final String FOUND_OFFSET_DISCREPANCIES = "found offset discrepancies";

  private static void checkOffsetDiscrepancies(
      List<Map.Entry<String, String>> offsetDiscrepancies, String msg, boolean strict) {
    if (!offsetDiscrepancies.isEmpty()) {
      for (Map.Entry<String, String> e : offsetDiscrepancies) {
        System.err.println("XXX " + (msg == null ? "" : msg + " ") + e.getValue());
      }
      if (strict) {
        assertTrue(FOUND_OFFSET_DISCREPANCIES, false);
      }
    }
  }

  private static Analyzer getAnalyzer(
      String id, boolean suppressExternalize, AttributeFactory attributeFactory) {
    return new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer =
            new MockTokenizer(
                attributeFactory,
                MockTokenizer.WHITESPACE,
                false,
                MockTokenizer.DEFAULT_MAX_TOKEN_LENGTH);
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
  private void testRandomStrings(
      final String id,
      int iterations,
      List<Map.Entry<String, String>> accumulateOffsetDiscrepancies)
      throws Exception {
    Transliterator t = Transliterator.getInstance(id);
    AttributeFactory attributeFactory = BaseTokenStreamTestCase.newAttributeFactory();
    Analyzer a = getAnalyzer(id, true, attributeFactory);
    Analyzer b = getAnalyzer(id, false, attributeFactory);
    // 20 is the default maxWordLength
    // `b`, the "decomposed" analyzer, is the one we really want to test
    try {
      checkRandomData(random(), b, iterations * RANDOM_MULTIPLIER, 20);
    } catch (IllegalStateException ex) {
      if (!"Any-Hex".equals(id) || !"Illegal codepoint".equals(ex.getMessage())) {
        throw ex;
      }
      // this is a special case that we want to swallow and just move on
    }
    long seed = random().nextLong();
    Random randomA = new Random(seed);
    Random randomB = new Random(seed);
    Transliterator.Position p = new Transliterator.Position();
    for (int i = iterations * RANDOM_MULTIPLIER; i >= 0; i--) {
      String text = TestUtil.randomAnalysisString(random(), 20, false);

      // first, generate expected "gold standard" result (raw, non-incremental Transliterator)
      Replaceable in = new ReplaceableString(text);
      p.contextStart = p.start = 0;
      p.limit = p.contextLimit = text.length();
      try {
        t.filteredTransliterate(in, p, false);
      } catch (IllegalStateException ex) {
        if (!"Any-Hex".equals(id) || !"Illegal codepoint".equals(ex.getMessage())) {
          throw ex;
        }
        // this is a special case, and rather than proceeding normally, we verify that the same
        // exception
        // is thrown by streaming impl
        try {
          checkToken(id, text, "N/A (should throw exception)", null, false);
          assertTrue("expected exception to be thrown", false);
        } catch (
            @SuppressWarnings("unused")
            IllegalStateException ex1) {
          // TODO: this looks buggy, should be using 'ex1'
          assertEquals("Illegal codepoint", ex.getMessage());
          return;
        }
      }
      String expected = in.toString();

      boolean referenceImplOk;
      try {
        checkToken(id, text, expected, null, true);
        referenceImplOk = true;
      } catch (
          @SuppressWarnings("unused")
          Throwable er) {
        // we don't care about errors in the "legacy"/"reference" impl
        referenceImplOk = false;
      }
      // NOTE: `checkToken` on the "decomposed" approach (analyzer b) should often be redundant with
      // checkAnalysisConsistency (below), but we do it here anyway just to be safe (and for the
      // case
      // where `referenceImplOk==false`, when there is no other way to validate against the
      // `expected`
      // value
      try {
        checkToken(id, text, expected, null, false);
      } catch (ComparisonFailure f) {
        if (!"Thai-Latin".equals(id)) {
          throw f;
        }
        // otherwise re-try without the BreakIterator bug mitigation
        ICUTransform2CharFilter.MITIGATE_BREAK_TRANSLITERATOR_POSITION_BUG = false;
        try {
          checkToken(id, text, expected, null, false);
          // if this succeeds, we _don't_ want to crosscheck against the referenceImpl, because it
          // does not mitigate the BreakIterator bug and will thus throw spurious errors
          referenceImplOk = false;
        } finally {
          ICUTransform2CharFilter.MITIGATE_BREAK_TRANSLITERATOR_POSITION_BUG = true; // restore
        }
      }

      AnalysisResult resB;
      try {
        resB = checkAnalysisConsistency(randomB, b, false, text);
      } catch (Throwable e) {
        System.err.println("caught " + e + " for '" + escape(text) + "'");
        throw e;
      }
      if (referenceImplOk) {
        AnalysisResult resA = checkAnalysisConsistency(randomA, a, false, text);
        try {
          // NOTE: we tolerate null positions when using
          // `AttributeFactory.DEFAULT_ATTRIBUTE_FACTORY`, which
          // lazily instantiates PositionIncrementAttribute and can in some cases report as null
          // when
          // on the initializing call to `getAttribute(PositionIncrementAttribute.class)` in
          // `checkAnalysisConsistency(...)`
          assertEquals(
              resA, resB, AttributeFactory.DEFAULT_ATTRIBUTE_FACTORY.equals(attributeFactory));
        } catch (Throwable e) {
          final String msg;
          if (accumulateOffsetDiscrepancies != null
              && (msg = e.getMessage()) != null
              && msg.startsWith(OFFSET_DISCREPANCY_MSG_PREFIX)) {
            accumulateOffsetDiscrepancies.add(
                new AbstractMap.SimpleImmutableEntry<>("'" + escape(text) + "'", msg));
          } else {
            throw e;
          }
        }
      }
    }
    a.close();
    b.close();
  }

  public void testParityWithFilter1() throws Exception {
    System.err.println("BEGIN testParityWithFilter1");
    String id = "Hang-Latn";
    String text = "\u02b7\u02f2\u02da\u02ce\u02d7\u02c3\u02b7\u02dd mb";
    AttributeFactory attributeFactory = BaseTokenStreamTestCase.newAttributeFactory();
    Analyzer a = getAnalyzer(id, true, attributeFactory);
    Analyzer b = getAnalyzer(id, false, attributeFactory);
    long seed = random().nextLong();
    AnalysisResult resA = checkAnalysisConsistency(new Random(seed), a, false, text);
    AnalysisResult resB = checkAnalysisConsistency(new Random(seed), b, false, text);
    assertEquals(resA, resB, false);
  }

  public void testParityWithFilter2() throws Exception {
    System.err.println("BEGIN testParityWithFilter2");
    String id = "si-si_Latn";
    String text = "\uf171\u0279\u00af \udbb5\udee0\ua991 zr \u1739\u1735\u1734\u1738\u173d";
    AttributeFactory attributeFactory = BaseTokenStreamTestCase.newAttributeFactory();
    Analyzer a = getAnalyzer(id, false, attributeFactory);
    Analyzer b = getAnalyzer(id, true, attributeFactory);
    long seed = random().nextLong();
    AnalysisResult resA = checkAnalysisConsistency(new Random(seed), a, false, text);
    AnalysisResult resB = checkAnalysisConsistency(new Random(seed), b, false, text);
    assertEquals(resA, resB, false);
  }

  /**
   * This test verifies that top-level filters are applied (or not!) properly to _all_ component
   * Transliterators. The three user-level input characters here are each handled differently in
   * illustrative ways: 1. composed input "â" matches the top-level filter, is decomposed, its ascii
   * "a" is mapped to ascii "i", and "i\u0302" (the decomposed product of composed input entirely
   * matched by top-level filter) is composed into "î" 2. for decomposed input "a\u0302", only ascii
   * "a" matches the top-level filter; it is mapped, but is _not_ composed with input "\u0302",
   * which did _not_ match the top-level filter 3. decomposed input "i\u0302" is completely ignored
   * (no part matches top-level filter)
   */
  public void testParityWithFilter3() throws Exception {
    System.err.println("BEGIN testParityWithFilter3");
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

    checkToken(id, text, expected, rules, true);
    checkToken(id, text, expected, rules, false);
  }

  public void testEmptyTerm() throws IOException {
    System.err.println("BEGIN testEmptyTerm");
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
    return getTransliteratingFilter(id, r, null, suppressUnicodeNormalizationExternalization);
  }

  @SuppressWarnings("resource")
  private static CharFilter getTransliteratingFilter(
      String id, Reader r, String rules, boolean suppressUnicodeNormalizationExternalization) {
    if (suppressUnicodeNormalizationExternalization) {
      Map<String, String> args = new HashMap<>();
      args.put("id", id);
      ICUTransformCharFilterFactory factory =
          new ICUTransformCharFilterFactory(
              args, suppressUnicodeNormalizationExternalization, rules);
      return (CharFilter) factory.create(r);
    } else {
      Transliterator t;
      if (rules == null) {
        t = Transliterator.getInstance(id);
      } else {
        t = Transliterator.createFromRules(id, rules, Transliterator.FORWARD);
      }
      return ICUTransform2CharFilterFactory.wrap(r, t);
    }
  }

  public void assertEquals(
      AnalysisResult expected, AnalysisResult actual, boolean tolerateNullPositions) {
    granularAssertEquals("tokens '" + escape(expected.text) + "'", expected.tokens, actual.tokens);
    assertEquals("type", expected.types, actual.types);
    if (!tolerateNullPositions || !(expected.positions == null ^ actual.positions == null)) {
      assertEquals(
          "positions '" + escape(expected.text) + "'", expected.positions, actual.positions);
    }
    assertEquals("positionLengths", expected.positionLengths, actual.positionLengths);
    assertEquals(
        formatOffsetMsg("Start", expected, actual), expected.startOffsets, actual.startOffsets);
    assertEquals(formatOffsetMsg("End", expected, actual), expected.endOffsets, actual.endOffsets);
  }

  private static void granularAssertEquals(String msg, List<String> expected, List<String> actual) {
    if (expected == null || actual == null || expected.size() != actual.size()) {
      assertEquals(msg, expected, actual);
    } else {
      Iterator<String> e = expected.iterator();
      Iterator<String> a = actual.iterator();
      while (e.hasNext()) {
        assertEquals(msg + " e=" + expected + ", a=" + actual, e.next(), a.next());
      }
    }
  }

  private static final String OFFSET_DISCREPANCY_MSG_PREFIX = "offsetDiscrepancy";

  private static String formatOffsetMsg(
      String type, AnalysisResult expected, AnalysisResult actual) {
    StringBuilder sb = new StringBuilder();
    sb.append(OFFSET_DISCREPANCY_MSG_PREFIX + type + " input='" + escape(expected.text) + "'");
    Iterator<Integer> esIter = expected.startOffsets.iterator();
    Iterator<Integer> eeIter = expected.endOffsets.iterator();
    Iterator<Integer> asIter = actual.startOffsets.iterator();
    Iterator<Integer> aeIter = actual.endOffsets.iterator();
    for (String s : expected.tokens) {
      sb.append("\n\tt='").append(escape(s)).append("'; ");
      sb.append("expected=[")
          .append(esIter.next())
          .append(", ")
          .append(eeIter.next())
          .append("]; ");
      sb.append("actual=[").append(asIter.next()).append(", ").append(aeIter.next()).append("]");
    }
    return sb.toString();
  }
}
