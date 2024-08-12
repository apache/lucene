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
package org.apache.lucene.search.spell;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.spell.WordBreakSpellChecker.BreakSuggestionSortMethod;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.English;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.IOUtils;
import org.hamcrest.MatcherAssert;

public class TestWordBreakSpellChecker extends LuceneTestCase {
  private Directory dir;
  private Analyzer analyzer;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
    analyzer = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, true);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, analyzer);

    for (int i = 900; i < 1112; i++) {
      Document doc = new Document();
      String num = English.intToEnglish(i).replace("-", " ").replace(",", "");
      doc.add(newTextField("numbers", num, Field.Store.NO));
      writer.addDocument(doc);
    }

    {
      Document doc = new Document();
      doc.add(newTextField("abba", "A B AB ABA BAB", Field.Store.NO));
      writer.addDocument(doc);
    }
    {
      Document doc = new Document();
      doc.add(newTextField("numbers", "thou hast sand betwixt thy toes", Field.Store.NO));
      writer.addDocument(doc);
    }
    {
      Document doc = new Document();
      doc.add(newTextField("numbers", "hundredeight eightyeight yeight", Field.Store.NO));
      writer.addDocument(doc);
    }
    {
      Document doc = new Document();
      doc.add(newTextField("numbers", "tres y cinco", Field.Store.NO));
      writer.addDocument(doc);
    }

    writer.commit();
    writer.close();
  }

  @Override
  public void tearDown() throws Exception {
    IOUtils.close(dir, analyzer);
    super.tearDown();
  }

  public void testMaxEvaluations() throws Exception {
    try (IndexReader ir = DirectoryReader.open(dir)) {

      final String input = "ab".repeat(5);
      final int maxEvals = 100;
      final int maxSuggestions = maxEvals * 2; // plenty

      WordBreakSpellChecker wbsp = new WordBreakSpellChecker();
      wbsp.setMinBreakWordLength(1);
      wbsp.setMinSuggestionFrequency(1);

      wbsp.setMaxChanges(2 * input.length()); // plenty
      wbsp.setMaxEvaluations(maxEvals);

      Term term = new Term("abba", input);
      SuggestWord[][] sw =
          wbsp.suggestWordBreaks(
              term,
              maxSuggestions,
              ir,
              SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX,
              BreakSuggestionSortMethod.NUM_CHANGES_THEN_MAX_FREQUENCY);

      // sanity check that our suggester isn't completely broken
      MatcherAssert.assertThat(sw.length, greaterThan(0));

      // if maxEvaluations is respected, we can't possibly have more suggestions than that
      MatcherAssert.assertThat(sw.length, lessThan(maxEvals));
    }
  }

  public void testSmallMaxEvaluations() throws Exception {
    // even using small maxEvals (relative to maxChanges) should produce
    // good results if possible

    try (IndexReader ir = DirectoryReader.open(dir)) {

      final int maxEvals = TestUtil.nextInt(random(), 6, 20);
      final int maxSuggestions = maxEvals * 10; // plenty

      WordBreakSpellChecker wbsp = new WordBreakSpellChecker();
      wbsp.setMinBreakWordLength(1);
      wbsp.setMinSuggestionFrequency(1);

      wbsp.setMaxChanges(maxEvals * 10); // plenty
      wbsp.setMaxEvaluations(maxEvals);

      Term term = new Term("abba", "ababab");
      SuggestWord[][] sw =
          wbsp.suggestWordBreaks(
              term,
              maxSuggestions,
              ir,
              SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX,
              BreakSuggestionSortMethod.NUM_CHANGES_THEN_MAX_FREQUENCY);

      // sanity check that our suggester isn't completely broken
      MatcherAssert.assertThat(sw.length, greaterThan(0));

      // if maxEvaluations is respected, we can't possibly have more suggestions than that
      MatcherAssert.assertThat(sw.length, lessThan(maxEvals));

      // we should have been able to find this "optimal" (due to fewest num changes)
      // suggestion before hitting our small maxEvals (and before any suggests with more breaks)
      assertEquals(2, sw[0].length);
      assertEquals("aba", sw[0][0].string);
      assertEquals("bab", sw[0][1].string);
    }
  }

  public void testCombiningWords() throws Exception {
    IndexReader ir = DirectoryReader.open(dir);
    WordBreakSpellChecker wbsp = new WordBreakSpellChecker();

    {
      Term[] terms = {
        new Term("numbers", "one"),
        new Term("numbers", "hun"),
        new Term("numbers", "dred"),
        new Term("numbers", "eight"),
        new Term("numbers", "y"),
        new Term("numbers", "eight"),
      };
      wbsp.setMaxChanges(3);
      wbsp.setMaxCombineWordLength(20);
      wbsp.setMinSuggestionFrequency(1);
      CombineSuggestion[] cs =
          wbsp.suggestWordCombinations(terms, 10, ir, SuggestMode.SUGGEST_ALWAYS);
      assertEquals(5, cs.length);

      assertSuggestionEquals(cs[0], "hundred", 1.0f, 1, 2);
      assertSuggestionEquals(cs[1], "eighty", 1.0f, 3, 4);
      assertSuggestionEquals(cs[2], "yeight", 1.0f, 4, 5);

      for (int i = 3; i < 5; i++) {
        assertEquals(3, cs[i].originalTermIndexes.length);
        assertEquals(2, cs[i].suggestion.score, 0);
        assertTrue(
            (cs[i].originalTermIndexes[0] == 1
                    && cs[i].originalTermIndexes[1] == 2
                    && cs[i].originalTermIndexes[2] == 3
                    && cs[i].suggestion.string.equals("hundredeight"))
                || (cs[i].originalTermIndexes[0] == 3
                    && cs[i].originalTermIndexes[1] == 4
                    && cs[i].originalTermIndexes[2] == 5
                    && cs[i].suggestion.string.equals("eightyeight")));
      }

      cs = wbsp.suggestWordCombinations(terms, 5, ir, SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX);
      assertEquals(2, cs.length);

      assertSuggestionEquals(cs[0], "hundred", 1.0f, 1, 2);
      assertSuggestionEquals(cs[1], "hundredeight", 2.0f, 1, 2, 3);
    }
    ir.close();
  }

  public void testBreakingWords() throws Exception {
    IndexReader ir = DirectoryReader.open(dir);
    WordBreakSpellChecker wbsp = new WordBreakSpellChecker();

    {
      Term term = new Term("numbers", "ninetynine");
      wbsp.setMaxChanges(1);
      wbsp.setMinBreakWordLength(1);
      wbsp.setMinSuggestionFrequency(1);
      SuggestWord[][] sw =
          wbsp.suggestWordBreaks(
              term,
              5,
              ir,
              SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX,
              BreakSuggestionSortMethod.NUM_CHANGES_THEN_MAX_FREQUENCY);
      assertEquals(1, sw.length);
      assertEquals(2, sw[0].length);
      assertSuggestionEquals(sw[0][0], "ninety", 1.0f);
      assertSuggestionEquals(sw[0][1], "nine", 1.0f);
    }
    {
      Term term = new Term("numbers", "onethousand");
      wbsp.setMaxChanges(1);
      wbsp.setMinBreakWordLength(1);
      wbsp.setMinSuggestionFrequency(1);
      SuggestWord[][] sw =
          wbsp.suggestWordBreaks(
              term,
              2,
              ir,
              SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX,
              BreakSuggestionSortMethod.NUM_CHANGES_THEN_MAX_FREQUENCY);
      assertEquals(1, sw.length);
      assertEquals(2, sw[0].length);
      assertSuggestionEquals(sw[0][0], "one", 1.0f);
      assertSuggestionEquals(sw[0][1], "thousand", 1.0f);

      wbsp.setMaxChanges(2);
      wbsp.setMinSuggestionFrequency(1);
      sw =
          wbsp.suggestWordBreaks(
              term,
              1,
              ir,
              SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX,
              BreakSuggestionSortMethod.NUM_CHANGES_THEN_MAX_FREQUENCY);
      assertEquals(1, sw.length);
      assertEquals(2, sw[0].length);

      wbsp.setMaxChanges(2);
      wbsp.setMinSuggestionFrequency(2);
      sw =
          wbsp.suggestWordBreaks(
              term,
              2,
              ir,
              SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX,
              BreakSuggestionSortMethod.NUM_CHANGES_THEN_MAX_FREQUENCY);
      assertEquals(1, sw.length);
      assertEquals(2, sw[0].length);

      wbsp.setMaxChanges(2);
      wbsp.setMinSuggestionFrequency(1);
      sw =
          wbsp.suggestWordBreaks(
              term,
              2,
              ir,
              SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX,
              BreakSuggestionSortMethod.NUM_CHANGES_THEN_MAX_FREQUENCY);
      assertEquals(2, sw.length);
      assertEquals(2, sw[0].length);
      assertSuggestionEquals(sw[0][0], "one", 1.0f);
      assertSuggestionEquals(sw[0][1], "thousand", 1.0f);
      MatcherAssert.assertThat(sw[0][1].freq, greaterThan(1));
      MatcherAssert.assertThat(sw[0][0].freq, greaterThan(sw[0][1].freq));

      assertEquals(3, sw[1].length);
      assertSuggestionEquals(sw[1][0], "one", 2.0f);
      assertSuggestionEquals(sw[1][1], "thou", 2.0f);
      assertSuggestionEquals(sw[1][2], "sand", 2.0f);
      MatcherAssert.assertThat(sw[1][0].freq, greaterThan(1));
      assertEquals(1, sw[1][1].freq);
      assertEquals(1, sw[1][2].freq);
    }
    {
      Term term = new Term("numbers", "onethousandonehundredeleven");
      wbsp.setMaxChanges(3);
      wbsp.setMinBreakWordLength(1);
      wbsp.setMinSuggestionFrequency(1);
      SuggestWord[][] sw =
          wbsp.suggestWordBreaks(
              term,
              5,
              ir,
              SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX,
              BreakSuggestionSortMethod.NUM_CHANGES_THEN_MAX_FREQUENCY);
      assertEquals(0, sw.length);

      wbsp.setMaxChanges(4);
      sw =
          wbsp.suggestWordBreaks(
              term,
              5,
              ir,
              SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX,
              BreakSuggestionSortMethod.NUM_CHANGES_THEN_MAX_FREQUENCY);
      assertEquals(1, sw.length);
      assertEquals(5, sw[0].length);

      wbsp.setMaxChanges(5);
      sw =
          wbsp.suggestWordBreaks(
              term,
              5,
              ir,
              SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX,
              BreakSuggestionSortMethod.NUM_CHANGES_THEN_MAX_FREQUENCY);
      assertEquals(2, sw.length);
      assertEquals(5, sw[0].length);
      assertEquals("thousand", sw[0][1].string);
      assertEquals(6, sw[1].length);
      assertEquals("thou", sw[1][1].string);
      assertEquals("sand", sw[1][2].string);
    }
    {
      // make sure we can handle 2-char codepoints
      Term term = new Term("numbers", "\uD864\uDC79");
      wbsp.setMaxChanges(1);
      wbsp.setMinBreakWordLength(1);
      wbsp.setMinSuggestionFrequency(1);
      SuggestWord[][] sw =
          wbsp.suggestWordBreaks(
              term,
              5,
              ir,
              SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX,
              BreakSuggestionSortMethod.NUM_CHANGES_THEN_MAX_FREQUENCY);
      assertEquals(0, sw.length);
    }

    ir.close();
  }

  public void testRandom() throws Exception {
    int numDocs = TestUtil.nextInt(random(), (10 * RANDOM_MULTIPLIER), (100 * RANDOM_MULTIPLIER));

    Directory dir = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, analyzer);
    int maxLength = TestUtil.nextInt(random(), 5, 50);
    List<String> originals = new ArrayList<>(numDocs);
    List<String[]> breaks = new ArrayList<>(numDocs);
    for (int i = 0; i < numDocs; i++) {
      String orig = "";
      if (random().nextBoolean()) {
        while (badTestString(orig)) {
          orig = TestUtil.randomSimpleString(random(), maxLength);
        }
      } else {
        while (badTestString(orig)) {
          orig = TestUtil.randomUnicodeString(random(), maxLength);
        }
      }

      originals.add(orig);
      int totalLength = orig.codePointCount(0, orig.length());
      int breakAt = orig.offsetByCodePoints(0, TestUtil.nextInt(random(), 1, totalLength - 1));
      String[] broken = new String[2];
      broken[0] = orig.substring(0, breakAt);
      broken[1] = orig.substring(breakAt);
      breaks.add(broken);
      Document doc = new Document();
      doc.add(newTextField("random_break", broken[0] + " " + broken[1], Field.Store.NO));
      doc.add(newTextField("random_combine", orig, Field.Store.NO));
      writer.addDocument(doc);
    }
    writer.commit();
    writer.close();

    IndexReader ir = DirectoryReader.open(dir);
    WordBreakSpellChecker wbsp = new WordBreakSpellChecker();
    wbsp.setMaxChanges(1);
    wbsp.setMinBreakWordLength(1);
    wbsp.setMinSuggestionFrequency(1);
    wbsp.setMaxCombineWordLength(maxLength);
    for (int i = 0; i < originals.size(); i++) {
      String orig = originals.get(i);
      String left = breaks.get(i)[0];
      String right = breaks.get(i)[1];
      {
        Term term = new Term("random_break", orig);

        SuggestWord[][] sw =
            wbsp.suggestWordBreaks(
                term,
                originals.size(),
                ir,
                SuggestMode.SUGGEST_ALWAYS,
                BreakSuggestionSortMethod.NUM_CHANGES_THEN_MAX_FREQUENCY);
        boolean failed = true;
        for (SuggestWord[] sw1 : sw) {
          assertEquals(2, sw1.length);
          if (sw1[0].string.equals(left) && sw1[1].string.equals(right)) {
            failed = false;
          }
        }
        assertFalse(
            "Failed getting break suggestions\n >Original: "
                + orig
                + "\n >Left: "
                + left
                + "\n >Right: "
                + right,
            failed);
      }
      {
        Term[] terms = {new Term("random_combine", left), new Term("random_combine", right)};
        CombineSuggestion[] cs =
            wbsp.suggestWordCombinations(terms, originals.size(), ir, SuggestMode.SUGGEST_ALWAYS);
        boolean failed = true;
        for (CombineSuggestion cs1 : cs) {
          assertEquals(2, cs1.originalTermIndexes.length);
          if (cs1.suggestion.string.equals(left + right)) {
            failed = false;
          }
        }
        assertFalse(
            "Failed getting combine suggestions\n >Original: "
                + orig
                + "\n >Left: "
                + left
                + "\n >Right: "
                + right,
            failed);
      }
    }
    IOUtils.close(ir, dir, analyzer);
  }

  private static void assertSuggestionEquals(
      CombineSuggestion cs, String word, float score, int... termIndexes) {
    assertEquals(word, cs.suggestion.string);
    assertEquals(score, cs.suggestion.score, 0);
    assertArrayEquals(termIndexes, cs.originalTermIndexes);
  }

  private static void assertSuggestionEquals(SuggestWord sw, String word, float score) {
    assertEquals(word, sw.string);
    assertEquals(score, sw.score, 0);
  }

  private static final Pattern mockTokenizerWhitespacePattern = Pattern.compile("[ \\t\\r\\n]");

  private boolean badTestString(String s) {
    return s.codePointCount(0, s.length()) < 2 || mockTokenizerWhitespacePattern.matcher(s).find();
  }
}
