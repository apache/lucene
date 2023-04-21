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
package org.apache.lucene.analysis.hunspell;

import static org.apache.lucene.analysis.hunspell.Dictionary.FLAG_UNSET;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.IOUtils;

public class TestSpellChecking extends LuceneTestCase {

  public void testEmpty() throws Exception {
    doTest("empty");
  }

  public void testBase() throws Exception {
    doTest("base");
  }

  public void testBaseUtf() throws Exception {
    doTest("base_utf");
  }

  public void testKeepcase() throws Exception {
    doTest("keepcase");
  }

  public void testAllcaps() throws Exception {
    doTest("allcaps");
  }

  public void testRepSuggestions() throws Exception {
    doTest("rep");
  }

  public void testPhSuggestions() throws Exception {
    doTest("ph");
  }

  public void testPhSuggestions2() throws Exception {
    doTest("ph2");
  }

  public void testForceUCase() throws Exception {
    doTest("forceucase");
  }

  public void testCheckSharpS() throws Exception {
    doTest("checksharps");
  }

  public void testIJ() throws Exception {
    doTest("IJ");
  }

  public void testI53643_numbersWithSeparators() throws Exception {
    doTest("i53643");
  }

  public void testCheckCompoundPattern() throws Exception {
    doTest("checkcompoundpattern");
  }

  public void testCheckCompoundPattern2() throws Exception {
    doTest("checkcompoundpattern2");
  }

  public void testCheckCompoundPattern3() throws Exception {
    doTest("checkcompoundpattern3");
  }

  public void testDotless_i() throws Exception {
    doTest("dotless_i");
  }

  public void testNeedAffixOnAffixes() throws Exception {
    doTest("needaffix5");
  }

  public void testCompoundFlag() throws Exception {
    doTest("compoundflag");
  }

  public void testFlagUtf8() throws Exception {
    doTest("flagutf8");
  }

  public void testCheckCompoundCase() throws Exception {
    doTest("checkcompoundcase");
  }

  public void testCheckCompoundDup() throws Exception {
    doTest("checkcompounddup");
  }

  public void testCheckCompoundTriple() throws Exception {
    doTest("checkcompoundtriple");
  }

  public void testSimplifiedTriple() throws Exception {
    doTest("simplifiedtriple");
  }

  public void testCompoundForbid() throws Exception {
    doTest("compoundforbid");
  }

  public void testBreak() throws Exception {
    doTest("break");
  }

  public void testBreakDefault() throws Exception {
    doTest("breakdefault");
  }

  public void testBreakOff() throws Exception {
    doTest("breakoff");
  }

  public void testCheckCompoundRep() throws Exception {
    doTest("checkcompoundrep");
  }

  public void testDisallowCompoundsWhenDictionaryContainsSeparatedWordPair() throws Exception {
    doTest("wordpair");
  }

  public void testCompoundrule() throws Exception {
    doTest("compoundrule");
  }

  public void testCompoundrule2() throws Exception {
    doTest("compoundrule2");
  }

  public void testCompoundrule3() throws Exception {
    doTest("compoundrule3");
  }

  public void testCompoundrule4() throws Exception {
    doTest("compoundrule4");
  }

  public void testCompoundrule5() throws Exception {
    doTest("compoundrule5");
  }

  public void testCompoundrule6() throws Exception {
    doTest("compoundrule6");
  }

  public void testCompoundrule7() throws Exception {
    doTest("compoundrule7");
  }

  public void testCompoundrule8() throws Exception {
    doTest("compoundrule8");
  }

  public void testDisallowCompoundOnlySuffixesAtTheVeryEnd() throws Exception {
    doTest("onlyincompound2");
  }

  public void testForbiddenWord() throws Exception {
    doTest("forbiddenword");
  }

  public void testForbiddenWord1() throws Exception {
    doTest("opentaal_forbiddenword1");
  }

  public void testForbiddenWord2() throws Exception {
    doTest("opentaal_forbiddenword2");
  }

  public void testGermanCompounding() throws Exception {
    doTest("germancompounding");
  }

  public void testGermanManualCase() throws Exception {
    doTest("germanManualCase");
  }

  public void testApplyOconvToSuggestions() throws Exception {
    doTest("oconv");
  }

  public void testModifyingSuggestions() throws Exception {
    doTest("sug");
  }

  public void testModifyingSuggestions2() throws Exception {
    doTest("sug2");
  }

  public void testGeneratedSuggestions() throws Exception {
    doTest("ngram");
  }

  public void testMaxNGramSugsDefaultIsNotUnlimited() throws Exception {
    doTest("maxNGramSugsDefault");
  }

  public void testMixedCaseSuggestionHeuristics() throws Exception {
    doTest("i58202");
  }

  public void testMapSuggestions() throws Exception {
    doTest("map");
  }

  public void testNoSuggest() throws Exception {
    doTest("nosuggest");
  }

  public void testUseUSKeyboardAsDefaultKeyValue() throws Exception {
    doTest("keyDefault");
  }

  public void testDoNotFailWhenExpandingRootWithLongerStrippingAffix() throws Exception {
    doTest("longStrip");
  }

  protected void doTest(String name) throws Exception {
    //noinspection ConstantConditions
    checkSpellCheckerExpectations(
        Path.of(getClass().getResource(name + ".aff").toURI()).getParent().resolve(name));
  }

  static void checkSpellCheckerExpectations(Path basePath) throws IOException, ParseException {
    InputStream affixStream = Files.newInputStream(Path.of(basePath.toString() + ".aff"));
    Path dicFile = Path.of(basePath + ".dic");
    InputStream dictStream = Files.newInputStream(dicFile);

    Hunspell speller;
    Map<String, Suggester> suggesters = new LinkedHashMap<>();
    try {
      Dictionary dictionary =
          new Dictionary(new ByteBuffersDirectory(), "dictionary", affixStream, dictStream);
      speller = new Hunspell(dictionary, TimeoutPolicy.NO_TIMEOUT, () -> {});
      Suggester suggester = new Suggester(dictionary);
      suggesters.put("default", suggester);
      suggesters.put("caching", suggester.withSuggestibleEntryCache());
      if (dictionary.compoundRules == null
          && dictionary.compoundBegin == FLAG_UNSET
          && dictionary.compoundFlag == FLAG_UNSET) {
        for (int n = 2; n <= 4; n++) {
          var checker = NGramFragmentChecker.fromAllSimpleWords(n, dictionary, () -> {});
          suggesters.put("ngram" + n, suggester.withFragmentChecker(checker));
        }
      }
    } finally {
      IOUtils.closeWhileHandlingException(affixStream);
      IOUtils.closeWhileHandlingException(dictStream);
    }

    Path good = Path.of(basePath + ".good");
    if (Files.exists(good)) {
      for (String word : Files.readAllLines(good)) {
        assertTrue("Unexpectedly considered misspelled: " + word, speller.spell(word.trim()));
      }
    }

    Path wrong = Path.of(basePath + ".wrong");
    Path sug = Path.of(basePath + ".sug");
    if (Files.exists(wrong)) {
      List<String> wrongWords = Files.readAllLines(wrong);
      for (String word : wrongWords) {
        assertFalse("Unexpectedly considered correct: " + word, speller.spell(word.trim()));
      }
      if (Files.exists(sug)) {
        String sugLines = Files.readString(sug).trim();
        for (Map.Entry<String, Suggester> e : suggesters.entrySet()) {
          assertEquals("Suggester=" + e.getKey(), sugLines, suggest(e.getValue(), wrongWords));
        }
      }
    } else {
      assertFalse(".sug file without .wrong file!", Files.exists(sug));
    }

    Set<String> everythingGenerated = expandWholeDictionary(dicFile, speller);
    if (everythingGenerated != null && !speller.dictionary.mayNeedInputCleaning()) {
      checkGoodSugWordsAreGenerated(speller, good, sug, everythingGenerated);
    }
  }

  private static String suggest(Suggester suggester, List<String> wrongWords) {
    return wrongWords.stream()
        .map(s -> String.join(", ", suggester.suggestNoTimeout(s, () -> {})))
        .filter(s -> !s.isEmpty())
        .collect(Collectors.joining("\n"));
  }

  private static Set<String> expandWholeDictionary(Path dic, Hunspell speller) throws IOException {
    Set<String> everythingGenerated = new HashSet<>();
    boolean generatedEverything = true;
    try (Stream<String> lines = Files.lines(dic, speller.dictionary.decoder.charset())) {
      for (String line : lines.skip(1).collect(Collectors.toList())) {
        int len = (int) line.chars().takeWhile(c -> !Character.isWhitespace(c) && c != '/').count();
        String word = line.substring(0, len).trim();
        if (word.isEmpty() || word.contains("\\")) {
          generatedEverything = false;
          continue;
        }

        List<AffixedWord> expanded =
            checkExpansionGeneratesCorrectWords(speller, word, dic.toString());
        expanded.forEach(w -> everythingGenerated.add(w.getWord().toLowerCase(Locale.ROOT)));
      }
    }
    return generatedEverything ? everythingGenerated : null;
  }

  private static void checkGoodSugWordsAreGenerated(
      Hunspell speller, Path good, Path sug, Set<String> everythingGenerated) throws IOException {
    Set<String> goodWords = new HashSet<>();
    if (Files.exists(good)) {
      Files.readAllLines(good).stream().map(String::trim).forEach(goodWords::add);
    }
    if (Files.exists(sug)) {
      Files.readAllLines(sug).stream()
          .flatMap(line -> Stream.of(line.split(", ")))
          .map(String::trim)
          .filter(s -> !s.contains(" "))
          .forEach(goodWords::add);
    }

    goodWords.removeAll(everythingGenerated);
    goodWords.removeIf(s -> !s.equals(s.toLowerCase(Locale.ROOT)));
    goodWords.removeIf(s -> speller.analyzeSimpleWord(s).isEmpty());

    assertTrue("Some *.good/sug words weren't generated: " + goodWords, goodWords.isEmpty());
  }

  static List<AffixedWord> checkExpansionGeneratesCorrectWords(
      Hunspell hunspell, String stem, String baseName) {
    List<AffixedWord> expanded = hunspell.getAllWordForms(stem);
    Set<AffixedWord> misspelled = new HashSet<>();
    for (AffixedWord word : expanded) {
      if (!hunspell.spell(word.getWord()) || hunspell.analyzeSimpleWord(word.getWord()).isEmpty()) {
        misspelled.add(word);
      }
    }
    if (!misspelled.isEmpty()) {
      fail("Misspelled words generated in " + baseName + ": " + misspelled);
    }

    if (expanded.stream().anyMatch(e -> e.getWord().equals(stem))) {
      EntrySuggestion suggestion =
          hunspell.compress(
              expanded.stream().map(AffixedWord::getWord).collect(Collectors.toList()));
      if (suggestion != null) {
        String message =
            ("Compression suggests a different stem from the original " + stem)
                + (" in " + baseName + ":" + suggestion);
        assertTrue(
            message,
            suggestion.getEntriesToEdit().stream().anyMatch(e -> e.getStem().equals(stem)));
      }
    }

    return expanded;
  }
}
