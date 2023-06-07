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

import static org.apache.lucene.analysis.hunspell.StemmerTestBase.loadDictionary;
import static org.apache.lucene.analysis.hunspell.TimeoutPolicy.NO_TIMEOUT;
import static org.apache.lucene.analysis.hunspell.TimeoutPolicy.RETURN_PARTIAL_RESULT;
import static org.apache.lucene.analysis.hunspell.TimeoutPolicy.THROW_EXCEPTION;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.Test;

public class TestHunspell extends LuceneTestCase {
  public void testCheckCanceled() throws Exception {
    AtomicBoolean canceled = new AtomicBoolean();
    Runnable checkCanceled =
        () -> {
          if (canceled.get()) {
            throw new CancellationException();
          }
        };
    Dictionary dictionary = loadDictionary(false, "simple.aff", "simple.dic");
    Hunspell hunspell = new Hunspell(dictionary, NO_TIMEOUT, checkCanceled);

    assertTrue(hunspell.spell("apache"));
    assertEquals(Collections.singletonList("apach"), hunspell.suggest("apac"));

    canceled.set(true);
    assertThrows(CancellationException.class, () -> hunspell.spell("apache"));
    assertThrows(CancellationException.class, () -> hunspell.suggest("apac"));
  }

  public void testCustomCheckCanceledGivesPartialResult() throws Exception {
    Dictionary dictionary = loadDictionary(false, "simple.aff", "simple.dic");

    List<String> expected = List.of("apach");
    assertEquals(expected, new Hunspell(dictionary, NO_TIMEOUT, () -> {}).suggest("apac"));

    AtomicInteger counter = new AtomicInteger();
    String msg = "msg";
    Runnable checkCanceled =
        () -> {
          if (counter.incrementAndGet() > 400) {
            throw new SuggestionTimeoutException(msg, null);
          }
        };

    Hunspell hunspell = new Hunspell(dictionary, RETURN_PARTIAL_RESULT, checkCanceled);
    // pass a long timeout so that slower CI servers are more predictable.
    assertEquals(expected, hunspell.suggest("apac", TimeUnit.DAYS.toMillis(1)));

    counter.set(0);
    var e =
        assertThrows(
            SuggestionTimeoutException.class,
            () -> new Suggester(dictionary).suggestNoTimeout("apac", checkCanceled));
    assertEquals(expected, e.getPartialResult());
    assertEquals("msg", e.getMessage());
  }

  public void testSuggestionTimeLimit() throws IOException, ParseException {
    int timeLimitMs = 10;

    Dictionary dictionary = loadDictionary(false, "timelimit.aff", "simple.dic");
    String word = "qazwsxedcrfvtgbyhnujm";

    Hunspell incomplete = new Hunspell(dictionary, RETURN_PARTIAL_RESULT, () -> {});
    assertFalse(incomplete.spell(word));
    assertEquals(Collections.emptyList(), incomplete.suggest(word, timeLimitMs));

    Hunspell throwing = new Hunspell(dictionary, THROW_EXCEPTION, () -> {});
    assertFalse(throwing.spell(word));

    SuggestionTimeoutException exception =
        assertThrows(SuggestionTimeoutException.class, () -> throwing.suggest(word, timeLimitMs));
    assertEquals(Collections.emptyList(), exception.getPartialResult());

    Hunspell noLimit = new Hunspell(dictionary, NO_TIMEOUT, () -> {});
    assertEquals(Collections.emptyList(), noLimit.suggest(word.substring(0, 5), timeLimitMs));
  }

  @Test
  public void testStemmingApi() throws Exception {
    Hunspell hunspell = loadNoTimeout("simple");
    assertEquals(Collections.singletonList("apach"), hunspell.getRoots("apache"));
    assertEquals(Collections.singletonList("foo"), hunspell.getRoots("foo"));
  }

  @Test
  public void testAnalysisApi() throws Exception {
    Hunspell hunspell = loadNoTimeout("base");
    assertEquals(hunspell.analyzeSimpleWord("nonexistent"), List.of());
    AffixedWord word = hunspell.analyzeSimpleWord("recreated").get(0);
    checkAffixedWord(word, "create", List.of("A"), List.of("D"));
  }

  @Test
  public void testAnalysisSeveralSuffixes() throws Exception {
    Hunspell hunspell = loadNoTimeout("needaffix5");
    AffixedWord word = hunspell.analyzeSimpleWord("pseudoprefoopseudosufbar").get(0);
    checkAffixedWord(word, "foo", List.of("C"), List.of("B", "A"));
  }

  @Test
  public void testAnalysisFlagLong() throws Exception {
    AffixedWord word = loadNoTimeout("flaglong").analyzeSimpleWord("foos").get(0);
    checkAffixedWord(word, "foo", List.of(), List.of("Y1"));
  }

  @Test
  public void testAnalysisFlagNum() throws Exception {
    AffixedWord word = loadNoTimeout("flagnum").analyzeSimpleWord("foos").get(0);
    checkAffixedWord(word, "foo", List.of(), List.of("65000"));
  }

  @Test
  public void testAnalysisMorphData() throws Exception {
    List<AffixedWord> words = loadNoTimeout("morphdata").analyzeSimpleWord("works");
    assertEquals(2, words.size());
    AffixedWord verb =
        words.get(words.get(0).getDictEntry().getMorphologicalData().contains("verb") ? 0 : 1);
    AffixedWord noun = words.get(words.get(0) != verb ? 0 : 1);
    assertNotNull(verb);
    assertNotNull(noun);
    checkAffixedWord(verb, "work", List.of(), List.of("A"));
    checkAffixedWord(noun, "work", List.of(), List.of("B"));

    assertEquals(List.of("worknoun"), noun.getDictEntry().getMorphologicalValues("st:"));
    assertEquals(List.of("workverb"), verb.getDictEntry().getMorphologicalValues("st:"));
    assertEquals("st:worknoun", noun.getDictEntry().getMorphologicalData());
    assertEquals("st:workverb", verb.getDictEntry().getMorphologicalData());
  }

  private void checkAffixedWord(
      AffixedWord word, String stem, List<String> prefixFlags, List<String> suffixFlags) {
    assertEquals(stem, word.getDictEntry().getStem());
    assertEquals(
        prefixFlags,
        word.getPrefixes().stream().map(AffixedWord.Affix::getFlag).collect(Collectors.toList()));
    assertEquals(
        suffixFlags,
        word.getSuffixes().stream().map(AffixedWord.Affix::getFlag).collect(Collectors.toList()));
  }

  private Hunspell loadNoTimeout(String name) throws Exception {
    return loadNoTimeout(loadDictionary(false, name + ".aff", name + ".dic"));
  }

  private static Hunspell loadNoTimeout(Dictionary dictionary) {
    return new Hunspell(dictionary, TimeoutPolicy.NO_TIMEOUT, () -> {});
  }

  @Test
  public void testExpandRootApi() throws Exception {
    Hunspell h = loadNoTimeout("base");
    String[] createFormsBase = {
      "create", "created", "creates", "creating", "creation", "creations"
    };
    List<String> expected =
        Stream.concat(
                Stream.of(createFormsBase).flatMap(s -> Stream.of(s, "pro" + s, "re" + s)),
                Stream.of("creative"))
            .sorted()
            .collect(Collectors.toList());

    Map<String, AffixedWord> expanded =
        TestSpellChecking.checkExpansionGeneratesCorrectWords(h, "create", "base").stream()
            .collect(Collectors.toMap(w -> w.getWord(), w -> w));
    assertEquals(expected, expanded.keySet().stream().sorted().collect(Collectors.toList()));

    checkAffixedWord(expanded.get("created"), "create", List.of(), List.of("D"));
    checkAffixedWord(expanded.get("recreated"), "create", List.of("A"), List.of("D"));

    WordFormGenerator generator = new WordFormGenerator(h.dictionary);
    List<AffixedWord> overrideFlag = generator.getAllWordForms("create", "U", () -> {});
    assertEquals(
        Set.of("create", "uncreate"),
        overrideFlag.stream().map(w -> w.getWord()).collect(Collectors.toSet()));

    List<AffixedWord> nonExistentRoot = generator.getAllWordForms("form", "S", () -> {});
    assertEquals(
        Set.of("form", "forms"),
        nonExistentRoot.stream().map(w -> w.getWord()).collect(Collectors.toSet()));
  }

  @Test
  public void testCompressingApi() throws Exception {
    Hunspell h = loadNoTimeout("base");
    String[] createQuery = {"create", "created", "creates", "creating", "creation"};
    checkCompression(h, "toEdit=[create/DGNS], toAdd=[], extra=[]", createQuery);
    checkCompression(h, "toEdit=[created], toAdd=[creates], extra=[]", "creates", "created");
    checkCompression(h, "toEdit=[], toAdd=[creation/S], extra=[]", "creation", "creations");
    checkCompression(h, "toEdit=[], toAdd=[abc, def], extra=[]", "abc", "def");
    checkCompression(h, "toEdit=[], toAdd=[form/S], extra=[]", "form", "forms");

    checkCompression(
        loadNoTimeout("compress"), "toEdit=[], toAdd=[form/X], extra=[forms]", "form", "formx");
  }

  @Test
  public void testCompressingIsMinimal() throws Exception {
    Hunspell h = loadNoTimeout("compress");
    checkCompression(
        h, "toEdit=[], toAdd=[form/GS], extra=[]", "formings", "forming", "form", "forms");
  }

  @Test
  public void testCompressingWithProhibition() throws Exception {
    WordFormGenerator gen = new WordFormGenerator(loadNoTimeout("compress").dictionary);
    assertEquals(
        "toEdit=[], toAdd=[form/S], extra=[]",
        gen.compress(List.of("form", "forms"), Set.of("formx"), () -> {}).internalsToString());
    assertEquals(
        "toEdit=[], toAdd=[form, formx], extra=[]",
        gen.compress(List.of("form", "formx"), Set.of("forms"), () -> {}).internalsToString());
  }

  private void checkCompression(Hunspell h, String expected, String... words) {
    assertEquals(expected, h.compress(List.of(words)).internalsToString());
  }

  @Test
  public void testSuggestionOrderStabilityOnDictionaryEditing() throws IOException, ParseException {
    String original = "some_word";

    List<String> words = new ArrayList<>();
    for (char c = 0; c < 65535; c++) {
      if (Character.isLetter(c)) {
        words.add(original + c);
      }
    }

    String smallDict = "1\n" + String.join("\n", words.subList(0, words.size() / 4));
    String largerDict = "1\n" + String.join("\n", words);
    Dictionary small =
        loadDictionary(
            false,
            new ByteArrayInputStream(new byte[0]),
            new ByteArrayInputStream(smallDict.getBytes(StandardCharsets.UTF_8)));
    Dictionary larger =
        loadDictionary(
            false,
            new ByteArrayInputStream(new byte[0]),
            new ByteArrayInputStream(largerDict.getBytes(StandardCharsets.UTF_8)));

    assertFalse(new Hunspell(small).spell(original));

    List<String> smallSug = loadNoTimeout(small).suggest(original);
    List<String> largerSug = loadNoTimeout(larger).suggest(original);
    assertEquals(smallSug.toString(), 4, smallSug.size());
    assertEquals(smallSug, largerSug);
  }
}
