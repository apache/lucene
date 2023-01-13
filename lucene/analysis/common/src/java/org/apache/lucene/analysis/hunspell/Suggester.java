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
import static org.apache.lucene.analysis.hunspell.TimeoutPolicy.NO_TIMEOUT;
import static org.apache.lucene.analysis.hunspell.WordContext.COMPOUND_BEGIN;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.lucene.util.CharsRef;

/**
 * A generator for misspelled word corrections based on Hunspell flags. The suggestions are searched
 * for in two main ways:
 *
 * <ol>
 *   <li>Modification: trying to insert/remove/delete/swap parts of the word to get something
 *       acceptable. The performance of this part depends heavily on the contents of TRY, MAP, REP,
 *       KEY directives in the .aff file. To speed up this part, consider using {@link
 *       #withFragmentChecker}.
 *   <li>Enumeration: if the modification hasn't produced "good enough" suggestions, the whole
 *       dictionary is scanned and simple affixes are added onto the entries to check if that
 *       produces anything similar to the given misspelled word. This depends on the dictionary size
 *       and the affix count, and it can take noticeable amount of time. To speed this up, {@link
 *       #withSuggestibleEntryCache()} can be used.
 * </ol>
 */
public class Suggester {
  private final Dictionary dictionary;
  private final SuggestibleEntryCache suggestibleCache;
  private final FragmentChecker fragmentChecker;

  public Suggester(Dictionary dictionary) {
    this(dictionary, null, FragmentChecker.EVERYTHING_POSSIBLE);
  }

  private Suggester(
      Dictionary dictionary, SuggestibleEntryCache suggestibleCache, FragmentChecker checker) {
    this.dictionary = dictionary;
    this.suggestibleCache = suggestibleCache;
    this.fragmentChecker = checker;
  }

  /**
   * Returns a copy of this suggester instance with better "Enumeration" phase performance (see
   * {@link Suggester} documentation), but using more memory. With this option, the dictionary
   * entries are stored as fast-to-iterate plain words instead of highly compressed prefix trees.
   */
  public Suggester withSuggestibleEntryCache() {
    return new Suggester(
        dictionary, SuggestibleEntryCache.buildCache(dictionary.words), fragmentChecker);
  }

  /**
   * Returns a copy of this suggester instance with {@link FragmentChecker} hint that can improve
   * the performance of the "Modification" phase performance.
   */
  public Suggester withFragmentChecker(FragmentChecker checker) {
    return new Suggester(dictionary, suggestibleCache, checker);
  }

  /**
   * Compute suggestions for the given misspelled word
   *
   * @param word the misspelled word to calculate suggestions for
   * @param checkCanceled an object that's periodically called, allowing to interrupt or suggestion
   *     generation by throwing an exception
   */
  public List<String> suggestNoTimeout(String word, Runnable checkCanceled) {
    LinkedHashSet<Suggestion> suggestions = new LinkedHashSet<>();
    return suggest(word, suggestions, handleCustomTimeoutException(checkCanceled, suggestions));
  }

  private Runnable handleCustomTimeoutException(
      Runnable checkCanceled, LinkedHashSet<Suggestion> suggestions) {
    return () -> {
      try {
        checkCanceled.run();
      } catch (SuggestionTimeoutException e) {
        if (e.getPartialResult() != null) {
          throw e;
        }

        throw new SuggestionTimeoutException(e.getMessage(), postprocess(suggestions));
      }
    };
  }

  /**
   * @param word the misspelled word to calculate suggestions for
   * @param timeLimitMs the duration limit in milliseconds after which the computation is interruped
   *     by an exception
   * @param checkCanceled an object that's periodically called, allowing to interrupt or suggestion
   *     generation by throwing an exception
   * @throws SuggestionTimeoutException if the computation takes too long. Use {@link
   *     SuggestionTimeoutException#getPartialResult()} to get the suggestions computed up to that
   *     point
   */
  public List<String> suggestWithTimeout(String word, long timeLimitMs, Runnable checkCanceled)
      throws SuggestionTimeoutException {
    LinkedHashSet<Suggestion> suggestions = new LinkedHashSet<>();
    Runnable checkTime = checkTimeLimit(word, suggestions, timeLimitMs, checkCanceled);
    return suggest(word, suggestions, handleCustomTimeoutException(checkTime, suggestions));
  }

  private List<String> suggest(
      String word, LinkedHashSet<Suggestion> suggestions, Runnable checkCanceled)
      throws SuggestionTimeoutException {
    checkCanceled.run();
    if (word.length() >= 100) return Collections.emptyList();

    if (dictionary.needsInputCleaning(word)) {
      word = dictionary.cleanInput(word, new StringBuilder()).toString();
    }

    Hunspell suggestionSpeller =
        new Hunspell(dictionary, NO_TIMEOUT, checkCanceled) {
          // Cache for expensive "findStem" requests issued when trying to split a compound word.
          // The suggestion algorithm issues many of them, often with the same text.
          // The cache can be large, but will be GC-ed after the "suggest" call.
          final Map<String, Optional<Root<CharsRef>>> compoundCache = new HashMap<>();

          @Override
          boolean acceptsStem(int formID) {
            return !dictionary.hasFlag(formID, dictionary.noSuggest)
                && !dictionary.hasFlag(formID, dictionary.subStandard);
          }

          @Override
          Root<CharsRef> findStem(
              char[] chars, int offset, int length, WordCase originalCase, WordContext context) {
            if (context == COMPOUND_BEGIN && originalCase == null) {
              return compoundCache
                  .computeIfAbsent(
                      new String(chars, offset, length),
                      __ ->
                          Optional.ofNullable(super.findStem(chars, offset, length, null, context)))
                  .orElse(null);
            }
            return super.findStem(chars, offset, length, originalCase, context);
          }
        };

    WordCase wordCase = WordCase.caseOf(word);
    if (dictionary.forceUCase != FLAG_UNSET && wordCase == WordCase.LOWER) {
      String title = dictionary.toTitleCase(word);
      if (suggestionSpeller.spell(title)) {
        return Collections.singletonList(title);
      }
    }

    boolean hasGoodSuggestions =
        new ModifyingSuggester(suggestionSpeller, suggestions, word, wordCase, fragmentChecker)
            .suggest();

    if (!hasGoodSuggestions && dictionary.maxNGramSuggestions > 0) {
      List<String> generated =
          new GeneratingSuggester(suggestionSpeller, suggestibleCache)
              .suggest(dictionary.toLowerCase(word), wordCase, suggestions);
      for (String raw : generated) {
        suggestions.add(new Suggestion(raw, word, wordCase, suggestionSpeller));
      }
    }

    if (word.contains("-") && suggestions.stream().noneMatch(s -> s.raw.contains("-"))) {
      for (String raw : modifyChunksBetweenDashes(word, suggestionSpeller, checkCanceled)) {
        suggestions.add(new Suggestion(raw, word, wordCase, suggestionSpeller));
      }
    }
    return postprocess(suggestions);
  }

  private Runnable checkTimeLimit(
      String word, Set<Suggestion> suggestions, long timeLimitMs, Runnable checkCanceled) {
    return new Runnable() {
      final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeLimitMs);
      int invocationCounter = 100;

      @Override
      public void run() {
        checkCanceled.run();
        if (--invocationCounter <= 0) {
          if (System.nanoTime() - deadline > 0) {
            stop();
          }
          invocationCounter = 100;
        }
      }

      private void stop() {
        String message = "Time limit of " + timeLimitMs + "ms exceeded for " + word;
        throw new SuggestionTimeoutException(message, postprocess(suggestions));
      }
    };
  }

  private List<String> postprocess(Collection<Suggestion> suggestions) {
    return suggestions.stream()
        .flatMap(s -> Arrays.stream(s.result))
        .distinct()
        .collect(Collectors.toList());
  }

  private List<String> modifyChunksBetweenDashes(
      String word, Hunspell speller, Runnable checkCanceled) {
    List<String> result = new ArrayList<>();
    int chunkStart = 0;
    while (chunkStart < word.length()) {
      int chunkEnd = word.indexOf('-', chunkStart);
      if (chunkEnd < 0) {
        chunkEnd = word.length();
      }

      if (chunkEnd > chunkStart) {
        String chunk = word.substring(chunkStart, chunkEnd);
        if (!speller.spell(chunk)) {
          for (String chunkSug : suggestNoTimeout(chunk, checkCanceled)) {
            String replaced = word.substring(0, chunkStart) + chunkSug + word.substring(chunkEnd);
            if (speller.spell(replaced)) {
              result.add(replaced);
            }
          }
        }
      }

      chunkStart = chunkEnd + 1;
    }
    return result;
  }
}
