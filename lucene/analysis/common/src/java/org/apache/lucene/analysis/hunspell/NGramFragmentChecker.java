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

import java.util.BitSet;
import java.util.Collection;
import java.util.Locale;
import java.util.function.Consumer;

/**
 * A {@link FragmentChecker} based on all character n-grams possible in a certain language, keeping
 * them in a relatively memory-efficient, but probabilistic data structure. The n-gram length should
 * be 2, 3 or 4.
 *
 * @see #fromAllSimpleWords for enumerating the whole dictionary automatically
 * @see #fromWords for creating an instance from a precomputed set of all word forms or n-grams
 */
public class NGramFragmentChecker implements FragmentChecker {
  private final int n;
  private final BitSet hashes;

  private NGramFragmentChecker(int n, BitSet hashes) {
    if (n < 2 || n > 4) throw new IllegalArgumentException("N should be between 2 and 4: " + n);

    this.n = n;
    this.hashes = hashes;

    if (hashes.cardinality() > hashes.size() * 2 / 3) {
      throw new IllegalArgumentException(
          "Too many collisions, please report this to dev@lucene.apache.org");
    }
  }

  int hashCount() {
    return hashes.cardinality();
  }

  @Override
  public boolean hasImpossibleFragmentAround(CharSequence word, int start, int end) {
    if (word.length() < n) {
      return false;
    }
    int firstIntersectingStart = Math.max(0, start - n + 1);
    int lastIntersectingStart = Math.min(end - 1, word.length() - n);
    for (int i = firstIntersectingStart; i <= lastIntersectingStart; i++) {
      if (!hashes.get(Math.abs(lowCollisionHash(word, i, i + n) % hashes.size()))) {
        return true;
      }
    }
    return false;
  }

  private static int lowCollisionHash(CharSequence chars, int offset, int end) {
    int result = 0;
    for (int i = offset; i < end; i++) {
      result = 239 * result + chars.charAt(i);
    }
    return result;
  }

  /**
   * Iterate the whole dictionary, derive all word forms (using {@link WordFormGenerator}), vary the
   * case to get all words acceptable by the spellchecker, and create a fragment checker based on
   * their {@code n}-grams. Note that this enumerates only words derivable by suffixes and prefixes.
   * If the language has compounds, some n-grams possible via those compounds can be missed. In the
   * latter case, consider using {@link #fromWords}.
   *
   * @param n the length of n-grams
   * @param dictionary the dictionary to traverse
   * @param checkCanceled an object that's periodically called, allowing to interrupt the traversal
   *     by throwing an exception
   */
  public static NGramFragmentChecker fromAllSimpleWords(
      int n, Dictionary dictionary, Runnable checkCanceled) {
    BitSet hashes = new BitSet(1 << (7 + n * 3)); // some empirical numbers
    processNGrams(n, dictionary, checkCanceled, collectHashes(hashes));
    return new NGramFragmentChecker(n, hashes);
  }

  private static NGramConsumer collectHashes(BitSet hashes) {
    return (word, start, end) ->
        hashes.set(Math.abs(lowCollisionHash(word, start, end) % hashes.size()));
  }

  /**
   * Create a fragment checker for n-grams found in the given words. The words can be n-grams
   * themselves or full words of the language. The words are case-sensitive, so be sure to include
   * upper-case and title-case variants if they're accepted by the spellchecker.
   *
   * @param n the length of the ngrams to consider.
   * @param words the strings to extract n-grams from
   */
  public static NGramFragmentChecker fromWords(int n, Collection<? extends CharSequence> words) {
    BitSet hashes = new BitSet(Integer.highestOneBit(words.size()) * 4);
    NGramConsumer consumer = collectHashes(hashes);
    for (CharSequence word : words) {
      consumer.processNGrams(n, word);
    }
    return new NGramFragmentChecker(n, hashes);
  }

  /**
   * Traverse the whole dictionary, generate all word forms of its entries, and process all n-grams
   * in these word forms. No duplication removal is done, so the {@code consumer} should be prepared
   * to duplicate n-grams. The traversal order is undefined.
   *
   * @param n the length of the n-grams
   * @param dictionary the dictionary to traverse
   * @param checkCanceled an object that's periodically called, allowing to interrupt the traversal
   *     by throwing an exception
   * @param consumer the n-gram consumer to be called for each n-gram
   */
  public static void processNGrams(
      int n, Dictionary dictionary, Runnable checkCanceled, NGramConsumer consumer) {
    WordFormGenerator gen =
        new WordFormGenerator(dictionary) {
          @Override
          protected boolean canStemToOriginal(AffixedWord derived) {
            return true; // overgenerate a bit, but avoid very expensive checks
          }
        };

    gen.generateAllSimpleWords(
        new Consumer<>() {
          DictEntry lastEntry = null;
          WordCase lastEntryCase = null;

          @Override
          public void accept(AffixedWord aw) {
            String word = aw.getWord();
            consumer.processNGrams(n, word);
            if (shouldVaryCase(aw.getDictEntry())) {
              consumer.processNGrams(n, word.toUpperCase(Locale.ROOT));
              if (word.length() > 1) {
                String capitalized =
                    Character.toUpperCase(word.charAt(0))
                        + word.substring(1, Math.min(n, word.length()));
                consumer.processNGrams(n, capitalized);
              }
            }
          }

          private boolean shouldVaryCase(DictEntry entry) {
            if (entry != lastEntry) {
              lastEntry = entry;
              lastEntryCase = WordCase.caseOf(entry.getStem());
            }

            return lastEntryCase != WordCase.MIXED && lastEntryCase != WordCase.NEUTRAL;
          }
        },
        checkCanceled);
  }

  /** A callback for n-gram ranges in words */
  public interface NGramConsumer {
    void processNGram(CharSequence word, int start, int end);

    /** Call {@link #processNGram} for each fragment of the length {@code n} in the given word */
    default void processNGrams(int n, CharSequence word) {
      if (word.length() >= n) {
        for (int i = 0; i <= word.length() - n; i++) {
          processNGram(word, i, i + n);
        }
      }
    }
  }
}
