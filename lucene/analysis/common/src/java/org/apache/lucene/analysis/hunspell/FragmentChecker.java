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

/**
 * An oracle for quickly checking that a specific part of a word can never be a valid word. This
 * allows speeding up the "Modification" part of {@link Suggester} by avoiding expensive checks on
 * impossible words. Implementations may use character case, n-grams or whatever they like.
 *
 * @see NGramFragmentChecker
 */
public interface FragmentChecker {
  FragmentChecker EVERYTHING_POSSIBLE = (word, start, end) -> false;

  /**
   * Check if the given word range intersects any fragment which is impossible in the current
   * language. For example, if the word is "aaax", and there are no "aaa" combinations in words
   * accepted by the spellchecker (but "aax" is valid), then {@code true} can be returned for all
   * ranges in {@code 0..3}, but not for {@code 3..4}.
   *
   * <p>The implementation must be monotonic: if some range is considered impossible, larger ranges
   * encompassing it should also produce {@code true}.
   *
   * @param word the whole word being checked for impossible substrings
   * @param start the start of the range in question, inclusive
   * @param end the end of the range in question, inclusive, not smaller than {@code start}
   */
  boolean hasImpossibleFragmentAround(CharSequence word, int start, int end);
}
