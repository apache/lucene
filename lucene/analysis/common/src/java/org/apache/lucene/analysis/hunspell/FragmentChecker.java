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
 * allows speeding up the "Modification" part of {@link Suggester} but avoiding expensive checks on
 * impossible words. Implementations may use character case, n-grams or whatever they like.
 *
 * @see NGramFragmentChecker
 */
public interface FragmentChecker {
  FragmentChecker EVERYTHING_POSSIBLE = (word, start, end) -> false;

  /**
   * Check if some substring of the word that intersects with the given offsets is impossible in the
   * current language. The implementation must be monotonic: if some range is considered impossible,
   * larger ranges encompassing it should also produce {@code true}.
   */
  boolean hasImpossibleFragmentAround(CharSequence word, int start, int end);
}
