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
package org.apache.lucene.analysis.th;

import static org.apache.lucene.analysis.util.StemmerUtil.delete;

/**
 * Normalizer for Thai.
 *
 * <p>Normalizes Thai text to handle common orthographic variations, typos, and encoding anomalies:
 *
 * <ul>
 *   <li>Removes zero-width characters (ZWSP U+200B, ZWNJ U+200C).
 *   <li>Replaces double Sara E (U+0E40 U+0E40) with Sara Ae (U+0E41).
 *   <li>Recomposes decomposed Sara Am (Nikhahit U+0E4D + optional tone mark + Sara Aa U+0E32) into
 *       canonical Sara Am (U+0E33).
 *   <li>Normalizes Lakkhangyao (U+0E45) to Sara Aa (U+0E32) unless preceded by Ru (U+0E24) or Lu
 *       (U+0E26).
 *   <li>Reorders misplaced tone marks (U+0E48-U+0E4B) or Thanthakhat (U+0E4C) with above/below
 *       vowels (U+0E31, U+0E34-U+0E3A, U+0E47) to canonical Unicode order (vowel before tone mark).
 *   <li>Reorders follow vowels (e.g. Sara Aa U+0E32, Sara Am U+0E33) and misplaced trailing tone
 *       marks.
 *   <li>Deduplicates repeated vowels, tone marks, and diacritics.
 *   <li>Removes dangling combining marks at the start of a token.
 * </ul>
 */
public class ThaiNormalizer {

  /**
   * Normalize an input buffer of Thai text.
   *
   * @param s input buffer
   * @param len length of input buffer
   * @return length of input buffer after normalization
   */
  public int normalize(char[] s, int len) {
    if (len == 0) {
      return 0;
    }

    // Step 1: Remove zero-width characters
    for (int i = 0; i < len; i++) {
      if (s[i] == '\u200B' || s[i] == '\u200C') {
        len = delete(s, i, len);
        i--;
      }
    }

    // Remove leading dangling non-base marks at token start
    while (len > 0 && isDanglingMark(s[0])) {
      len = delete(s, 0, len);
    }

    // Step 2: Handle compositions (Double Sara E -> Sara Ae, Sara Am recomposition, Lakkhangyao)
    for (int i = 0; i < len; i++) {
      char c = s[i];

      // Double Sara E -> Sara Ae
      if (c == '\u0E40' && i + 1 < len && s[i + 1] == '\u0E40') {
        s[i] = '\u0E41';
        len = delete(s, i + 1, len);
        continue;
      }

      // Nikhahit (U+0E4D) + optional tone mark + Sara Aa (U+0E32) -> optional tone mark + Sara Am
      // (U+0E33)
      if (c == '\u0E4D') {
        if (i + 1 < len && s[i + 1] == '\u0E32') {
          s[i] = '\u0E33';
          len = delete(s, i + 1, len);
          continue;
        } else if (i + 2 < len && isToneMark(s[i + 1]) && s[i + 2] == '\u0E32') {
          // Nikhahit + Tone + Sara Aa -> Tone + Sara Am
          char tone = s[i + 1];
          s[i] = tone;
          s[i + 1] = '\u0E33';
          len = delete(s, i + 2, len);
          continue;
        }
      }

      // Tone mark + Nikhahit + Sara Aa -> Tone + Sara Am
      if (isToneMark(c) && i + 2 < len && s[i + 1] == '\u0E4D' && s[i + 2] == '\u0E32') {
        s[i + 1] = '\u0E33';
        len = delete(s, i + 2, len);
        continue;
      }

      // Lakkhangyao (U+0E45) -> Sara Aa (U+0E32) unless preceded by Ru (U+0E24) or Lu (U+0E26)
      if (c == '\u0E45') {
        if (i == 0 || (s[i - 1] != '\u0E24' && s[i - 1] != '\u0E26')) {
          s[i] = '\u0E32';
        }
      }
    }

    // Step 3: Canonical reordering
    // Standard order: Consonant -> Above/Below Vowel -> Tone mark / Thanthakhat
    // Also: Follow vowel (Sara Aa / Sara Am) + Tone mark -> Tone mark + Follow vowel
    boolean changed = true;
    while (changed) {
      changed = false;
      for (int i = 0; i < len - 1; i++) {
        // Tone mark / Thanthakhat followed by Above/Below Vowel
        if (isToneMarkOrThanthakhat(s[i]) && isAboveOrBelowVowel(s[i + 1])) {
          char tmp = s[i];
          s[i] = s[i + 1];
          s[i + 1] = tmp;
          changed = true;
        }
        // Follow vowel followed by Tone mark (e.g., Sara Am + Tone -> Tone + Sara Am)
        else if (isFollowVowel(s[i]) && isToneMark(s[i + 1])) {
          char tmp = s[i];
          s[i] = s[i + 1];
          s[i + 1] = tmp;
          changed = true;
        }
      }
    }

    // Step 4: Deduplicate repeated vowels, tone marks, and diacritics
    for (int i = 0; i < len - 1; i++) {
      // Identical consecutive combining mark or vowel
      if (isDeduplicable(s[i]) && s[i] == s[i + 1]) {
        len = delete(s, i + 1, len);
        i--;
      }
      // If two different consecutive tone marks appear, keep the last one
      else if (isToneMark(s[i]) && isToneMark(s[i + 1])) {
        len = delete(s, i, len);
        i--;
      }
    }

    return len;
  }

  private static boolean isToneMark(char c) {
    return c >= '\u0E48' && c <= '\u0E4B';
  }

  private static boolean isToneMarkOrThanthakhat(char c) {
    return isToneMark(c) || c == '\u0E4C';
  }

  private static boolean isAboveOrBelowVowel(char c) {
    return c == '\u0E31' || (c >= '\u0E34' && c <= '\u0E3A') || c == '\u0E47';
  }

  private static boolean isFollowVowel(char c) {
    return c == '\u0E30' || c == '\u0E32' || c == '\u0E33' || c == '\u0E45';
  }

  private static boolean isDanglingMark(char c) {
    return isAboveOrBelowVowel(c) || isToneMarkOrThanthakhat(c) || c == '\u0E4D' || c == '\u0E4E';
  }

  private static boolean isDeduplicable(char c) {
    return (c >= '\u0E30' && c <= '\u0E3A') || c == '\u0E45' || (c >= '\u0E47' && c <= '\u0E4E');
  }
}
