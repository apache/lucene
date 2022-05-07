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
package org.apache.lucene.analysis.fa;

import static org.apache.lucene.analysis.util.StemmerUtil.*;

import java.util.Arrays;

/**
 * Stemmer for Persian.
 *
 * <p>Stemming is done in-place for efficiency, operating on a termbuffer.
 *
 * <p>Stemming is defined as:
 *
 * <ul>
 *   <li>Removal of attached definite article, conjunction, and prepositions.
 *   <li>Stemming of common suffixes.
 * </ul>
 */
public class PersianStemmer {
  private static final char ALEF = '\u0627';
  private static final char HEH = '\u0647';
  private static final char TEH = '\u062A';
  private static final char REH = '\u0631';
  private static final char NOON = '\u0646';
  private static final char YEH = '\u064A';
  private static final char ZWNJ = '\u200c'; // ZERO WIDTH NON-JOINER character

  private static final char[][] suffixes = {
    ("" + ALEF + TEH).toCharArray(),
    ("" + ALEF + NOON).toCharArray(),
    ("" + TEH + REH + YEH + NOON).toCharArray(),
    ("" + TEH + REH).toCharArray(),
    ("" + YEH + YEH).toCharArray(),
    ("" + YEH).toCharArray(),
    ("" + HEH + ALEF).toCharArray(),
    ("" + ZWNJ).toCharArray(),
  };

  /**
   * Stem an input buffer of Persian text.
   *
   * @param s input buffer
   * @param len length of input buffer
   * @return length of input buffer after normalization
   */
  public int stem(char[] s, int len) {
    len = stemSuffix(s, len);

    return len;
  }

  /**
   * Stem suffix(es) off a Persian word.
   *
   * @param s input buffer
   * @param len length of input buffer
   * @return new length of input buffer after stemming
   */
  private int stemSuffix(char[] s, int len) {
    for (char[] suffix : suffixes) {
      if (endsWithCheckLength(s, len, suffix)) {
        len = deleteN(s, len - suffix.length, len, suffix.length);
      }
    }

    return len;
  }

  /**
   * Returns true if the suffix matches and can be stemmed
   *
   * @param s input buffer
   * @param len length of input buffer
   * @param suffix suffix to check
   * @return true if the suffix matches and can be stemmed
   */
  private boolean endsWithCheckLength(char[] s, int len, char[] suffix) {
    if (len < suffix.length + 2) { // all suffixes require at least 2 characters after stemming
      return false;
    }

    return Arrays.equals(s, len - suffix.length, len, suffix, 0, suffix.length);
  }
}
