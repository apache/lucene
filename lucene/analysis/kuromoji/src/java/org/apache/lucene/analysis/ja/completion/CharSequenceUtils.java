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
package org.apache.lucene.analysis.ja.completion;

/** Utility functions for {@link org.apache.lucene.analysis.ja.JapaneseCompletionFilter} */
public class CharSequenceUtils {

  /** Checks if a char sequence is composed only of lowercase alphabets */
  public static boolean isLowercaseAlphabets(CharSequence s) {
    for (int i = 0; i < s.length(); i++) {
      char ch = s.charAt(i);
      if (!(isHalfWidthLowercaseAlphabet(ch) || isFullWidthLowercaseAlphabet(ch))) {
        return false;
      }
    }
    return true;
  }

  /** Checks if a char sequence is composed only of Katakana or hiragana */
  public static boolean isKana(CharSequence s) {
    for (int i = 0; i < s.length(); i++) {
      char ch = s.charAt(i);
      if (!(isHiragana(ch) || isKatakana(ch))) {
        return false;
      }
    }
    return true;
  }

  /** Checks if a char sequence is composed only of Katakana or lowercase alphabets */
  public static boolean isKatakanaOrHWAlphabets(CharSequence ref) {
    for (int i = 0; i < ref.length(); i++) {
      char ch = ref.charAt(i);
      if (!isKatakana(ch) && !isHalfWidthLowercaseAlphabet(ch)) {
        return false;
      }
    }
    return true;
  }

  /** Checks if a char is a Hiragana */
  private static boolean isHiragana(char ch) {
    return ch >= 0x3040 && ch <= 0x309f;
  }

  /** Checks if a char is a Katakana */
  private static boolean isKatakana(char ch) {
    return ch >= 0x30a0 && ch <= 0x30ff;
  }

  /** Checks if a char is a half-width lowercase alphabet */
  private static boolean isHalfWidthLowercaseAlphabet(char ch) {
    return ch >= 0x61 && ch <= 0x7a;
  }

  /** Checks if a char is a full-width lowercase alphabet */
  public static boolean isFullWidthLowercaseAlphabet(char ch) {
    return ch >= 0xff41 && ch <= 0xff5a;
  }

  /** Convert all hiragana in a string into kanataka */
  public static String toKatakana(CharSequence s) {
    char[] chars = new char[s.length()];
    for (int i = 0; i < s.length(); i++) {
      char ch = s.charAt(i);
      // if the character is from 'ぁ' to 'ゖ' or 'ゝ' or 'ゞ', can be converted to katakana.
      if (ch >= 0x3041 && ch <= 0x3096 || ch == 0x309d || ch == 0x309e) {
        chars[i] = (char) (ch + 0x60);
      } else {
        chars[i] = ch;
      }
    }
    return new String(chars);
  }

  private CharSequenceUtils() {}
}
