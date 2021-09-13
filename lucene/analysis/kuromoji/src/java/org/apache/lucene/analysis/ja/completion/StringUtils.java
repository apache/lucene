package org.apache.lucene.analysis.ja.completion;

import org.apache.lucene.util.CharsRef;

public class StringUtils {
  public static boolean isLowercaseAlphabets(String s) {
    for (int i = 0; i < s.length(); i++) {
      char ch = s.charAt(i);
      if (!(isHalfWidthLowercaseAlphabet(ch) || isFullWidthLowercaseAlphabet(ch))) {
        return false;
      }
    }
    return true;
  }

  public static boolean isKana(String s) {
    for (int i = 0; i < s.length(); i++) {
      char ch = s.charAt(i);
      if (!(isHiragana(ch) || isKatakana(ch))) {
        return false;
      }
    }
    return true;
  }

  public static boolean isHiragana(String s) {
    for (int i = 0; i < s.length(); i++) {
      char ch = s.charAt(i);
      if (!isHiragana(ch)) {
        return false;
      }
    }
    return true;
  }

  public static boolean isKatakana(String s) {
    for (int i = 0; i < s.length(); i++) {
      char ch = s.charAt(i);
      if (!isKatakana(ch)) {
        return false;
      }
    }
    return true;
  }

  public static boolean isKatakanaOrHWAlphabets(String s) {
    for (int i = 0; i < s.length(); i++) {
      char ch = s.charAt(i);
      if (!isKatakana(ch) && !isHalfWidthLowercaseAlphabet(ch)) {
        return false;
      }
    }
    return true;
  }

  public static boolean isKatakanaOrHWAlphabets(CharsRef ref) {
    for (int i = 0; i < ref.length(); i++) {
      char ch = ref.charAt(i);
      if (!isKatakana(ch) && !isHalfWidthLowercaseAlphabet(ch)) {
        return false;
      }
    }
    return true;
  }

  public static boolean isHiragana(char ch) {
    return ch >= 0x3040 && ch <= 0x309f;
  }

  public static boolean isKatakana(char ch) {
    return ch >= 0x30a0 && ch <= 0x30ff;
  }

  public static boolean isHalfWidthLowercaseAlphabet(char ch) {
    return ch >= 0x61 && ch <= 0x7a;
  }

  public static boolean isFullWidthLowercaseAlphabet(char ch) {
    return ch >= 0xff41 && ch <= 0xff5a;
  }

  public static String toKatakana(String s) {
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
}
