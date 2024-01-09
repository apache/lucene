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
package org.apache.lucene.queryparser.flexible.standard.parser;

import java.util.Locale;
import org.apache.lucene.queryparser.flexible.core.messages.QueryParserMessages;
import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax;
import org.apache.lucene.queryparser.flexible.core.util.UnescapedCharSequence;
import org.apache.lucene.queryparser.flexible.messages.MessageImpl;

/** Implementation of {@link EscapeQuerySyntax} for the standard lucene syntax. */
public class EscapeQuerySyntaxImpl implements EscapeQuerySyntax {

  private static final char[] wildcardChars = {'*', '?'};

  private static final String[] escapableTermExtraFirstChars = {"+", "-", "@"};

  private static final String[] escapableTermChars = {
    "\"", "<", ">", "=", "!", "(", ")", "^", "[", "{", ":", "]", "}", "~", "/"
  };

  // TODO: check what to do with these "*", "?", "\\"
  private static final String[] escapableQuotedChars = {"\""};
  private static final String[] escapableWhiteChars = {" ", "\t", "\n", "\r", "\f", "\b", "\u3000"};
  private static final String[] escapableWordTokens = {
    "AND", "OR", "NOT", "TO", "WITHIN", "SENTENCE", "PARAGRAPH", "INORDER"
  };

  private static CharSequence escapeChar(CharSequence str, Locale locale) {
    if (str == null || str.isEmpty()) return str;

    CharSequence buffer = str;

    // regular escapable char for terms
    for (String escapableTermChar : escapableTermChars) {
      buffer = escapeIgnoringCase(buffer, escapableTermChar.toLowerCase(locale), "\\", locale);
    }

    // first char of a term as more escaping chars
    for (String escapableTermExtraFirstChar : escapableTermExtraFirstChars) {
      if (buffer.charAt(0) == escapableTermExtraFirstChar.charAt(0)) {
        buffer = "\\" + buffer;
        break;
      }
    }

    return buffer;
  }

  private static CharSequence escapeQuoted(CharSequence str, Locale locale) {
    if (str == null || str.isEmpty()) return str;

    CharSequence buffer = str;

    for (String escapableQuotedChar : escapableQuotedChars) {
      buffer = escapeIgnoringCase(buffer, escapableQuotedChar.toLowerCase(locale), "\\", locale);
    }
    return buffer;
  }

  private static CharSequence escapeTerm(CharSequence term, Locale locale) {
    if (term == null || term.isEmpty()) return term;

    // escape single chars
    term = escapeChar(term, locale);
    term = escapeWhiteChar(term, locale);

    // escape parser words
    for (String escapableWordToken : escapableWordTokens) {
      if (escapableWordToken.equalsIgnoreCase(term.toString())) return "\\" + term;
    }
    return term;
  }

  /**
   * Prepend every case-insensitive occurrence of the {@code sequence1} in the {@code string} with
   * the {@code escapeChar}. When the {@code sequence1} is empty, every character in the {@code
   * string} is escaped.
   *
   * @param string string to apply escaping to
   * @param sequence1 the old character sequence in lowercase
   * @param escapeChar the escape character to prefix sequence1 in the returned string
   * @return CharSequence with every occurrence of {@code sequence1} prepended with {@code
   *     escapeChar}
   */
  private static CharSequence escapeIgnoringCase(
      CharSequence string, CharSequence sequence1, CharSequence escapeChar, Locale locale) {
    if (escapeChar == null || sequence1 == null || string == null) throw new NullPointerException();

    int count = string.length();
    int sequence1Length = sequence1.length();

    // empty search string - escape every character
    if (sequence1Length == 0) {
      StringBuilder result = new StringBuilder(count * (1 + escapeChar.length()));
      for (int i = 0; i < count; i++) {
        result.append(escapeChar);
        result.append(string.charAt(i));
      }
      return result;
    }

    // normal case
    String lowercase = string.toString().toLowerCase(locale);
    StringBuilder result = new StringBuilder();
    char first = sequence1.charAt(0);
    int start = 0, copyStart = 0, firstIndex;
    while (start < count) {
      if ((firstIndex = lowercase.indexOf(first, start)) == -1) break;
      boolean found = true;
      if (sequence1.length() > 1) {
        if (firstIndex + sequence1Length > count) break;
        for (int i = 1; i < sequence1Length; i++) {
          if (lowercase.charAt(firstIndex + i) != sequence1.charAt(i)) {
            found = false;
            break;
          }
        }
      }
      if (found) {
        result.append(string, copyStart, firstIndex);
        result.append(escapeChar);
        result.append(string, firstIndex, firstIndex + sequence1Length);
        copyStart = start = firstIndex + sequence1Length;
      } else {
        start = firstIndex + 1;
      }
    }
    if (result.isEmpty() && copyStart == 0) return string;
    result.append(string, copyStart, string.length());
    return result;
  }

  /**
   * escape all tokens that are part of the parser syntax on a given string
   *
   * @param str string to get replaced
   * @param locale locale to be used when performing string compares
   * @return the new String
   */
  private static CharSequence escapeWhiteChar(CharSequence str, Locale locale) {
    if (str == null || str.isEmpty()) return str;

    CharSequence buffer = str;

    for (String escapableWhiteChar : escapableWhiteChars) {
      buffer = escapeIgnoringCase(buffer, escapableWhiteChar.toLowerCase(locale), "\\", locale);
    }
    return buffer;
  }

  @Override
  public CharSequence escape(CharSequence text, Locale locale, Type type) {
    if (text == null || text.isEmpty()) return text;

    // escape wildcards and the escape char (this has to be performed before anything else)
    // since we need to preserve the UnescapedCharSequence and escape the original escape chars
    if (text instanceof UnescapedCharSequence) {
      text = ((UnescapedCharSequence) text).toStringEscaped(wildcardChars);
    } else {
      text = new UnescapedCharSequence(text).toStringEscaped(wildcardChars);
    }

    if (type == Type.STRING) {
      return escapeQuoted(text, locale);
    } else {
      return escapeTerm(text, locale);
    }
  }

  /**
   * Returns a String where the escape char has been removed, or kept only once if there was a
   * double escape.
   *
   * <p>Supports escaped Unicode characters, e.g. translates {@code \u005Cu0041} to {@code A}.
   */
  public static UnescapedCharSequence discardEscapeChar(CharSequence input) throws ParseException {
    // Create char array to hold unescaped char sequence
    char[] output = new char[input.length()];
    boolean[] wasEscaped = new boolean[input.length()];

    // The length of the output can be less than the input
    // due to discarded escape chars. This variable holds
    // the actual length of the output
    int length = 0;

    // We remember whether the last processed character was
    // an escape character
    boolean lastCharWasEscapeChar = false;

    // The multiplier the current unicode digit must be multiplied with.
    // E.g. the first digit must be multiplied with 16^3, the second with 16^2...
    int codePointMultiplier = 0;

    // Used to calculate the codepoint of the escaped unicode character
    int codePoint = 0;

    for (int i = 0; i < input.length(); i++) {
      char curChar = input.charAt(i);
      if (codePointMultiplier > 0) {
        codePoint += hexToInt(curChar) * codePointMultiplier;
        codePointMultiplier >>>= 4;
        if (codePointMultiplier == 0) {
          output[length++] = (char) codePoint;
          codePoint = 0;
        }
      } else if (lastCharWasEscapeChar) {
        if (curChar == 'u') {
          // found an escaped unicode character
          codePointMultiplier = 16 * 16 * 16;
        } else {
          // this character was escaped
          output[length] = curChar;
          wasEscaped[length] = true;
          length++;
        }
        lastCharWasEscapeChar = false;
      } else {
        if (curChar == '\\') {
          lastCharWasEscapeChar = true;
        } else {
          output[length] = curChar;
          length++;
        }
      }
    }

    if (codePointMultiplier > 0) {
      throw new ParseException(
          new MessageImpl(QueryParserMessages.INVALID_SYNTAX_ESCAPE_UNICODE_TRUNCATION));
    }

    if (lastCharWasEscapeChar) {
      throw new ParseException(
          new MessageImpl(QueryParserMessages.INVALID_SYNTAX_ESCAPE_CHARACTER));
    }

    return new UnescapedCharSequence(output, wasEscaped, 0, length);
  }

  /** Returns the numeric value of the hexadecimal character */
  private static int hexToInt(char c) throws ParseException {
    if ('0' <= c && c <= '9') {
      return c - '0';
    } else if ('a' <= c && c <= 'f') {
      return c - 'a' + 10;
    } else if ('A' <= c && c <= 'F') {
      return c - 'A' + 10;
    } else {
      throw new ParseException(
          new MessageImpl(QueryParserMessages.INVALID_SYNTAX_ESCAPE_NONE_HEX_UNICODE, c));
    }
  }
}
