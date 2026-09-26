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

import java.io.IOException;
import java.io.Reader;
import org.apache.lucene.analysis.charfilter.BaseCharFilter;

/**
 * A {@link org.apache.lucene.analysis.CharFilter} that normalizes Thai text at the character
 * stream level before tokenization.
 *
 * <p>Applying normalization before tokenization is critical for Thai, because typographic
 * anomalies (such as two consecutive Sara E characters instead of Sara Ae, or zero-width
 * characters) prevent the dictionary-based {@link java.text.BreakIterator} from locating word
 * boundaries, causing multiple adjacent words to be mistakenly merged into one token.
 *
 * <p>Normalizations performed:
 * <ul>
 *   <li>Removes Zero-Width characters (U+200B ZWSP, U+200C ZWNJ, U+200D ZWJ, U+FEFF BOM)
 *   <li>Replaces double Sara E (เ + เ, U+0E40 U+0E40) with Sara Ae (แ, U+0E41)
 *   <li>Recomposes decomposed Sara Am (U+0E4D + U+0E32 &rarr; U+0E33, and U+0E4D + tone + U+0E32 &rarr; tone + U+0E33)
 *   <li>Deduplicates consecutive repeated diacritics / tone marks (e.g. ดีี &rarr; ดี)
 *   <li>Reorders misplaced tone mark before an above/below vowel to canonical order
 *   <li>Replaces Lakkhangyao (ๅ, U+0E45) with Sara Aa (า, U+0E32) unless preceded by Ru (ฤ) or Lu (ฦ)
 * </ul>
 *
 * <p>All character deletions and contractions correctly update the character offset map
 * using {@link #addOffCorrectMap(int, int)}.
 *
 * @since 11.0.0
 */
public class ThaiCharFilter extends BaseCharFilter {

  private final char[] inputBuffer = new char[8];
  private int inBufLen = 0;
  private boolean eof = false;

  private int outputOff = 0;
  private int cumulativeDiff = 0;
  private char prevChar = 0;
  private char pendingOutputChar = 0;

  /**
   * Creates a new ThaiCharFilter wrapping the given {@link Reader}.
   *
   * @param in the input reader
   */
  public ThaiCharFilter(Reader in) {
    super(in);
  }

  @Override
  public int read() throws IOException {
    if (pendingOutputChar != 0) {
      char c = pendingOutputChar;
      pendingOutputChar = 0;
      prevChar = c;
      outputOff++;
      return c;
    }

    while (true) {
      fillBuffer();

      if (inBufLen == 0) {
        return -1;
      }

      char c0 = inputBuffer[0];

      // 1. Zero-width character removal (U+200B, U+200C, U+200D, U+FEFF)
      if (c0 == '\u200B' || c0 == '\u200C' || c0 == '\u200D' || c0 == '\uFEFF') {
        shiftBuffer(1);
        cumulativeDiff++;
        addOffCorrectMap(outputOff, cumulativeDiff);
        continue;
      }

      // 2. Deduplicate consecutive identical diacritics / tone marks
      if (isThaiDiacritic(c0) && c0 == prevChar) {
        shiftBuffer(1);
        cumulativeDiff++;
        addOffCorrectMap(outputOff, cumulativeDiff);
        continue;
      }

      // 3. Double Sara E (เ + เ -> แ)
      if (c0 == '\u0E40' && inBufLen >= 2 && inputBuffer[1] == '\u0E40') {
        shiftBuffer(2);
        cumulativeDiff++;
        addOffCorrectMap(outputOff + 1, cumulativeDiff);
        prevChar = '\u0E41';
        outputOff++;
        return '\u0E41';
      }

      // 4. Sara Am recomposition
      if (c0 == '\u0E4D') {
        // Subcase 4A: Nikhahit + Sara Aa (ํ + า -> ำ)
        if (inBufLen >= 2 && inputBuffer[1] == '\u0E32') {
          shiftBuffer(2);
          cumulativeDiff++;
          addOffCorrectMap(outputOff + 1, cumulativeDiff);
          prevChar = '\u0E33';
          outputOff++;
          return '\u0E33';
        }
        // Subcase 4B: Nikhahit + Tone + Sara Aa (ํ + tone + า -> tone + ำ)
        if (inBufLen >= 3 && isThaiTone(inputBuffer[1]) && inputBuffer[2] == '\u0E32') {
          char tone = inputBuffer[1];
          shiftBuffer(3);
          pendingOutputChar = '\u0E33';
          cumulativeDiff++;
          addOffCorrectMap(outputOff + 2, cumulativeDiff);
          prevChar = tone;
          outputOff++;
          return tone;
        }
      }

      // 5. Misplaced tone mark before above/below vowel: swap to canonical order
      if (isThaiTone(c0) && inBufLen >= 2 && isThaiAboveBelowVowel(inputBuffer[1])) {
        char vowel = inputBuffer[1];
        char tone = c0;
        inputBuffer[0] = vowel;
        inputBuffer[1] = tone;
        // Continue loop to process the swapped vowel at index 0
        continue;
      }

      // 6. Lakkhangyao (ๅ, U+0E45) -> Sara Aa (า, U+0E32) unless preceded by Ru (ฤ) or Lu (ฦ)
      if (c0 == '\u0E45') {
        if (prevChar != '\u0E24' && prevChar != '\u0E26') {
          c0 = '\u0E32';
        }
      }

      // Default: emit character c0
      shiftBuffer(1);
      prevChar = c0;
      outputOff++;
      return c0;
    }
  }

  @Override
  public int read(char[] cbuf, int off, int len) throws IOException {
    if (len <= 0) {
      return 0;
    }
    int numRead = 0;
    for (int i = off; i < off + len; i++) {
      int c = read();
      if (c == -1) {
        break;
      }
      cbuf[i] = (char) c;
      numRead++;
    }
    return numRead == 0 ? -1 : numRead;
  }

  private void fillBuffer() throws IOException {
    while (!eof && inBufLen < 4) {
      int ch = input.read();
      if (ch == -1) {
        eof = true;
        break;
      }
      inputBuffer[inBufLen++] = (char) ch;
    }
  }

  private void shiftBuffer(int count) {
    int remaining = inBufLen - count;
    if (remaining > 0) {
      System.arraycopy(inputBuffer, count, inputBuffer, 0, remaining);
    }
    inBufLen = remaining;
  }

  private static boolean isThaiDiacritic(char c) {
    return c == '\u0E31' || (c >= '\u0E34' && c <= '\u0E3A') || (c >= '\u0E47' && c <= '\u0E4E');
  }

  private static boolean isThaiTone(char c) {
    return (c >= '\u0E48' && c <= '\u0E4B') || c == '\u0E4C';
  }

  private static boolean isThaiAboveBelowVowel(char c) {
    return c == '\u0E31' || (c >= '\u0E34' && c <= '\u0E3A') || c == '\u0E47';
  }
}
