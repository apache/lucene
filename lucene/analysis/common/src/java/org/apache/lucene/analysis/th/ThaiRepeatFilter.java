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
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.util.CharsRefBuilder;

/**
 * A {@link TokenFilter} that handles Thai Maiyamok (ๆ, U+0E46), which denotes word repetition
 * (reduplication).
 *
 * <p>When a standalone Maiyamok token is encountered, it is replaced with the preceding term. When
 * a token with trailing Maiyamok is encountered, trailing Maiyamok characters are stripped and
 * duplicate token(s) are emitted.
 */
public final class ThaiRepeatFilter extends TokenFilter {

  /** Thai Maiyamok character (ๆ) */
  public static final char MAIYAMOK = '\u0E46';

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final PositionIncrementAttribute posIncAtt =
      addAttribute(PositionIncrementAttribute.class);

  private final CharsRefBuilder lastTerm = new CharsRefBuilder();
  private int pendingRepeats = 0;
  private int repeatIndex = 0;
  private int[] repeatStarts = new int[4];
  private int[] repeatEnds = new int[4];
  private State savedState;

  /**
   * Creates a new ThaiRepeatFilter.
   *
   * @param input the input {@link TokenStream}
   */
  public ThaiRepeatFilter(TokenStream input) {
    super(input);
  }

  @Override
  public boolean incrementToken() throws IOException {
    if (pendingRepeats > 0) {
      restoreState(savedState);
      termAtt.copyBuffer(lastTerm.chars(), 0, lastTerm.length());
      posIncAtt.setPositionIncrement(1);
      offsetAtt.setOffset(repeatStarts[repeatIndex], repeatEnds[repeatIndex]);
      repeatIndex++;
      pendingRepeats--;
      return true;
    }

    while (input.incrementToken()) {
      char[] buffer = termAtt.buffer();
      int len = termAtt.length();
      int origStart = offsetAtt.startOffset();
      int origEnd = offsetAtt.endOffset();

      // Case 1: Token consists entirely of Maiyamok characters (e.g. "ๆ" or "ๆๆ")
      if (isAllMaiyamok(buffer, len)) {
        if (lastTerm.length() == 0) {
          // No preceding token (dangling Maiyamok at stream start); discard
          continue;
        }
        int count = len;
        termAtt.copyBuffer(lastTerm.chars(), 0, lastTerm.length());
        if (count > 1) {
          int firstEnd = Math.min(origEnd, origStart + 1);
          offsetAtt.setOffset(origStart, firstEnd);
          ensureRepeatCapacity(count - 1);
          for (int i = 0; i < count - 1; i++) {
            int s = Math.min(origEnd, origStart + 1 + i);
            int e = (i == count - 2) ? origEnd : Math.min(origEnd, s + 1);
            repeatStarts[i] = s;
            repeatEnds[i] = e;
          }
          savedState = captureState();
          pendingRepeats = count - 1;
          repeatIndex = 0;
        }
        return true;
      }

      // Case 2: Token has trailing Maiyamok attached (e.g. "เร็วๆ")
      int maiyamokCount = 0;
      while (len - 1 - maiyamokCount >= 0 && buffer[len - 1 - maiyamokCount] == MAIYAMOK) {
        maiyamokCount++;
      }
      if (maiyamokCount > 0) {
        int baseLen = len - maiyamokCount;
        termAtt.setLength(baseLen);
        lastTerm.copyChars(termAtt.buffer(), 0, termAtt.length());

        int baseEnd = Math.max(origStart, origEnd - maiyamokCount);
        offsetAtt.setOffset(origStart, baseEnd);

        ensureRepeatCapacity(maiyamokCount);
        for (int i = 0; i < maiyamokCount; i++) {
          int s = Math.min(origEnd, baseEnd + i);
          int e = (i == maiyamokCount - 1) ? origEnd : Math.min(origEnd, s + 1);
          repeatStarts[i] = s;
          repeatEnds[i] = e;
        }

        savedState = captureState();
        pendingRepeats = maiyamokCount;
        repeatIndex = 0;
        return true;
      }

      // Normal token without Maiyamok
      lastTerm.copyChars(buffer, 0, len);
      return true;
    }

    return false;
  }

  private void ensureRepeatCapacity(int count) {
    if (count > repeatStarts.length) {
      repeatStarts = new int[count];
      repeatEnds = new int[count];
    }
  }

  private static boolean isAllMaiyamok(char[] buffer, int len) {
    if (len == 0) {
      return false;
    }
    for (int i = 0; i < len; i++) {
      if (buffer[i] != MAIYAMOK) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    lastTerm.clear();
    pendingRepeats = 0;
    repeatIndex = 0;
    savedState = null;
  }
}
