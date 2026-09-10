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
package org.apache.lucene.analysis.ko;

import java.io.IOException;
import java.io.Reader;
import org.apache.lucene.analysis.charfilter.BaseCharFilter;

/**
 * A {@link org.apache.lucene.analysis.CharFilter} that composes modern Hangul conjoining jamo into
 * precomposed Hangul syllables.
 *
 * <p>This filter only handles canonical modern Hangul syllable sequences:
 *
 * <ul>
 *   <li>leading consonants U+1100..U+1112
 *   <li>vowels U+1161..U+1175
 *   <li>optional trailing consonants U+11A8..U+11C2
 * </ul>
 *
 * <p>Compatibility jamo and archaic jamo are left unchanged. This is intended as an opt-in
 * normalization step before {@link KoreanTokenizer}, whose dictionary is built for precomposed
 * Hangul syllables.
 *
 * <p>This filter composes only full conjoining-jamo sequences (L, V, optional T). A precomposed LV
 * syllable followed by a conjoining trailing consonant (e.g., U+D558 하 + U+11AB), which full NFC
 * would compose into 한, is intentionally left unchanged; fully decomposed (NFD) input never
 * contains that shape.
 */
public final class HangulCompositionCharFilter extends BaseCharFilter {
  private static final int S_BASE = 0xAC00;
  private static final int L_BASE = 0x1100;
  private static final int V_BASE = 0x1161;
  private static final int T_BASE = 0x11A7;

  private static final int L_COUNT = 19;
  private static final int V_COUNT = 21;
  private static final int T_COUNT = 28;
  private static final int N_COUNT = V_COUNT * T_COUNT;

  private int pushbackChar = -1;
  private int outputOffset = 0;

  /** Default constructor that takes a {@link Reader}. */
  public HangulCompositionCharFilter(Reader input) {
    super(input);
  }

  @Override
  public int read() throws IOException {
    int ch = nextInputChar();
    if (ch == -1) {
      return -1;
    }

    int lIndex = leadingIndex(ch);
    if (lIndex < 0) {
      return emit(ch);
    }

    int v = nextInputChar();
    if (v == -1) {
      return emit(ch);
    }

    int vIndex = vowelIndex(v);
    if (vIndex < 0) {
      pushBack(v);
      return emit(ch);
    }

    int consumedChars = 2;
    int tIndex = 0;
    int t = nextInputChar();
    if (t != -1) {
      tIndex = trailingIndex(t);
      if (tIndex > 0) {
        consumedChars = 3;
      } else {
        pushBack(t);
        tIndex = 0;
      }
    }

    int syllable = S_BASE + (lIndex * N_COUNT) + (vIndex * T_COUNT) + tIndex;
    int cumulativeDiff = getLastCumulativeDiff() + consumedChars - 1;
    addOffCorrectMap(outputOffset + 1, cumulativeDiff);
    return emit(syllable);
  }

  @Override
  public int read(char[] cbuf, int off, int len) throws IOException {
    int numRead = 0;
    for (int i = off; i < off + len; i++) {
      int ch = read();
      if (ch == -1) {
        break;
      }
      cbuf[i] = (char) ch;
      numRead++;
    }
    return numRead == 0 ? -1 : numRead;
  }

  private int nextInputChar() throws IOException {
    if (pushbackChar != -1) {
      int ch = pushbackChar;
      pushbackChar = -1;
      return ch;
    }
    return input.read();
  }

  private void pushBack(int ch) {
    assert pushbackChar == -1;
    pushbackChar = ch;
  }

  private int emit(int ch) {
    outputOffset++;
    return ch;
  }

  private static int leadingIndex(int ch) {
    int index = ch - L_BASE;
    return index >= 0 && index < L_COUNT ? index : -1;
  }

  private static int vowelIndex(int ch) {
    int index = ch - V_BASE;
    return index >= 0 && index < V_COUNT ? index : -1;
  }

  private static int trailingIndex(int ch) {
    int index = ch - T_BASE;
    return index > 0 && index < T_COUNT ? index : -1;
  }
}
