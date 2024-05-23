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
package org.apache.lucene.analysis.ja;

import java.io.IOException;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.hppc.CharObjectHashMap;

/**
 * A {@link TokenFilter} that normalizes small letters (捨て仮名) in katakana into normal letters. For
 * instance, "ストップウォッチ" will be translated to "ストツプウオツチ".
 *
 * <p>This filter is useful if you want to search against old style Japanese text such as patents,
 * legal, contract policies, etc.
 */
public final class JapaneseKatakanaUppercaseFilter extends TokenFilter {
  private static final CharObjectHashMap<Character> LETTER_MAPPINGS;

  static {
    // supported characters are:
    // ァ ィ ゥ ェ ォ ヵ ㇰ ヶ ㇱ ㇲ ッ ㇳ ㇴ ㇵ ㇶ ㇷ ㇷ゚ ㇸ ㇹ ㇺ ャ ュ ョ ㇻ ㇼ ㇽ ㇾ ㇿ ヮ
    LETTER_MAPPINGS =
        CharObjectHashMap.from(
            new char[] {
              'ァ', 'ィ', 'ゥ', 'ェ', 'ォ', 'ヵ', 'ㇰ', 'ヶ', 'ㇱ', 'ㇲ', 'ッ', 'ㇳ', 'ㇴ', 'ㇵ', 'ㇶ', 'ㇷ', 'ㇸ',
              'ㇹ', 'ㇺ', 'ャ', 'ュ', 'ョ', 'ㇻ', 'ㇼ', 'ㇽ', 'ㇾ', 'ㇿ', 'ヮ'
            },
            new Character[] {
              'ア', 'イ', 'ウ', 'エ', 'オ', 'カ', 'ク', 'ケ', 'シ', 'ス', 'ツ', 'ト', 'ヌ', 'ハ', 'ヒ', 'フ', 'ヘ',
              'ホ', 'ム', 'ヤ', 'ユ', 'ヨ', 'ラ', 'リ', 'ル', 'レ', 'ロ', 'ワ'
            });
  }

  private final CharTermAttribute termAttr = addAttribute(CharTermAttribute.class);

  public JapaneseKatakanaUppercaseFilter(TokenStream input) {
    super(input);
  }

  @Override
  public boolean incrementToken() throws IOException {
    if (!input.incrementToken()) {
      return false;
    }
    final char[] termBuffer = termAttr.buffer();
    for (int i = 0, length = termAttr.length(); i < length; i++) {
      if (termBuffer[i] == 'ㇷ' && i + 1 < length && termBuffer[i + 1] == '゚') {
        // ㇷ゚detected, replace it by プ.
        termBuffer[i] = 'プ';
        int remaining = length - (i + 2);
        if (remaining > 0) {
          System.arraycopy(termBuffer, i + 2, termBuffer, i + 1, remaining);
        }
        termAttr.setLength(--length);
      } else {
        Character c = LETTER_MAPPINGS.get(termBuffer[i]);
        if (c != null) {
          termBuffer[i] = c;
        }
      }
    }
    return true;
  }
}
