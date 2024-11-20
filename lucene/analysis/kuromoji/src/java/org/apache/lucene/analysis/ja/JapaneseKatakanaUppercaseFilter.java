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

import static org.apache.lucene.analysis.ja.JapaneseFilterUtil.createCharMap;

import java.io.IOException;
import java.util.Map;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.internal.hppc.CharObjectHashMap;

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
        createCharMap(
            Map.entry('ァ', 'ア'),
            Map.entry('ィ', 'イ'),
            Map.entry('ゥ', 'ウ'),
            Map.entry('ェ', 'エ'),
            Map.entry('ォ', 'オ'),
            Map.entry('ヵ', 'カ'),
            Map.entry('ㇰ', 'ク'),
            Map.entry('ヶ', 'ケ'),
            Map.entry('ㇱ', 'シ'),
            Map.entry('ㇲ', 'ス'),
            Map.entry('ッ', 'ツ'),
            Map.entry('ㇳ', 'ト'),
            Map.entry('ㇴ', 'ヌ'),
            Map.entry('ㇵ', 'ハ'),
            Map.entry('ㇶ', 'ヒ'),
            Map.entry('ㇷ', 'フ'),
            Map.entry('ㇸ', 'ヘ'),
            Map.entry('ㇹ', 'ホ'),
            Map.entry('ㇺ', 'ム'),
            Map.entry('ャ', 'ヤ'),
            Map.entry('ュ', 'ユ'),
            Map.entry('ョ', 'ヨ'),
            Map.entry('ㇻ', 'ラ'),
            Map.entry('ㇼ', 'リ'),
            Map.entry('ㇽ', 'ル'),
            Map.entry('ㇾ', 'レ'),
            Map.entry('ㇿ', 'ロ'),
            Map.entry('ヮ', 'ワ'));
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
    int newLength = termAttr.length();
    for (int from = 0, to = 0, length = newLength; from < length; from++, to++) {
      char c = termBuffer[from];
      if (c == 'ㇷ' && from + 1 < length && termBuffer[from + 1] == '゚') {
        // ㇷ゚detected, replace it by プ.
        termBuffer[to] = 'プ';
        from++;
        newLength--;
      } else {
        Character mappedChar = LETTER_MAPPINGS.get(c);
        termBuffer[to] = mappedChar == null ? c : mappedChar;
      }
    }
    termAttr.setLength(newLength);
    return true;
  }
}
