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
package org.apache.lucene.analysis.sq;

import org.apache.lucene.analysis.util.StemmerUtil;

/**
 * Normalizer for Albanian.
 *
 * <p>Standard Albanian orthography has two letters outside ASCII, <code>ë</code> and <code>ç
 * </code>. Both are very frequently written as plain <code>e</code> and <code>c</code> in informal
 * text, on keyboards without those keys, and in search queries, so the two spellings must index to
 * the same term. This normalizer folds:
 *
 * <ul>
 *   <li><code>ë</code> → <code>e</code>, <code>ç</code> → <code>c</code>, whether precomposed or
 *       written as a base letter followed by a combining diaeresis / cedilla (text copied from PDFs
 *       and some web pages comes in that decomposed form)
 *   <li>the circumflex and acute vowels of Gheg writing (<code>â ê î ô û</code> for nasal vowels,
 *       <code>á é í ó ú</code> for long ones, as in <code>âsht</code>, <code>nânë</code>, <code>
 *       Malcís</code>) → plain vowels
 *   <li>elided clitics written with an apostrophe are dropped (<code>ç'është</code> → <code>është
 *       </code>, <code>s'ka</code> → <code>ka</code>, <code>t'i</code> → <code>i</code>), so the
 *       remaining word can be matched against the stop list
 *   <li>other apostrophes inside a word are removed (<code>Kur'an</code> → <code>kuran</code>),
 *       covering U+0027, U+2019 and U+02BC which all occur in Albanian text
 * </ul>
 *
 * <p>The folding is applied to both indexed text and queries, so it also lets the {@link
 * AlbanianLightStemmer} work on a single spelling of every suffix.
 */
public class AlbanianNormalizer {

  /**
   * Normalize an input buffer of Albanian text.
   *
   * @param s input buffer
   * @param len length of input buffer
   * @return length of input buffer after normalization
   */
  public int normalize(char[] s, int len) {
    for (int i = 0; i < len; i++) {
      switch (s[i]) {
        case 'ë':
        case 'ê':
        case 'é':
          s[i] = 'e';
          break;
        case 'Ë':
        case 'Ê':
        case 'É':
          s[i] = 'E';
          break;
        case 'ç':
          s[i] = 'c';
          break;
        case 'Ç':
          s[i] = 'C';
          break;
        case 'â':
        case 'á':
          s[i] = 'a';
          break;
        case 'Â':
        case 'Á':
          s[i] = 'A';
          break;
        case 'î':
        case 'í':
          s[i] = 'i';
          break;
        case 'Î':
        case 'Í':
          s[i] = 'I';
          break;
        case 'ô':
        case 'ó':
          s[i] = 'o';
          break;
        case 'Ô':
        case 'Ó':
          s[i] = 'O';
          break;
        case 'û':
        case 'ú':
          s[i] = 'u';
          break;
        case 'Û':
        case 'Ú':
          s[i] = 'U';
          break;
        case '̈': // combining diaeresis (e + ̈ = ë)
        case '̧': // combining cedilla (c + ̧ = ç)
        case '̂': // combining circumflex (Gheg â ê î ô û)
        case '́': // combining acute (Gheg á é í ó ú)
          len = StemmerUtil.delete(s, i, len);
          i--;
          break;
        case '\'': // apostrophe
        case '’': // right single quotation mark
        case 'ʼ': // modifier letter apostrophe
          if (i == 1 && len > 2) {
            // a one-letter elided clitic: ç'është, s'ka, t'i, m'u, q'u
            len = StemmerUtil.deleteN(s, 0, len, 2);
            i = -1;
          } else {
            len = StemmerUtil.delete(s, i, len);
            i--;
          }
          break;
        default:
          break;
      }
    }
    return len;
  }
}
