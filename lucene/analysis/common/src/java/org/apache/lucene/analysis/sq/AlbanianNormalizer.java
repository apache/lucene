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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.util.StemmerUtil;
import org.apache.lucene.util.IOUtils;

/**
 * Normalizer for Albanian.
 *
 * <p>Standard Albanian orthography has two letters outside ASCII, <code>ë</code> and <code>ç
 * </code>. Both are very frequently written as plain <code>e</code> and <code>c</code> in informal
 * text, on keyboards without those keys, and in search queries, so the two spellings must index to
 * the same term. This normalizer folds:
 *
 * <ul>
 *   <li><code>ë</code> → <code>e</code>, and <code>ç</code> and <code>c</code> → <code>q</code>,
 *       whether precomposed or written as a base letter followed by a combining diaeresis / cedilla
 *       (text copied from PDFs and some web pages comes in that decomposed form)
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
 * <p><code>ç</code>, <code>c</code> and <code>q</code> become one class because that is how the
 * letter is typed: without the cedilla as <code>c</code>, or as <code>q</code>, which sounds the
 * same to most speakers (search logs of an Albanian site show <code>cdo</code> and <code>qdo
 * </code> far more often than <code>çdo</code>). The class is spelled <code>q</code> so that the
 * stemmer's palatal rules (<code>miq</code> / <code>mik</code>) keep working on folded text. A few
 * genuine pairs merge (<code>qaj</code> / <code>çaj</code>), the same trade-off as <code>ë</code> /
 * <code>e</code>.
 *
 * <p>Words that really have a <code>c</code> (<code>cili</code>, <code>caktuar</code>, <code>
 * shumica</code>, <code>kec</code>) keep it: they are listed, as whole words or stem prefixes, in
 * {@value #KEEP_C_FILE}, so <code>kec</code> stays apart from <code>keq</code> while <code>cdo
 * </code> still meets <code>çdo</code>.
 *
 * <p>The folding is applied to both indexed text and queries, so it also lets the {@link
 * AlbanianLightStemmer} work on a single spelling of every suffix.
 */
public class AlbanianNormalizer {

  /** The words that keep their c: {@value}, next to this class (see the file's header). */
  public static final String KEEP_C_FILE = "cwords.txt";

  private static final CharArraySet KEEP_C_WORDS;
  private static final CharArraySet KEEP_C_PREFIXES;

  static {
    CharArraySet words = new CharArraySet(16, false);
    CharArraySet prefixes = new CharArraySet(256, false);
    try (BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(
                IOUtils.requireResourceNonNull(
                    AlbanianNormalizer.class.getResourceAsStream(KEEP_C_FILE), KEEP_C_FILE),
                StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.strip();
        if (line.isEmpty() || line.startsWith("#")) {
          continue;
        }
        if (line.endsWith("*")) {
          prefixes.add(line.substring(0, line.length() - 1));
        } else {
          words.add(line);
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Unable to load " + KEEP_C_FILE, e);
    }
    KEEP_C_WORDS = CharArraySet.unmodifiableSet(words);
    KEEP_C_PREFIXES = CharArraySet.unmodifiableSet(prefixes);
  }

  /**
   * Whether the word really has a c (cili, caktuar, kec) and keeps it: it is, lowercased and with ë
   * read as e, one of the words in {@link #KEEP_C_FILE} or starts with one of its prefixes.
   */
  static boolean keepsC(char[] s, int len) {
    int first = -1;
    for (int i = 0; i < len; i++) {
      if (s[i] == 'c' || s[i] == 'C') {
        first = i;
        break;
      }
    }
    if (first < 0) {
      return false;
    }
    char[] key = new char[len];
    for (int i = 0; i < len; i++) {
      char ch = Character.toLowerCase(s[i]);
      key[i] = ch == 'ë' ? 'e' : ch;
    }
    if (KEEP_C_WORDS.contains(key, 0, len)) {
      return true;
    }
    for (int k = first + 1; k <= len; k++) {
      if (KEEP_C_PREFIXES.contains(key, 0, k)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Normalize an input buffer of Albanian text.
   *
   * @param s input buffer
   * @param len length of input buffer
   * @return length of input buffer after normalization
   */
  public int normalize(char[] s, int len) {
    boolean keepC = keepsC(s, len);
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
          // ç is typed as c (no cedilla) and as q (the same sound to most speakers): one class,
          // spelled q so the stemmer's palatal rules (miq / mik) still apply after folding
          s[i] = 'q';
          break;
        case 'c':
          // unless the word really has a c (cili, kec): those are listed in cwords.txt
          if (!keepC) {
            s[i] = 'q';
          }
          break;
        case 'Ç':
          s[i] = 'Q';
          break;
        case 'C':
          if (!keepC) {
            s[i] = 'Q';
          }
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
