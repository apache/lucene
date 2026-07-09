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
package org.apache.lucene.analysis;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.internal.hppc.LongHashSet;

/**
 * Removes stop words from a token stream.
 *
 * <p>When all stop words are short ASCII (up to 8 chars), packs them into longs and uses a {@link
 * LongHashSet} for O(1) lookup without pointer chasing. Otherwise falls back to {@link
 * CharArraySet}.
 */
public class StopFilter extends FilteringTokenFilter {

  private static final int MAX_PACK_LEN = 8;

  private final LongHashSet packedStopWords;
  private final CharArraySet fallback;
  private final boolean ignoreCase;
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

  /**
   * Constructs a filter which removes words from the input TokenStream that are named in the Set.
   *
   * @param in Input stream
   * @param stopWords A {@link CharArraySet} representing the stopwords.
   * @see #makeStopSet(java.lang.String...)
   */
  public StopFilter(TokenStream in, CharArraySet stopWords) {
    super(in);
    Objects.requireNonNull(stopWords, "stopWords");
    this.ignoreCase = stopWords.getIgnoreCase();
    long[] packed = tryPackAll(stopWords);
    this.packedStopWords = packed != null ? toLongHashSet(packed) : null;
    this.fallback = packed != null ? null : stopWords;
  }

  /**
   * Builds a Set from an array of stop words, appropriate for passing into the StopFilter
   * constructor. This permits this stopWords construction to be cached once when an Analyzer is
   * constructed.
   *
   * @param stopWords An array of stopwords
   * @see #makeStopSet(java.lang.String[], boolean) passing false to ignoreCase
   */
  public static CharArraySet makeStopSet(String... stopWords) {
    return makeStopSet(stopWords, false);
  }

  /**
   * Builds a Set from an array of stop words, appropriate for passing into the StopFilter
   * constructor. This permits this stopWords construction to be cached once when an Analyzer is
   * constructed.
   *
   * @param stopWords A List of Strings or char[] or any other toString()-able list representing the
   *     stopwords
   * @return A Set ({@link CharArraySet}) containing the words
   * @see #makeStopSet(java.lang.String[], boolean) passing false to ignoreCase
   */
  public static CharArraySet makeStopSet(List<?> stopWords) {
    return makeStopSet(stopWords, false);
  }

  /**
   * Creates a stopword set from the given stopword array.
   *
   * @param stopWords An array of stopwords
   * @param ignoreCase If true, all words are lower cased first.
   * @return a Set containing the words
   */
  public static CharArraySet makeStopSet(String[] stopWords, boolean ignoreCase) {
    return makeStopSet(Arrays.asList(Objects.requireNonNull(stopWords, "stopWords")), ignoreCase);
  }

  /**
   * Creates a stopword set from the given stopword list.
   *
   * @param stopWords A List of Strings or char[] or any other toString()-able list representing the
   *     stopwords
   * @param ignoreCase if true, all words are lower cased first
   * @return A Set ({@link CharArraySet}) containing the words
   */
  public static CharArraySet makeStopSet(List<?> stopWords, boolean ignoreCase) {
    Objects.requireNonNull(stopWords, "stopWords");
    CharArraySet stopSet = new CharArraySet(stopWords.size(), ignoreCase);
    stopSet.addAll(stopWords);
    return stopSet;
  }

  @Override
  protected boolean accept() {
    if (fallback != null) {
      return !fallback.contains(termAtt.buffer(), 0, termAtt.length());
    }
    long packed = pack(termAtt.buffer(), 0, termAtt.length(), ignoreCase);
    return packed < 0 || !packedStopWords.contains(packed);
  }

  /**
   * Tries to pack all stop words into longs. Returns null if any word is non-ASCII or longer than 8
   * chars. Keys from case-insensitive sets are already stored lowercased by CharArrayMap.
   */
  private static long[] tryPackAll(CharArraySet stopWords) {
    long[] result = new long[stopWords.size()];
    int i = 0;
    for (Object obj : stopWords) {
      char[] word = (char[]) obj;
      long packed = pack(word, 0, word.length, false);
      if (packed < 0) return null;
      result[i++] = packed;
    }
    return result;
  }

  private static LongHashSet toLongHashSet(long[] keys) {
    var set = new LongHashSet(keys.length);
    for (long key : keys) {
      set.add(key);
    }
    return set;
  }

  static long pack(char[] buf, int off, int len, boolean toLowerCase) {
    if (len > MAX_PACK_LEN || len == 0) return -1;
    long packed = 0;
    for (int i = 0; i < len; i++) {
      char c = buf[off + i];
      if (c > 127) return -1;
      if (toLowerCase && c >= 'A' && c <= 'Z') c = (char) (c | 0x20);
      packed = (packed << 8) | c;
    }
    return packed;
  }
}
