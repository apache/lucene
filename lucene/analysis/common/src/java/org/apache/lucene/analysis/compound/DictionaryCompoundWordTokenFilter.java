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
package org.apache.lucene.analysis.compound;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;

/**
 * A {@link org.apache.lucene.analysis.TokenFilter} that decomposes compound words found in many
 * Germanic languages.
 *
 * <p>"Donaudampfschiff" becomes Donau, dampf, schiff so that you can find "Donaudampfschiff" even
 * when you only enter "schiff". It uses a brute-force algorithm to achieve this.
 */
public class DictionaryCompoundWordTokenFilter extends CompoundWordTokenFilterBase {

  private boolean onlyLongestMatchNoSubwords = false;

  /**
   * Creates a new {@link DictionaryCompoundWordTokenFilter}
   *
   * @param input the {@link org.apache.lucene.analysis.TokenStream} to process
   * @param dictionary the word dictionary to match against.
   */
  public DictionaryCompoundWordTokenFilter(TokenStream input, CharArraySet dictionary) {
    super(input, dictionary);
    if (dictionary == null) {
      throw new IllegalArgumentException("dictionary must not be null");
    }
  }

  /**
   * Creates a new {@link DictionaryCompoundWordTokenFilter}
   *
   * @param input the {@link org.apache.lucene.analysis.TokenStream} to process
   * @param dictionary the word dictionary to match against.
   * @param minWordSize only words longer than this get processed
   * @param minSubwordSize only subwords longer than this get to the output stream
   * @param maxSubwordSize only subwords shorter than this get to the output stream
   * @param onlyLongestMatchIgnoreSubwords Subwords are igored, e.g. if a word contains 'schwein',
   *     only the longer word 'schwein' will be extracted, the subword 'wein' will be ignored.
   *     Supersede parameter onlyLongestMatch
   */
  public DictionaryCompoundWordTokenFilter(
      TokenStream input,
      CharArraySet dictionary,
      int minWordSize,
      int minSubwordSize,
      int maxSubwordSize,
      boolean onlyLongestMatchIgnoreSubwords) {
    super(input, dictionary, minWordSize, minSubwordSize, maxSubwordSize, false);
    this.onlyLongestMatchNoSubwords = onlyLongestMatchIgnoreSubwords;

    if (dictionary == null) {
      throw new IllegalArgumentException("dictionary must not be null");
    }
  }

  @Override
  protected void decompose() {
    boolean onlyLongestMatch = this.onlyLongestMatch || onlyLongestMatchNoSubwords;
    final int len = termAtt.length();
    for (int i = 0; i <= len - this.minSubwordSize; ++i) {
      CompoundToken longestMatchToken = null;
      for (int j = this.minSubwordSize; j <= this.maxSubwordSize; ++j) {
        if (i + j > len) {
          break;
        }
        if (dictionary.contains(termAtt.buffer(), i, j)) {
          if (onlyLongestMatch) {
            if (longestMatchToken != null) {
              if (longestMatchToken.txt.length() < j) {
                longestMatchToken = new CompoundToken(i, j);
              }
            } else {
              longestMatchToken = new CompoundToken(i, j);
            }
          } else {
            tokens.add(new CompoundToken(i, j));
          }
        }
      }

      if (longestMatchToken != null) {
        tokens.add(longestMatchToken);
        if (onlyLongestMatchNoSubwords) {
          i += longestMatchToken.txt.length() - 1;
        }
      }
    }
  }
}
