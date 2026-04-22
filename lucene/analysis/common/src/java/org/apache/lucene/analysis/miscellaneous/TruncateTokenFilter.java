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
package org.apache.lucene.analysis.miscellaneous;

import static java.lang.Character.isHighSurrogate;
import static java.lang.Character.isLowSurrogate;

import java.io.IOException;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.KeywordAttribute;

/**
 * A token filter for truncating the terms into a specific length (number of codepoints). Fixed
 * prefix truncation, as a stemming method, produces good results on Turkish language. It is
 * reported that F5, using first 5 characters, produced best results in <a
 * href="https://doi.org/10.1002/asi.20750">Information Retrieval on Turkish Texts</a>
 *
 * <p>Since Lucene 10.5, the filter is able to correctly handle codepoints and truncates after the
 * given number of codepoints, no longer producing incomplete surrogate pairs. Use the modern
 * factory method {@link #truncateAfterCodePoints(TokenStream, int)} to enable this mode. Legacy
 * behaviour is still available with {@link #truncateAfterChars(TokenStream, int)}
 */
public final class TruncateTokenFilter extends TokenFilter {

  private final CharTermAttribute termAttribute = addAttribute(CharTermAttribute.class);
  private final KeywordAttribute keywordAttr = addAttribute(KeywordAttribute.class);

  private final int truncateAfter;
  private final boolean useCodePoints;

  /** Returns a filter with a prefix of {@code nCodePoints}. */
  public static TruncateTokenFilter truncateAfterCodePoints(TokenStream input, int nCodePoints) {
    return new TruncateTokenFilter(input, nCodePoints, true);
  }

  /**
   * Returns a filter with a prefix of {@code nChars} Java Characters. This may split surrogate
   * pairs.
   */
  public static TruncateTokenFilter truncateAfterChars(TokenStream input, int nChars) {
    return new TruncateTokenFilter(input, nChars, false);
  }

  /**
   * Instantiates filter with a prefix of {@code nChars} Java Characters. This may split surrogate
   * pairs.
   *
   * @deprecated This constructor is deprecated, use {@link #truncateAfterChars(TokenStream, int)}
   *     for backwards compatibility, or {@link #truncateAfterCodePoints(TokenStream, int)} to be
   *     unicode conformant.
   */
  @Deprecated
  public TruncateTokenFilter(TokenStream input, int nChars) {
    this(input, nChars, false);
  }

  private TruncateTokenFilter(TokenStream input, int truncateAfter, boolean useCodePoints) {
    super(input);
    if (truncateAfter < 1) {
      throw new IllegalArgumentException(
          "truncateAfter parameter must be a positive number: " + truncateAfter);
    }
    this.truncateAfter = truncateAfter;
    this.useCodePoints = useCodePoints;
  }

  @Override
  public final boolean incrementToken() throws IOException {
    if (!input.incrementToken()) {
      return false;
    }
    if (keywordAttr.isKeyword()) {
      return true;
    }
    final int len = termAttribute.length();
    if (len <= truncateAfter) {
      // the term is short enough, so we do not need to modify it
      // (works for both chars and codepoints)
      return true;
    }
    if (useCodePoints) {
      // code based on ICU4J's com.ibm.icu.text.UTF16#findOffsetFromCodePoint(...) implementation:
      final char[] arr = termAttribute.buffer();
      int ofs = 0, remaining = truncateAfter;
      while (ofs < len && remaining > 0) {
        if (isHighSurrogate(arr[ofs++]) && ofs < len && isLowSurrogate(arr[ofs])) {
          ofs++;
        }
        remaining--;
      }
      // check if we actually reached the limit and set new length based on calculated offset:
      if (remaining == 0) {
        termAttribute.setLength(ofs);
      }
    } else {
      termAttribute.setLength(truncateAfter);
    }
    return true;
  }
}
