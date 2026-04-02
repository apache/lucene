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
 * <p>Since Lucene 10.5, the filter correctly handles codepoints and truncates after the given
 * number of codepoints, no longer producing incomplete surrogate pairs.
 */
public final class TruncateTokenFilter extends TokenFilter {

  private final CharTermAttribute termAttribute = addAttribute(CharTermAttribute.class);
  private final KeywordAttribute keywordAttr = addAttribute(KeywordAttribute.class);

  private final int maxCodePoints;

  public TruncateTokenFilter(TokenStream input, int maxCodePoints) {
    super(input);
    if (maxCodePoints < 1) {
      throw new IllegalArgumentException(
          "maxCodePoints parameter must be a positive number: " + maxCodePoints);
    }
    this.maxCodePoints = maxCodePoints;
  }

  @Override
  public final boolean incrementToken() throws IOException {
    if (!input.incrementToken()) {
      return false;
    }
    if (keywordAttr.isKeyword()) {
      return true;
    }
    if (termAttribute.length() <= maxCodePoints) {
      // the term is short enough in utf-16 chars, so we do not need to modify it
      return true;
    }
    try {
      // calculate the offset
      int truncateAtChar =
          Character.offsetByCodePoints(
              termAttribute.buffer(), 0, termAttribute.length(), 0, maxCodePoints);
      termAttribute.setLength(truncateAtChar);
    } catch (IndexOutOfBoundsException _) {
      // the term attribute is shorter than the calculated length
    }
    return true;
  }
}
