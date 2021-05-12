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
package org.apache.lucene.analysis.no;

import java.io.IOException;
import java.util.EnumSet;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.ScandinavianNormalizationFilter;
import org.apache.lucene.analysis.miscellaneous.ScandinavianNormalizer;
import org.apache.lucene.analysis.miscellaneous.ScandinavianNormalizer.Foldings;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

/**
 * This filter normalize use of the interchangeable Scandinavian characters æÆäÄöÖøØ and folded
 * variants (ae, oe, aa) by transforming them to åÅæÆøØ. This is similar to
 * ScandinavianNormalizationFilter, except for the folding rules customized for Norwegian.
 *
 * <p>blåbærsyltetøj == blåbärsyltetöj == blaabaersyltetoej
 *
 * @see ScandinavianNormalizationFilter
 */
public final class NorwegianNormalizationFilter extends TokenFilter {
  private final ScandinavianNormalizer normalizer;

  public NorwegianNormalizationFilter(TokenStream input) {
    super(input);
    this.normalizer = new ScandinavianNormalizer(EnumSet.of(Foldings.AE, Foldings.OE, Foldings.AA));
  }

  private final CharTermAttribute charTermAttribute = addAttribute(CharTermAttribute.class);

  @Override
  public final boolean incrementToken() throws IOException {
    if (!input.incrementToken()) {
      return false;
    }
    charTermAttribute.setLength(
        normalizer.processToken(charTermAttribute.buffer(), charTermAttribute.length()));
    return true;
  }
}
