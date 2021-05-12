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

import static org.apache.lucene.analysis.miscellaneous.ScandinavianNormalizer.ALL_FOLDINGS;

import java.io.IOException;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

/**
 * This filter normalize use of the interchangeable Scandinavian characters æÆäÄöÖøØ and folded
 * variants (aa, ao, ae, oe and oo) by transforming them to åÅæÆøØ.
 *
 * <p>It's a semantically less destructive solution than {@link ScandinavianFoldingFilter}, most
 * useful when a person with a Norwegian or Danish keyboard queries a Swedish index and vice versa.
 * This filter does <b>not</b> the common Swedish folds of å and ä to a nor ö to o.
 *
 * <p>blåbærsyltetøj == blåbärsyltetöj == blaabaarsyltetoej but not blabarsyltetoj räksmörgås ==
 * ræksmørgås == ræksmörgaos == raeksmoergaas but not raksmorgas
 *
 * <p>There are also separate filters for Norwegian, Danish and Swedish with slightly differing
 * settings
 *
 * @see ScandinavianFoldingFilter
 */
public final class ScandinavianNormalizationFilter extends TokenFilter {

  private final ScandinavianNormalizer normalizer;

  public ScandinavianNormalizationFilter(TokenStream input) {
    super(input);
    this.normalizer = new ScandinavianNormalizer(ALL_FOLDINGS);
  }

  private final CharTermAttribute charTermAttribute = addAttribute(CharTermAttribute.class);

  @Override
  public boolean incrementToken() throws IOException {
    if (!input.incrementToken()) {
      return false;
    }
    charTermAttribute.setLength(
        normalizer.processToken(charTermAttribute.buffer(), charTermAttribute.length()));
    return true;
  }
}
