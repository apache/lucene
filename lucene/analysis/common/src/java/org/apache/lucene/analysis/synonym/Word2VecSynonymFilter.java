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

package org.apache.lucene.analysis.synonym;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.synonym.SynonymProvider.WeightedSynonym;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.search.BoostAttribute;

/**
 * Applies single-token synonyms from a Word2Vec trained network to an incoming {@link TokenStream}.
 *
 * @lucene.experimental
 */
public final class Word2VecSynonymFilter extends TokenFilter {

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final PositionIncrementAttribute posIncrAtt =
      addAttribute(PositionIncrementAttribute.class);
  private final PositionLengthAttribute posLenAtt = addAttribute(PositionLengthAttribute.class);
  private final BoostAttribute boostAtt = addAttribute(BoostAttribute.class);
  private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);

  private final SynonymProvider synonymProvider;
  private final LinkedList<WeightedSynonym> outputBuffer = new LinkedList<>();
  private State lastState = null;

  /**
   * Apply previously built synonymProvider to incoming tokens.
   *
   * @param input input tokenstream
   * @param synonymProvider synonym provider
   */
  public Word2VecSynonymFilter(TokenStream input, SynonymProvider synonymProvider) {
    super(input);
    this.synonymProvider = synonymProvider;
  }

  @Override
  public boolean incrementToken() throws IOException {

    if (!outputBuffer.isEmpty()) {
      // We still have pending outputs from a prior synonym match:
      releaseBufferedToken();
      return true;
    }

    if (input.incrementToken()) {
      String term = new String(termAtt.buffer(), 0, termAtt.length());
      List<WeightedSynonym> synonyms = this.synonymProvider.getSynonyms(term);
      if (synonyms.size() > 0) {
        this.lastState = captureState();
        this.outputBuffer.addAll(synonyms);
      }
      return true;
    }
    return false;
  }

  private void releaseBufferedToken() {
    assert outputBuffer.size() > 0;

    WeightedSynonym synonym = outputBuffer.pollFirst();
    clearAttributes();
    restoreState(this.lastState);
    termAtt.setEmpty();
    termAtt.append(synonym.getTerm());
    boostAtt.setBoost(synonym.getWeight());
    typeAtt.setType(SynonymGraphFilter.TYPE_SYNONYM);
    posLenAtt.setPositionLength(1);
    posIncrAtt.setPositionIncrement(0);
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    outputBuffer.clear();
  }
}
