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

package org.apache.lucene.analysis.opennlp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.opennlp.tools.NLPChunkerOp;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.SentenceAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.IgnoreRandomChains;

/**
 * Run OpenNLP chunker. Prerequisite: the OpenNLPTokenizer and OpenNLPPOSFilter must precede this
 * filter. Tags terms in the TypeAttribute, replacing the POS tags previously put there by
 * OpenNLPPOSFilter.
 */
@IgnoreRandomChains(reason = "other filters must precede this one (see docs)")
public final class OpenNLPChunkerFilter extends TokenFilter {
  private int tokenNum = 0;
  private final NLPChunkerOp chunkerOp;
  private final SentenceAttributeExtractor sentenceAttributeExtractor;

  public OpenNLPChunkerFilter(TokenStream input, NLPChunkerOp chunkerOp) {
    super(input);
    this.chunkerOp = chunkerOp;
    sentenceAttributeExtractor =
        new SentenceAttributeExtractor(input, addAttribute(SentenceAttribute.class));
  }

  @Override
  public boolean incrementToken() throws IOException {
    List<AttributeSource> sentenceTokenAttrs = sentenceAttributeExtractor.getSentenceAttributes();
    boolean isEndOfCurrentSentence = tokenNum >= sentenceTokenAttrs.size();
    if (isEndOfCurrentSentence) {
      boolean noSentencesLeft =
          sentenceAttributeExtractor.allSentencesProcessed() || nextSentence().isEmpty();
      if (noSentencesLeft) {
        return false;
      }
    }
    clearAttributes();
    sentenceTokenAttrs.get(tokenNum++).copyTo(this);
    return true;
  }

  private List<AttributeSource> nextSentence() throws IOException {
    tokenNum = 0;
    List<String> termList = new ArrayList<>();
    List<String> posTagList = new ArrayList<>();
    for (AttributeSource attributeSource : sentenceAttributeExtractor.extractSentenceAttributes()) {
      termList.add(attributeSource.getAttribute(CharTermAttribute.class).toString());
      posTagList.add(attributeSource.getAttribute(TypeAttribute.class).type());
    }
    String[] sentenceTerms = termList.toArray(new String[0]);
    String[] sentenceTermPOSTags = posTagList.toArray(new String[0]);
    assignTokenTypes(chunkerOp.getChunks(sentenceTerms, sentenceTermPOSTags, null));
    return sentenceAttributeExtractor.getSentenceAttributes();
  }

  private void assignTokenTypes(String[] tags) {
    for (int i = 0; i < tags.length; ++i) {
      sentenceAttributeExtractor
          .getSentenceAttributes()
          .get(i)
          .getAttribute(TypeAttribute.class)
          .setType(tags[i]);
    }
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    sentenceAttributeExtractor.reset();
    clear();
  }

  private void clear() {
    tokenNum = 0;
  }
}
