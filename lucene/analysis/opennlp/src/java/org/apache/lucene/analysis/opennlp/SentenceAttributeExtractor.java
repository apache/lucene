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
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.SentenceAttribute;
import org.apache.lucene.util.AttributeSource;

/**
 * Iterate through sentence tokens and cache their attributes. Could consider moving this to a more
 * central location to be used by other sentence-aware components.
 *
 * <p>May want to consider making this its own Filter so that extracted sentence token attributes
 * can be shared by downstream sentence-aware filters.
 */
public class SentenceAttributeExtractor {

  private final TokenStream input;
  private final SentenceAttribute sentenceAtt;
  private final List<AttributeSource> sentenceTokenAttrs = new ArrayList<>();
  private AttributeSource prevAttributeSource;
  private int currSentence = 0;
  private boolean hasNextToken = true;

  public SentenceAttributeExtractor(TokenStream input, SentenceAttribute sentenceAtt) {
    this.input = input;
    this.sentenceAtt = sentenceAtt;
  }

  // If this class were a stand-alone filter it could conceivably extract the attributes once
  // and cache a reference to those attributes in SentenceAttribute. That way downstream filters
  // could read the full sentence without having to independently extract it.
  public List<AttributeSource> extractSentenceAttributes() throws IOException {
    sentenceTokenAttrs.clear();
    boolean hasNext;
    do {
      hasNextToken = input.incrementToken();
      int currSentenceTmp = sentenceAtt.getSentenceIndex();
      hasNext = (currSentence == currSentenceTmp && hasNextToken);
      currSentence = currSentenceTmp;
      if (prevAttributeSource != null) {
        sentenceTokenAttrs.add(prevAttributeSource);
      }
      prevAttributeSource = input.cloneAttributes();
    } while (hasNext);
    return sentenceTokenAttrs;
  }

  public List<AttributeSource> getSentenceAttributes() {
    return sentenceTokenAttrs;
  }

  public boolean allSentencesProcessed() {
    return !hasNextToken;
  }

  public void reset() {
    hasNextToken = true;
    sentenceTokenAttrs.clear();
    currSentence = 0;
    prevAttributeSource = null;
  }
}
