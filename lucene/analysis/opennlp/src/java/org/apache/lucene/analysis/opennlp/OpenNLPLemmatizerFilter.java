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
import java.util.Iterator;
import java.util.List;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.opennlp.tools.NLPLemmatizerOp;
import org.apache.lucene.analysis.tokenattributes.*;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.IgnoreRandomChains;

/**
 * Runs OpenNLP dictionary-based and/or MaxEnt lemmatizers.
 *
 * <p>Both a dictionary-based lemmatizer and a MaxEnt lemmatizer are supported, via the "dictionary"
 * and "lemmatizerModel" params, respectively. If both are configured, the dictionary-based
 * lemmatizer is tried first, and then the MaxEnt lemmatizer is consulted for out-of-vocabulary
 * tokens.
 *
 * <p>The dictionary file must be encoded as UTF-8, with one entry per line, in the form <code>
 * word[tab]lemma[tab]part-of-speech</code>
 */
@IgnoreRandomChains(reason = "LUCENE-10352: no dictionary support yet")
public class OpenNLPLemmatizerFilter extends TokenFilter {
  private final NLPLemmatizerOp lemmatizerOp;
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final KeywordAttribute keywordAtt = addAttribute(KeywordAttribute.class);
  private Iterator<AttributeSource> sentenceTokenAttrsIter = null;
  private final SentenceAttributeExtractor sentenceAttributeExtractor;
  private String[] lemmas = new String[0]; // lemmas for non-keyword tokens
  private int lemmaNum = 0; // lemma counter

  public OpenNLPLemmatizerFilter(TokenStream input, NLPLemmatizerOp lemmatizerOp) {
    super(input);
    this.lemmatizerOp = lemmatizerOp;
    sentenceAttributeExtractor =
        new SentenceAttributeExtractor(input, addAttribute(SentenceAttribute.class));
  }

  @Override
  public final boolean incrementToken() throws IOException {
    boolean isEndOfCurrentSentence = lemmaNum >= lemmas.length;
    if (isEndOfCurrentSentence) {
      boolean noSentencesLeft =
          sentenceAttributeExtractor.allSentencesProcessed() || nextSentence().isEmpty();
      if (noSentencesLeft) {
        return false;
      }
    }
    clearAttributes();
    sentenceTokenAttrsIter.next().copyTo(this);
    if (!keywordAtt.isKeyword()) {
      termAtt.setEmpty().append(lemmas[lemmaNum++]);
    }
    return true;
  }

  private List<AttributeSource> nextSentence() throws IOException {
    lemmaNum = 0;
    List<String> tokenList = new ArrayList<>();
    List<String> typeList = new ArrayList<>();
    List<AttributeSource> sentenceAttributes =
        sentenceAttributeExtractor.extractSentenceAttributes();
    for (AttributeSource attributeSource : sentenceAttributes) {
      if (!attributeSource.getAttribute(KeywordAttribute.class).isKeyword()) {
        tokenList.add(attributeSource.getAttribute(CharTermAttribute.class).toString());
        typeList.add(attributeSource.getAttribute(TypeAttribute.class).type());
      }
    }
    String[] sentenceTokens = tokenList.toArray(new String[0]);
    String[] sentenceTokenTypes = typeList.toArray(new String[0]);
    lemmas = lemmatizerOp.lemmatize(sentenceTokens, sentenceTokenTypes);
    sentenceTokenAttrsIter = sentenceAttributes.iterator();
    return sentenceAttributeExtractor.getSentenceAttributes();
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    sentenceAttributeExtractor.reset();
    clear();
  }

  private void clear() {
    sentenceTokenAttrsIter = null;
    lemmas = new String[0];
    lemmaNum = 0;
  }
}
