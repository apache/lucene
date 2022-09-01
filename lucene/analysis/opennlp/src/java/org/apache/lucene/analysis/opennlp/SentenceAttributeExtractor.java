package org.apache.lucene.analysis.opennlp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.SentenceAttribute;
import org.apache.lucene.util.AttributeSource;

public class SentenceAttributeExtractor {

  private final TokenStream input;
  private final SentenceAttribute sentenceAtt;
  private final List<AttributeSource> sentenceTokenAttrs = new ArrayList<>();
  private AttributeSource prevAttributeSource;
  private int currSentence = 0;
  private boolean moreTokensAvailable = true;

  public SentenceAttributeExtractor(TokenStream input, SentenceAttribute sentenceAtt) {
    this.input = input;
    this.sentenceAtt = sentenceAtt;
  }

  public List<AttributeSource> extractSentenceAttributes() throws IOException {
    sentenceTokenAttrs.clear();
    boolean hasNext;
    do {
      moreTokensAvailable = input.incrementToken();
      int currSentenceTmp = sentenceAtt.getSentenceIndex();
      hasNext = currSentence == currSentenceTmp && moreTokensAvailable;
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

  public boolean areMoreTokensAvailable() {
    return moreTokensAvailable;
  }

  public void reset() {
    moreTokensAvailable = true;
    sentenceTokenAttrs.clear();
    currSentence = 0;
    prevAttributeSource = null;
  }
}
