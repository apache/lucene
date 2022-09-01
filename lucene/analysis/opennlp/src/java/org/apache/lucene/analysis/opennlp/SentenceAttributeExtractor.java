package org.apache.lucene.analysis.opennlp;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.SentenceAttribute;
import org.apache.lucene.util.AttributeSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

    public boolean hasNext() throws IOException {
        moreTokensAvailable = input.incrementToken();
        int currSentenceTmp = sentenceAtt.getSentence();
        boolean hasNext = currSentence == currSentenceTmp && moreTokensAvailable;
        currSentence = currSentenceTmp;
        if (prevAttributeSource != null) {
            sentenceTokenAttrs.add(prevAttributeSource);
        }
        prevAttributeSource = input.cloneAttributes();
        return hasNext;
    }

    public List<AttributeSource> extractSentenceAttributes() throws IOException {
        sentenceTokenAttrs.clear();
        while (hasNext()) {
        }
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
