package org.apache.lucene.analysis.tokenattributes;

import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.AttributeReflector;

/** Default implementation of {@link SentenceAttribute}.
 * <p>The current implementation is coincidentally identical to {@link FlagsAttributeImpl}
 * It was decided to keep it separate because this attribute will NOT be an implied bitmap
 * */
public class SentenceAttributeImpl extends AttributeImpl implements SentenceAttribute {
    private static final int NO_SENTENCE = 0;
    private int sentence = NO_SENTENCE;

    /** Initialize this attribute to default */
    public SentenceAttributeImpl() {}

    @Override
    public void clear() {
        sentence = NO_SENTENCE;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other instanceof SentenceAttributeImpl) {
            return ((SentenceAttributeImpl) other).sentence == sentence;
        }

        return false;
    }

    @Override
    public int hashCode() {
        return sentence;
    }

    @Override
    public void copyTo(AttributeImpl target) {
        SentenceAttribute t = (SentenceAttribute) target;
        t.setSentence(sentence);
    }

    @Override
    public void reflectWith(AttributeReflector reflector) {
        reflector.reflect(SentenceAttribute.class, "sentences", sentence);
    }

    @Override
    public int getSentence() {
        return sentence;
    }

    @Override
    public void setSentence(int sentence) {
        this.sentence = sentence;
    }
}
