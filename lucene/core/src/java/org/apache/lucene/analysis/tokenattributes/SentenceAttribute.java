package org.apache.lucene.analysis.tokenattributes;

import org.apache.lucene.util.Attribute;

/**
 * This attribute tracks what sentence a given token belongs to
 */
public interface SentenceAttribute extends Attribute {

    /**
     * Get the sentence index for the current token
     *
     * @return The index of the sentence
     * @see #getSentence()
     */
    int getSentence();

    /**
     * Set the sentence of the current token
     *
     * @see #setSentence(int sentenceIndex)
     */
    void setSentence(int sentenceIndex);
}
