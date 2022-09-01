package org.apache.lucene.analysis.tokenattributes;

import org.apache.lucene.util.Attribute;

/**
 * This attribute tracks what sentence a given token belongs to as well as potentially other
 * sentence specific attributes.
 */
public interface SentenceAttribute extends Attribute {

  /**
   * Get the sentence index for the current token
   *
   * @return The index of the sentence
   * @see #getSentenceIndex()
   */
  int getSentenceIndex();

  /**
   * Set the sentence of the current token
   *
   * @see #setSentenceIndex(int sentenceIndex)
   */
  void setSentenceIndex(int sentenceIndex);
}
