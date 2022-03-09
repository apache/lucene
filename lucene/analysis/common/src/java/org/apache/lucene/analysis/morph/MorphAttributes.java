package org.apache.lucene.analysis.morph;

public interface MorphAttributes {
  /**
   * Get left id of specified word
   *
   * @return left id
   */
  int getLeftId(int morphId);

  /**
   * Get right id of specified word
   *
   * @return right id
   */
  int getRightId(int morphId);

  /**
   * Get word cost of specified word
   *
   * @return word's cost
   */
  int getWordCost(int morphId);
}
