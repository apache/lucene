package org.apache.lucene.analysis.morph;

public interface Dictionary<T extends MorphAttributes> {
  /**
   * Get left id of specified word
   *
   * @return left id
   */
  default int getLeftId(int morphId) {
    return getMorphAttributes().getLeftId(morphId);
  }

  /**
   * Get right id of specified word
   *
   * @return right id
   */
  default int getRightId(int morphId) {
    return getMorphAttributes().getRightId(morphId);
  }

  /**
   * Get word cost of specified word
   *
   * @return word's cost
   */
  default int getWordCost(int morphId) {
    return getMorphAttributes().getWordCost(morphId);
  }

  T getMorphAttributes();
}
