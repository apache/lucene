package org.apache.lucene.util.hnsw;

import org.apache.lucene.util.LongValues;

public interface KnnResultsProvider {
  KnnResults getKnnResults();
  void setVectorToOrd(IntToIntFunction vectorToOrd);
  int k();
}
