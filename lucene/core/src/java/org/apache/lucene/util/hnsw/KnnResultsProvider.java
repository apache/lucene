package org.apache.lucene.util.hnsw;

public interface KnnResultsProvider {
  KnnResults getKnnResults();

  int k();
}
