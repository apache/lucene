package org.apache.lucene.codecs;

/** Sparse adjacency graph interface. */
public interface SparseEdgeGraph {
  void addEdge(int fromDocID, int toDocID, float weight);

  int[] getNeighbors(int docID);

  float[] getWeights(int docID);

  int size();

  void ensureVertex(int docID);
}
