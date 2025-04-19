package org.apache.lucene.codecs;

import java.io.IOException;

/**
 * Approximate binning for documents using hash-based encoding of sparse
 * graph connectivity patterns. Documents with similar edge structure are
 * likely to hash into the same bin.
 */
public final class ApproximateDocBinner {

  private ApproximateDocBinner() {}

  /**
   * Assigns each document to a bin based on hashed connectivity to neighbors.
   * Hashing incorporates neighbor IDs and edge weights. This approximates 
   * locality by ensuring similar documents hash to similar bins.
   *
   * @param graph sparse document similarity graph
   * @param maxDoc total number of documents
   * @param numBins total bins to assign (must be power of 2 for masking)
   * @return array of bin IDs per document
   */
  public static int[] assign(SparseEdgeGraph graph, int maxDoc, int numBins) throws IOException {
    if (Integer.bitCount(numBins) != 1) {
      throw new IllegalArgumentException("numBins must be a power of 2");
    }

    final int[] bins = new int[maxDoc];
    final int mask = numBins - 1;

    for (int docID = 0; docID < maxDoc; docID++) {
      int[] neighbors = graph.getNeighbors(docID);
      if (neighbors.length == 0) {
        bins[docID] = 0;
        continue;
      }

      float[] weights = graph.getWeights(docID);
      int hash = 0;
      for (int i = 0; i < neighbors.length; i++) {
        if (weights[i] < 0.01f) continue; // prune noisy weak edges
        hash = mix(hash, neighbors[i], weights[i]);
      }

      bins[docID] = hash & mask;
    }

    return bins;
  }

  private static int mix(int h, int neighbor, float weight) {
    int x = neighbor * 31 + Float.floatToIntBits(weight);
    h ^= Integer.rotateLeft(x, 13);
    h *= 0x5bd1e995;
    return h;
  }
}