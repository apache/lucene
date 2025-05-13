/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.lucene.codecs;

import java.io.IOException;

/**
 * Assigns approximate bin IDs to documents based on their neighbors in a sparse similarity graph.
 * Documents with similar neighbors will likely be assigned to the same bin.
 *
 * <p>This implementation hashes neighbor IDs and edge weights to compute a stable bin ID. It is
 * useful for approximate grouping without clustering.
 */
public final class ApproximateDocBinner {

  private ApproximateDocBinner() {}

  /**
   * Computes bin assignments based on hashed neighborhood structure.
   *
   * @param graph the sparse graph over documents
   * @param maxDoc number of documents in the segment
   * @param numBins total bins (must be power of 2)
   * @return array of bin IDs per document in [0, numBins)
   * @throws IOException if graph access fails
   */
  public static int[] assign(SparseEdgeGraph graph, int maxDoc, int numBins) throws IOException {
    if (Integer.bitCount(numBins) != 1) {
      throw new IllegalArgumentException("numBins must be a power of 2");
    }

    final int[] bins = new int[maxDoc];
    final int mask = numBins - 1;

    for (int docID = 0; docID < maxDoc; docID++) {
      final int[] neighbors = graph.getNeighbors(docID);
      final float[] weights = graph.getWeights(docID);

      if (neighbors.length == 0) {
        bins[docID] = 0;
        continue;
      }

      int hash = 0;
      for (int i = 0; i < neighbors.length; i++) {
        if (weights[i] < 0.01f) {
          continue;
        }
        hash = mixHash(hash, neighbors[i], weights[i]);
      }

      bins[docID] = finalizeHash(hash) & mask;
    }

    return bins;
  }

  private static int mixHash(int current, int neighborID, float weight) {
    int scaledWeight = (int) (weight * 1000);
    int combined = neighborID * 31 + scaledWeight;
    current ^= Integer.rotateLeft(combined, 13);
    current *= 0x5bd1e995;
    return current;
  }

  private static int finalizeHash(int h) {
    h ^= h >>> 16;
    h *= 0x85ebca6b;
    h ^= h >>> 13;
    h *= 0xc2b2ae35;
    h ^= h >>> 16;
    return h;
  }
}
