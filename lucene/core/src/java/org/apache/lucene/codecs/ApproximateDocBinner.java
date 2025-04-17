/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.codecs;

import java.io.IOException;

/** Approximate binning for documents using locality-sensitive hashing over sparse edge graph. */
public final class ApproximateDocBinner {

  private ApproximateDocBinner() {}

  /**
   * Assign documents to bins approximately based on edge distribution.
   *
   * @param graph sparse edge graph
   * @param maxDoc number of documents
   * @param numBins number of bins (must be power of 2)
   * @return bin assignments for each document
   */
  public static int[] assign(SparseEdgeGraph graph, int maxDoc, int numBins) throws IOException {
    if (Integer.bitCount(numBins) != 1) {
      throw new IllegalArgumentException("numBins must be a power of 2");
    }

    final int[] bins = new int[maxDoc];
    final int mask = numBins - 1;

    for (int docID = 0; docID < maxDoc; docID++) {
      int[] neighbors = graph.getNeighbors(docID);
      float[] weights = graph.getWeights(docID);
      int hash = 0;
      for (int i = 0; i < neighbors.length; i++) {
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
