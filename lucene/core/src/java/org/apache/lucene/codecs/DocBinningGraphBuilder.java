/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Partitions documents into bins using iterative graph bisection.
 *
 * <p>This utility approximates a balanced clustering of documents using a sparse similarity graph.
 * It uses fixed seed points and edge weights to recursively assign documents to bins, ensuring that
 * similar documents are grouped together.
 */
public final class DocBinningGraphBuilder {

  private DocBinningGraphBuilder() {}

  /**
   * Assigns each document to a bin by performing recursive bisection over a similarity graph.
   *
   * @param graph the sparse edge graph of document similarities
   * @param maxDoc total number of documents
   * @param numBins number of bins (must be a power of 2)
   * @return an array mapping docID to bin ID
   * @throws IOException if an error occurs during graph access
   */
  public static int[] computeBins(SparseEdgeGraph graph, int maxDoc, int numBins)
      throws IOException {
    if (maxDoc <= 0 || Integer.bitCount(numBins) != 1) {
      throw new IllegalArgumentException("maxDoc must be > 0 and numBins must be a power of 2");
    }

    final int[] docToBin = new int[maxDoc];
    final int[] docIndices = new int[maxDoc];
    for (int i = 0; i < maxDoc; i++) {
      docToBin[i] = -1;
      docIndices[i] = i;
    }

    final Deque<PartitionTask> queue = new ArrayDeque<>();
    queue.add(new PartitionTask(0, maxDoc, 0, numBins));

    while (!queue.isEmpty()) {
      PartitionTask task = queue.removeFirst();
      final int size = task.end - task.start;
      if (task.binCount == 1 || size <= 1) {
        for (int i = task.start; i < task.end; i++) {
          docToBin[docIndices[i]] = task.binBase;
        }
        continue;
      }

      final int seedA = docIndices[task.start];
      final int seedB = docIndices[task.end - 1];

      final int[] left = new int[size];
      final int[] right = new int[size];
      int leftSize = 0;
      int rightSize = 0;

      for (int i = 0; i < size; i++) {
        final int docID = docIndices[task.start + i];
        final float simA = similarity(graph, docID, seedA);
        final float simB = similarity(graph, docID, seedB);
        if (simA >= simB) {
          left[leftSize++] = docID;
        } else {
          right[rightSize++] = docID;
        }
      }

      System.arraycopy(left, 0, docIndices, task.start, leftSize);
      System.arraycopy(right, 0, docIndices, task.start + leftSize, rightSize);

      final int mid = task.start + leftSize;
      final int nextBin = task.binCount >>> 1;

      queue.add(new PartitionTask(task.start, mid, task.binBase, nextBin));
      queue.add(new PartitionTask(mid, task.end, task.binBase + nextBin, nextBin));
    }

    for (int i = 0; i < maxDoc; i++) {
      if (docToBin[i] == -1) {
        throw new IllegalStateException("Unassigned docID: " + i);
      }
    }

    return docToBin;
  }

  /**
   * Computes similarity between two documents using the sparse graph.
   *
   * <p>The similarity is computed as the sum of edge weights from docA to docB.
   */
  private static float similarity(SparseEdgeGraph graph, int docA, int docB) {
    int[] neighbors = graph.getNeighbors(docA);
    float[] weights = graph.getWeights(docA);
    float score = 0f;
    for (int i = 0; i < neighbors.length; i++) {
      if (neighbors[i] == docB) {
        score += weights[i];
      }
    }
    return score;
  }

  /** Internal task used to represent a partition step. */
  private record PartitionTask(int start, int end, int binBase, int binCount) {}
}
