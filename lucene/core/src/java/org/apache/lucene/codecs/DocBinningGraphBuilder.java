package org.apache.lucene.codecs;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;

/** Non-recursive document binner using iterative graph bisection. */
public final class DocBinningGraphBuilder {

  private DocBinningGraphBuilder() {}

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

  private static final class PartitionTask {
    final int start;
    final int end;
    final int binBase;
    final int binCount;

    PartitionTask(int start, int end, int binBase, int binCount) {
      this.start = start;
      this.end = end;
      this.binBase = binBase;
      this.binCount = binCount;
    }
  }
}
