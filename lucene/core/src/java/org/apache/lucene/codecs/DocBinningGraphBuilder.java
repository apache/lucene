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

/**
 * Computes document-to-bin assignments using recursive graph bisection.
 */
public final class DocBinningGraphBuilder {

    private DocBinningGraphBuilder() {
    }

    /**
     * Partition documents into bins using recursive graph bisection.
     *
     * @param adjacency graph representation
     * @param maxDoc    number of documents
     * @param numBins   must be a power of 2
     * @return array mapping docID to assigned bin
     */
    public static int[] computeBins(SparseEdgeGraph adjacency, int maxDoc, int numBins) throws IOException {
        if (maxDoc <= 0 || Integer.bitCount(numBins) != 1) {
            throw new IllegalArgumentException("maxDoc must be > 0 and numBins must be a power of 2");
        }

        final int[] docToBin = new int[maxDoc];
        final int[] docIndices = new int[maxDoc];
        for (int i = 0; i < maxDoc; i++) {
            docToBin[i] = -1; // mark as unassigned
            docIndices[i] = i;
        }
        final float[] scratch = new float[maxDoc];

        partition(adjacency, docToBin, docIndices, 0, maxDoc, 0, numBins, scratch);

        for (int i = 0; i < maxDoc; i++) {
            if (docToBin[i] == -1) {
                throw new IllegalStateException("DocID " + i + " was not assigned to any bin");
            }
        }

        return docToBin;
    }

    private static void partition(SparseEdgeGraph adjacency, int[] docToBin, int[] docIndices,
                                  int start, int end, int binOffset, int numBins, float[] scratch) {
        final int size = end - start;
        if (numBins == 1 || size <= 1) {
            for (int i = start; i < end; i++) {
                docToBin[docIndices[i]] = binOffset;
            }
            return;
        }

        final int seedDocA = docIndices[start];
        final int seedDocB = docIndices[end - 1];

        final int[] leftPartition = new int[size];
        final int[] rightPartition = new int[size];
        int leftSize = 0;
        int rightSize = 0;

        for (int i = 0; i < size; i++) {
            final int docID = docIndices[start + i];
            final float weightToA = edgeWeight(adjacency, docID, seedDocA);
            final float weightToB = edgeWeight(adjacency, docID, seedDocB);

            if (weightToA >= weightToB) {
                leftPartition[leftSize++] = docID;
            } else {
                rightPartition[rightSize++] = docID;
            }
        }

        for (int i = 0; i < leftSize; i++) {
            docIndices[start + i] = leftPartition[i];
        }
        for (int i = 0; i < rightSize; i++) {
            docIndices[start + leftSize + i] = rightPartition[i];
        }

        final int mid = start + leftSize;
        final int nextBinCount = numBins >>> 1;

        partition(adjacency, docToBin, docIndices, start, mid, binOffset, nextBinCount, scratch);
        partition(adjacency, docToBin, docIndices, mid, end, binOffset + nextBinCount, nextBinCount, scratch);
    }

    private static float edgeWeight(SparseEdgeGraph graph, int docID, int targetDocID) {
        final int[] neighbors = graph.getNeighbors(docID);
        final float[] weights = graph.getWeights(docID);
        float totalWeight = 0f;
        for (int i = 0; i < neighbors.length; i++) {
            if (neighbors[i] == targetDocID) {
                totalWeight += weights[i];
            }
        }
        return totalWeight;
    }
}