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
package org.apache.lucene.util.hnsw;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.HnswGraphProvider;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.internal.hppc.IntIntHashMap;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.FixedBitSet;

/** This merger merges graph in a concurrent manner, by using {@link HnswConcurrentMergeBuilder} */
public class ConcurrentHnswMerger extends IncrementalHnswGraphMerger {

  private final TaskExecutor taskExecutor;
  private final int numWorker;

  /**
   * @param fieldInfo FieldInfo for the field being merged
   */
  public ConcurrentHnswMerger(
      FieldInfo fieldInfo,
      RandomVectorScorerSupplier scorerSupplier,
      int M,
      int beamWidth,
      TaskExecutor taskExecutor,
      int numWorker) {
    super(fieldInfo, scorerSupplier, M, beamWidth);
    this.taskExecutor = taskExecutor;
    this.numWorker = numWorker;
  }

  @Override
  protected HnswBuilder createBuilder(KnnVectorValues mergedVectorValues, int maxOrd)
      throws IOException {
    OnHeapHnswGraph graph;
    BitSet initializedNodes = null;

    if (largestGraphReader == null) {
      graph = new OnHeapHnswGraph(M, maxOrd);
    } else {
      KnnVectorsReader initReader = largestGraphReader.reader();
      MergeState.DocMap initDocMap = largestGraphReader.initDocMap();
      int initGraphSize = largestGraphReader.graphSize();
      HnswGraph initializerGraph = ((HnswGraphProvider) initReader).getGraph(fieldInfo.name);

      if (initializerGraph.size() == 0) {
        graph = new OnHeapHnswGraph(M, maxOrd);
      } else {
        initializedNodes = new FixedBitSet(maxOrd);
        int[] oldToNewOrdinalMap =
            getNewOrdMapping(
                fieldInfo,
                initReader,
                initDocMap,
                initGraphSize,
                mergedVectorValues,
                initializedNodes);
        graph =
            InitializedHnswGraphBuilder.initGraph(
                initializerGraph, oldToNewOrdinalMap, maxOrd, beamWidth, scorerSupplier);
      }
    }
    return new HnswConcurrentMergeBuilder(
        taskExecutor, numWorker, scorerSupplier, beamWidth, graph, initializedNodes);
  }

  /**
   * Creates a new mapping from old ordinals to new ordinals and returns the total number of vectors
   * in the newly merged segment.
   *
   * @param mergedVectorValues vector values in the merged segment
   * @param initializedNodes track what nodes have been initialized
   * @return the mapping from old ordinals to new ordinals
   * @throws IOException If an error occurs while reading from the merge state
   */
  private static int[] getNewOrdMapping(
      FieldInfo fieldInfo,
      KnnVectorsReader initReader,
      MergeState.DocMap initDocMap,
      int initGraphSize,
      KnnVectorValues mergedVectorValues,
      BitSet initializedNodes)
      throws IOException {
    KnnVectorValues.DocIndexIterator initializerIterator = null;

    switch (fieldInfo.getVectorEncoding()) {
      case BYTE -> initializerIterator = initReader.getByteVectorValues(fieldInfo.name).iterator();
      case FLOAT32 ->
          initializerIterator = initReader.getFloatVectorValues(fieldInfo.name).iterator();
    }

    IntIntHashMap newIdToOldOrdinal = new IntIntHashMap(initGraphSize);
    int maxNewDocID = -1;
    for (int docId = initializerIterator.nextDoc();
        docId != NO_MORE_DOCS;
        docId = initializerIterator.nextDoc()) {
      int newId = initDocMap.get(docId);
      if (newId == -1) {
        continue;
      }
      maxNewDocID = Math.max(newId, maxNewDocID);
      assert newIdToOldOrdinal.containsKey(newId) == false;
      newIdToOldOrdinal.put(newId, initializerIterator.index());
    }

    if (maxNewDocID == -1) {
      return new int[0];
    }
    final int[] oldToNewOrdinalMap = new int[initGraphSize];
    Arrays.fill(oldToNewOrdinalMap, -1);
    KnnVectorValues.DocIndexIterator mergedVectorIterator = mergedVectorValues.iterator();
    for (int newDocId = mergedVectorIterator.nextDoc();
        newDocId <= maxNewDocID;
        newDocId = mergedVectorIterator.nextDoc()) {
      int oldOrd = newIdToOldOrdinal.getOrDefault(newDocId, -1);
      if (oldOrd != -1) {
        int newOrd = mergedVectorIterator.index();
        initializedNodes.set(newOrd);
        oldToNewOrdinalMap[oldOrd] = newOrd;
      }
    }
    return oldToNewOrdinalMap;
  }
}
