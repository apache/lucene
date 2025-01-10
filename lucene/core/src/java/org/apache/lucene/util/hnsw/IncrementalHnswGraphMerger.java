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
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.HnswGraphProvider;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.internal.hppc.IntIntHashMap;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.InfoStream;

/**
 * This selects the biggest Hnsw graph from the provided merge state and initializes a new
 * HnswGraphBuilder with that graph as a starting point.
 *
 * @lucene.experimental
 */
public class IncrementalHnswGraphMerger implements HnswGraphMerger {

  protected final FieldInfo fieldInfo;
  protected final RandomVectorScorerSupplier scorerSupplier;
  protected final int M;
  protected final int beamWidth;

  protected KnnVectorsReader initReader;
  protected MergeState.DocMap initDocMap;
  protected int initGraphSize;

  /**
   * @param fieldInfo FieldInfo for the field being merged
   */
  public IncrementalHnswGraphMerger(
      FieldInfo fieldInfo, RandomVectorScorerSupplier scorerSupplier, int M, int beamWidth) {
    this.fieldInfo = fieldInfo;
    this.scorerSupplier = scorerSupplier;
    this.M = M;
    this.beamWidth = beamWidth;
  }

  /**
   * Adds a reader to the graph merger if it meets the following criteria: 1. Does not contain any
   * deleted docs 2. Is a HnswGraphProvider 3. Has the most docs of any previous reader that met the
   * above criteria
   */
  @Override
  public IncrementalHnswGraphMerger addReader(
      KnnVectorsReader reader, MergeState.DocMap docMap, Bits liveDocs) throws IOException {
    if (hasDeletes(liveDocs) || !(reader instanceof HnswGraphProvider)) {
      return this;
    }
    HnswGraph graph = ((HnswGraphProvider) reader).getGraph(fieldInfo.name);
    if (graph == null || graph.size() == 0) {
      return this;
    }
    int candidateVectorCount = 0;
    switch (fieldInfo.getVectorEncoding()) {
      case BYTE -> {
        ByteVectorValues byteVectorValues = reader.getByteVectorValues(fieldInfo.name);
        if (byteVectorValues == null) {
          return this;
        }
        candidateVectorCount = byteVectorValues.size();
      }
      case FLOAT32 -> {
        FloatVectorValues vectorValues = reader.getFloatVectorValues(fieldInfo.name);
        if (vectorValues == null) {
          return this;
        }
        candidateVectorCount = vectorValues.size();
      }
    }
    if (candidateVectorCount > initGraphSize) {
      initReader = reader;
      initDocMap = docMap;
      initGraphSize = candidateVectorCount;
    }
    return this;
  }

  /**
   * Builds a new HnswGraphBuilder using the biggest graph from the merge state as a starting point.
   * If no valid readers were added to the merge state, a new graph is created.
   *
   * @param mergedVectorValues vector values in the merged segment
   * @param maxOrd max num of vectors that will be merged into the graph
   * @return HnswGraphBuilder
   * @throws IOException If an error occurs while reading from the merge state
   */
  protected HnswBuilder createBuilder(KnnVectorValues mergedVectorValues, int maxOrd)
      throws IOException {
    if (initReader == null) {
      return HnswGraphBuilder.create(
          scorerSupplier, M, beamWidth, HnswGraphBuilder.randSeed, maxOrd);
    }

    HnswGraph initializerGraph = ((HnswGraphProvider) initReader).getGraph(fieldInfo.name);
    if (initializerGraph.size() == 0) {
      return HnswGraphBuilder.create(
          scorerSupplier, M, beamWidth, HnswGraphBuilder.randSeed, maxOrd);
    }

    BitSet initializedNodes = new FixedBitSet(maxOrd);
    int[] oldToNewOrdinalMap = getNewOrdMapping(mergedVectorValues, initializedNodes);
    return InitializedHnswGraphBuilder.fromGraph(
        scorerSupplier,
        beamWidth,
        HnswGraphBuilder.randSeed,
        initializerGraph,
        oldToNewOrdinalMap,
        initializedNodes,
        maxOrd);
  }

  @Override
  public OnHeapHnswGraph merge(
      KnnVectorValues mergedVectorValues, InfoStream infoStream, int maxOrd) throws IOException {
    HnswBuilder builder = createBuilder(mergedVectorValues, maxOrd);
    builder.setInfoStream(infoStream);
    return builder.build(maxOrd);
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
  protected final int[] getNewOrdMapping(
      KnnVectorValues mergedVectorValues, BitSet initializedNodes) throws IOException {
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
      maxNewDocID = Math.max(newId, maxNewDocID);
      newIdToOldOrdinal.put(newId, initializerIterator.index());
    }

    if (maxNewDocID == -1) {
      return new int[0];
    }
    final int[] oldToNewOrdinalMap = new int[initGraphSize];
    KnnVectorValues.DocIndexIterator mergedVectorIterator = mergedVectorValues.iterator();
    for (int newDocId = mergedVectorIterator.nextDoc();
        newDocId <= maxNewDocID;
        newDocId = mergedVectorIterator.nextDoc()) {
      int hashDocIndex = newIdToOldOrdinal.indexOf(newDocId);
      if (newIdToOldOrdinal.indexExists(hashDocIndex)) {
        int newOrd = mergedVectorIterator.index();
        initializedNodes.set(newOrd);
        oldToNewOrdinalMap[newIdToOldOrdinal.indexGet(hashDocIndex)] = newOrd;
      }
    }
    return oldToNewOrdinalMap;
  }

  private static boolean hasDeletes(Bits liveDocs) {
    if (liveDocs == null) {
      return false;
    }

    for (int i = 0; i < liveDocs.length(); i++) {
      if (!liveDocs.get(i)) {
        return true;
      }
    }
    return false;
  }
}
