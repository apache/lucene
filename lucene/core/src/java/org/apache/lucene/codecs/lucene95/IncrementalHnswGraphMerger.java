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
package org.apache.lucene.codecs.lucene95;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.codecs.HnswGraphProvider;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.apache.lucene.util.hnsw.HnswGraphBuilder;
import org.apache.lucene.util.hnsw.InitializedHnswGraphBuilder;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;

/**
 * This selects the biggest Hnsw graph from the provided merge state and initializes a new
 * HnswGraphBuilder with that graph as a starting point.
 */
public class IncrementalHnswGraphMerger {

  private final MergeState mergeState;
  private final FieldInfo fieldInfo;

  /**
   * @param mergeState MergeState containing the readers to merge
   * @param fieldInfo FieldInfo for the field being merged
   */
  public IncrementalHnswGraphMerger(MergeState mergeState, FieldInfo fieldInfo) {
    this.mergeState = mergeState;
    this.fieldInfo = fieldInfo;
  }

  /**
   * Selects the biggest Hnsw graph from the provided merge state and initializes a new
   * HnswGraphBuilder with that graph as a starting point.
   *
   * @param scorerSupplier ScorerSupplier to use for scoring vectors
   * @param M The number of connections to allow per node
   * @param beamWidth The number of nodes to consider when searching for the nearest neighbor
   * @return HnswGraphBuilder initialized with the biggest graph from the merge state
   * @throws IOException If an error occurs while reading from the merge state
   */
  public HnswGraphBuilder build(RandomVectorScorerSupplier scorerSupplier, int M, int beamWidth)
      throws IOException {
    // Find the KnnVectorReader with the most docs that meets the following criteria:
    //  1. Does not contain any deleted docs
    //  2. Is a HnswGraphProvider/PerFieldKnnVectorReader
    // If no readers exist that meet this criteria, return -1. If they do, return their index in
    // merge state
    int maxCandidateVectorCount = 0;
    int initializerIndex = -1;

    for (int i = 0; i < mergeState.liveDocs.length; i++) {
      KnnVectorsReader currKnnVectorsReader = mergeState.knnVectorsReaders[i];
      if (mergeState.knnVectorsReaders[i]
          instanceof PerFieldKnnVectorsFormat.FieldsReader candidateReader) {
        currKnnVectorsReader = candidateReader.getFieldReader(fieldInfo.name);
      }

      if (!allMatch(mergeState.liveDocs[i])
          || !(currKnnVectorsReader instanceof HnswGraphProvider)) {
        continue;
      }

      int candidateVectorCount = 0;
      switch (fieldInfo.getVectorEncoding()) {
        case BYTE -> {
          ByteVectorValues byteVectorValues =
              currKnnVectorsReader.getByteVectorValues(fieldInfo.name);
          if (byteVectorValues == null) {
            continue;
          }
          candidateVectorCount = byteVectorValues.size();
        }
        case FLOAT32 -> {
          FloatVectorValues vectorValues =
              currKnnVectorsReader.getFloatVectorValues(fieldInfo.name);
          if (vectorValues == null) {
            continue;
          }
          candidateVectorCount = vectorValues.size();
        }
      }

      if (candidateVectorCount > maxCandidateVectorCount) {
        maxCandidateVectorCount = candidateVectorCount;
        initializerIndex = i;
      }
    }
    if (initializerIndex == -1) {
      return HnswGraphBuilder.create(scorerSupplier, M, beamWidth, HnswGraphBuilder.randSeed);
    }

    HnswGraph initializerGraph =
        getHnswGraphFromReader(mergeState.knnVectorsReaders[initializerIndex]);
    Map<Integer, Integer> ordinalMapper = getOldToNewOrdinalMap(initializerIndex);
    return new InitializedHnswGraphBuilder(
        scorerSupplier, M, beamWidth, HnswGraphBuilder.randSeed, initializerGraph, ordinalMapper);
  }

  private HnswGraph getHnswGraphFromReader(KnnVectorsReader knnVectorsReader) throws IOException {
    if (knnVectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader perFieldReader
        && perFieldReader.getFieldReader(fieldInfo.name) instanceof HnswGraphProvider fieldReader) {
      return fieldReader.getGraph(fieldInfo.name);
    }

    if (knnVectorsReader instanceof HnswGraphProvider provider) {
      return provider.getGraph(fieldInfo.name);
    }

    // We should not reach here because knnVectorsReader's type is checked in
    // selectGraphForInitialization
    throw new IllegalArgumentException(
        "Invalid KnnVectorsReader type for field: "
            + fieldInfo.name
            + ". Must be Lucene95HnswVectorsReader or newer");
  }

  private Map<Integer, Integer> getOldToNewOrdinalMap(int initializerIndex) throws IOException {

    DocIdSetIterator initializerIterator = null;
    switch (fieldInfo.getVectorEncoding()) {
      case BYTE -> initializerIterator =
          mergeState.knnVectorsReaders[initializerIndex].getByteVectorValues(fieldInfo.name);
      case FLOAT32 -> initializerIterator =
          mergeState.knnVectorsReaders[initializerIndex].getFloatVectorValues(fieldInfo.name);
    }

    MergeState.DocMap initializerDocMap = mergeState.docMaps[initializerIndex];

    Map<Integer, Integer> newIdToOldOrdinal = new HashMap<>();
    int oldOrd = 0;
    int maxNewDocID = -1;
    for (int oldId = initializerIterator.nextDoc();
        oldId != NO_MORE_DOCS;
        oldId = initializerIterator.nextDoc()) {
      if (isCurrentVectorNull(initializerIterator)) {
        continue;
      }
      int newId = initializerDocMap.get(oldId);
      maxNewDocID = Math.max(newId, maxNewDocID);
      newIdToOldOrdinal.put(newId, oldOrd);
      oldOrd++;
    }

    if (maxNewDocID == -1) {
      return Collections.emptyMap();
    }

    Map<Integer, Integer> oldToNewOrdinalMap = new HashMap<>();

    DocIdSetIterator vectorIterator = null;
    switch (fieldInfo.getVectorEncoding()) {
      case BYTE -> vectorIterator =
          KnnVectorsWriter.MergedVectorValues.mergeByteVectorValues(fieldInfo, mergeState);
      case FLOAT32 -> vectorIterator =
          KnnVectorsWriter.MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState);
    }

    int newOrd = 0;
    for (int newDocId = vectorIterator.nextDoc();
        newDocId <= maxNewDocID;
        newDocId = vectorIterator.nextDoc()) {
      if (isCurrentVectorNull(vectorIterator)) {
        continue;
      }

      if (newIdToOldOrdinal.containsKey(newDocId)) {
        oldToNewOrdinalMap.put(newIdToOldOrdinal.get(newDocId), newOrd);
      }
      newOrd++;
    }

    return oldToNewOrdinalMap;
  }

  private static boolean isCurrentVectorNull(DocIdSetIterator docIdSetIterator) throws IOException {
    if (docIdSetIterator instanceof FloatVectorValues) {
      return ((FloatVectorValues) docIdSetIterator).vectorValue() == null;
    }

    if (docIdSetIterator instanceof ByteVectorValues) {
      return ((ByteVectorValues) docIdSetIterator).vectorValue() == null;
    }

    return true;
  }

  private static boolean allMatch(Bits bits) {
    if (bits == null) {
      return true;
    }

    for (int i = 0; i < bits.length(); i++) {
      if (!bits.get(i)) {
        return false;
      }
    }
    return true;
  }
}
