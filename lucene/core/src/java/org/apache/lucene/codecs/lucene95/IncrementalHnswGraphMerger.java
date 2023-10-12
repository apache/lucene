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

  private KnnVectorsReader curReader;
  private MergeState.DocMap curDocMap;
  private int curGraphSize;
  private final FieldInfo fieldInfo;
  private final RandomVectorScorerSupplier scorerSupplier;
  private final int M;
  private final int beamWidth;

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
   * deleted docs 2. Is a HnswGraphProvider/PerFieldKnnVectorReader 3. Has the most docs of any
   * previous reader that met the above criteria
   *
   * @param reader KnnVectorsReader to add to the merger
   * @param docMap MergeState.DocMap for the reader
   * @param liveDocs Bits representing live docs, can be null
   * @return this
   * @throws IOException If an error occurs while reading from the merge state
   */
  IncrementalHnswGraphMerger addReader(
      KnnVectorsReader reader, MergeState.DocMap docMap, Bits liveDocs) throws IOException {
    KnnVectorsReader currKnnVectorsReader = reader;
    if (reader instanceof PerFieldKnnVectorsFormat.FieldsReader candidateReader) {
      currKnnVectorsReader = candidateReader.getFieldReader(fieldInfo.name);
    }

    if (!(currKnnVectorsReader instanceof HnswGraphProvider) || !allMatch(liveDocs)) {
      return this;
    }

    int candidateVectorCount = 0;
    switch (fieldInfo.getVectorEncoding()) {
      case BYTE -> {
        ByteVectorValues byteVectorValues =
            currKnnVectorsReader.getByteVectorValues(fieldInfo.name);
        if (byteVectorValues == null) {
          return this;
        }
        candidateVectorCount = byteVectorValues.size();
      }
      case FLOAT32 -> {
        FloatVectorValues vectorValues = currKnnVectorsReader.getFloatVectorValues(fieldInfo.name);
        if (vectorValues == null) {
          return this;
        }
        candidateVectorCount = vectorValues.size();
      }
    }
    if (candidateVectorCount > curGraphSize) {
      curReader = currKnnVectorsReader;
      curDocMap = docMap;
      curGraphSize = candidateVectorCount;
    }
    return this;
  }

  /**
   * Builds a new HnswGraphBuilder using the biggest graph from the merge state as a starting point.
   * If no valid readers were added to the merge state, a new graph is created.
   *
   * @param mergeState MergeState for the merge
   * @return HnswGraphBuilder
   * @throws IOException If an error occurs while reading from the merge state
   */
  public HnswGraphBuilder createBuilder(MergeState mergeState) throws IOException {
    if (curReader == null) {
      return HnswGraphBuilder.create(scorerSupplier, M, beamWidth, HnswGraphBuilder.randSeed);
    }

    HnswGraph initializerGraph = ((HnswGraphProvider) curReader).getGraph(fieldInfo.name);
    Map<Integer, Integer> ordinalMapper = getOldToNewOrdinalMap(mergeState);
    return new InitializedHnswGraphBuilder(
        scorerSupplier, M, beamWidth, HnswGraphBuilder.randSeed, initializerGraph, ordinalMapper);
  }

  private Map<Integer, Integer> getOldToNewOrdinalMap(MergeState mergeState) throws IOException {

    DocIdSetIterator initializerIterator = null;
    switch (fieldInfo.getVectorEncoding()) {
      case BYTE -> initializerIterator = curReader.getByteVectorValues(fieldInfo.name);
      case FLOAT32 -> initializerIterator = curReader.getFloatVectorValues(fieldInfo.name);
    }

    Map<Integer, Integer> newIdToOldOrdinal = new HashMap<>();
    int oldOrd = 0;
    int maxNewDocID = -1;
    for (int oldId = initializerIterator.nextDoc();
        oldId != NO_MORE_DOCS;
        oldId = initializerIterator.nextDoc()) {
      if (isCurrentVectorNull(initializerIterator)) {
        continue;
      }
      int newId = curDocMap.get(oldId);
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
