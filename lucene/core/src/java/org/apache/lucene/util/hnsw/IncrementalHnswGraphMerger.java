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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.HnswGraphProvider;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.internal.hppc.IntIntHashMap;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.InfoStream;

/**
 * This merges multiple graphs in a single thread in incremental fashion.
 *
 * @lucene.experimental
 */
public class IncrementalHnswGraphMerger implements HnswGraphMerger {

  protected final FieldInfo fieldInfo;
  protected final RandomVectorScorerSupplier scorerSupplier;
  protected final int M;
  protected final int beamWidth;

  protected List<GraphReader> graphReaders = new ArrayList<>();
  protected GraphReader largestGraphReader;

  private int numReaders = 0;

  /**
   * The maximum acceptable deletion percentage for a graph to be considered as the base graph.
   * Graphs with deletion percentages above this threshold are not used for initialization as they
   * may have degraded connectivity.
   *
   * <p>A value of 40 means that if more than 40% of the graph's original vectors have been deleted,
   * the graph will not be selected as the base.
   */
  private final int DELETE_PCT_THRESHOLD = 40;

  /** Represents a vector reader that contains graph info. */
  protected record GraphReader(
      KnnVectorsReader reader, MergeState.DocMap initDocMap, int graphSize) {}

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
   * Adds a reader to the graph merger if it meets the following criteria: 1. does not contain any
   * deleted vector 2. is a HnswGraphProvider
   */
  @Override
  public IncrementalHnswGraphMerger addReader(
      KnnVectorsReader reader, MergeState.DocMap docMap, Bits liveDocs) throws IOException {
    numReaders++;
    if (!(reader instanceof HnswGraphProvider)) {
      return this;
    }
    HnswGraph graph = ((HnswGraphProvider) reader).getGraph(fieldInfo.name);
    if (graph == null || graph.size() == 0) {
      return this;
    }

    KnnVectorValues knnVectorValues =
        switch (fieldInfo.getVectorEncoding()) {
          case BYTE -> reader.getByteVectorValues(fieldInfo.name);
          case FLOAT32 -> reader.getFloatVectorValues(fieldInfo.name);
        };

    int candidateVectorCount = countLiveVectors(liveDocs, knnVectorValues);
    int graphSize = graph.size();

    GraphReader graphReader = new GraphReader(reader, docMap, graphSize);

    int deletePct = ((graphSize - candidateVectorCount) * 100) / graphSize;

    if (deletePct <= DELETE_PCT_THRESHOLD
        && (largestGraphReader == null || candidateVectorCount > largestGraphReader.graphSize)) {
      largestGraphReader = graphReader;
    }

    // if graph has no deletes
    if (candidateVectorCount == graphSize) {
      graphReaders.add(graphReader);
    }

    return this;
  }

  /**
   * Builds a new HnswGraphBuilder
   *
   * @param mergedVectorValues vector values in the merged segment
   * @param maxOrd max num of vectors that will be merged into the graph
   * @return HnswGraphBuilder
   * @throws IOException If an error occurs while reading from the merge state
   */
  protected HnswBuilder createBuilder(KnnVectorValues mergedVectorValues, int maxOrd)
      throws IOException {
    if (largestGraphReader == null) {
      return HnswGraphBuilder.create(
          scorerSupplier, M, beamWidth, HnswGraphBuilder.randSeed, maxOrd);
    }
    if (!graphReaders.contains(largestGraphReader)) {
      graphReaders.addFirst(largestGraphReader);
    } else {
      graphReaders.sort(Comparator.comparingInt(GraphReader::graphSize).reversed());
    }

    final BitSet initializedNodes =
        graphReaders.size() == numReaders ? null : new FixedBitSet(maxOrd);
    int[][] ordMaps = getNewOrdMapping(mergedVectorValues, initializedNodes);
    HnswGraph[] graphs = new HnswGraph[graphReaders.size()];
    for (int i = 0; i < graphReaders.size(); i++) {
      HnswGraph graph = ((HnswGraphProvider) graphReaders.get(i).reader).getGraph(fieldInfo.name);
      if (graph.size() == 0) {
        throw new IllegalStateException("Graph should not be empty");
      }
      graphs[i] = graph;
    }

    return MergingHnswGraphBuilder.fromGraphs(
        scorerSupplier,
        beamWidth,
        HnswGraphBuilder.randSeed,
        graphs,
        ordMaps,
        maxOrd,
        initializedNodes);
  }

  protected final int[][] getNewOrdMapping(
      KnnVectorValues mergedVectorValues, BitSet initializedNodes) throws IOException {
    final int numGraphs = graphReaders.size();
    IntIntHashMap[] newDocIdToOldOrdinals = new IntIntHashMap[numGraphs];
    final int[][] oldToNewOrdinalMap = new int[numGraphs][];
    for (int i = 0; i < numGraphs; i++) {
      KnnVectorValues.DocIndexIterator vectorsIter = null;
      switch (fieldInfo.getVectorEncoding()) {
        case BYTE ->
            vectorsIter = graphReaders.get(i).reader.getByteVectorValues(fieldInfo.name).iterator();
        case FLOAT32 ->
            vectorsIter =
                graphReaders.get(i).reader.getFloatVectorValues(fieldInfo.name).iterator();
      }
      newDocIdToOldOrdinals[i] = new IntIntHashMap(graphReaders.get(i).graphSize);
      MergeState.DocMap docMap = graphReaders.get(i).initDocMap();
      for (int docId = vectorsIter.nextDoc();
          docId != NO_MORE_DOCS;
          docId = vectorsIter.nextDoc()) {
        int newDocId = docMap.get(docId);
        newDocIdToOldOrdinals[i].put(newDocId, vectorsIter.index());
      }
      oldToNewOrdinalMap[i] = new int[graphReaders.get(i).graphSize];
      Arrays.fill(oldToNewOrdinalMap[i], -1);
    }

    KnnVectorValues.DocIndexIterator mergedVectorIterator = mergedVectorValues.iterator();
    for (int docId = mergedVectorIterator.nextDoc();
        docId < NO_MORE_DOCS;
        docId = mergedVectorIterator.nextDoc()) {
      int newOrd = mergedVectorIterator.index();
      for (int i = 0; i < numGraphs; i++) {
        int oldOrd = newDocIdToOldOrdinals[i].getOrDefault(docId, -1);
        if (oldOrd != -1) {
          oldToNewOrdinalMap[i][oldOrd] = newOrd;
          if (initializedNodes != null) {
            initializedNodes.set(newOrd);
          }
          break;
        }
      }
    }
    return oldToNewOrdinalMap;
  }

  @Override
  public OnHeapHnswGraph merge(
      KnnVectorValues mergedVectorValues, InfoStream infoStream, int maxOrd) throws IOException {
    HnswBuilder builder = createBuilder(mergedVectorValues, maxOrd);
    builder.setInfoStream(infoStream);
    return builder.build(maxOrd);
  }

  private static int countLiveVectors(Bits liveDocs, KnnVectorValues knnVectorValues)
      throws IOException {
    if (liveDocs == null) {
      return knnVectorValues.size();
    }

    int count = 0;
    DocIdSetIterator docIdSetIterator = knnVectorValues.iterator();
    for (int doc = docIdSetIterator.nextDoc();
        doc != NO_MORE_DOCS;
        doc = docIdSetIterator.nextDoc()) {
      if (liveDocs.get(doc)) {
        count++;
      }
    }
    return count;
  }
}
