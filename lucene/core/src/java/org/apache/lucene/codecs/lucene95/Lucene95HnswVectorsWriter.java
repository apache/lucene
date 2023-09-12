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

import static org.apache.lucene.codecs.lucene95.Lucene95HnswVectorsFormat.DIRECT_MONOTONIC_BLOCK_SHIFT;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.*;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.*;
import org.apache.lucene.util.hnsw.*;
import org.apache.lucene.util.hnsw.HnswGraph.NodesIterator;
import org.apache.lucene.util.packed.DirectMonotonicWriter;

/**
 * Writes vector values and knn graphs to index segments.
 *
 * @lucene.experimental
 */
public final class Lucene95HnswVectorsWriter extends KnnVectorsWriter {

  private final SegmentWriteState segmentWriteState;
  private final IndexOutput meta, vectorData, vectorIndex;
  private final int M;
  private final int beamWidth;

  private final List<FieldWriter<?>> fields = new ArrayList<>();
  private boolean finished;

  Lucene95HnswVectorsWriter(SegmentWriteState state, int M, int beamWidth) throws IOException {
    this.M = M;
    this.beamWidth = beamWidth;
    segmentWriteState = state;
    String metaFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, Lucene95HnswVectorsFormat.META_EXTENSION);

    String vectorDataFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            Lucene95HnswVectorsFormat.VECTOR_DATA_EXTENSION);

    String indexDataFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            Lucene95HnswVectorsFormat.VECTOR_INDEX_EXTENSION);
    boolean success = false;
    try {
      meta = state.directory.createOutput(metaFileName, state.context);
      vectorData = state.directory.createOutput(vectorDataFileName, state.context);
      vectorIndex = state.directory.createOutput(indexDataFileName, state.context);

      CodecUtil.writeIndexHeader(
          meta,
          Lucene95HnswVectorsFormat.META_CODEC_NAME,
          Lucene95HnswVectorsFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      CodecUtil.writeIndexHeader(
          vectorData,
          Lucene95HnswVectorsFormat.VECTOR_DATA_CODEC_NAME,
          Lucene95HnswVectorsFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      CodecUtil.writeIndexHeader(
          vectorIndex,
          Lucene95HnswVectorsFormat.VECTOR_INDEX_CODEC_NAME,
          Lucene95HnswVectorsFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      success = true;
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }

  @Override
  public KnnFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
    FieldWriter<?> newField =
        FieldWriter.create(fieldInfo, M, beamWidth, segmentWriteState.infoStream);
    fields.add(newField);
    return newField;
  }

  @Override
  public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
    for (FieldWriter<?> field : fields) {
      if (sortMap == null) {
        writeField(field, maxDoc);
      } else {
        writeSortingField(field, maxDoc, sortMap);
      }
    }
  }

  @Override
  public void finish() throws IOException {
    if (finished) {
      throw new IllegalStateException("already finished");
    }
    finished = true;

    if (meta != null) {
      // write end of fields marker
      meta.writeInt(-1);
      CodecUtil.writeFooter(meta);
    }
    if (vectorData != null) {
      CodecUtil.writeFooter(vectorData);
      CodecUtil.writeFooter(vectorIndex);
    }
  }

  @Override
  public long ramBytesUsed() {
    long total = 0;
    for (FieldWriter<?> field : fields) {
      total += field.ramBytesUsed();
    }
    return total;
  }

  private void writeField(FieldWriter<?> fieldData, int maxDoc) throws IOException {
    // write vector values
    long vectorDataOffset = vectorData.alignFilePointer(Float.BYTES);
    switch (fieldData.fieldInfo.getVectorEncoding()) {
      case BYTE:
        writeByteVectors(fieldData);
        break;
      case FLOAT32:
        writeFloat32Vectors(fieldData);
        break;
      default:
        throw new AssertionError();
    }
    long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;

    // write graph
    long vectorIndexOffset = vectorIndex.getFilePointer();
    OnHeapHnswGraph graph = fieldData.getGraph();
    int[][] graphLevelNodeOffsets = writeGraph(graph);
    long vectorIndexLength = vectorIndex.getFilePointer() - vectorIndexOffset;

    writeMeta(
        fieldData.fieldInfo,
        maxDoc,
        vectorDataOffset,
        vectorDataLength,
        vectorIndexOffset,
        vectorIndexLength,
        fieldData.docsWithField,
        graph,
        graphLevelNodeOffsets);
  }

  private void writeFloat32Vectors(FieldWriter<?> fieldData) throws IOException {
    final ByteBuffer buffer =
        ByteBuffer.allocate(fieldData.dim * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    for (Object v : fieldData.vectors) {
      buffer.asFloatBuffer().put((float[]) v);
      vectorData.writeBytes(buffer.array(), buffer.array().length);
    }
  }

  private void writeByteVectors(FieldWriter<?> fieldData) throws IOException {
    for (Object v : fieldData.vectors) {
      byte[] vector = (byte[]) v;
      vectorData.writeBytes(vector, vector.length);
    }
  }

  private void writeSortingField(FieldWriter<?> fieldData, int maxDoc, Sorter.DocMap sortMap)
      throws IOException {
    final int[] docIdOffsets = new int[sortMap.size()];
    int offset = 1; // 0 means no vector for this (field, document)
    DocIdSetIterator iterator = fieldData.docsWithField.iterator();
    for (int docID = iterator.nextDoc();
        docID != DocIdSetIterator.NO_MORE_DOCS;
        docID = iterator.nextDoc()) {
      int newDocID = sortMap.oldToNew(docID);
      docIdOffsets[newDocID] = offset++;
    }
    DocsWithFieldSet newDocsWithField = new DocsWithFieldSet();
    final int[] ordMap = new int[offset - 1]; // new ord to old ord
    final int[] oldOrdMap = new int[offset - 1]; // old ord to new ord
    int ord = 0;
    int doc = 0;
    for (int docIdOffset : docIdOffsets) {
      if (docIdOffset != 0) {
        ordMap[ord] = docIdOffset - 1;
        oldOrdMap[docIdOffset - 1] = ord;
        newDocsWithField.add(doc);
        ord++;
      }
      doc++;
    }

    // write vector values
    long vectorDataOffset;
    switch (fieldData.fieldInfo.getVectorEncoding()) {
      case BYTE:
        vectorDataOffset = writeSortedByteVectors(fieldData, ordMap);
        break;
      case FLOAT32:
        vectorDataOffset = writeSortedFloat32Vectors(fieldData, ordMap);
        break;
      default:
        throw new AssertionError();
    }
    long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;

    // write graph
    long vectorIndexOffset = vectorIndex.getFilePointer();
    OnHeapHnswGraph graph = fieldData.getGraph();
    int[][] graphLevelNodeOffsets = graph == null ? new int[0][] : new int[graph.numLevels()][];
    HnswGraph mockGraph = reconstructAndWriteGraph(graph, ordMap, oldOrdMap, graphLevelNodeOffsets);
    long vectorIndexLength = vectorIndex.getFilePointer() - vectorIndexOffset;

    writeMeta(
        fieldData.fieldInfo,
        maxDoc,
        vectorDataOffset,
        vectorDataLength,
        vectorIndexOffset,
        vectorIndexLength,
        newDocsWithField,
        mockGraph,
        graphLevelNodeOffsets);
  }

  private long writeSortedFloat32Vectors(FieldWriter<?> fieldData, int[] ordMap)
      throws IOException {
    long vectorDataOffset = vectorData.alignFilePointer(Float.BYTES);
    final ByteBuffer buffer =
        ByteBuffer.allocate(fieldData.dim * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    for (int ordinal : ordMap) {
      float[] vector = (float[]) fieldData.vectors.get(ordinal);
      buffer.asFloatBuffer().put(vector);
      vectorData.writeBytes(buffer.array(), buffer.array().length);
    }
    return vectorDataOffset;
  }

  private long writeSortedByteVectors(FieldWriter<?> fieldData, int[] ordMap) throws IOException {
    long vectorDataOffset = vectorData.alignFilePointer(Float.BYTES);
    for (int ordinal : ordMap) {
      byte[] vector = (byte[]) fieldData.vectors.get(ordinal);
      vectorData.writeBytes(vector, vector.length);
    }
    return vectorDataOffset;
  }

  /**
   * Reconstructs the graph given the old and new node ids.
   *
   * <p>Additionally, the graph node connections are written to the vectorIndex.
   *
   * @param graph The current on heap graph
   * @param newToOldMap the new node ids indexed to the old node ids
   * @param oldToNewMap the old node ids indexed to the new node ids
   * @param levelNodeOffsets where to place the new offsets for the nodes in the vector index.
   * @return The graph
   * @throws IOException if writing to vectorIndex fails
   */
  private HnswGraph reconstructAndWriteGraph(
      OnHeapHnswGraph graph, int[] newToOldMap, int[] oldToNewMap, int[][] levelNodeOffsets)
      throws IOException {
    if (graph == null) return null;

    List<int[]> nodesByLevel = new ArrayList<>(graph.numLevels());
    nodesByLevel.add(null);

    int maxOrd = graph.size();
    int maxConnOnLevel = M * 2;
    NodesIterator nodesOnLevel0 = graph.getNodesOnLevel(0);
    levelNodeOffsets[0] = new int[nodesOnLevel0.size()];
    while (nodesOnLevel0.hasNext()) {
      int node = nodesOnLevel0.nextInt();
      NeighborArray neighbors = graph.getNeighbors(0, newToOldMap[node]);
      long offset = vectorIndex.getFilePointer();
      reconstructAndWriteNeigbours(neighbors, oldToNewMap, maxConnOnLevel, maxOrd);
      levelNodeOffsets[0][node] = Math.toIntExact(vectorIndex.getFilePointer() - offset);
    }

    maxConnOnLevel = M;
    for (int level = 1; level < graph.numLevels(); level++) {
      NodesIterator nodesOnLevel = graph.getNodesOnLevel(level);
      int[] newNodes = new int[nodesOnLevel.size()];
      int n = 0;
      while (nodesOnLevel.hasNext()) {
        newNodes[n++] = oldToNewMap[nodesOnLevel.nextInt()];
      }
      Arrays.sort(newNodes);
      nodesByLevel.add(newNodes);
      levelNodeOffsets[level] = new int[newNodes.length];
      int nodeOffsetIndex = 0;
      for (int node : newNodes) {
        NeighborArray neighbors = graph.getNeighbors(level, newToOldMap[node]);
        long offset = vectorIndex.getFilePointer();
        reconstructAndWriteNeigbours(neighbors, oldToNewMap, maxConnOnLevel, maxOrd);
        levelNodeOffsets[level][nodeOffsetIndex++] =
            Math.toIntExact(vectorIndex.getFilePointer() - offset);
      }
    }
    return new HnswGraph() {
      @Override
      public int nextNeighbor() {
        throw new UnsupportedOperationException("Not supported on a mock graph");
      }

      @Override
      public void seek(int level, int target) {
        throw new UnsupportedOperationException("Not supported on a mock graph");
      }

      @Override
      public int size() {
        return graph.size();
      }

      @Override
      public int numLevels() {
        return graph.numLevels();
      }

      @Override
      public int entryNode() {
        throw new UnsupportedOperationException("Not supported on a mock graph");
      }

      @Override
      public NodesIterator getNodesOnLevel(int level) {
        if (level == 0) {
          return graph.getNodesOnLevel(0);
        } else {
          return new ArrayNodesIterator(nodesByLevel.get(level), nodesByLevel.get(level).length);
        }
      }
    };
  }

  private void reconstructAndWriteNeigbours(
      NeighborArray neighbors, int[] oldToNewMap, int maxConnOnLevel, int maxOrd)
      throws IOException {
    int size = neighbors.size();
    vectorIndex.writeVInt(size);

    // Destructively modify; it's ok we are discarding it after this
    int[] nnodes = neighbors.node();
    for (int i = 0; i < size; i++) {
      nnodes[i] = oldToNewMap[nnodes[i]];
    }
    Arrays.sort(nnodes, 0, size);
    // Now that we have sorted, do delta encoding to minimize the required bits to store the
    // information
    for (int i = size - 1; i > 0; --i) {
      assert nnodes[i] < maxOrd : "node too large: " + nnodes[i] + ">=" + maxOrd;
      nnodes[i] -= nnodes[i - 1];
    }
    for (int i = 0; i < size; i++) {
      vectorIndex.writeVInt(nnodes[i]);
    }
  }

  @Override
  public void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
    long vectorDataOffset = vectorData.alignFilePointer(Float.BYTES);
    IndexOutput tempVectorData =
        segmentWriteState.directory.createTempOutput(
            vectorData.getName(), "temp", segmentWriteState.context);
    IndexInput vectorDataInput = null;
    boolean success = false;
    try {
      // write the vector data to a temporary file
      // write the vector data to a temporary file
      final DocsWithFieldSet docsWithField;
      switch (fieldInfo.getVectorEncoding()) {
        case BYTE:
          docsWithField =
              writeByteVectorData(
                  tempVectorData, MergedVectorValues.mergeByteVectorValues(fieldInfo, mergeState));
          break;
        case FLOAT32:
          docsWithField =
              writeVectorData(
                  tempVectorData, MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState));
          break;
        default:
          throw new IllegalArgumentException(
              "unknown vector encoding=" + fieldInfo.getVectorEncoding());
      }
      CodecUtil.writeFooter(tempVectorData);
      IOUtils.close(tempVectorData);

      // copy the temporary file vectors to the actual data file
      vectorDataInput =
          segmentWriteState.directory.openInput(
              tempVectorData.getName(), segmentWriteState.context);
      vectorData.copyBytes(vectorDataInput, vectorDataInput.length() - CodecUtil.footerLength());
      CodecUtil.retrieveChecksum(vectorDataInput);
      long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;
      long vectorIndexOffset = vectorIndex.getFilePointer();
      // build the graph using the temporary vector data
      // we use Lucene95HnswVectorsReader.DenseOffHeapVectorValues for the graph construction
      // doesn't need to know docIds
      // TODO: separate random access vector values from DocIdSetIterator?
      int byteSize = fieldInfo.getVectorDimension() * fieldInfo.getVectorEncoding().byteSize;
      OnHeapHnswGraph graph = null;
      int[][] vectorIndexNodeOffsets = null;
      if (docsWithField.cardinality() != 0) {
        int initializerIndex = selectGraphForInitialization(mergeState, fieldInfo);
        switch (fieldInfo.getVectorEncoding()) {
          case BYTE:
            OffHeapByteVectorValues.DenseOffHeapVectorValues bytesValues =
                new OffHeapByteVectorValues.DenseOffHeapVectorValues(
                    fieldInfo.getVectorDimension(),
                    docsWithField.cardinality(),
                    vectorDataInput,
                    byteSize);
            RandomVectorScorerSupplier scorerBytesSupplier =
                RandomVectorScorerSupplier.createBytes(
                    bytesValues, fieldInfo.getVectorSimilarityFunction());
            HnswGraphBuilder bytesGraphBuilder =
                createHnswGraphBuilder(
                    mergeState, fieldInfo, scorerBytesSupplier, initializerIndex);
            bytesGraphBuilder.setInfoStream(segmentWriteState.infoStream);
            graph = bytesGraphBuilder.build(bytesValues.size());
            break;

          case FLOAT32:
            OffHeapFloatVectorValues.DenseOffHeapVectorValues vectorValues =
                new OffHeapFloatVectorValues.DenseOffHeapVectorValues(
                    fieldInfo.getVectorDimension(),
                    docsWithField.cardinality(),
                    vectorDataInput,
                    byteSize);
            RandomVectorScorerSupplier scorerSupplier =
                RandomVectorScorerSupplier.createFloats(
                    vectorValues, fieldInfo.getVectorSimilarityFunction());
            HnswGraphBuilder hnswGraphBuilder =
                createHnswGraphBuilder(mergeState, fieldInfo, scorerSupplier, initializerIndex);
            hnswGraphBuilder.setInfoStream(segmentWriteState.infoStream);
            graph = hnswGraphBuilder.build(vectorValues.size());
            break;
        }
        vectorIndexNodeOffsets = writeGraph(graph);
      }
      long vectorIndexLength = vectorIndex.getFilePointer() - vectorIndexOffset;
      writeMeta(
          fieldInfo,
          segmentWriteState.segmentInfo.maxDoc(),
          vectorDataOffset,
          vectorDataLength,
          vectorIndexOffset,
          vectorIndexLength,
          docsWithField,
          graph,
          vectorIndexNodeOffsets);
      success = true;
    } finally {
      IOUtils.close(vectorDataInput);
      if (success) {
        segmentWriteState.directory.deleteFile(tempVectorData.getName());
      } else {
        IOUtils.closeWhileHandlingException(tempVectorData);
        IOUtils.deleteFilesIgnoringExceptions(
            segmentWriteState.directory, tempVectorData.getName());
      }
    }
  }

  private HnswGraphBuilder createHnswGraphBuilder(
      MergeState mergeState,
      FieldInfo fieldInfo,
      RandomVectorScorerSupplier scorerSupplier,
      int initializerIndex)
      throws IOException {
    if (initializerIndex == -1) {
      return HnswGraphBuilder.create(scorerSupplier, M, beamWidth, HnswGraphBuilder.randSeed);
    }

    HnswGraph initializerGraph =
        getHnswGraphFromReader(fieldInfo.name, mergeState.knnVectorsReaders[initializerIndex]);
    Map<Integer, Integer> ordinalMapper =
        getOldToNewOrdinalMap(mergeState, fieldInfo, initializerIndex);
    return HnswGraphBuilder.create(
        scorerSupplier, M, beamWidth, HnswGraphBuilder.randSeed, initializerGraph, ordinalMapper);
  }

  private int selectGraphForInitialization(MergeState mergeState, FieldInfo fieldInfo)
      throws IOException {
    // Find the KnnVectorReader with the most docs that meets the following criteria:
    //  1. Does not contain any deleted docs
    //  2. Is a Lucene95HnswVectorsReader/PerFieldKnnVectorReader
    // If no readers exist that meet this criteria, return -1. If they do, return their index in
    // merge state
    int maxCandidateVectorCount = 0;
    int initializerIndex = -1;

    for (int i = 0; i < mergeState.liveDocs.length; i++) {
      KnnVectorsReader currKnnVectorsReader = mergeState.knnVectorsReaders[i];
      if (mergeState.knnVectorsReaders[i] instanceof PerFieldKnnVectorsFormat.FieldsReader) {
        PerFieldKnnVectorsFormat.FieldsReader candidateReader =
            (PerFieldKnnVectorsFormat.FieldsReader) mergeState.knnVectorsReaders[i];
        currKnnVectorsReader = candidateReader.getFieldReader(fieldInfo.name);
      }

      if (!allMatch(mergeState.liveDocs[i])
          || !(currKnnVectorsReader instanceof Lucene95HnswVectorsReader)) {
        continue;
      }
      Lucene95HnswVectorsReader candidateReader = (Lucene95HnswVectorsReader) currKnnVectorsReader;

      int candidateVectorCount = 0;
      switch (fieldInfo.getVectorEncoding()) {
        case BYTE:
          ByteVectorValues byteVectorValues = candidateReader.getByteVectorValues(fieldInfo.name);
          if (byteVectorValues == null) {
            continue;
          }
          candidateVectorCount = byteVectorValues.size();
          break;
        case FLOAT32:
          FloatVectorValues vectorValues = candidateReader.getFloatVectorValues(fieldInfo.name);
          if (vectorValues == null) {
            continue;
          }
          candidateVectorCount = vectorValues.size();
          break;
      }

      if (candidateVectorCount > maxCandidateVectorCount) {
        maxCandidateVectorCount = candidateVectorCount;
        initializerIndex = i;
      }
    }
    return initializerIndex;
  }

  private HnswGraph getHnswGraphFromReader(String fieldName, KnnVectorsReader knnVectorsReader)
      throws IOException {
    if (knnVectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader) {
      PerFieldKnnVectorsFormat.FieldsReader perFieldReader =
          (PerFieldKnnVectorsFormat.FieldsReader) knnVectorsReader;
      if (perFieldReader.getFieldReader(fieldName) instanceof Lucene95HnswVectorsReader) {
        Lucene95HnswVectorsReader fieldReader =
            (Lucene95HnswVectorsReader) perFieldReader.getFieldReader(fieldName);
        return fieldReader.getGraph(fieldName);
      }
    }

    if (knnVectorsReader instanceof Lucene95HnswVectorsReader) {
      return ((Lucene95HnswVectorsReader) knnVectorsReader).getGraph(fieldName);
    }

    // We should not reach here because knnVectorsReader's type is checked in
    // selectGraphForInitialization
    throw new IllegalArgumentException(
        "Invalid KnnVectorsReader type for field: "
            + fieldName
            + ". Must be Lucene95HnswVectorsReader or newer");
  }

  private Map<Integer, Integer> getOldToNewOrdinalMap(
      MergeState mergeState, FieldInfo fieldInfo, int initializerIndex) throws IOException {

    DocIdSetIterator initializerIterator = null;

    switch (fieldInfo.getVectorEncoding()) {
      case BYTE:
        initializerIterator =
            mergeState.knnVectorsReaders[initializerIndex].getByteVectorValues(fieldInfo.name);
        break;
      case FLOAT32:
        initializerIterator =
            mergeState.knnVectorsReaders[initializerIndex].getFloatVectorValues(fieldInfo.name);
        break;
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
      case BYTE:
        vectorIterator = MergedVectorValues.mergeByteVectorValues(fieldInfo, mergeState);
        break;
      case FLOAT32:
        vectorIterator = MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState);
        break;
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

  private boolean isCurrentVectorNull(DocIdSetIterator docIdSetIterator) throws IOException {
    if (docIdSetIterator instanceof FloatVectorValues) {
      return ((FloatVectorValues) docIdSetIterator).vectorValue() == null;
    }

    if (docIdSetIterator instanceof ByteVectorValues) {
      return ((ByteVectorValues) docIdSetIterator).vectorValue() == null;
    }

    return true;
  }

  private boolean allMatch(Bits bits) {
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

  /**
   * @param graph Write the graph in a compressed format
   * @return The non-cumulative offsets for the nodes. Should be used to create cumulative offsets.
   * @throws IOException if writing to vectorIndex fails
   */
  private int[][] writeGraph(OnHeapHnswGraph graph) throws IOException {
    if (graph == null) return new int[0][0];
    // write vectors' neighbours on each level into the vectorIndex file
    int countOnLevel0 = graph.size();
    int[][] offsets = new int[graph.numLevels()][];
    for (int level = 0; level < graph.numLevels(); level++) {
      NodesIterator nodesOnLevel = graph.getNodesOnLevel(level);
      offsets[level] = new int[nodesOnLevel.size()];
      int nodeOffsetId = 0;
      while (nodesOnLevel.hasNext()) {
        int node = nodesOnLevel.nextInt();
        NeighborArray neighbors = graph.getNeighbors(level, node);
        int size = neighbors.size();
        // Write size in VInt as the neighbors list is typically small
        long offsetStart = vectorIndex.getFilePointer();
        vectorIndex.writeVInt(size);
        // Destructively modify; it's ok we are discarding it after this
        int[] nnodes = neighbors.node();
        Arrays.sort(nnodes, 0, size);
        // Now that we have sorted, do delta encoding to minimize the required bits to store the
        // information
        for (int i = size - 1; i > 0; --i) {
          assert nnodes[i] < countOnLevel0 : "node too large: " + nnodes[i] + ">=" + countOnLevel0;
          nnodes[i] -= nnodes[i - 1];
        }
        for (int i = 0; i < size; i++) {
          vectorIndex.writeVInt(nnodes[i]);
        }
        offsets[level][nodeOffsetId++] =
            Math.toIntExact(vectorIndex.getFilePointer() - offsetStart);
      }
    }
    return offsets;
  }

  private void writeMeta(
      FieldInfo field,
      int maxDoc,
      long vectorDataOffset,
      long vectorDataLength,
      long vectorIndexOffset,
      long vectorIndexLength,
      DocsWithFieldSet docsWithField,
      HnswGraph graph,
      int[][] graphLevelNodeOffsets)
      throws IOException {
    meta.writeInt(field.number);
    meta.writeInt(field.getVectorEncoding().ordinal());
    meta.writeInt(field.getVectorSimilarityFunction().ordinal());
    meta.writeVLong(vectorDataOffset);
    meta.writeVLong(vectorDataLength);
    meta.writeVLong(vectorIndexOffset);
    meta.writeVLong(vectorIndexLength);
    meta.writeVInt(field.getVectorDimension());

    // write docIDs
    int count = docsWithField.cardinality();
    meta.writeInt(count);
    if (count == 0) {
      meta.writeLong(-2); // docsWithFieldOffset
      meta.writeLong(0L); // docsWithFieldLength
      meta.writeShort((short) -1); // jumpTableEntryCount
      meta.writeByte((byte) -1); // denseRankPower
    } else if (count == maxDoc) {
      meta.writeLong(-1); // docsWithFieldOffset
      meta.writeLong(0L); // docsWithFieldLength
      meta.writeShort((short) -1); // jumpTableEntryCount
      meta.writeByte((byte) -1); // denseRankPower
    } else {
      long offset = vectorData.getFilePointer();
      meta.writeLong(offset); // docsWithFieldOffset
      final short jumpTableEntryCount =
          IndexedDISI.writeBitSet(
              docsWithField.iterator(), vectorData, IndexedDISI.DEFAULT_DENSE_RANK_POWER);
      meta.writeLong(vectorData.getFilePointer() - offset); // docsWithFieldLength
      meta.writeShort(jumpTableEntryCount);
      meta.writeByte(IndexedDISI.DEFAULT_DENSE_RANK_POWER);

      // write ordToDoc mapping
      long start = vectorData.getFilePointer();
      meta.writeLong(start);
      meta.writeVInt(DIRECT_MONOTONIC_BLOCK_SHIFT);
      // dense case and empty case do not need to store ordToMap mapping
      final DirectMonotonicWriter ordToDocWriter =
          DirectMonotonicWriter.getInstance(meta, vectorData, count, DIRECT_MONOTONIC_BLOCK_SHIFT);
      DocIdSetIterator iterator = docsWithField.iterator();
      for (int doc = iterator.nextDoc();
          doc != DocIdSetIterator.NO_MORE_DOCS;
          doc = iterator.nextDoc()) {
        ordToDocWriter.add(doc);
      }
      ordToDocWriter.finish();
      meta.writeLong(vectorData.getFilePointer() - start);
    }

    meta.writeVInt(M);
    // write graph nodes on each level
    if (graph == null) {
      meta.writeVInt(0);
    } else {
      meta.writeVInt(graph.numLevels());
      long valueCount = 0;
      for (int level = 0; level < graph.numLevels(); level++) {
        NodesIterator nodesOnLevel = graph.getNodesOnLevel(level);
        valueCount += nodesOnLevel.size();
        if (level > 0) {
          int[] nol = new int[nodesOnLevel.size()];
          int numberConsumed = nodesOnLevel.consume(nol);
          assert numberConsumed == nodesOnLevel.size();
          meta.writeVInt(nol.length); // number of nodes on a level
          for (int i = nodesOnLevel.size() - 1; i > 0; --i) {
            nol[i] -= nol[i - 1];
          }
          for (int n : nol) {
            assert n >= 0 : "delta encoding for nodes failed; expected nodes to be sorted";
            meta.writeVInt(n);
          }
        } else {
          assert nodesOnLevel.size() == count : "Level 0 expects to have all nodes";
        }
      }
      long start = vectorIndex.getFilePointer();
      meta.writeLong(start);
      meta.writeVInt(DIRECT_MONOTONIC_BLOCK_SHIFT);
      final DirectMonotonicWriter memoryOffsetsWriter =
          DirectMonotonicWriter.getInstance(
              meta, vectorIndex, valueCount, DIRECT_MONOTONIC_BLOCK_SHIFT);
      long cumulativeOffsetSum = 0;
      for (int[] levelOffsets : graphLevelNodeOffsets) {
        for (int v : levelOffsets) {
          memoryOffsetsWriter.add(cumulativeOffsetSum);
          cumulativeOffsetSum += v;
        }
      }
      memoryOffsetsWriter.finish();
      meta.writeLong(vectorIndex.getFilePointer() - start);
    }
  }

  /**
   * Writes the byte vector values to the output and returns a set of documents that contains
   * vectors.
   */
  private static DocsWithFieldSet writeByteVectorData(
      IndexOutput output, ByteVectorValues byteVectorValues) throws IOException {
    DocsWithFieldSet docsWithField = new DocsWithFieldSet();
    for (int docV = byteVectorValues.nextDoc();
        docV != NO_MORE_DOCS;
        docV = byteVectorValues.nextDoc()) {
      // write vector
      byte[] binaryValue = byteVectorValues.vectorValue();
      assert binaryValue.length == byteVectorValues.dimension() * VectorEncoding.BYTE.byteSize;
      output.writeBytes(binaryValue, binaryValue.length);
      docsWithField.add(docV);
    }
    return docsWithField;
  }

  /**
   * Writes the vector values to the output and returns a set of documents that contains vectors.
   */
  private static DocsWithFieldSet writeVectorData(
      IndexOutput output, FloatVectorValues floatVectorValues) throws IOException {
    DocsWithFieldSet docsWithField = new DocsWithFieldSet();
    ByteBuffer buffer =
        ByteBuffer.allocate(floatVectorValues.dimension() * VectorEncoding.FLOAT32.byteSize)
            .order(ByteOrder.LITTLE_ENDIAN);
    for (int docV = floatVectorValues.nextDoc();
        docV != NO_MORE_DOCS;
        docV = floatVectorValues.nextDoc()) {
      // write vector
      float[] value = floatVectorValues.vectorValue();
      buffer.asFloatBuffer().put(value);
      output.writeBytes(buffer.array(), buffer.limit());
      docsWithField.add(docV);
    }
    return docsWithField;
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(meta, vectorData, vectorIndex);
  }

  private abstract static class FieldWriter<T> extends KnnFieldVectorsWriter<T> {
    private final FieldInfo fieldInfo;
    private final int dim;
    private final DocsWithFieldSet docsWithField;
    private final List<T> vectors;
    private final HnswGraphBuilder hnswGraphBuilder;

    private int lastDocID = -1;
    private int node = 0;

    static FieldWriter<?> create(FieldInfo fieldInfo, int M, int beamWidth, InfoStream infoStream)
        throws IOException {
      int dim = fieldInfo.getVectorDimension();
      switch (fieldInfo.getVectorEncoding()) {
        case BYTE:
          return new FieldWriter<byte[]>(fieldInfo, M, beamWidth, infoStream) {
            @Override
            public byte[] copyValue(byte[] value) {
              return ArrayUtil.copyOfSubArray(value, 0, dim);
            }
          };
        case FLOAT32:
          return new FieldWriter<float[]>(fieldInfo, M, beamWidth, infoStream) {
            @Override
            public float[] copyValue(float[] value) {
              return ArrayUtil.copyOfSubArray(value, 0, dim);
            }
          };
        default:
          throw new AssertionError();
      }
    }

    @SuppressWarnings("unchecked")
    FieldWriter(FieldInfo fieldInfo, int M, int beamWidth, InfoStream infoStream)
        throws IOException {
      this.fieldInfo = fieldInfo;
      this.dim = fieldInfo.getVectorDimension();
      this.docsWithField = new DocsWithFieldSet();
      vectors = new ArrayList<>();
      RAVectorValues<T> raVectors = new RAVectorValues<>(vectors, dim);
      final RandomVectorScorerSupplier scorerSupplier;
      switch (fieldInfo.getVectorEncoding()) {
        case BYTE:
          scorerSupplier =
              RandomVectorScorerSupplier.createBytes(
                  (RandomAccessVectorValues<byte[]>) raVectors,
                  fieldInfo.getVectorSimilarityFunction());
          break;
        case FLOAT32:
          scorerSupplier =
              RandomVectorScorerSupplier.createFloats(
                  (RandomAccessVectorValues<float[]>) raVectors,
                  fieldInfo.getVectorSimilarityFunction());
          break;
        default:
          throw new IllegalArgumentException(
              "unknown vector encoding=" + fieldInfo.getVectorEncoding());
      }
      hnswGraphBuilder =
          HnswGraphBuilder.create(scorerSupplier, M, beamWidth, HnswGraphBuilder.randSeed);
      hnswGraphBuilder.setInfoStream(infoStream);
    }

    @Override
    public void addValue(int docID, T vectorValue) throws IOException {
      if (docID == lastDocID) {
        throw new IllegalArgumentException(
            "VectorValuesField \""
                + fieldInfo.name
                + "\" appears more than once in this document (only one value is allowed per field)");
      }
      assert docID > lastDocID;
      docsWithField.add(docID);
      vectors.add(copyValue(vectorValue));
      hnswGraphBuilder.addGraphNode(node);
      node++;
      lastDocID = docID;
    }

    OnHeapHnswGraph getGraph() {
      if (vectors.size() > 0) {
        return hnswGraphBuilder.getGraph();
      } else {
        return null;
      }
    }

    @Override
    public long ramBytesUsed() {
      if (vectors.size() == 0) return 0;
      return docsWithField.ramBytesUsed()
          + (long) vectors.size()
              * (RamUsageEstimator.NUM_BYTES_OBJECT_REF + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER)
          + (long) vectors.size()
              * fieldInfo.getVectorDimension()
              * fieldInfo.getVectorEncoding().byteSize
          + hnswGraphBuilder.getGraph().ramBytesUsed();
    }
  }

  private static class RAVectorValues<T> implements RandomAccessVectorValues<T> {
    private final List<T> vectors;
    private final int dim;

    RAVectorValues(List<T> vectors, int dim) {
      this.vectors = vectors;
      this.dim = dim;
    }

    @Override
    public int size() {
      return vectors.size();
    }

    @Override
    public int dimension() {
      return dim;
    }

    @Override
    public T vectorValue(int targetOrd) throws IOException {
      return vectors.get(targetOrd);
    }

    @Override
    public RandomAccessVectorValues<T> copy() throws IOException {
      return this;
    }
  }
}
