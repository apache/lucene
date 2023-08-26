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

package org.apache.lucene.codecs.lucene98;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.ScalarQuantizer;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.apache.lucene.util.hnsw.HnswGraph.NodesIterator;
import org.apache.lucene.util.hnsw.HnswGraphBuilder;
import org.apache.lucene.util.hnsw.NeighborArray;
import org.apache.lucene.util.hnsw.OnHeapHnswGraph;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;
import org.apache.lucene.util.packed.DirectMonotonicWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.lucene.codecs.lucene98.Lucene98QuantizedHnswVectorsFormat.DIRECT_MONOTONIC_BLOCK_SHIFT;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Writes vector values and knn graphs to index segments.
 *
 * @lucene.experimental
 */
public final class Lucene98QuantizedHnswVectorsWriter extends KnnVectorsWriter {

  private final static float REQUANTIZATION_ERROR_RATE = 1e-5f;
  private final SegmentWriteState segmentWriteState;
  private final IndexOutput meta, vectorData, quantizedVectorData, vectorIndex;
  private final int M;
  private final int beamWidth;
  private final int quantile;

  private final List<FieldWriter> fields = new ArrayList<>();
  private boolean finished;

  Lucene98QuantizedHnswVectorsWriter(SegmentWriteState state, int M, int beamWidth, int quantile) throws IOException {
    this.M = M;
    this.beamWidth = beamWidth;
    this.quantile = quantile;
    segmentWriteState = state;
    String metaFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, Lucene98QuantizedHnswVectorsFormat.META_EXTENSION);

    String vectorDataFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            Lucene98QuantizedHnswVectorsFormat.VECTOR_DATA_EXTENSION);

    String quantizedVectorDataFileName =
            IndexFileNames.segmentFileName(
                    state.segmentInfo.name,
                    state.segmentSuffix,
                    Lucene98QuantizedHnswVectorsFormat.QUANTIZED_VECTOR_DATA_EXTENSION);

    String indexDataFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            Lucene98QuantizedHnswVectorsFormat.VECTOR_INDEX_EXTENSION);
    boolean success = false;
    try {
      meta = state.directory.createOutput(metaFileName, state.context);
      vectorData = state.directory.createOutput(vectorDataFileName, state.context);
      quantizedVectorData = state.directory.createOutput(quantizedVectorDataFileName, state.context);
      vectorIndex = state.directory.createOutput(indexDataFileName, state.context);

      CodecUtil.writeIndexHeader(
          meta,
          Lucene98QuantizedHnswVectorsFormat.META_CODEC_NAME,
          Lucene98QuantizedHnswVectorsFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      CodecUtil.writeIndexHeader(
          vectorData,
          Lucene98QuantizedHnswVectorsFormat.VECTOR_DATA_CODEC_NAME,
          Lucene98QuantizedHnswVectorsFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      CodecUtil.writeIndexHeader(
              quantizedVectorData,
              Lucene98QuantizedHnswVectorsFormat.QUANTIZED_VECTOR_DATA_CODEC_NAME,
              Lucene98QuantizedHnswVectorsFormat.VERSION_CURRENT,
              state.segmentInfo.getId(),
              state.segmentSuffix);
      CodecUtil.writeIndexHeader(
          vectorIndex,
          Lucene98QuantizedHnswVectorsFormat.VECTOR_INDEX_CODEC_NAME,
          Lucene98QuantizedHnswVectorsFormat.VERSION_CURRENT,
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
    if (fieldInfo.getVectorEncoding() != VectorEncoding.FLOAT32) {
      throw new IllegalArgumentException("Only float32 vector fields are supported");
    }
    FieldWriter newField =
        FieldWriter.create(fieldInfo, M, beamWidth, quantile, segmentWriteState.infoStream);
    fields.add(newField);
    return newField;
  }

  @Override
  public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
    for (FieldWriter field : fields) {
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
      CodecUtil.writeFooter(quantizedVectorData);
      CodecUtil.writeFooter(vectorIndex);
    }
  }

  @Override
  public long ramBytesUsed() {
    long total = 0;
    for (FieldWriter field : fields) {
      total += field.ramBytesUsed();
    }
    return total;
  }

  private void writeField(FieldWriter fieldData, int maxDoc) throws IOException {
    // write raw vector values
    long vectorDataOffset = vectorData.alignFilePointer(Float.BYTES);
    writeFloat32Vectors(fieldData);
    long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;


    long quantizedVectorDataOffset = quantizedVectorData.alignFilePointer(Float.BYTES);
    writeQuantizedVectors(fieldData);
    long quantizedVectorDataLength = quantizedVectorData.getFilePointer() - quantizedVectorDataOffset;

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
        quantizedVectorDataOffset,
        quantizedVectorDataLength,
        vectorIndexOffset,
        vectorIndexLength,
        fieldData.minQuantile,
        fieldData.maxQuantile,
        fieldData.docsWithField,
        graph,
        graphLevelNodeOffsets);
  }

  private void writeFloat32Vectors(FieldWriter fieldData) throws IOException {
    final ByteBuffer buffer =
        ByteBuffer.allocate(fieldData.dim * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    for (float[] v : fieldData.floatVectors) {
      buffer.asFloatBuffer().put(v);
      vectorData.writeBytes(buffer.array(), buffer.array().length);
    }
  }

  private void writeQuantizedVectors(FieldWriter fieldData) throws IOException {
    ScalarQuantizer scalarQuantizer = fieldData.createQuantizer();
    for (float[] v : fieldData.floatVectors) {
      byte[] vector = scalarQuantizer.quantize(v);
      quantizedVectorData.writeBytes(vector, vector.length);
      float offsetCorrection = fieldData.calculateVectorOffsetCorrection(vector, scalarQuantizer);
      quantizedVectorData.writeInt(Float.floatToIntBits(offsetCorrection));
    }
  }

  private void writeSortingField(FieldWriter fieldData, int maxDoc, Sorter.DocMap sortMap)
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
    long vectorDataOffset = writeSortedFloat32Vectors(fieldData, ordMap);
    long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;
    long quantizedVectorDataOffset = writeSortedQuantizedVectors(fieldData, ordMap);
    long quantizedVectorLength = quantizedVectorData.getFilePointer() - vectorDataOffset;

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
        quantizedVectorDataOffset,
        quantizedVectorLength,
        vectorIndexOffset,
        vectorIndexLength,
        fieldData.minQuantile,
        fieldData.maxQuantile,
        newDocsWithField,
        mockGraph,
        graphLevelNodeOffsets);
  }

  private long writeSortedFloat32Vectors(FieldWriter fieldData, int[] ordMap)
      throws IOException {
    long vectorDataOffset = vectorData.alignFilePointer(Float.BYTES);
    final ByteBuffer buffer =
        ByteBuffer.allocate(fieldData.dim * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    for (int ordinal : ordMap) {
      float[] vector = fieldData.floatVectors.get(ordinal);
      buffer.asFloatBuffer().put(vector);
      vectorData.writeBytes(buffer.array(), buffer.array().length);
    }
    return vectorDataOffset;
  }

  private long writeSortedQuantizedVectors(FieldWriter fieldData, int[] ordMap) throws IOException {
    long vectorDataOffset = quantizedVectorData.alignFilePointer(Float.BYTES);
    ScalarQuantizer scalarQuantizer = fieldData.createQuantizer();
    for (int ordinal : ordMap) {
      float[] v = fieldData.floatVectors.get(ordinal);
      byte[] vector = scalarQuantizer.quantize(v);
      quantizedVectorData.writeBytes(vector, vector.length);
      float offsetCorrection = fieldData.calculateVectorOffsetCorrection(vector, scalarQuantizer);
      quantizedVectorData.writeInt(Float.floatToIntBits(offsetCorrection));
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
    NodesIterator nodesOnLevel0 = graph.getNodesOnLevel(0);
    levelNodeOffsets[0] = new int[nodesOnLevel0.size()];
    while (nodesOnLevel0.hasNext()) {
      int node = nodesOnLevel0.nextInt();
      NeighborArray neighbors = graph.getNeighbors(0, newToOldMap[node]);
      long offset = vectorIndex.getFilePointer();
      reconstructAndWriteNeigbours(neighbors, oldToNewMap, maxOrd);
      levelNodeOffsets[0][node] = Math.toIntExact(vectorIndex.getFilePointer() - offset);
    }

    for (int level = 1; level < graph.numLevels(); level++) {
      NodesIterator nodesOnLevel = graph.getNodesOnLevel(level);
      int[] newNodes = new int[nodesOnLevel.size()];
      for (int n = 0; nodesOnLevel.hasNext(); n++) {
        newNodes[n] = oldToNewMap[nodesOnLevel.nextInt()];
      }
      Arrays.sort(newNodes);
      nodesByLevel.add(newNodes);
      levelNodeOffsets[level] = new int[newNodes.length];
      int nodeOffsetIndex = 0;
      for (int node : newNodes) {
        NeighborArray neighbors = graph.getNeighbors(level, newToOldMap[node]);
        long offset = vectorIndex.getFilePointer();
        reconstructAndWriteNeigbours(neighbors, oldToNewMap, maxOrd);
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

  private void reconstructAndWriteNeigbours(NeighborArray neighbors, int[] oldToNewMap, int maxOrd)
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
    final long vectorDataOffset = vectorData.alignFilePointer(Float.BYTES);
    final long quantizedVectorDataOffset = quantizedVectorData.alignFilePointer(Float.BYTES);
    IndexOutput tempVectorData =
        segmentWriteState.directory.createTempOutput(
            vectorData.getName(), "temp", segmentWriteState.context);
    IndexOutput tempQuantizedVectorData =
            segmentWriteState.directory.createTempOutput(
                    quantizedVectorData.getName(), "temp", segmentWriteState.context);
    IndexInput vectorDataInput = null;
    IndexInput quantizedVectorDataInput = null;
    boolean success = false;
    try {
      // write the vector data to a temporary file
      DocsWithFieldSet docsWithField = writeVectorData(
              tempVectorData, MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState));
      CodecUtil.writeFooter(tempVectorData);
      IOUtils.close(tempVectorData);
      // copy the temporary file vectors to the actual data file
      vectorDataInput =
          segmentWriteState.directory.openInput(
              tempVectorData.getName(), segmentWriteState.context);
      vectorData.copyBytes(vectorDataInput, vectorDataInput.length() - CodecUtil.footerLength());
      CodecUtil.retrieveChecksum(vectorDataInput);
      final long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;
      final long vectorIndexOffset = vectorIndex.getFilePointer();
      int byteSize = fieldInfo.getVectorDimension() * VectorEncoding.FLOAT32.byteSize;
      OffHeapFloatVectorValues.DenseOffHeapVectorValues floatVectorValues =
              new OffHeapFloatVectorValues.DenseOffHeapVectorValues(
                      fieldInfo.getVectorDimension(),
                      docsWithField.cardinality(),
                      vectorDataInput,
                      byteSize);
      RequantizationState requantizationState = requiresReQuantization(mergeState, fieldInfo);
      final float[] minMaxQuantiles;
      final QuantizedByteVectorValues quantizedByteVectorValues;
      if (requantizationState.requiresRequantization) {
        FloatVectorValues values = floatVectorValues.copy();
        ScalarQuantizer quantizer = ScalarQuantizer.fromVectors(values, quantile);
        quantizedByteVectorValues = new QuantizedFloatVectorValues(
                floatVectorValues.copy(),
                fieldInfo.getVectorSimilarityFunction(),
                quantizer
        );
        writeQuantizedVectorData(
                tempQuantizedVectorData,
                quantizedByteVectorValues
        );
        CodecUtil.writeFooter(tempVectorData);
        IOUtils.close(tempVectorData);
        minMaxQuantiles = new float[] {quantizer.getLowerQuantile(), quantizer.getUpperQuantile()};
      } else {
        minMaxQuantiles = new float[] {requantizationState.lowerQuantile, requantizationState.upperQuantile};
        quantizedByteVectorValues = MergedQuantizedVectorValues.mergeQuantizedByteVectorValues(fieldInfo, mergeState);
        CodecUtil.writeFooter(tempVectorData);
        IOUtils.close(tempVectorData);
      }
      writeQuantizedVectorData(
              tempQuantizedVectorData,
              quantizedByteVectorValues
      );
      CodecUtil.writeFooter(tempVectorData);
      IOUtils.close(tempVectorData);
      // copy the temporary file vectors to the actual data file
      quantizedVectorDataInput =
              segmentWriteState.directory.openInput(
                      tempQuantizedVectorData.getName(), segmentWriteState.context);
      quantizedVectorData.copyBytes(quantizedVectorDataInput, quantizedVectorDataInput.length() - CodecUtil.footerLength());
      CodecUtil.retrieveChecksum(quantizedVectorDataInput);
      final long quantizedVectorDataLength = quantizedVectorDataInput.getFilePointer() - quantizedVectorDataOffset;
      OffHeapQuantizedByteVectorValues.DenseOffHeapVectorValues offHeapQuantizedByteVectorValues =
              new OffHeapQuantizedByteVectorValues.DenseOffHeapVectorValues(
                      fieldInfo.getVectorDimension(),
                      docsWithField.cardinality(),
                      quantizedVectorDataInput);

      // build the graph using the temporary vector data
      // we use Lucene98HnswVectorsReader.DenseOffHeapVectorValues for the graph construction
      // doesn't need to know docIds
      // TODO: separate random access vector values from DocIdSetIterator?
      OnHeapHnswGraph graph = null;
      int[][] vectorIndexNodeOffsets = null;
      if (docsWithField.cardinality() != 0) {
        // build graph
        int initializerIndex = selectGraphForInitialization(mergeState, fieldInfo);
        HnswGraphBuilder<byte[]> hnswGraphBuilder =
                createHnswGraphBuilder(mergeState, fieldInfo, vectorValues, initializerIndex);
        hnswGraphBuilder.setInfoStream(segmentWriteState.infoStream);
        yield hnswGraphBuilder.build(vectorValues.copy());
        graph =
            switch (fieldInfo.getVectorEncoding()) {
              case BYTE -> {
                OffHeapQuantizedByteVectorValues.DenseOffHeapVectorValues vectorValues =
                    new OffHeapQuantizedByteVectorValues.DenseOffHeapVectorValues(
                        fieldInfo.getVectorDimension(),
                        docsWithField.cardinality(),
                        vectorDataInput,
                        byteSize);
                HnswGraphBuilder<byte[]> hnswGraphBuilder =
                    createHnswGraphBuilder(mergeState, fieldInfo, vectorValues, initializerIndex);
                hnswGraphBuilder.setInfoStream(segmentWriteState.infoStream);
                yield hnswGraphBuilder.build(vectorValues.copy());
              }
              case FLOAT32 -> {
                OffHeapFloatVectorValues.DenseOffHeapVectorValues vectorValues =
                    new OffHeapFloatVectorValues.DenseOffHeapVectorValues(
                        fieldInfo.getVectorDimension(),
                        docsWithField.cardinality(),
                        vectorDataInput,
                        byteSize);
                HnswGraphBuilder<float[]> hnswGraphBuilder =
                    createHnswGraphBuilder(mergeState, fieldInfo, vectorValues, initializerIndex);
                hnswGraphBuilder.setInfoStream(segmentWriteState.infoStream);
                yield hnswGraphBuilder.build(vectorValues.copy());
              }
            };
        vectorIndexNodeOffsets = writeGraph(graph);
      }
      long vectorIndexLength = vectorIndex.getFilePointer() - vectorIndexOffset;
      writeMeta(
          fieldInfo,
          segmentWriteState.segmentInfo.maxDoc(),
          vectorDataOffset,
          vectorDataLength,
          quantizedVectorDataOffset,
          quantizedVectorDataLength,
          vectorIndexOffset,
          vectorIndexLength,
          minMaxQuantiles[0],
          minMaxQuantiles[1],
          docsWithField,
          graph,
          vectorIndexNodeOffsets);
      success = true;
    } finally {
      IOUtils.close(vectorDataInput, quantizedVectorDataInput);
      if (success) {
        segmentWriteState.directory.deleteFile(tempVectorData.getName());
        segmentWriteState.directory.deleteFile(tempQuantizedVectorData.getName());
      } else {
        IOUtils.closeWhileHandlingException(tempVectorData, tempQuantizedVectorData);
        IOUtils.deleteFilesIgnoringExceptions(
            segmentWriteState.directory, tempVectorData.getName(), tempQuantizedVectorData.getName());
      }
    }
  }

  private HnswGraphBuilder<float[]> createHnswGraphBuilder(
      MergeState mergeState,
      FieldInfo fieldInfo,
      VectorScorerSupplier vectorScorerSupplier,
      int initializerIndex)
      throws IOException {
    if (initializerIndex == -1) {
      return HnswGraphBuilder.create(
              vectorScorerSupplier,
          fieldInfo.getVectorEncoding(),
          M,
          beamWidth,
          HnswGraphBuilder.randSeed);
    }

    HnswGraph initializerGraph =
        getHnswGraphFromReader(fieldInfo.name, mergeState.knnVectorsReaders[initializerIndex]);
    Map<Integer, Integer> ordinalMapper =
        getOldToNewOrdinalMap(mergeState, fieldInfo, initializerIndex);
    return HnswGraphBuilder.create(
            vectorScorerSupplier,
        fieldInfo.getVectorEncoding(),
        M,
        beamWidth,
        HnswGraphBuilder.randSeed,
        initializerGraph,
        ordinalMapper);
  }

  private RequantizationState requiresReQuantization(MergeState mergeState, FieldInfo fieldInfo) {
    float[] minQuantiles = new float[mergeState.liveDocs.length];
    float[] maxQuantiles = new float[mergeState.liveDocs.length];
    float minQuantile = Float.POSITIVE_INFINITY;
    float maxQuantile = Float.NEGATIVE_INFINITY;
    boolean mustRequantize = false;
    for (int i = 0; i < mergeState.liveDocs.length; i++) {
      KnnVectorsReader currKnnVectorsReader = mergeState.knnVectorsReaders[i];
      if (mergeState.knnVectorsReaders[i]
              instanceof PerFieldKnnVectorsFormat.FieldsReader candidateReader) {
        currKnnVectorsReader = candidateReader.getFieldReader(fieldInfo.name);
      }
      if (currKnnVectorsReader instanceof Lucene98QuantizedHnswVectorsReader reader) {
        float[] minAndMaxQuantiles = reader.getQuantiles(fieldInfo.name);
        minQuantiles[i] = minAndMaxQuantiles[0];
        maxQuantiles[i] = minAndMaxQuantiles[1];
        minQuantile = Math.min(minQuantile, minAndMaxQuantiles[0]);
        maxQuantile = Math.min(maxQuantile, minAndMaxQuantiles[1]);
        for (int j = 0; j < i && mustRequantize == false; j++) {
          mustRequantize =
                  Math.abs(minAndMaxQuantiles[0] - minQuantiles[j]) <= REQUANTIZATION_ERROR_RATE
                  || Math.abs(minAndMaxQuantiles[1] - maxQuantiles[j]) <= REQUANTIZATION_ERROR_RATE;
        }
        if (mustRequantize) {
          return new RequantizationState(Float.NaN, Float.NaN, true);
        }
      } else {
        throw new IllegalArgumentException(
                "attempting to merge in unknown codec ["
                        + currKnnVectorsReader.toString()
                        + "] for field ["
                        + fieldInfo.name
                        + "]");
      }
    }
    return new RequantizationState(minQuantile, maxQuantile, false);
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
      if (mergeState.knnVectorsReaders[i]
          instanceof PerFieldKnnVectorsFormat.FieldsReader candidateReader) {
        currKnnVectorsReader = candidateReader.getFieldReader(fieldInfo.name);
      }

      if (!allMatch(mergeState.liveDocs[i])
          || !(currKnnVectorsReader instanceof Lucene98QuantizedHnswVectorsReader candidateReader)) {
        continue;
      }

      int candidateVectorCount = 0;
      switch (fieldInfo.getVectorEncoding()) {
        case BYTE -> {
          ByteVectorValues byteVectorValues = candidateReader.getByteVectorValues(fieldInfo.name);
          if (byteVectorValues == null) {
            continue;
          }
          candidateVectorCount = byteVectorValues.size();
        }
        case FLOAT32 -> {
          FloatVectorValues vectorValues = candidateReader.getFloatVectorValues(fieldInfo.name);
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
    return initializerIndex;
  }

  private HnswGraph getHnswGraphFromReader(String fieldName, KnnVectorsReader knnVectorsReader)
      throws IOException {
    if (knnVectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader perFieldReader
        && perFieldReader.getFieldReader(fieldName)
            instanceof Lucene98QuantizedHnswVectorsReader fieldReader) {
      return fieldReader.getGraph(fieldName);
    }

    if (knnVectorsReader instanceof Lucene98QuantizedHnswVectorsReader) {
      return ((Lucene98QuantizedHnswVectorsReader) knnVectorsReader).getGraph(fieldName);
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

    DocIdSetIterator initializerIterator = mergeState.knnVectorsReaders[initializerIndex].getFloatVectorValues(fieldInfo.name);

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

    DocIdSetIterator vectorIterator = MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState);

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
      int[] sortedNodes = getSortedNodes(graph.getNodesOnLevel(level));
      offsets[level] = new int[sortedNodes.length];
      int nodeOffsetId = 0;
      for (int node : sortedNodes) {
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

  public static int[] getSortedNodes(NodesIterator nodesOnLevel) {
    int[] sortedNodes = new int[nodesOnLevel.size()];
    for (int n = 0; nodesOnLevel.hasNext(); n++) {
      sortedNodes[n] = nodesOnLevel.nextInt();
    }
    Arrays.sort(sortedNodes);
    return sortedNodes;
  }

  private void writeMeta(
      FieldInfo field,
      int maxDoc,
      long vectorDataOffset,
      long vectorDataLength,
      long quantizedVectorDataOffset,
      long quantizedVectorDataLength,
      long vectorIndexOffset,
      long vectorIndexLength,
      float lowerQuantile,
      float upperQuantile,
      DocsWithFieldSet docsWithField,
      HnswGraph graph,
      int[][] graphLevelNodeOffsets)
      throws IOException {
    meta.writeInt(field.number);
    meta.writeInt(field.getVectorEncoding().ordinal());
    meta.writeInt(field.getVectorSimilarityFunction().ordinal());
    meta.writeVLong(vectorDataOffset);
    meta.writeVLong(vectorDataLength);
    meta.writeVLong(quantizedVectorDataOffset);
    meta.writeVLong(quantizedVectorDataLength);
    meta.writeVLong(vectorIndexOffset);
    meta.writeVLong(vectorIndexLength);
    meta.writeVInt(field.getVectorDimension());
    meta.writeInt(Float.floatToIntBits(lowerQuantile));
    meta.writeInt(Float.floatToIntBits(upperQuantile));

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
          Arrays.sort(nol);
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

  private static void writeQuantizedVectorData(
          IndexOutput output, QuantizedByteVectorValues quantizedByteVectorValues) throws IOException {
    for (int docV = quantizedByteVectorValues.nextDoc();
         docV != NO_MORE_DOCS;
         docV = quantizedByteVectorValues.nextDoc()) {
      // write vector
      byte[] binaryValue = quantizedByteVectorValues.vectorValue();
      assert binaryValue.length == quantizedByteVectorValues.dimension() * VectorEncoding.BYTE.byteSize;
      output.writeBytes(binaryValue, binaryValue.length);
      output.writeInt(Float.floatToIntBits(quantizedByteVectorValues.getScoreCorrectionConstant()));
    }
  }


  @Override
  public void close() throws IOException {
    IOUtils.close(meta, vectorData, vectorIndex);
  }

  private static class FieldWriter extends KnnFieldVectorsWriter<float[]> {
    private final FieldInfo fieldInfo;
    private final int dim;
    private final DocsWithFieldSet docsWithField;
    private final List<float[]> floatVectors;
    private final HnswGraphBuilder<float[]> hnswGraphBuilder;
    private final boolean normalize;
    private final int quantile;
    private float minQuantile = Float.POSITIVE_INFINITY;
    private float maxQuantile = Float.NEGATIVE_INFINITY;

    private int lastDocID = -1;
    private int node = 0;

    static FieldWriter create(FieldInfo fieldInfo, int M, int beamWidth, int quantile, InfoStream infoStream)
        throws IOException {
      return new FieldWriter(
              fieldInfo,
              M,
              beamWidth,
              quantile,
              fieldInfo.getVectorSimilarityFunction() == VectorSimilarityFunction.COSINE,
              infoStream
      );
    }

    FieldWriter(
            FieldInfo fieldInfo,
            int M,
            int beamWidth,
            int quantile,
            boolean normalize,
            InfoStream infoStream
    )
        throws IOException {
      this.fieldInfo = fieldInfo;
      this.dim = fieldInfo.getVectorDimension();
      this.quantile = quantile;
      this.normalize = normalize;
      this.docsWithField = new DocsWithFieldSet();
      floatVectors = new ArrayList<>();
      hnswGraphBuilder =
          HnswGraphBuilder.create(
              new RAVectorValues(floatVectors, dim),
              fieldInfo.getVectorEncoding(),
              fieldInfo.getVectorSimilarityFunction(),
              M,
              beamWidth,
              HnswGraphBuilder.randSeed);
      hnswGraphBuilder.setInfoStream(infoStream);
    }

    @Override
    public void addValue(int docID, float[] vectorValue) throws IOException {
      if (docID == lastDocID) {
        throw new IllegalArgumentException(
            "VectorValuesField \""
                + fieldInfo.name
                + "\" appears more than once in this document (only one value is allowed per field)");
      }
      assert docID > lastDocID;
      docsWithField.add(docID);
      float[] copy = copyValue(vectorValue);
      if (normalize) {
        // vectorize?
        VectorUtil.l2normalize(copy);
      }
      if (quantile == 100) {
        // vectorize?
        for (float v : copy) {
          minQuantile = Math.min(v, minQuantile);
          maxQuantile = Math.max(v, maxQuantile);
        }
      }

      floatVectors.add(copy);
      hnswGraphBuilder.addGraphNode(node, vectorValue);
      node++;
      lastDocID = docID;
    }

    OnHeapHnswGraph getGraph() {
      if (floatVectors.size() > 0) {
        return hnswGraphBuilder.getGraph();
      } else {
        return null;
      }
    }

    private ScalarQuantizer createQuantizer() throws IOException {
      if (quantile == 100) {
        return new ScalarQuantizer(new float[]{minQuantile, maxQuantile});
      } else {
        return ScalarQuantizer.fromVectors(new FloatVectorWrapper(floatVectors), quantile);
      }
    }



    @Override
    public long ramBytesUsed() {
      if (floatVectors.size() == 0) return 0;
      return docsWithField.ramBytesUsed()
          + (long) floatVectors.size()
              * (RamUsageEstimator.NUM_BYTES_OBJECT_REF + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER)
          + (long) floatVectors.size()
              * (RamUsageEstimator.NUM_BYTES_OBJECT_REF + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER)
          + (long) floatVectors.size()
              * fieldInfo.getVectorDimension()
              * fieldInfo.getVectorEncoding().byteSize
          + hnswGraphBuilder.getGraph().ramBytesUsed();
    }

    @Override
    public float[] copyValue(float[] value) {
      return ArrayUtil.copyOfSubArray(value, 0, dim);
    }
  }

  private float calculateVectorOffsetCorrection(
          byte[] quantizedVector,
          ScalarQuantizer scalarQuantizer,
          VectorSimilarityFunction vectorSimilarityFunction
  ) {
    if (vectorSimilarityFunction != VectorSimilarityFunction.EUCLIDEAN) {
      int sum = 0;
      for (byte b : quantizedVector) {
        sum += b;
      }
      return sum * scalarQuantizer.getAlpha() * scalarQuantizer.getOffset();
    }
    return 0f;
  }

  private static class RAVectorValues implements RandomAccessVectorValues<float[]> {
    private final List<float[]> vectors;
    private final int dim;

    RAVectorValues(List<float[]> vectors, int dim) {
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
    public float[] vectorValue(int targetOrd) throws IOException {
      return vectors.get(targetOrd);
    }

    @Override
    public RAVectorValues copy() throws IOException {
      return this;
    }
  }

  private static class FloatVectorWrapper extends FloatVectorValues {
    private final List<float[]> vectorList;
    protected int curDoc = -1;

    private FloatVectorWrapper(List<float[]> vectorList) {
      this.vectorList = vectorList;
    }

    @Override
    public int dimension() {
      return vectorList.get(0).length;
    }

    @Override
    public int size() {
      return vectorList.size();
    }

    @Override
    public float[] vectorValue() throws IOException {
      if (curDoc == -1 || curDoc >= vectorList.size()) {
        throw new IOException("Current doc not set or too many iterations");
      }
      return vectorList.get(curDoc);
    }

    @Override
    public int docID() {
      if (curDoc >= vectorList.size()) {
        return NO_MORE_DOCS;
      }
      return curDoc;
    }

    @Override
    public int nextDoc() throws IOException {
      curDoc++;
      return docID();
    }

    @Override
    public int advance(int target) throws IOException {
      curDoc = target;
      return docID();
    }
  }

  private static class RequantizationState {
    private final float lowerQuantile;
    private final float upperQuantile;
    private final boolean requiresRequantization;

    private RequantizationState(float lowerQuantile, float upperQuantile, boolean requiresRequantization) {
      this.lowerQuantile = lowerQuantile;
      this.upperQuantile = upperQuantile;
      this.requiresRequantization = requiresRequantization;
    }
  }

  private static class QuantizedByteVectorValueSub extends DocIDMerger.Sub {

    final QuantizedByteVectorValues values;

    QuantizedByteVectorValueSub(MergeState.DocMap docMap, QuantizedByteVectorValues values) {
      super(docMap);
      this.values = values;
      assert values.docID() == -1;
    }

    @Override
    public int nextDoc() throws IOException {
      return values.nextDoc();
    }
  }

    /** Returns a merged view over all the segment's {@link QuantizedByteVectorValues}. */
    static class MergedQuantizedVectorValues extends QuantizedByteVectorValues {
      public static QuantizedByteVectorValues mergeQuantizedByteVectorValues(
              FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        assert fieldInfo != null && fieldInfo.hasVectorValues();
        if (fieldInfo.getVectorEncoding() != VectorEncoding.FLOAT32) {
          throw new UnsupportedOperationException(
                  "Cannot merge vectors encoded as [" + fieldInfo.getVectorEncoding() + "] as quantized BYTE");
        }
        List<QuantizedByteVectorValueSub> subs = new ArrayList<>();
        for (int i = 0; i < mergeState.knnVectorsReaders.length; i++) {
          KnnVectorsReader knnVectorsReader = mergeState.knnVectorsReaders[i];
          if (knnVectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader perFieldKnnVectorsFormat) {
            knnVectorsReader = perFieldKnnVectorsFormat.getFieldReader(fieldInfo.name);
          }
          if (knnVectorsReader instanceof Lucene98QuantizedHnswVectorsReader reader) {
            subs.add(new QuantizedByteVectorValueSub(mergeState.docMaps[i], reader.getQuantizedVectorValues(fieldInfo.name)));
          } else if (knnVectorsReader != null){
            throw new UnsupportedOperationException(
                    "Cannot merge vectors from codec other than Lucene98QuantizedHnswVectorsFormat");
          }
        }
        return new MergedQuantizedVectorValues(subs, mergeState);
      }
      private final List<QuantizedByteVectorValueSub> subs;
      private final DocIDMerger<QuantizedByteVectorValueSub> docIdMerger;
      private final int size;

      private int docId;
      QuantizedByteVectorValueSub current;

      private MergedQuantizedVectorValues(List<QuantizedByteVectorValueSub> subs, MergeState mergeState)
              throws IOException {
        this.subs = subs;
        docIdMerger = DocIDMerger.of(subs, mergeState.needsIndexSort);
        int totalSize = 0;
        for (QuantizedByteVectorValueSub sub : subs) {
          totalSize += sub.values.size();
        }
        size = totalSize;
        docId = -1;
      }

      @Override
      public byte[] vectorValue() throws IOException {
        return current.values.vectorValue();
      }

      @Override
      public int docID() {
        return docId;
      }

      @Override
      public int nextDoc() throws IOException {
        current = docIdMerger.next();
        if (current == null) {
          docId = NO_MORE_DOCS;
        } else {
          docId = current.mappedDocID;
        }
        return docId;
      }

      @Override
      public int advance(int target) {
        throw new UnsupportedOperationException();
      }

      @Override
      public int size() {
        return size;
      }

      @Override
      public int dimension() {
        return subs.get(0).values.dimension();
      }

      @Override
      float getScoreCorrectionConstant() {
        return current.values.getScoreCorrectionConstant();
      }
    }

    private class QuantizedFloatVectorValues extends QuantizedByteVectorValues {
      private final FloatVectorValues values;
      private final ScalarQuantizer quantizer;
      private final byte[] quantizedVector;

      private final VectorSimilarityFunction vectorSimilarityFunction;

      public QuantizedFloatVectorValues(FloatVectorValues values, VectorSimilarityFunction vectorSimilarityFunction, ScalarQuantizer quantizer) {
        this.values = values;
        this.quantizer = quantizer;
        this.quantizedVector = new byte[values.dimension()];
        this.vectorSimilarityFunction = vectorSimilarityFunction;
      }

      @Override
      float getScoreCorrectionConstant() {
        return calculateVectorOffsetCorrection(quantizedVector, quantizer, vectorSimilarityFunction);
      }

      @Override
      public int dimension() {
        return values.dimension();
      }

      @Override
      public int size() {
        return values.size();
      }

      @Override
      public byte[] vectorValue() throws IOException {
        return quantizedVector;
      }

      @Override
      public int docID() {
        return values.docID();
      }

      @Override
      public int nextDoc() throws IOException {
        int doc = values.nextDoc();
        if (doc != NO_MORE_DOCS) {
          quantizer.quantizeTo(values.vectorValue(), quantizedVector);
        }
        return doc;
      }

      @Override
      public int advance(int target) throws IOException {
        int doc = values.advance(target);
        if (doc != NO_MORE_DOCS) {
          quantizer.quantizeTo(values.vectorValue(), quantizedVector);
        }
        return doc;
      }
    }
}
