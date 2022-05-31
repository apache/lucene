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

package org.apache.lucene.codecs.lucene93;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.RandomAccessVectorValuesProducer;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.hnsw.HnswGraph.NodesIterator;
import org.apache.lucene.util.hnsw.HnswGraphBuilder;
import org.apache.lucene.util.hnsw.NeighborArray;
import org.apache.lucene.util.hnsw.OnHeapHnswGraph;
import org.apache.lucene.util.packed.DirectMonotonicWriter;

import java.io.IOException;
import java.util.Arrays;
import java.util.Stack;

import static org.apache.lucene.codecs.lucene93.Lucene93HnswVectorsFormat.DIRECT_MONOTONIC_BLOCK_SHIFT;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Writes vector values and knn graphs to index segments.
 *
 * @lucene.experimental
 */
public final class Lucene93HnswVectorsWriter extends KnnVectorsWriter {

  private final SegmentWriteState segmentWriteState;
  private final IndexOutput meta, vectorData, vectorIndex;
  private final int maxDoc;

  private final int M;
  private final int beamWidth;
  private boolean finished;

  Lucene93HnswVectorsWriter(SegmentWriteState state, int M, int beamWidth) throws IOException {
    this.M = M;
    this.beamWidth = beamWidth;

    assert state.fieldInfos.hasVectorValues();
    segmentWriteState = state;

    String metaFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, Lucene93HnswVectorsFormat.META_EXTENSION);

    String vectorDataFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            Lucene93HnswVectorsFormat.VECTOR_DATA_EXTENSION);

    String indexDataFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            Lucene93HnswVectorsFormat.VECTOR_INDEX_EXTENSION);

    boolean success = false;
    try {
      meta = state.directory.createOutput(metaFileName, state.context);
      vectorData = state.directory.createOutput(vectorDataFileName, state.context);
      vectorIndex = state.directory.createOutput(indexDataFileName, state.context);

      CodecUtil.writeIndexHeader(
          meta,
          Lucene93HnswVectorsFormat.META_CODEC_NAME,
          Lucene93HnswVectorsFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      CodecUtil.writeIndexHeader(
          vectorData,
          Lucene93HnswVectorsFormat.VECTOR_DATA_CODEC_NAME,
          Lucene93HnswVectorsFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      CodecUtil.writeIndexHeader(
          vectorIndex,
          Lucene93HnswVectorsFormat.VECTOR_INDEX_CODEC_NAME,
          Lucene93HnswVectorsFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      maxDoc = state.segmentInfo.maxDoc();
      success = true;
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }

  @Override
  public void writeField(FieldInfo fieldInfo, KnnVectorsReader knnVectorsReader)
      throws IOException {
    long vectorDataOffset = vectorData.alignFilePointer(Float.BYTES);
    VectorValues vectors = knnVectorsReader.getVectorValues(fieldInfo.name);

    IndexOutput tempVectorData =
        segmentWriteState.directory.createTempOutput(
            vectorData.getName(), "temp", segmentWriteState.context);
    IndexInput vectorDataInput = null;
    boolean success = false;
    try {
      // write the vector data to a temporary file
      DocsWithFieldSet docsWithField = writeVectorData(tempVectorData, vectors);
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
      // we use Lucene92HnswVectorsReader.DenseOffHeapVectorValues for the graph construction
      // doesn't need to know docIds
      // TODO: separate random access vector values from DocIdSetIterator?
      //for writing the graph, it's fine it's dense, in the graph, we are just adding the vectorIds
      OffHeapVectorValues offHeapVectors =
          new OffHeapVectorValues.DenseOffHeapVectorValues(
                vectors.dimension(), vectors.size(), vectorDataInput);
      OnHeapHnswGraph graph =
          offHeapVectors.size() == 0
              ? null
              : writeGraph(offHeapVectors, fieldInfo.getVectorSimilarityFunction());
      long vectorIndexLength = vectorIndex.getFilePointer() - vectorIndexOffset;
      writeMeta(
          fieldInfo,
          vectorDataOffset,
          vectorDataLength,
          vectorIndexOffset,
          vectorIndexLength,
          docsWithField,
          vectors,
          graph);
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

  /**
   * Writes the vector values to the output and returns a set of documents that contains vectors.
   */
  private static DocsWithFieldSet writeVectorData(IndexOutput output, VectorValues vectors)
      throws IOException {
    DocsWithFieldSet docsWithField = new DocsWithFieldSet();
    Stack<Integer> valuesPerDocumemts = new Stack<>();
    for (int docV = vectors.nextDoc(); docV != NO_MORE_DOCS; docV = vectors.nextDoc()) {
      int valuesPerDocument = 0;
      for(long vectorId = vectors.nextOrd();vectorId!=-1;vectorId = vectors.nextOrd()) {
        // write vector
        BytesRef binaryValue = vectors.binaryValue();
        assert binaryValue.length == vectors.dimension() * Float.BYTES;
        output.writeBytes(binaryValue.bytes, binaryValue.offset, binaryValue.length);
        valuesPerDocument++;
      }
      valuesPerDocumemts.push(valuesPerDocument);
      docsWithField.add(docV);
    }
    int[] valuesPerDocument = new int[docsWithField.cardinality()];
    for(int i=valuesPerDocument.length-1; i>-1;i--){
      valuesPerDocument[i] = valuesPerDocumemts.pop();
    }
    docsWithField.setValuesPerDocument(valuesPerDocument);
    return docsWithField;
  }

  private void writeMeta(
          FieldInfo field,
          long vectorDataOffset,
          long vectorDataLength,
          long vectorIndexOffset,
          long vectorIndexLength,
          DocsWithFieldSet docsWithField,
          VectorValues vectors,
          OnHeapHnswGraph graph)
      throws IOException {
    meta.writeInt(field.number);
    meta.writeInt(field.getVectorSimilarityFunction().ordinal());
    meta.writeVLong(vectorDataOffset);
    meta.writeVLong(vectorDataLength);
    meta.writeVLong(vectorIndexOffset);
    meta.writeVLong(vectorIndexLength);
    meta.writeInt(field.getVectorDimension());

    // write docIDs
    meta.writeInt(vectors.size());
    if (vectors.size() == 0) {
      meta.writeLong(-2); // docsWithFieldOffset
      meta.writeLong(0L); // docsWithFieldLength
      meta.writeShort((short) -1); // jumpTableEntryCount
      meta.writeByte((byte) -1); // denseRankPower
    } else if (!field.isVectorMultiValued() && vectors.size() == maxDoc ) {
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
          DirectMonotonicWriter.getInstance(meta, vectorData, vectors.size(), DIRECT_MONOTONIC_BLOCK_SHIFT);
      DocIdSetIterator iterator = docsWithField.iterator();
      int[] valuesPerDocument = docsWithField.getValuesPerDocument();
      for (int doc = iterator.nextDoc();
           doc != DocIdSetIterator.NO_MORE_DOCS;
           doc = iterator.nextDoc()) {
        int documentVectorsCount = valuesPerDocument[doc];
        for (int i = 0; i < documentVectorsCount; i++) {
          ordToDocWriter.add(doc);
        }
      }
      ordToDocWriter.finish();
      meta.writeLong(vectorData.getFilePointer() - start);
    }

    meta.writeInt(M);
    // write graph nodes on each level
    if (graph == null) {
      meta.writeInt(0);
    } else {
      meta.writeInt(graph.numLevels());
      for (int level = 0; level < graph.numLevels(); level++) {
        NodesIterator nodesOnLevel = graph.getNodesOnLevel(level);
        meta.writeInt(nodesOnLevel.size()); // number of nodes on a level
        if (level > 0) {
          while (nodesOnLevel.hasNext()) {
            int node = nodesOnLevel.nextInt();
            meta.writeInt(node); // list of nodes on a level
          }
        }
      }
    }
  }

  private OnHeapHnswGraph writeGraph(
      RandomAccessVectorValuesProducer vectorValues, VectorSimilarityFunction similarityFunction)
      throws IOException {

    // build graph
    HnswGraphBuilder hnswGraphBuilder =
        new HnswGraphBuilder(
            vectorValues, similarityFunction, M, beamWidth, HnswGraphBuilder.randSeed);
    hnswGraphBuilder.setInfoStream(segmentWriteState.infoStream);
    OnHeapHnswGraph graph = hnswGraphBuilder.build(vectorValues.randomAccess());

    // write vectors' neighbours on each level into the vectorIndex file
    int countOnLevel0 = graph.size();
    for (int level = 0; level < graph.numLevels(); level++) {
      int maxConnOnLevel = level == 0 ? (M * 2) : M;
      NodesIterator nodesOnLevel = graph.getNodesOnLevel(level);
      while (nodesOnLevel.hasNext()) {
        int node = nodesOnLevel.nextInt();
        NeighborArray neighbors = graph.getNeighbors(level, node);
        int size = neighbors.size();
        vectorIndex.writeInt(size);
        // Destructively modify; it's ok we are discarding it after this
        int[] nnodes = neighbors.node();
        Arrays.sort(nnodes, 0, size);
        for (int i = 0; i < size; i++) {
          int nnode = nnodes[i];
          assert nnode < countOnLevel0 : "node too large: " + nnode + ">=" + countOnLevel0;
          vectorIndex.writeInt(nnode);
        }
        // if number of connections < maxConn, add bogus values up to maxConn to have predictable
        // offsets
        for (int i = size; i < maxConnOnLevel; i++) {
          vectorIndex.writeInt(0);
        }
      }
    }
    return graph;
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
  public void close() throws IOException {
    IOUtils.close(meta, vectorData, vectorIndex);
  }
}
