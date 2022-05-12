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

package org.apache.lucene.backward_codecs.lucene91;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
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

/**
 * Writes vector values and knn graphs to index segments.
 *
 * @lucene.experimental
 */
public final class Lucene91HnswVectorsWriter extends KnnVectorsWriter {

  private final SegmentWriteState segmentWriteState;
  private final IndexOutput meta, vectorData, vectorIndex;
  private final int maxDoc;

  private final int maxConn;
  private final int beamWidth;
  private boolean finished;

  Lucene91HnswVectorsWriter(SegmentWriteState state, int maxConn, int beamWidth)
      throws IOException {
    this.maxConn = maxConn;
    this.beamWidth = beamWidth;

    assert state.fieldInfos.hasVectorValues();
    segmentWriteState = state;

    String metaFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, Lucene91HnswVectorsFormat.META_EXTENSION);

    String vectorDataFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            Lucene91HnswVectorsFormat.VECTOR_DATA_EXTENSION);

    String indexDataFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            Lucene91HnswVectorsFormat.VECTOR_INDEX_EXTENSION);

    boolean success = false;
    try {
      meta = state.directory.createOutput(metaFileName, state.context);
      vectorData = state.directory.createOutput(vectorDataFileName, state.context);
      vectorIndex = state.directory.createOutput(indexDataFileName, state.context);

      CodecUtil.writeIndexHeader(
          meta,
          Lucene91HnswVectorsFormat.META_CODEC_NAME,
          Lucene91HnswVectorsFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      CodecUtil.writeIndexHeader(
          vectorData,
          Lucene91HnswVectorsFormat.VECTOR_DATA_CODEC_NAME,
          Lucene91HnswVectorsFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      CodecUtil.writeIndexHeader(
          vectorIndex,
          Lucene91HnswVectorsFormat.VECTOR_INDEX_CODEC_NAME,
          Lucene91HnswVectorsFormat.VERSION_CURRENT,
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
      // we pass null for ordToDoc mapping, for the graph construction doesn't need to know docIds
      // TODO: separate random access vector values from DocIdSetIterator?
      Lucene91HnswVectorsReader.OffHeapVectorValues offHeapVectors =
          new Lucene91HnswVectorsReader.OffHeapVectorValues(
              vectors.dimension(), docsWithField.cardinality(), null, vectorDataInput);
      Lucene91OnHeapHnswGraph graph =
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
    for (int docV = vectors.nextDoc(); docV != NO_MORE_DOCS; docV = vectors.nextDoc()) {
      // write vector
      BytesRef binaryValue = vectors.binaryValue();
      assert binaryValue.length == vectors.dimension() * Float.BYTES;
      output.writeBytes(binaryValue.bytes, binaryValue.offset, binaryValue.length);
      docsWithField.add(docV);
    }
    return docsWithField;
  }

  private void writeMeta(
      FieldInfo field,
      long vectorDataOffset,
      long vectorDataLength,
      long vectorIndexOffset,
      long vectorIndexLength,
      DocsWithFieldSet docsWithField,
      Lucene91OnHeapHnswGraph graph)
      throws IOException {
    meta.writeInt(field.number);
    meta.writeInt(field.getVectorSimilarityFunction().ordinal());
    meta.writeVLong(vectorDataOffset);
    meta.writeVLong(vectorDataLength);
    meta.writeVLong(vectorIndexOffset);
    meta.writeVLong(vectorIndexLength);
    meta.writeInt(field.getVectorDimension());

    // write docIDs
    int count = docsWithField.cardinality();
    meta.writeInt(count);
    if (count == maxDoc) {
      meta.writeByte((byte) -1); // dense marker, each document has a vector value
    } else {
      meta.writeByte((byte) 0); // sparse marker, some documents don't have vector values
      DocIdSetIterator iter = docsWithField.iterator();
      for (int doc = iter.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iter.nextDoc()) {
        meta.writeInt(doc);
      }
    }

    meta.writeInt(maxConn);
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

  private Lucene91OnHeapHnswGraph writeGraph(
      RandomAccessVectorValuesProducer vectorValues, VectorSimilarityFunction similarityFunction)
      throws IOException {

    // build graph
    Lucene91HnswGraphBuilder hnswGraphBuilder =
        new Lucene91HnswGraphBuilder(
            vectorValues,
            similarityFunction,
            maxConn,
            beamWidth,
            Lucene91HnswGraphBuilder.randSeed);
    hnswGraphBuilder.setInfoStream(segmentWriteState.infoStream);
    Lucene91OnHeapHnswGraph graph = hnswGraphBuilder.build(vectorValues.randomAccess());

    // write vectors' neighbours on each level into the vectorIndex file
    int countOnLevel0 = graph.size();
    for (int level = 0; level < graph.numLevels(); level++) {
      NodesIterator nodesOnLevel = graph.getNodesOnLevel(level);
      while (nodesOnLevel.hasNext()) {
        int node = nodesOnLevel.nextInt();
        Lucene91NeighborArray neighbors = graph.getNeighbors(level, node);
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
        for (int i = size; i < maxConn; i++) {
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
