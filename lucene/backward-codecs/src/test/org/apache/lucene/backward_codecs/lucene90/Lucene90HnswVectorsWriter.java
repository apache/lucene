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

package org.apache.lucene.backward_codecs.lucene90;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import org.apache.lucene.codecs.BufferingKnnVectorsWriter;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;

/**
 * Writes vector values and knn graphs to index segments.
 *
 * @lucene.experimental
 */
public final class Lucene90HnswVectorsWriter extends BufferingKnnVectorsWriter {

  private final SegmentWriteState segmentWriteState;
  private final IndexOutput meta, vectorData, vectorIndex;

  private final int maxConn;
  private final int beamWidth;
  private boolean finished;

  Lucene90HnswVectorsWriter(SegmentWriteState state, int maxConn, int beamWidth)
      throws IOException {
    this.maxConn = maxConn;
    this.beamWidth = beamWidth;

    segmentWriteState = state;

    String metaFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, Lucene90HnswVectorsFormat.META_EXTENSION);

    String vectorDataFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            Lucene90HnswVectorsFormat.VECTOR_DATA_EXTENSION);

    String indexDataFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            Lucene90HnswVectorsFormat.VECTOR_INDEX_EXTENSION);

    boolean success = false;
    try {
      meta = state.directory.createOutput(metaFileName, state.context);
      vectorData = state.directory.createOutput(vectorDataFileName, state.context);
      vectorIndex = state.directory.createOutput(indexDataFileName, state.context);

      CodecUtil.writeIndexHeader(
          meta,
          Lucene90HnswVectorsFormat.META_CODEC_NAME,
          Lucene90HnswVectorsFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      CodecUtil.writeIndexHeader(
          vectorData,
          Lucene90HnswVectorsFormat.VECTOR_DATA_CODEC_NAME,
          Lucene90HnswVectorsFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      CodecUtil.writeIndexHeader(
          vectorIndex,
          Lucene90HnswVectorsFormat.VECTOR_INDEX_CODEC_NAME,
          Lucene90HnswVectorsFormat.VERSION_CURRENT,
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
  public void writeField(FieldInfo fieldInfo, FloatVectorValues floatVectorValues, int maxDoc)
      throws IOException {
    long vectorDataOffset = vectorData.alignFilePointer(Float.BYTES);

    IndexOutput tempVectorData =
        segmentWriteState.directory.createTempOutput(
            vectorData.getName(), "temp", segmentWriteState.context);
    IndexInput vectorDataInput = null;
    boolean success = false;
    try {
      // write the vector data to a temporary file
      // TODO - use a better data structure; a bitset? DocsWithFieldSet is p.p. in o.a.l.index
      int[] docIds = writeVectorData(tempVectorData, floatVectorValues);
      CodecUtil.writeFooter(tempVectorData);
      IOUtils.close(tempVectorData);

      // copy the temporary file vectors to the actual data file
      vectorDataInput =
          segmentWriteState.directory.openInput(
              tempVectorData.getName(), segmentWriteState.context);
      vectorData.copyBytes(vectorDataInput, vectorDataInput.length() - CodecUtil.footerLength());
      CodecUtil.retrieveChecksum(vectorDataInput);

      // build the graph using the temporary vector data
      Lucene90HnswVectorsReader.OffHeapFloatVectorValues offHeapVectors =
          new Lucene90HnswVectorsReader.OffHeapFloatVectorValues(
              floatVectorValues.dimension(),
              docIds,
              fieldInfo.getVectorSimilarityFunction(),
              vectorDataInput);

      long[] offsets = new long[docIds.length];
      long vectorIndexOffset = vectorIndex.getFilePointer();
      writeGraph(
          vectorIndex,
          offHeapVectors,
          fieldInfo.getVectorSimilarityFunction(),
          vectorIndexOffset,
          offsets,
          maxConn,
          beamWidth);

      long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;
      long vectorIndexLength = vectorIndex.getFilePointer() - vectorIndexOffset;
      writeMeta(
          fieldInfo,
          vectorDataOffset,
          vectorDataLength,
          vectorIndexOffset,
          vectorIndexLength,
          docIds);
      writeGraphOffsets(meta, offsets);
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

  @Override
  protected void writeField(FieldInfo fieldInfo, ByteVectorValues byteVectorValues, int maxDoc) {
    throw new UnsupportedOperationException("byte vectors not supported in this version");
  }

  /**
   * Writes the vector values to the output and returns a mapping from dense ordinals to document
   * IDs. The length of the returned array matches the total number of documents with a vector
   * (which excludes deleted documents), so it may be less than {@link FloatVectorValues#size()}.
   */
  private static int[] writeVectorData(IndexOutput output, FloatVectorValues vectors)
      throws IOException {
    int[] docIds = new int[vectors.size()];
    int count = 0;
    ByteBuffer binaryVector =
        ByteBuffer.allocate(vectors.dimension() * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    for (int docV = vectors.nextDoc(); docV != NO_MORE_DOCS; docV = vectors.nextDoc(), count++) {
      // write vector
      float[] vectorValue = vectors.vectorValue();
      binaryVector.asFloatBuffer().put(vectorValue);
      output.writeBytes(binaryVector.array(), binaryVector.limit());
      docIds[count] = docV;
    }

    if (docIds.length > count) {
      return ArrayUtil.copyOfSubArray(docIds, 0, count);
    }
    return docIds;
  }

  private void writeMeta(
      FieldInfo field,
      long vectorDataOffset,
      long vectorDataLength,
      long indexDataOffset,
      long indexDataLength,
      int[] docIds)
      throws IOException {
    meta.writeInt(field.number);
    meta.writeInt(field.getVectorSimilarityFunction().ordinal());
    meta.writeVLong(vectorDataOffset);
    meta.writeVLong(vectorDataLength);
    meta.writeVLong(indexDataOffset);
    meta.writeVLong(indexDataLength);
    meta.writeInt(field.getVectorDimension());
    meta.writeInt(docIds.length);
    for (int docId : docIds) {
      // TODO: delta-encode, or write as bitset
      meta.writeVInt(docId);
    }
  }

  private void writeGraphOffsets(IndexOutput out, long[] offsets) throws IOException {
    long last = 0;
    for (long offset : offsets) {
      out.writeVLong(offset - last);
      last = offset;
    }
  }

  private void writeGraph(
      IndexOutput graphData,
      RandomAccessVectorValues.Floats vectorValues,
      VectorSimilarityFunction similarityFunction,
      long graphDataOffset,
      long[] offsets,
      int maxConn,
      int beamWidth)
      throws IOException {
    Lucene90HnswGraphBuilder hnswGraphBuilder =
        new Lucene90HnswGraphBuilder(
            vectorValues,
            similarityFunction,
            maxConn,
            beamWidth,
            Lucene90HnswGraphBuilder.randSeed);
    hnswGraphBuilder.setInfoStream(segmentWriteState.infoStream);
    Lucene90OnHeapHnswGraph graph = hnswGraphBuilder.build(vectorValues.copy());

    for (int ord = 0; ord < offsets.length; ord++) {
      // write graph
      offsets[ord] = graphData.getFilePointer() - graphDataOffset;

      Lucene90NeighborArray neighbors = graph.getNeighbors(ord);
      int size = neighbors.size();

      // Destructively modify; it's ok we are discarding it after this
      int[] nodes = neighbors.node();
      Arrays.sort(nodes, 0, size);
      graphData.writeInt(size);

      int lastNode = -1; // to make the assertion work?
      for (int i = 0; i < size; i++) {
        int node = nodes[i];
        assert node > lastNode : "nodes out of order: " + lastNode + "," + node;
        assert node < offsets.length : "node too large: " + node + ">=" + offsets.length;
        graphData.writeVInt(node - lastNode);
        lastNode = node;
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
  public void close() throws IOException {
    IOUtils.close(meta, vectorData, vectorIndex);
  }
}
