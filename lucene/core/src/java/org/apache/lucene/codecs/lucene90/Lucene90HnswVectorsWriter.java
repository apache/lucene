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

package org.apache.lucene.codecs.lucene90;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.RandomAccessVectorValuesProducer;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.apache.lucene.util.hnsw.HnswGraphBuilder;
import org.apache.lucene.util.hnsw.NeighborArray;

/**
 * Writes vector values and knn graphs to index segments.
 *
 * @lucene.experimental
 */
public final class Lucene90HnswVectorsWriter extends KnnVectorsWriter {

  private final SegmentWriteState segmentWriteState;
  private final IndexOutput meta, vectorData, vectorIndex;

  private final int maxConn;
  private final int beamWidth;
  private boolean finished;

  Lucene90HnswVectorsWriter(SegmentWriteState state, int maxConn, int beamWidth)
      throws IOException {
    this.maxConn = maxConn;
    this.beamWidth = beamWidth;

    assert state.fieldInfos.hasVectorValues();
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
  public void writeField(FieldInfo fieldInfo, KnnVectorsReader knnVectorsReader)
      throws IOException {
    VectorValues vectors = knnVectorsReader.getVectorValues(fieldInfo.name);
    if ((vectors instanceof RandomAccessVectorValuesProducer) == false) {
      throw new IllegalArgumentException(
          "Indexing an HNSW graph requires a random access vector values, got " + vectors);
    }

    long pos = vectorData.getFilePointer();
    // write floats aligned at 4 bytes. This will not survive CFS, but it shows a small benefit when
    // CFS is not used, eg for larger indexes
    long padding = (4 - (pos & 0x3)) & 0x3;
    long vectorDataOffset = pos + padding;
    for (int i = 0; i < padding; i++) {
      vectorData.writeByte((byte) 0);
    }
    // TODO - use a better data structure; a bitset? DocsWithFieldSet is p.p. in o.a.l.index
    int[] docIds = new int[vectors.size()];
    int count = 0;
    for (int docV = vectors.nextDoc(); docV != NO_MORE_DOCS; docV = vectors.nextDoc(), count++) {
      // write vector
      writeVectorValue(vectors);
      docIds[count] = docV;
    }
    long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;

    long vectorIndexOffset = vectorIndex.getFilePointer();
    HnswGraph graph =
        writeGraph(
            (RandomAccessVectorValuesProducer) vectors,
            fieldInfo.getVectorSimilarityFunction(),
            count);
    long vectorIndexLength = vectorIndex.getFilePointer() - vectorIndexOffset;

    writeMeta(
        fieldInfo,
        vectorDataOffset,
        vectorDataLength,
        vectorIndexOffset,
        vectorIndexLength,
        count,
        docIds,
        graph);
  }

  private void writeMeta(
      FieldInfo field,
      long vectorDataOffset,
      long vectorDataLength,
      long vectorIndexOffset,
      long vectorIndexLength,
      int size,
      int[] docIds,
      HnswGraph graph)
      throws IOException {
    meta.writeInt(field.number);
    meta.writeInt(field.getVectorSimilarityFunction().ordinal());
    meta.writeVLong(vectorDataOffset);
    meta.writeVLong(vectorDataLength);
    meta.writeVLong(vectorIndexOffset);
    meta.writeVLong(vectorIndexLength);
    meta.writeInt(field.getVectorDimension());
    meta.writeInt(size);
    for (int i = 0; i < size; i++) {
      // TODO: delta-encode, or write as bitset
      meta.writeVInt(docIds[i]);
    }

    meta.writeInt(maxConn);

    // write graph nodes on each level
    if (graph == null) {
      meta.writeInt(0);
    } else {
      meta.writeInt(graph.numLevels());
      for (int level = 0; level < graph.numLevels(); level++) {
        DocIdSetIterator nodesOnLevel = graph.getAllNodesOnLevel(level);
        int countOnLevel = (int) nodesOnLevel.cost();
        meta.writeInt(countOnLevel); // number of nodes on a level
        if (level > 0) {
          for (int node = nodesOnLevel.nextDoc();
              node != DocIdSetIterator.NO_MORE_DOCS;
              node = nodesOnLevel.nextDoc()) {
            meta.writeVInt(node); // list of nodes on a level
          }
        }
      }
    }
  }

  private void writeVectorValue(VectorValues vectors) throws IOException {
    // write vector value
    BytesRef binaryValue = vectors.binaryValue();
    assert binaryValue.length == vectors.dimension() * Float.BYTES;
    vectorData.writeBytes(binaryValue.bytes, binaryValue.offset, binaryValue.length);
  }

  private HnswGraph writeGraph(
      RandomAccessVectorValuesProducer vectorValues,
      VectorSimilarityFunction similarityFunction,
      int count)
      throws IOException {

    if (count == 0) return null;

    // build graph
    HnswGraphBuilder hnswGraphBuilder =
        new HnswGraphBuilder(
            vectorValues, similarityFunction, maxConn, beamWidth, HnswGraphBuilder.randSeed);
    hnswGraphBuilder.setInfoStream(segmentWriteState.infoStream);
    HnswGraph graph = hnswGraphBuilder.build(vectorValues.randomAccess());

    // write vectors' neighbours on each level into the vectorIndex file
    int countOnLevel0 = graph.size();
    for (int level = 0; level < graph.numLevels(); level++) {
      DocIdSetIterator nodesOnLevel = graph.getAllNodesOnLevel(level);
      for (int node = nodesOnLevel.nextDoc();
          node != DocIdSetIterator.NO_MORE_DOCS;
          node = nodesOnLevel.nextDoc()) {
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
