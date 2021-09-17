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
  private final IndexOutput meta, vectorData, graphIndex, graphData;

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

    String graphIndexFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            Lucene90HnswVectorsFormat.GRAPH_INDEX_EXTENSION);

    String graphDataFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            Lucene90HnswVectorsFormat.GRAPH_DATA_EXTENSION);

    boolean success = false;
    try {
      meta = state.directory.createOutput(metaFileName, state.context);
      vectorData = state.directory.createOutput(vectorDataFileName, state.context);
      graphIndex = state.directory.createOutput(graphIndexFileName, state.context);
      graphData = state.directory.createOutput(graphDataFileName, state.context);

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
          graphIndex,
          Lucene90HnswVectorsFormat.GRAPH_INDEX_CODEC_NAME,
          Lucene90HnswVectorsFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      CodecUtil.writeIndexHeader(
          graphData,
          Lucene90HnswVectorsFormat.GRAPH_DATA_CODEC_NAME,
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
  public void writeField(FieldInfo fieldInfo, VectorValues vectors) throws IOException {
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

    int numOfLevels;
    long graphIndexOffset = graphIndex.getFilePointer();
    long graphDataOffset = graphData.getFilePointer();
    if (vectors instanceof RandomAccessVectorValuesProducer) {
      numOfLevels =
          writeGraph(
              (RandomAccessVectorValuesProducer) vectors, fieldInfo.getVectorSimilarityFunction());
    } else {
      throw new IllegalArgumentException(
          "Indexing an HNSW graph requires a random access vector values, got " + vectors);
    }
    long graphIndexLength = graphIndex.getFilePointer() - graphIndexOffset;
    long graphDataLength = graphData.getFilePointer() - graphDataOffset;

    writeMeta(
        fieldInfo,
        vectorDataOffset,
        vectorDataLength,
        graphIndexOffset,
        graphIndexLength,
        graphDataOffset,
        graphDataLength,
        numOfLevels,
        count,
        docIds);
  }

  private void writeMeta(
      FieldInfo field,
      long vectorDataOffset,
      long vectorDataLength,
      long graphIndexOffset,
      long graphIndexLength,
      long graphDataOffset,
      long graphDataLength,
      int numOfLevels,
      int size,
      int[] docIds)
      throws IOException {
    meta.writeInt(field.number);
    meta.writeInt(field.getVectorSimilarityFunction().ordinal());
    meta.writeVLong(vectorDataOffset);
    meta.writeVLong(vectorDataLength);
    meta.writeVLong(graphIndexOffset);
    meta.writeVLong(graphIndexLength);
    meta.writeVLong(graphDataOffset);
    meta.writeVLong(graphDataLength);
    meta.writeInt(numOfLevels);
    meta.writeInt(field.getVectorDimension());
    meta.writeInt(size);
    for (int i = 0; i < size; i++) {
      // TODO: delta-encode, or write as bitset
      meta.writeVInt(docIds[i]);
    }
  }

  private void writeVectorValue(VectorValues vectors) throws IOException {
    // write vector value
    BytesRef binaryValue = vectors.binaryValue();
    assert binaryValue.length == vectors.dimension() * Float.BYTES;
    vectorData.writeBytes(binaryValue.bytes, binaryValue.offset, binaryValue.length);
  }

  private int writeGraph(
      RandomAccessVectorValuesProducer vectorValues, VectorSimilarityFunction similarityFunction)
      throws IOException {

    // build graph
    HnswGraphBuilder hnswGraphBuilder =
        new HnswGraphBuilder(
            vectorValues, similarityFunction, maxConn, beamWidth, HnswGraphBuilder.randSeed);
    hnswGraphBuilder.setInfoStream(segmentWriteState.infoStream);
    HnswGraph graph = hnswGraphBuilder.build(vectorValues.randomAccess());

    graphIndex.writeInt(graph.numOfLevels()); // number of levels
    for (int level = 0; level < graph.numOfLevels(); level++) {
      DocIdSetIterator nodesOnLevel = graph.getAllNodesOnLevel(level);
      int countOnLevel = (int) nodesOnLevel.cost();
      // write graph nodes on the level into the graphIndex file
      graphIndex.writeInt(countOnLevel); // number of nodes on a level
      if (level > 0) {
        for (int node = nodesOnLevel.nextDoc();
            node != DocIdSetIterator.NO_MORE_DOCS;
            node = nodesOnLevel.nextDoc()) {
          graphIndex.writeVInt(node); // list of nodes on a level
        }
      }
    }

    long lastOffset = 0;
    int countOnLevel0 = graph.size();
    long graphDataOffset = graphData.getFilePointer();
    for (int level = 0; level < graph.numOfLevels(); level++) {
      DocIdSetIterator nodesOnLevel = graph.getAllNodesOnLevel(level);
      for (int node = nodesOnLevel.nextDoc();
          node != DocIdSetIterator.NO_MORE_DOCS;
          node = nodesOnLevel.nextDoc()) {
        // write graph offsets on the level into the graphIndex file
        long offset = graphData.getFilePointer() - graphDataOffset;
        graphIndex.writeVLong(offset - lastOffset);
        lastOffset = offset;

        // write neighbours on the level into the graphData file
        NeighborArray neighbors = graph.getNeighbors(level, node);
        int size = neighbors.size();
        graphData.writeInt(size);
        // Destructively modify; it's ok we are discarding it after this
        int[] nnodes = neighbors.node();
        Arrays.sort(nnodes, 0, size);
        int lastNode = -1; // to make the assertion work?
        for (int i = 0; i < size; i++) {
          int nnode = nnodes[i];
          assert nnode > lastNode : "nodes out of order: " + lastNode + "," + nnode;
          assert nnode < countOnLevel0 : "node too large: " + nnode + ">=" + countOnLevel0;
          graphData.writeVInt(nnode - lastNode);
          lastNode = nnode;
        }
      }
    }

    return graph.numOfLevels();
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
      CodecUtil.writeFooter(graphIndex);
      CodecUtil.writeFooter(graphData);
    }
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(meta, vectorData, graphIndex, graphData);
  }
}
