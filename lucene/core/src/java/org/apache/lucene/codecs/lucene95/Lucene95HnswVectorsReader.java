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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.apache.lucene.util.hnsw.HnswGraphSearcher;
import org.apache.lucene.util.hnsw.NeighborQueue;
import org.apache.lucene.util.packed.DirectMonotonicReader;

/**
 * Reads vectors from the index segments along with index data structures supporting KNN search.
 *
 * @lucene.experimental
 */
public final class Lucene95HnswVectorsReader extends KnnVectorsReader {

  private static final long SHALLOW_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(Lucene95HnswVectorsFormat.class);

  private final FieldInfos fieldInfos;
  private final Map<String, FieldEntry> fields = new HashMap<>();
  private final IndexInput vectorData;
  private final IndexInput vectorIndex;

  Lucene95HnswVectorsReader(SegmentReadState state) throws IOException {
    this.fieldInfos = state.fieldInfos;
    int versionMeta = readMetadata(state);
    boolean success = false;
    try {
      vectorData =
          openDataInput(
              state,
              versionMeta,
              Lucene95HnswVectorsFormat.VECTOR_DATA_EXTENSION,
              Lucene95HnswVectorsFormat.VECTOR_DATA_CODEC_NAME);
      vectorIndex =
          openDataInput(
              state,
              versionMeta,
              Lucene95HnswVectorsFormat.VECTOR_INDEX_EXTENSION,
              Lucene95HnswVectorsFormat.VECTOR_INDEX_CODEC_NAME);
      success = true;
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }

  private int readMetadata(SegmentReadState state) throws IOException {
    String metaFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, Lucene95HnswVectorsFormat.META_EXTENSION);
    int versionMeta = -1;
    try (ChecksumIndexInput meta =
        state.directory.openChecksumInput(metaFileName, IOContext.READONCE)) {
      Throwable priorE = null;
      try {
        versionMeta =
            CodecUtil.checkIndexHeader(
                meta,
                Lucene95HnswVectorsFormat.META_CODEC_NAME,
                Lucene95HnswVectorsFormat.VERSION_START,
                Lucene95HnswVectorsFormat.VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix);
        readFields(meta, state.fieldInfos);
      } catch (Throwable exception) {
        priorE = exception;
      } finally {
        CodecUtil.checkFooter(meta, priorE);
      }
    }
    return versionMeta;
  }

  private static IndexInput openDataInput(
      SegmentReadState state, int versionMeta, String fileExtension, String codecName)
      throws IOException {
    String fileName =
        IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, fileExtension);
    IndexInput in = state.directory.openInput(fileName, state.context);
    boolean success = false;
    try {
      int versionVectorData =
          CodecUtil.checkIndexHeader(
              in,
              codecName,
              Lucene95HnswVectorsFormat.VERSION_START,
              Lucene95HnswVectorsFormat.VERSION_CURRENT,
              state.segmentInfo.getId(),
              state.segmentSuffix);
      if (versionMeta != versionVectorData) {
        throw new CorruptIndexException(
            "Format versions mismatch: meta="
                + versionMeta
                + ", "
                + codecName
                + "="
                + versionVectorData,
            in);
      }
      CodecUtil.retrieveChecksum(in);
      success = true;
      return in;
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(in);
      }
    }
  }

  private void readFields(ChecksumIndexInput meta, FieldInfos infos) throws IOException {
    for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) {
      FieldInfo info = infos.fieldInfo(fieldNumber);
      if (info == null) {
        throw new CorruptIndexException("Invalid field number: " + fieldNumber, meta);
      }
      FieldEntry fieldEntry = readField(meta);
      validateFieldEntry(info, fieldEntry);
      fields.put(info.name, fieldEntry);
    }
  }

  private void validateFieldEntry(FieldInfo info, FieldEntry fieldEntry) {
    int dimension = info.getVectorDimension();
    if (dimension != fieldEntry.dimension) {
      throw new IllegalStateException(
          "Inconsistent vector dimension for field=\""
              + info.name
              + "\"; "
              + dimension
              + " != "
              + fieldEntry.dimension);
    }

    int byteSize;
    switch (info.getVectorEncoding()) {
      case BYTE:
        byteSize = Byte.BYTES;
        break;
      case FLOAT32:
        byteSize = Float.BYTES;
        break;
      default:
        throw new AssertionError();
    }
    long vectorBytes = Math.multiplyExact((long) dimension, byteSize);
    long numBytes = Math.multiplyExact(vectorBytes, fieldEntry.size);
    if (numBytes != fieldEntry.vectorDataLength) {
      throw new IllegalStateException(
          "Vector data length "
              + fieldEntry.vectorDataLength
              + " not matching size="
              + fieldEntry.size
              + " * dim="
              + dimension
              + " * byteSize="
              + byteSize
              + " = "
              + numBytes);
    }
  }

  private VectorSimilarityFunction readSimilarityFunction(DataInput input) throws IOException {
    int similarityFunctionId = input.readInt();
    if (similarityFunctionId < 0
        || similarityFunctionId >= VectorSimilarityFunction.values().length) {
      throw new CorruptIndexException(
          "Invalid similarity function id: " + similarityFunctionId, input);
    }
    return VectorSimilarityFunction.values()[similarityFunctionId];
  }

  private VectorEncoding readVectorEncoding(DataInput input) throws IOException {
    int encodingId = input.readInt();
    if (encodingId < 0 || encodingId >= VectorEncoding.values().length) {
      throw new CorruptIndexException("Invalid vector encoding id: " + encodingId, input);
    }
    return VectorEncoding.values()[encodingId];
  }

  private FieldEntry readField(IndexInput input) throws IOException {
    VectorEncoding vectorEncoding = readVectorEncoding(input);
    VectorSimilarityFunction similarityFunction = readSimilarityFunction(input);
    return new FieldEntry(input, vectorEncoding, similarityFunction);
  }

  @Override
  public long ramBytesUsed() {
    return Lucene95HnswVectorsReader.SHALLOW_SIZE
        + RamUsageEstimator.sizeOfMap(
            fields, RamUsageEstimator.shallowSizeOfInstance(FieldEntry.class));
  }

  @Override
  public void checkIntegrity() throws IOException {
    CodecUtil.checksumEntireFile(vectorData);
    CodecUtil.checksumEntireFile(vectorIndex);
  }

  @Override
  public FloatVectorValues getFloatVectorValues(String field) throws IOException {
    FieldEntry fieldEntry = fields.get(field);
    if (fieldEntry.vectorEncoding != VectorEncoding.FLOAT32) {
      throw new IllegalArgumentException(
          "field=\""
              + field
              + "\" is encoded as: "
              + fieldEntry.vectorEncoding
              + " expected: "
              + VectorEncoding.FLOAT32);
    }
    return OffHeapFloatVectorValues.load(fieldEntry, vectorData);
  }

  @Override
  public ByteVectorValues getByteVectorValues(String field) throws IOException {
    FieldEntry fieldEntry = fields.get(field);
    if (fieldEntry.vectorEncoding != VectorEncoding.BYTE) {
      throw new IllegalArgumentException(
          "field=\""
              + field
              + "\" is encoded as: "
              + fieldEntry.vectorEncoding
              + " expected: "
              + VectorEncoding.FLOAT32);
    }
    return OffHeapByteVectorValues.load(fieldEntry, vectorData);
  }

  @Override
  public TopDocs search(String field, float[] target, int k, Bits acceptDocs, int visitedLimit)
      throws IOException {
    FieldEntry fieldEntry = fields.get(field);

    if (fieldEntry.size() == 0) {
      return new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]);
    }
    if (fieldEntry.vectorEncoding != VectorEncoding.FLOAT32) {
      return new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]);
    }

    // bound k by total number of vectors to prevent oversizing data structures
    k = Math.min(k, fieldEntry.size());
    OffHeapFloatVectorValues vectorValues = OffHeapFloatVectorValues.load(fieldEntry, vectorData);

    NeighborQueue results =
        HnswGraphSearcher.search(
            target,
            k,
            vectorValues,
            fieldEntry.vectorEncoding,
            fieldEntry.similarityFunction,
            getGraph(fieldEntry),
            vectorValues.getAcceptOrds(acceptDocs),
            visitedLimit);

    int i = 0;
    ScoreDoc[] scoreDocs = new ScoreDoc[Math.min(results.size(), k)];
    while (results.size() > 0) {
      int node = results.topNode();
      float score = results.topScore();
      results.pop();
      scoreDocs[scoreDocs.length - ++i] = new ScoreDoc(vectorValues.ordToDoc(node), score);
    }

    TotalHits.Relation relation =
        results.incomplete()
            ? TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO
            : TotalHits.Relation.EQUAL_TO;
    return new TopDocs(new TotalHits(results.visitedCount(), relation), scoreDocs);
  }

  @Override
  public TopDocs search(String field, byte[] target, int k, Bits acceptDocs, int visitedLimit)
      throws IOException {
    FieldEntry fieldEntry = fields.get(field);

    if (fieldEntry.size() == 0) {
      return new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]);
    }
    if (fieldEntry.vectorEncoding != VectorEncoding.BYTE) {
      return new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]);
    }

    // bound k by total number of vectors to prevent oversizing data structures
    k = Math.min(k, fieldEntry.size());
    OffHeapByteVectorValues vectorValues = OffHeapByteVectorValues.load(fieldEntry, vectorData);

    NeighborQueue results =
        HnswGraphSearcher.search(
            target,
            k,
            vectorValues,
            fieldEntry.vectorEncoding,
            fieldEntry.similarityFunction,
            getGraph(fieldEntry),
            vectorValues.getAcceptOrds(acceptDocs),
            visitedLimit);

    int i = 0;
    ScoreDoc[] scoreDocs = new ScoreDoc[Math.min(results.size(), k)];
    while (results.size() > 0) {
      int node = results.topNode();
      float score = results.topScore();
      results.pop();
      scoreDocs[scoreDocs.length - ++i] = new ScoreDoc(vectorValues.ordToDoc(node), score);
    }

    TotalHits.Relation relation =
        results.incomplete()
            ? TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO
            : TotalHits.Relation.EQUAL_TO;
    return new TopDocs(new TotalHits(results.visitedCount(), relation), scoreDocs);
  }

  /** Get knn graph values; used for testing */
  public HnswGraph getGraph(String field) throws IOException {
    FieldInfo info = fieldInfos.fieldInfo(field);
    if (info == null) {
      throw new IllegalArgumentException("No such field '" + field + "'");
    }
    FieldEntry entry = fields.get(field);
    if (entry != null && entry.vectorIndexLength > 0) {
      return getGraph(entry);
    } else {
      return HnswGraph.EMPTY;
    }
  }

  private HnswGraph getGraph(FieldEntry entry) throws IOException {
    return new OffHeapHnswGraph(entry, vectorIndex);
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(vectorData, vectorIndex);
  }

  static class FieldEntry implements Accountable {
    private static final long SHALLOW_SIZE =
        RamUsageEstimator.shallowSizeOfInstance(FieldEntry.class);
    final VectorSimilarityFunction similarityFunction;
    final VectorEncoding vectorEncoding;
    final long vectorDataOffset;
    final long vectorDataLength;
    final long vectorIndexOffset;
    final long vectorIndexLength;
    final int M;
    final int numLevels;
    final int dimension;
    final int size;
    final int[][] nodesByLevel;
    // for each level the start offsets in vectorIndex file from where to read neighbours
    final DirectMonotonicReader.Meta offsetsMeta;
    final long offsetsOffset;
    final int offsetsBlockShift;
    final long offsetsLength;

    // the following four variables used to read docIds encoded by IndexDISI
    // special values of docsWithFieldOffset are -1 and -2
    // -1 : dense
    // -2 : empty
    // other: sparse
    final long docsWithFieldOffset;
    final long docsWithFieldLength;
    final short jumpTableEntryCount;
    final byte denseRankPower;

    // the following four variables used to read ordToDoc encoded by DirectMonotonicWriter
    // note that only spare case needs to store ordToDoc
    final long addressesOffset;
    final int blockShift;
    final DirectMonotonicReader.Meta meta;
    final long addressesLength;

    FieldEntry(
        IndexInput input,
        VectorEncoding vectorEncoding,
        VectorSimilarityFunction similarityFunction)
        throws IOException {
      this.similarityFunction = similarityFunction;
      this.vectorEncoding = vectorEncoding;
      vectorDataOffset = input.readVLong();
      vectorDataLength = input.readVLong();
      vectorIndexOffset = input.readVLong();
      vectorIndexLength = input.readVLong();
      dimension = input.readVInt();
      size = input.readInt();

      docsWithFieldOffset = input.readLong();
      docsWithFieldLength = input.readLong();
      jumpTableEntryCount = input.readShort();
      denseRankPower = input.readByte();

      // dense or empty
      if (docsWithFieldOffset == -1 || docsWithFieldOffset == -2) {
        addressesOffset = 0;
        blockShift = 0;
        meta = null;
        addressesLength = 0;
      } else {
        // sparse
        addressesOffset = input.readLong();
        blockShift = input.readVInt();
        meta = DirectMonotonicReader.loadMeta(input, size, blockShift);
        addressesLength = input.readLong();
      }

      // read nodes by level
      M = input.readVInt();
      numLevels = input.readVInt();
      nodesByLevel = new int[numLevels][];
      long numberOfOffsets = 0;
      for (int level = 0; level < numLevels; level++) {
        if (level > 0) {
          int numNodesOnLevel = input.readVInt();
          numberOfOffsets += numNodesOnLevel;
          nodesByLevel[level] = new int[numNodesOnLevel];
          nodesByLevel[level][0] = input.readVInt();
          for (int i = 1; i < numNodesOnLevel; i++) {
            nodesByLevel[level][i] = nodesByLevel[level][i - 1] + input.readVInt();
          }
        } else {
          numberOfOffsets += size;
        }
      }
      if (numberOfOffsets > 0) {
        offsetsOffset = input.readLong();
        offsetsBlockShift = input.readVInt();
        offsetsMeta = DirectMonotonicReader.loadMeta(input, numberOfOffsets, offsetsBlockShift);
        offsetsLength = input.readLong();
      } else {
        offsetsOffset = 0;
        offsetsBlockShift = 0;
        offsetsMeta = null;
        offsetsLength = 0;
      }
    }

    int size() {
      return size;
    }

    @Override
    public long ramBytesUsed() {
      return SHALLOW_SIZE
          + Arrays.stream(nodesByLevel).mapToLong(nodes -> RamUsageEstimator.sizeOf(nodes)).sum()
          + RamUsageEstimator.sizeOf(meta)
          + RamUsageEstimator.sizeOf(offsetsMeta);
    }
  }

  /** Read the nearest-neighbors graph from the index input */
  private static final class OffHeapHnswGraph extends HnswGraph {

    final IndexInput dataIn;
    final int[][] nodesByLevel;
    final int numLevels;
    final int entryNode;
    final int size;
    int arcCount;
    int arcUpTo;
    int arc;
    private final DirectMonotonicReader graphLevelNodeOffsets;
    private final long[] graphLevelNodeIndexOffsets;
    // Allocated to be M*2 to track the current neighbors being explored
    private final int[] currentNeighborsBuffer;

    OffHeapHnswGraph(FieldEntry entry, IndexInput vectorIndex) throws IOException {
      this.dataIn =
          vectorIndex.slice("graph-data", entry.vectorIndexOffset, entry.vectorIndexLength);
      this.nodesByLevel = entry.nodesByLevel;
      this.numLevels = entry.numLevels;
      this.entryNode = numLevels > 1 ? nodesByLevel[numLevels - 1][0] : 0;
      this.size = entry.size();
      final RandomAccessInput addressesData =
          vectorIndex.randomAccessSlice(entry.offsetsOffset, entry.offsetsLength);
      this.graphLevelNodeOffsets =
          DirectMonotonicReader.getInstance(entry.offsetsMeta, addressesData);
      this.currentNeighborsBuffer = new int[entry.M * 2];
      graphLevelNodeIndexOffsets = new long[numLevels];
      graphLevelNodeIndexOffsets[0] = 0;
      for (int i = 1; i < numLevels; i++) {
        // nodesByLevel is `null` for the zeroth level as we know its size
        int nodeCount = nodesByLevel[i - 1] == null ? size : nodesByLevel[i - 1].length;
        graphLevelNodeIndexOffsets[i] = graphLevelNodeIndexOffsets[i - 1] + nodeCount;
      }
    }

    @Override
    public void seek(int level, int targetOrd) throws IOException {
      int targetIndex =
          level == 0
              ? targetOrd
              : Arrays.binarySearch(nodesByLevel[level], 0, nodesByLevel[level].length, targetOrd);
      assert targetIndex >= 0;
      // unsafe; no bounds checking
      dataIn.seek(graphLevelNodeOffsets.get(targetIndex + graphLevelNodeIndexOffsets[level]));
      arcCount = dataIn.readVInt();
      if (arcCount > 0) {
        currentNeighborsBuffer[0] = dataIn.readVInt();
        for (int i = 1; i < arcCount; i++) {
          currentNeighborsBuffer[i] = currentNeighborsBuffer[i - 1] + dataIn.readVInt();
        }
      }
      arc = -1;
      arcUpTo = 0;
    }

    @Override
    public int size() {
      return size;
    }

    @Override
    public int nextNeighbor() throws IOException {
      if (arcUpTo >= arcCount) {
        return NO_MORE_DOCS;
      }
      arc = currentNeighborsBuffer[arcUpTo];
      ++arcUpTo;
      return arc;
    }

    @Override
    public int numLevels() throws IOException {
      return numLevels;
    }

    @Override
    public int entryNode() throws IOException {
      return entryNode;
    }

    @Override
    public NodesIterator getNodesOnLevel(int level) {
      if (level == 0) {
        return new ArrayNodesIterator(size());
      } else {
        return new ArrayNodesIterator(nodesByLevel[level], nodesByLevel[level].length);
      }
    }
  }
}
