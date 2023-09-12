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
import java.util.HashMap;
import java.util.Map;
import java.util.function.IntUnaryOperator;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.apache.lucene.util.hnsw.HnswGraphSearcher;
import org.apache.lucene.util.hnsw.OrdinalTranslatedKnnCollector;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;
import org.apache.lucene.util.hnsw.RandomVectorScorer;

/**
 * Reads vectors from the index segments along with index data structures supporting KNN search.
 *
 * @lucene.experimental
 */
public final class Lucene91HnswVectorsReader extends KnnVectorsReader {

  private final Map<String, FieldEntry> fields = new HashMap<>();
  private final IndexInput vectorData;
  private final IndexInput vectorIndex;

  Lucene91HnswVectorsReader(SegmentReadState state) throws IOException {
    int versionMeta = readMetadata(state);
    boolean success = false;
    try {
      vectorData =
          openDataInput(
              state,
              versionMeta,
              Lucene91HnswVectorsFormat.VECTOR_DATA_EXTENSION,
              Lucene91HnswVectorsFormat.VECTOR_DATA_CODEC_NAME);
      vectorIndex =
          openDataInput(
              state,
              versionMeta,
              Lucene91HnswVectorsFormat.VECTOR_INDEX_EXTENSION,
              Lucene91HnswVectorsFormat.VECTOR_INDEX_CODEC_NAME);
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
            state.segmentInfo.name, state.segmentSuffix, Lucene91HnswVectorsFormat.META_EXTENSION);
    int versionMeta = -1;
    try (ChecksumIndexInput meta = state.directory.openChecksumInput(metaFileName, state.context)) {
      Throwable priorE = null;
      try {
        versionMeta =
            CodecUtil.checkIndexHeader(
                meta,
                Lucene91HnswVectorsFormat.META_CODEC_NAME,
                Lucene91HnswVectorsFormat.VERSION_START,
                Lucene91HnswVectorsFormat.VERSION_CURRENT,
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
              Lucene91HnswVectorsFormat.VERSION_START,
              Lucene91HnswVectorsFormat.VERSION_CURRENT,
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

    long numBytes = (long) fieldEntry.size() * dimension * Float.BYTES;
    if (numBytes != fieldEntry.vectorDataLength) {
      throw new IllegalStateException(
          "Vector data length "
              + fieldEntry.vectorDataLength
              + " not matching size="
              + fieldEntry.size()
              + " * dim="
              + dimension
              + " * 4 = "
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

  private FieldEntry readField(DataInput input) throws IOException {
    VectorSimilarityFunction similarityFunction = readSimilarityFunction(input);
    return new FieldEntry(input, similarityFunction);
  }

  @Override
  public long ramBytesUsed() {
    long totalBytes = RamUsageEstimator.shallowSizeOfInstance(Lucene91HnswVectorsFormat.class);
    totalBytes +=
        RamUsageEstimator.sizeOfMap(
            fields, RamUsageEstimator.shallowSizeOfInstance(FieldEntry.class));
    for (FieldEntry entry : fields.values()) {
      totalBytes += RamUsageEstimator.sizeOf(entry.ordToDoc);
    }
    return totalBytes;
  }

  @Override
  public void checkIntegrity() throws IOException {
    CodecUtil.checksumEntireFile(vectorData);
    CodecUtil.checksumEntireFile(vectorIndex);
  }

  @Override
  public FloatVectorValues getFloatVectorValues(String field) throws IOException {
    FieldEntry fieldEntry = fields.get(field);
    return getOffHeapVectorValues(fieldEntry);
  }

  @Override
  public ByteVectorValues getByteVectorValues(String field) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void search(String field, float[] target, KnnCollector knnCollector, Bits acceptDocs)
      throws IOException {
    FieldEntry fieldEntry = fields.get(field);

    if (fieldEntry.size() == 0) {
      return;
    }

    OffHeapFloatVectorValues vectorValues = getOffHeapVectorValues(fieldEntry);
    RandomVectorScorer scorer =
        RandomVectorScorer.createFloats(vectorValues, fieldEntry.similarityFunction, target);
    HnswGraphSearcher.search(
        scorer,
        new OrdinalTranslatedKnnCollector(knnCollector, vectorValues::ordToDoc),
        getGraph(fieldEntry),
        getAcceptOrds(acceptDocs, fieldEntry));
  }

  @Override
  public void search(String field, byte[] target, KnnCollector knnCollector, Bits acceptDocs)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  private OffHeapFloatVectorValues getOffHeapVectorValues(FieldEntry fieldEntry)
      throws IOException {
    IndexInput bytesSlice =
        vectorData.slice("vector-data", fieldEntry.vectorDataOffset, fieldEntry.vectorDataLength);
    return new OffHeapFloatVectorValues(
        fieldEntry.dimension, fieldEntry.size(), fieldEntry.ordToDoc, bytesSlice);
  }

  private Bits getAcceptOrds(Bits acceptDocs, FieldEntry fieldEntry) {
    if (fieldEntry.ordToDoc == null) {
      return acceptDocs;
    }
    if (acceptDocs == null) {
      return null;
    }
    return new Bits() {
      @Override
      public boolean get(int index) {
        return acceptDocs.get(fieldEntry.ordToDoc(index));
      }

      @Override
      public int length() {
        return fieldEntry.size;
      }
    };
  }

  private HnswGraph getGraph(FieldEntry entry) throws IOException {
    IndexInput bytesSlice =
        vectorIndex.slice("graph-data", entry.vectorIndexOffset, entry.vectorIndexLength);
    return new OffHeapHnswGraph(entry, bytesSlice);
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(vectorData, vectorIndex);
  }

  private static class FieldEntry {

    final VectorSimilarityFunction similarityFunction;
    final long vectorDataOffset;
    final long vectorDataLength;
    final long vectorIndexOffset;
    final long vectorIndexLength;
    final int maxConn;
    final int numLevels;
    final int dimension;
    private final int size;
    final int[] ordToDoc;
    private final IntUnaryOperator ordToDocOperator;
    final int[][] nodesByLevel;
    // for each level the start offsets in vectorIndex file from where to read neighbours
    final long[] graphOffsetsByLevel;

    FieldEntry(DataInput input, VectorSimilarityFunction similarityFunction) throws IOException {
      this.similarityFunction = similarityFunction;
      vectorDataOffset = input.readVLong();
      vectorDataLength = input.readVLong();
      vectorIndexOffset = input.readVLong();
      vectorIndexLength = input.readVLong();
      dimension = input.readInt();
      size = input.readInt();

      int denseSparseMarker = input.readByte();
      if (denseSparseMarker == -1) {
        ordToDoc = null; // each document has a vector value
      } else {
        assert denseSparseMarker == 0;
        // TODO: Can we read docIDs from disk directly instead of loading giant arrays in memory?
        //  Or possibly switch to something like DirectMonotonicReader if it doesn't slow down
        // searches.

        // as not all docs have vector values, fill a mapping from dense vector ordinals to docIds
        ordToDoc = new int[size];
        for (int i = 0; i < size; i++) {
          int doc = input.readInt();
          ordToDoc[i] = doc;
        }
      }
      ordToDocOperator = ordToDoc == null ? IntUnaryOperator.identity() : (ord) -> ordToDoc[ord];

      // read nodes by level
      maxConn = input.readInt();
      numLevels = input.readInt();
      nodesByLevel = new int[numLevels][];
      for (int level = 0; level < numLevels; level++) {
        int numNodesOnLevel = input.readInt();
        if (level == 0) {
          // we don't store nodes for level 0th, as this level contains all nodes
          assert numNodesOnLevel == size;
          nodesByLevel[0] = null;
        } else {
          nodesByLevel[level] = new int[numNodesOnLevel];
          for (int i = 0; i < numNodesOnLevel; i++) {
            nodesByLevel[level][i] = input.readInt();
          }
        }
      }

      // calculate for each level the start offsets in vectorIndex file from where to read
      // neighbours
      graphOffsetsByLevel = new long[numLevels];
      final long connectionsAndSizeBytes =
          Math.multiplyExact(Math.addExact(1L, maxConn), Integer.BYTES);
      for (int level = 0; level < numLevels; level++) {
        if (level == 0) {
          graphOffsetsByLevel[level] = 0;
        } else {
          int numNodesOnPrevLevel = level == 1 ? size : nodesByLevel[level - 1].length;
          graphOffsetsByLevel[level] =
              Math.addExact(
                  graphOffsetsByLevel[level - 1],
                  Math.multiplyExact(connectionsAndSizeBytes, numNodesOnPrevLevel));
        }
      }
    }

    int size() {
      return size;
    }

    int ordToDoc(int ord) {
      return ordToDocOperator.applyAsInt(ord);
    }
  }

  /** Read the vector values from the index input. This supports both iterated and random access. */
  static class OffHeapFloatVectorValues extends FloatVectorValues
      implements RandomAccessVectorValues<float[]> {

    private final int dimension;
    private final int size;
    private final int[] ordToDoc;
    private final IntUnaryOperator ordToDocOperator;
    private final IndexInput dataIn;
    private final int byteSize;
    private final float[] value;

    private int ord = -1;
    private int doc = -1;

    OffHeapFloatVectorValues(int dimension, int size, int[] ordToDoc, IndexInput dataIn) {
      this.dimension = dimension;
      this.size = size;
      this.ordToDoc = ordToDoc;
      ordToDocOperator = ordToDoc == null ? IntUnaryOperator.identity() : (ord) -> ordToDoc[ord];
      this.dataIn = dataIn;
      byteSize = Float.BYTES * dimension;
      value = new float[dimension];
    }

    @Override
    public int dimension() {
      return dimension;
    }

    @Override
    public int size() {
      return size;
    }

    @Override
    public float[] vectorValue() throws IOException {
      dataIn.seek((long) ord * byteSize);
      dataIn.readFloats(value, 0, value.length);
      return value;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() {
      if (++ord >= size) {
        doc = NO_MORE_DOCS;
      } else {
        doc = ordToDocOperator.applyAsInt(ord);
      }
      return doc;
    }

    @Override
    public int advance(int target) {
      assert docID() < target;

      if (ordToDoc == null) {
        ord = target;
      } else {
        ord = Arrays.binarySearch(ordToDoc, ord + 1, ordToDoc.length, target);
        if (ord < 0) {
          ord = -(ord + 1);
        }
      }

      if (ord < size) {
        doc = ordToDocOperator.applyAsInt(ord);
      } else {
        doc = NO_MORE_DOCS;
      }
      return doc;
    }

    @Override
    public RandomAccessVectorValues<float[]> copy() {
      return new OffHeapFloatVectorValues(dimension, size, ordToDoc, dataIn.clone());
    }

    @Override
    public float[] vectorValue(int targetOrd) throws IOException {
      dataIn.seek((long) targetOrd * byteSize);
      dataIn.readFloats(value, 0, value.length);
      return value;
    }
  }

  /** Read the nearest-neighbors graph from the index input */
  private static final class OffHeapHnswGraph extends HnswGraph {

    final IndexInput dataIn;
    final int[][] nodesByLevel;
    final long[] graphOffsetsByLevel;
    final int numLevels;
    final int entryNode;
    final int size;
    final long bytesForConns;

    int arcCount;
    int arcUpTo;
    int arc;

    OffHeapHnswGraph(FieldEntry entry, IndexInput dataIn) {
      this.dataIn = dataIn;
      this.nodesByLevel = entry.nodesByLevel;
      this.numLevels = entry.numLevels;
      this.entryNode = numLevels > 1 ? nodesByLevel[numLevels - 1][0] : 0;
      this.size = entry.size();
      this.graphOffsetsByLevel = entry.graphOffsetsByLevel;
      this.bytesForConns = Math.multiplyExact(Math.addExact(entry.maxConn, 1L), Integer.BYTES);
    }

    @Override
    public void seek(int level, int targetOrd) throws IOException {
      int targetIndex =
          level == 0
              ? targetOrd
              : Arrays.binarySearch(nodesByLevel[level], 0, nodesByLevel[level].length, targetOrd);
      assert targetIndex >= 0;
      long graphDataOffset = graphOffsetsByLevel[level] + targetIndex * bytesForConns;
      // unsafe; no bounds checking
      dataIn.seek(graphDataOffset);
      arcCount = dataIn.readInt();
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
      ++arcUpTo;
      arc = dataIn.readInt();
      return arc;
    }

    @Override
    public int numLevels() {
      return numLevels;
    }

    @Override
    public int entryNode() {
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
