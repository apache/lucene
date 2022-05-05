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

package org.apache.lucene.codecs.lucene91;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.RandomAccessVectorValues;
import org.apache.lucene.index.RandomAccessVectorValuesProducer;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
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
public final class Lucene91HnswVectorsReader extends KnnVectorsReader {

  private final FieldInfos fieldInfos;
  private final Map<String, FieldEntry> fields = new HashMap<>();
  private final IndexInput vectorData;
  private final IndexInput vectorIndex;

  Lucene91HnswVectorsReader(SegmentReadState state) throws IOException {
    this.fieldInfos = state.fieldInfos;
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
    return in;
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

  private FieldEntry readField(IndexInput input) throws IOException {
    VectorSimilarityFunction similarityFunction = readSimilarityFunction(input);
    return new FieldEntry(input, similarityFunction);
  }

  @Override
  public long ramBytesUsed() {
    long totalBytes = RamUsageEstimator.shallowSizeOfInstance(Lucene91HnswVectorsFormat.class);
    totalBytes +=
        RamUsageEstimator.sizeOfMap(
            fields, RamUsageEstimator.shallowSizeOfInstance(FieldEntry.class));
    return totalBytes;
  }

  @Override
  public void checkIntegrity() throws IOException {
    CodecUtil.checksumEntireFile(vectorData);
    CodecUtil.checksumEntireFile(vectorIndex);
  }

  @Override
  public VectorValues getVectorValues(String field) throws IOException {
    FieldEntry fieldEntry = fields.get(field);
    return OffHeapVectorValues.load(fieldEntry, vectorData);
  }

  @Override
  public TopDocs search(String field, float[] target, int k, Bits acceptDocs, int visitedLimit)
      throws IOException {
    FieldEntry fieldEntry = fields.get(field);

    if (fieldEntry.size() == 0) {
      return new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]);
    }

    // bound k by total number of vectors to prevent oversizing data structures
    k = Math.min(k, fieldEntry.size());
    OffHeapVectorValues vectorValues = OffHeapVectorValues.load(fieldEntry, vectorData);

    NeighborQueue results =
        HnswGraphSearcher.search(
            target,
            k,
            vectorValues,
            fieldEntry.similarityFunction,
            getGraph(fieldEntry),
            vectorValues.getAcceptOrds(acceptDocs),
            visitedLimit);

    int i = 0;
    ScoreDoc[] scoreDocs = new ScoreDoc[Math.min(results.size(), k)];
    while (results.size() > 0) {
      int node = results.topNode();
      float score = fieldEntry.similarityFunction.convertToScore(results.topScore());
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
    final int[][] nodesByLevel;
    // for each level the start offsets in vectorIndex file from where to read neighbours
    final long[] graphOffsetsByLevel;
    final long docsWithFieldOffset;
    final long docsWithFieldLength;
    final short jumpTableEntryCount;
    final byte denseRankPower;
    final long addressesOffset;
    final int blockShift;
    final DirectMonotonicReader.Meta meta;
    final long addressesLength;

    FieldEntry(IndexInput input, VectorSimilarityFunction similarityFunction) throws IOException {
      this.similarityFunction = similarityFunction;
      vectorDataOffset = input.readVLong();
      vectorDataLength = input.readVLong();
      vectorIndexOffset = input.readVLong();
      vectorIndexLength = input.readVLong();
      dimension = input.readInt();
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
      for (int level = 0; level < numLevels; level++) {
        if (level == 0) {
          graphOffsetsByLevel[level] = 0;
        } else {
          int numNodesOnPrevLevel = level == 1 ? size : nodesByLevel[level - 1].length;
          graphOffsetsByLevel[level] =
              graphOffsetsByLevel[level - 1] + (1 + maxConn) * Integer.BYTES * numNodesOnPrevLevel;
        }
      }
    }

    int size() {
      return size;
    }
  }

  static class DenseOffHeapVectorValues extends OffHeapVectorValues {

    private int doc = -1;

    public DenseOffHeapVectorValues(int dimension, int size, IndexInput slice) {
      super(dimension, size, slice);
    }

    @Override
    public float[] vectorValue() throws IOException {
      slice.seek((long) doc * byteSize);
      slice.readFloats(value, 0, value.length);
      return value;
    }

    @Override
    public BytesRef binaryValue() throws IOException {
      slice.seek((long) doc * byteSize);
      slice.readBytes(byteBuffer.array(), byteBuffer.arrayOffset(), byteSize, false);
      return binaryValue;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() throws IOException {
      return advance(doc + 1);
    }

    @Override
    public int advance(int target) throws IOException {
      assert docID() < target;
      if (target >= size) {
        return doc = NO_MORE_DOCS;
      }
      return doc = target;
    }

    @Override
    public RandomAccessVectorValues randomAccess() throws IOException {
      return new DenseOffHeapVectorValues(dimension, size, slice.clone());
    }

    @Override
    public int ordToDoc(int ord) {
      return ord;
    }

    @Override
    Bits getAcceptOrds(Bits acceptDocs) {
      return acceptDocs;
    }
  }

  static class SparseOffHeapVectorValues extends OffHeapVectorValues {
    private final DirectMonotonicReader ordToDoc;
    private final IndexedDISI disi;
    // dataIn was used to init a new IndexedDIS for #randomAccess()
    private final IndexInput dataIn;
    private final FieldEntry fieldEntry;

    public SparseOffHeapVectorValues(FieldEntry fieldEntry, IndexInput dataIn, IndexInput slice)
        throws IOException {

      super(fieldEntry.dimension, fieldEntry.size, slice);
      this.fieldEntry = fieldEntry;
      final RandomAccessInput addressesData =
          dataIn.randomAccessSlice(fieldEntry.addressesOffset, fieldEntry.addressesLength);
      this.dataIn = dataIn;
      this.ordToDoc = DirectMonotonicReader.getInstance(fieldEntry.meta, addressesData);
      this.disi =
          new IndexedDISI(
              dataIn,
              fieldEntry.docsWithFieldOffset,
              fieldEntry.docsWithFieldLength,
              fieldEntry.jumpTableEntryCount,
              fieldEntry.denseRankPower,
              fieldEntry.size);
    }

    @Override
    public float[] vectorValue() throws IOException {
      slice.seek((long) (disi.index()) * byteSize);
      slice.readFloats(value, 0, value.length);
      return value;
    }

    @Override
    public BytesRef binaryValue() throws IOException {
      slice.seek((long) (disi.index()) * byteSize);
      slice.readBytes(byteBuffer.array(), byteBuffer.arrayOffset(), byteSize, false);
      return binaryValue;
    }

    @Override
    public int docID() {
      return disi.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      return disi.nextDoc();
    }

    @Override
    public int advance(int target) throws IOException {
      assert docID() < target;
      return disi.advance(target);
    }

    @Override
    public RandomAccessVectorValues randomAccess() throws IOException {
      return new SparseOffHeapVectorValues(fieldEntry, dataIn, slice.clone());
    }

    @Override
    public int ordToDoc(int ord) {
      return (int) ordToDoc.get(ord);
    }

    @Override
    Bits getAcceptOrds(Bits acceptDocs) {
      if (acceptDocs == null) {
        return null;
      }
      return new Bits() {
        @Override
        public boolean get(int index) {
          return acceptDocs.get(ordToDoc(index));
        }

        @Override
        public int length() {
          return size;
        }
      };
    }
  }

  static class EmptyOffHeapVectorValues extends OffHeapVectorValues {

    public EmptyOffHeapVectorValues(int dimension) {
      super(dimension, 0, null);
    }

    private int doc = -1;

    @Override
    public int dimension() {
      return super.dimension();
    }

    @Override
    public int size() {
      return 0;
    }

    @Override
    public float[] vectorValue() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public BytesRef binaryValue() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() throws IOException {
      return advance(doc + 1);
    }

    @Override
    public int advance(int target) throws IOException {
      return doc = NO_MORE_DOCS;
    }

    @Override
    public long cost() {
      return 0;
    }

    @Override
    public RandomAccessVectorValues randomAccess() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public float[] vectorValue(int targetOrd) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public BytesRef binaryValue(int targetOrd) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int ordToDoc(int ord) {
      throw new UnsupportedOperationException();
    }

    @Override
    Bits getAcceptOrds(Bits acceptDocs) {
      return null;
    }
  }

  /** Read the vector values from the index input. This supports both iterated and random access. */
  abstract static class OffHeapVectorValues extends VectorValues
      implements RandomAccessVectorValues, RandomAccessVectorValuesProducer {

    protected final int dimension;
    protected final int size;
    protected final IndexInput slice;
    protected final BytesRef binaryValue;
    protected final ByteBuffer byteBuffer;
    protected final int byteSize;
    protected final float[] value;

    OffHeapVectorValues(int dimension, int size, IndexInput slice) {
      this.dimension = dimension;
      this.size = size;
      this.slice = slice;
      byteSize = Float.BYTES * dimension;
      byteBuffer = ByteBuffer.allocate(byteSize);
      value = new float[dimension];
      binaryValue = new BytesRef(byteBuffer.array(), byteBuffer.arrayOffset(), byteSize);
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
    public long cost() {
      return size;
    }

    @Override
    public float[] vectorValue(int targetOrd) throws IOException {
      slice.seek((long) targetOrd * byteSize);
      slice.readFloats(value, 0, value.length);
      return value;
    }

    @Override
    public BytesRef binaryValue(int targetOrd) throws IOException {
      readValue(targetOrd);
      return binaryValue;
    }

    private void readValue(int targetOrd) throws IOException {
      slice.seek((long) targetOrd * byteSize);
      slice.readBytes(byteBuffer.array(), byteBuffer.arrayOffset(), byteSize);
    }

    public abstract int ordToDoc(int ord);

    private static OffHeapVectorValues load(FieldEntry fieldEntry, IndexInput vectorData)
        throws IOException {
      if (fieldEntry.docsWithFieldOffset == -2) {
        return new EmptyOffHeapVectorValues(fieldEntry.dimension);
      }
      IndexInput bytesSlice =
          vectorData.slice("vector-data", fieldEntry.vectorDataOffset, fieldEntry.vectorDataLength);
      if (fieldEntry.docsWithFieldOffset == -1) {
        return new DenseOffHeapVectorValues(fieldEntry.dimension, fieldEntry.size, bytesSlice);
      } else {
        return new SparseOffHeapVectorValues(fieldEntry, vectorData, bytesSlice);
      }
    }

    abstract Bits getAcceptOrds(Bits acceptDocs);
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
      this.bytesForConns = ((long) entry.maxConn + 1) * Integer.BYTES;
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
        return new NodesIterator(size());
      } else {
        return new NodesIterator(nodesByLevel[level], nodesByLevel[level].length);
      }
    }
  }
}
