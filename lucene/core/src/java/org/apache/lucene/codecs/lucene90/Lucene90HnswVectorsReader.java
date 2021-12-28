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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.KnnGraphValues;
import org.apache.lucene.index.RandomAccessVectorValues;
import org.apache.lucene.index.RandomAccessVectorValuesProducer;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.apache.lucene.util.hnsw.NeighborQueue;

/**
 * Reads vectors from the index segments along with index data structures supporting KNN search.
 *
 * @lucene.experimental
 */
public final class Lucene90HnswVectorsReader extends KnnVectorsReader {

  private final FieldInfos fieldInfos;
  private final Map<String, FieldEntry> fields = new HashMap<>();
  private final IndexInput vectorData;
  private final IndexInput vectorIndex;

  Lucene90HnswVectorsReader(SegmentReadState state) throws IOException {
    this.fieldInfos = state.fieldInfos;
    int versionMeta = readMetadata(state);
    boolean success = false;
    try {
      vectorData =
          openDataInput(
              state,
              versionMeta,
              Lucene90HnswVectorsFormat.VECTOR_DATA_EXTENSION,
              Lucene90HnswVectorsFormat.VECTOR_DATA_CODEC_NAME);
      vectorIndex =
          openDataInput(
              state,
              versionMeta,
              Lucene90HnswVectorsFormat.VECTOR_INDEX_EXTENSION,
              Lucene90HnswVectorsFormat.VECTOR_INDEX_CODEC_NAME);
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
            state.segmentInfo.name, state.segmentSuffix, Lucene90HnswVectorsFormat.META_EXTENSION);
    int versionMeta = -1;
    try (ChecksumIndexInput meta = state.directory.openChecksumInput(metaFileName, state.context)) {
      Throwable priorE = null;
      try {
        versionMeta =
            CodecUtil.checkIndexHeader(
                meta,
                Lucene90HnswVectorsFormat.META_CODEC_NAME,
                Lucene90HnswVectorsFormat.VERSION_START,
                Lucene90HnswVectorsFormat.VERSION_CURRENT,
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
            Lucene90HnswVectorsFormat.VERSION_START,
            Lucene90HnswVectorsFormat.VERSION_CURRENT,
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

  private FieldEntry readField(DataInput input) throws IOException {
    VectorSimilarityFunction similarityFunction = readSimilarityFunction(input);
    return new FieldEntry(input, similarityFunction);
  }

  @Override
  public long ramBytesUsed() {
    long totalBytes = RamUsageEstimator.shallowSizeOfInstance(Lucene90HnswVectorsReader.class);
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
  public VectorValues getVectorValues(String field) throws IOException {
    FieldEntry fieldEntry = fields.get(field);
    if (fieldEntry == null || fieldEntry.dimension == 0) {
      return null;
    }
    return getOffHeapVectorValues(fieldEntry);
  }

  @Override
  public TopDocs search(String field, float[] target, int k, Bits acceptDocs) throws IOException {
    FieldEntry fieldEntry = fields.get(field);
    if (fieldEntry == null || fieldEntry.dimension == 0) {
      return null;
    }

    // bound k by total number of vectors to prevent oversizing data structures
    k = Math.min(k, fieldEntry.size());

    OffHeapVectorValues vectorValues = getOffHeapVectorValues(fieldEntry);
    // use a seed that is fixed for the index so we get reproducible results for the same query
    NeighborQueue results =
        HnswGraph.search(
            target,
            k,
            vectorValues,
            fieldEntry.similarityFunction,
            getGraphValues(fieldEntry),
            getAcceptOrds(acceptDocs, fieldEntry));
    int i = 0;
    ScoreDoc[] scoreDocs = new ScoreDoc[Math.min(results.size(), k)];
    while (results.size() > 0) {
      int node = results.topNode();
      float score = fieldEntry.similarityFunction.convertToScore(results.topScore());
      results.pop();
      scoreDocs[scoreDocs.length - ++i] = new ScoreDoc(fieldEntry.ordToDoc[node], score);
    }
    // always return >= the case where we can assert == is only when there are fewer than topK
    // vectors in the index
    return new TopDocs(
        new TotalHits(results.visitedCount(), TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),
        scoreDocs);
  }

  private OffHeapVectorValues getOffHeapVectorValues(FieldEntry fieldEntry) throws IOException {
    IndexInput bytesSlice =
        vectorData.slice("vector-data", fieldEntry.vectorDataOffset, fieldEntry.vectorDataLength);
    return new OffHeapVectorValues(fieldEntry, bytesSlice);
  }

  private Bits getAcceptOrds(Bits acceptDocs, FieldEntry fieldEntry) {
    if (acceptDocs == null) {
      return null;
    }
    return new Bits() {
      @Override
      public boolean get(int index) {
        return acceptDocs.get(fieldEntry.ordToDoc[index]);
      }

      @Override
      public int length() {
        return fieldEntry.ordToDoc.length;
      }
    };
  }

  public KnnGraphValues getGraphValues(String field) throws IOException {
    FieldInfo info = fieldInfos.fieldInfo(field);
    if (info == null) {
      throw new IllegalArgumentException("No such field '" + field + "'");
    }
    FieldEntry entry = fields.get(field);
    if (entry != null && entry.vectorIndexLength > 0) {
      return getGraphValues(entry);
    } else {
      return KnnGraphValues.EMPTY;
    }
  }

  private KnnGraphValues getGraphValues(FieldEntry entry) throws IOException {
    IndexInput bytesSlice =
        vectorIndex.slice("graph-data", entry.vectorIndexOffset, entry.vectorIndexLength);
    return new IndexedKnnGraphReader(entry, bytesSlice);
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
    final int[] ordToDoc;
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
      int size = input.readInt();
      ordToDoc = new int[size];
      for (int i = 0; i < size; i++) {
        int doc = input.readVInt();
        ordToDoc[i] = doc;
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
            nodesByLevel[level][i] = input.readVInt();
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
              graphOffsetsByLevel[level - 1] + (1 + maxConn) * 4 * numNodesOnPrevLevel;
        }
      }
    }

    int size() {
      return ordToDoc.length;
    }
  }

  /** Read the vector values from the index input. This supports both iterated and random access. */
  private static class OffHeapVectorValues extends VectorValues
      implements RandomAccessVectorValues, RandomAccessVectorValuesProducer {

    final FieldEntry fieldEntry;
    final IndexInput dataIn;

    final BytesRef binaryValue;
    final ByteBuffer byteBuffer;
    final int byteSize;
    final float[] value;

    int ord = -1;
    int doc = -1;

    OffHeapVectorValues(FieldEntry fieldEntry, IndexInput dataIn) {
      this.fieldEntry = fieldEntry;
      this.dataIn = dataIn;
      byteSize = Float.BYTES * fieldEntry.dimension;
      byteBuffer = ByteBuffer.allocate(byteSize);
      value = new float[fieldEntry.dimension];
      binaryValue = new BytesRef(byteBuffer.array(), byteBuffer.arrayOffset(), byteSize);
    }

    @Override
    public int dimension() {
      return fieldEntry.dimension;
    }

    @Override
    public int size() {
      return fieldEntry.size();
    }

    @Override
    public float[] vectorValue() throws IOException {
      dataIn.seek((long) ord * byteSize);
      dataIn.readFloats(value, 0, value.length);
      return value;
    }

    @Override
    public BytesRef binaryValue() throws IOException {
      dataIn.seek((long) ord * byteSize);
      dataIn.readBytes(byteBuffer.array(), byteBuffer.arrayOffset(), byteSize, false);
      return binaryValue;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() {
      if (++ord >= size()) {
        doc = NO_MORE_DOCS;
      } else {
        doc = fieldEntry.ordToDoc[ord];
      }
      return doc;
    }

    @Override
    public int advance(int target) {
      assert docID() < target;
      ord = Arrays.binarySearch(fieldEntry.ordToDoc, ord + 1, fieldEntry.ordToDoc.length, target);
      if (ord < 0) {
        ord = -(ord + 1);
      }
      assert ord <= fieldEntry.ordToDoc.length;
      if (ord == fieldEntry.ordToDoc.length) {
        doc = NO_MORE_DOCS;
      } else {
        doc = fieldEntry.ordToDoc[ord];
      }
      return doc;
    }

    @Override
    public long cost() {
      return fieldEntry.size();
    }

    @Override
    public RandomAccessVectorValues randomAccess() {
      return new OffHeapVectorValues(fieldEntry, dataIn.clone());
    }

    @Override
    public float[] vectorValue(int targetOrd) throws IOException {
      dataIn.seek((long) targetOrd * byteSize);
      dataIn.readFloats(value, 0, value.length);
      return value;
    }

    @Override
    public BytesRef binaryValue(int targetOrd) throws IOException {
      readValue(targetOrd);
      return binaryValue;
    }

    private void readValue(int targetOrd) throws IOException {
      dataIn.seek((long) targetOrd * byteSize);
      dataIn.readBytes(byteBuffer.array(), byteBuffer.arrayOffset(), byteSize);
    }
  }

  /** Read the nearest-neighbors graph from the index input */
  private static final class IndexedKnnGraphReader extends KnnGraphValues {

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

    IndexedKnnGraphReader(FieldEntry entry, IndexInput dataIn) {
      this.dataIn = dataIn;
      this.nodesByLevel = entry.nodesByLevel;
      this.numLevels = entry.numLevels;
      this.entryNode = numLevels > 1 ? nodesByLevel[numLevels - 1][0] : 0;
      this.size = entry.size();
      this.graphOffsetsByLevel = entry.graphOffsetsByLevel;
      this.bytesForConns = ((long) entry.maxConn + 1) * 4;
    }

    @Override
    public void seek(int level, int targetOrd) throws IOException {
      int targetIndex =
          level == 0
              ? targetOrd
              : Arrays.binarySearch(nodesByLevel[level], 0, nodesByLevel[level].length, targetOrd);
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
    public DocIdSetIterator getAllNodesOnLevel(int level) {
      if (level == 0) {
        return new DocIdSetIterator() {
          int numNodes = size();
          int idx = -1;

          @Override
          public int docID() {
            return idx;
          }

          @Override
          public int nextDoc() {
            idx++;
            if (idx >= numNodes) {
              idx = NO_MORE_DOCS;
              return NO_MORE_DOCS;
            }
            return idx;
          }

          @Override
          public long cost() {
            return numNodes;
          }

          @Override
          public int advance(int target) {
            throw new UnsupportedOperationException("Not supported");
          }
        };
      } else {
        return new DocIdSetIterator() {
          final int[] nodes = nodesByLevel[level];
          int idx = -1;

          @Override
          public int docID() {
            return nodes[idx];
          }

          @Override
          public int nextDoc() {
            idx++;
            if (idx >= nodes.length) {
              idx = NO_MORE_DOCS;
              return NO_MORE_DOCS;
            }
            return nodes[idx];
          }

          @Override
          public long cost() {
            return nodes.length;
          }

          @Override
          public int advance(int target) {
            throw new UnsupportedOperationException("Not supported");
          }
        };
      }
    }
  }
}
