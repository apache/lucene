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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
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
  private final IndexInput graphIndex;
  private final IndexInput graphData;
  private final long checksumSeed;

  Lucene90HnswVectorsReader(SegmentReadState state) throws IOException {
    this.fieldInfos = state.fieldInfos;
    int versionMeta = readMetadata(state, Lucene90HnswVectorsFormat.META_EXTENSION);
    long[] checksumRef = new long[1];
    boolean success = false;
    try {
      vectorData =
          openDataInput(
              state,
              versionMeta,
              Lucene90HnswVectorsFormat.VECTOR_DATA_EXTENSION,
              Lucene90HnswVectorsFormat.VECTOR_DATA_CODEC_NAME,
              checksumRef);
      graphIndex =
          openDataInput(
              state,
              versionMeta,
              Lucene90HnswVectorsFormat.GRAPH_INDEX_EXTENSION,
              Lucene90HnswVectorsFormat.GRAPH_INDEX_CODEC_NAME,
              checksumRef);
      graphData =
          openDataInput(
              state,
              versionMeta,
              Lucene90HnswVectorsFormat.GRAPH_DATA_EXTENSION,
              Lucene90HnswVectorsFormat.GRAPH_DATA_CODEC_NAME,
              checksumRef);
      // fill graph nodes and offsets by level.
      // TODO: should we do this on the first field access?
      fillGraphNodesAndOffsetsByLevel();
      success = true;
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
    checksumSeed = checksumRef[0];
  }

  private int readMetadata(SegmentReadState state, String fileExtension) throws IOException {
    String metaFileName =
        IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, fileExtension);
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
      SegmentReadState state,
      int versionMeta,
      String fileExtension,
      String codecName,
      long[] checksumRef)
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
    checksumRef[0] = CodecUtil.retrieveChecksum(in);
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

  private void fillGraphNodesAndOffsetsByLevel() throws IOException {
    for (FieldEntry entry : fields.values()) {
      IndexInput input =
          graphIndex.slice("graph-index", entry.graphIndexOffset, entry.graphIndexLength);
      int numOfLevels = input.readInt();
      assert entry.numOfLevels == numOfLevels;
      int[] numOfNodesByLevel = new int[numOfLevels];

      // read nodes by level
      for (int level = 0; level < numOfLevels; level++) {
        numOfNodesByLevel[level] = input.readInt();
        if (level == 0) {
          entry.nodesByLevel.add(null);
        } else {
          final int[] nodesOnLevel = new int[numOfNodesByLevel[level]];
          for (int i = 0; i < numOfNodesByLevel[level]; i++) {
            nodesOnLevel[i] = input.readVInt();
          }
          entry.nodesByLevel.add(nodesOnLevel);
        }
      }

      // read offsets by level
      long offset = 0;
      for (int level = 0; level < numOfLevels; level++) {
        long[] ordOffsets = new long[numOfNodesByLevel[level]];
        for (int i = 0; i < ordOffsets.length; i++) {
          offset += input.readVLong();
          ordOffsets[i] = offset;
        }
        entry.ordOffsetsByLevel.add(ordOffsets);
      }
    }
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
    CodecUtil.checksumEntireFile(graphIndex);
    CodecUtil.checksumEntireFile(graphData);
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

    OffHeapVectorValues vectorValues = getOffHeapVectorValues(fieldEntry);

    // use a seed that is fixed for the index so we get reproducible results for the same query
    final Random random = new Random(checksumSeed);
    NeighborQueue results =
        HnswGraph.search(
            target,
            k,
            k,
            vectorValues,
            fieldEntry.similarityFunction,
            getGraphValues(fieldEntry),
            getAcceptOrds(acceptDocs, fieldEntry),
            random);
    int i = 0;
    ScoreDoc[] scoreDocs = new ScoreDoc[Math.min(results.size(), k)];
    boolean reversed = fieldEntry.similarityFunction.reversed;
    while (results.size() > 0) {
      int node = results.topNode();
      float score = results.topScore();
      results.pop();
      if (reversed) {
        score = 1 / (1 + score);
      }
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
    if (entry != null && entry.graphIndexLength > 0) {
      return getGraphValues(entry);
    } else {
      return KnnGraphValues.EMPTY;
    }
  }

  private KnnGraphValues getGraphValues(FieldEntry entry) throws IOException {
    IndexInput bytesSlice =
        graphData.slice("graph-data", entry.graphDataOffset, entry.graphDataLength);
    return new IndexedKnnGraphReader(entry, bytesSlice);
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(vectorData, graphIndex, graphData);
  }

  private static class FieldEntry {

    final VectorSimilarityFunction similarityFunction;
    final long vectorDataOffset;
    final long vectorDataLength;
    final long graphIndexOffset;
    final long graphIndexLength;
    final long graphDataOffset;
    final long graphDataLength;
    final int numOfLevels;
    final int dimension;
    final int[] ordToDoc;
    final List<int[]> nodesByLevel;
    final List<long[]> ordOffsetsByLevel;

    FieldEntry(DataInput input, VectorSimilarityFunction similarityFunction) throws IOException {
      this.similarityFunction = similarityFunction;
      vectorDataOffset = input.readVLong();
      vectorDataLength = input.readVLong();
      graphIndexOffset = input.readVLong();
      graphIndexLength = input.readVLong();
      graphDataOffset = input.readVLong();
      graphDataLength = input.readVLong();
      numOfLevels = input.readInt();
      dimension = input.readInt();
      int size = input.readInt();

      ordToDoc = new int[size];
      for (int i = 0; i < size; i++) {
        int doc = input.readVInt();
        ordToDoc[i] = doc;
      }
      nodesByLevel = new ArrayList<>(numOfLevels);
      ordOffsetsByLevel = new ArrayList<>(numOfLevels);
    }

    int size() {
      return ordToDoc.length;
    }
  }

  /** Read the vector values from the index input. This supports both iterated and random access. */
  private class OffHeapVectorValues extends VectorValues
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

    final FieldEntry entry;
    final IndexInput dataIn;
    final int entryNode;

    int arcCount;
    int arcUpTo;
    int arc;

    IndexedKnnGraphReader(FieldEntry entry, IndexInput dataIn) {
      this.entry = entry;
      this.dataIn = dataIn;
      this.entryNode =
          entry.numOfLevels == 1 ? 0 : entry.nodesByLevel.get(entry.numOfLevels - 1)[0];
    }

    @Override
    public void seek(int level, int targetOrd) throws IOException {
      long graphDataOffset;
      if (level == 0) {
        graphDataOffset = entry.ordOffsetsByLevel.get(0)[targetOrd];
      } else {
        int targetIndex =
            Arrays.binarySearch(
                entry.nodesByLevel.get(level), 0, entry.nodesByLevel.get(level).length, targetOrd);
        assert targetIndex >= 0;
        graphDataOffset = entry.ordOffsetsByLevel.get(level)[targetIndex];
      }

      // unsafe; no bounds checking
      dataIn.seek(graphDataOffset);
      arcCount = dataIn.readInt();
      arc = -1;
      arcUpTo = 0;
    }

    @Override
    public int size() {
      return entry.size();
    }

    @Override
    public int nextNeighbor() throws IOException {
      if (arcUpTo >= arcCount) {
        return NO_MORE_DOCS;
      }
      ++arcUpTo;
      arc += dataIn.readVInt();
      return arc;
    }

    @Override
    public int numOfLevels() throws IOException {
      return entry.numOfLevels;
    }

    @Override
    public int entryNode() throws IOException {
      return entryNode;
    }

    @Override
    public DocIdSetIterator getAllNodesOnLevel(int level) {
      return new DocIdSetIterator() {
        int[] nodes = level == 0 ? null : entry.nodesByLevel.get(level);
        int numOfNodes = level == 0 ? size() : nodes.length;
        int idx = -1;

        @Override
        public int docID() {
          return level == 0 ? idx : nodes[idx];
        }

        @Override
        public int nextDoc() {
          idx++;
          if (idx >= numOfNodes) {
            idx = NO_MORE_DOCS;
            return NO_MORE_DOCS;
          }
          return level == 0 ? idx : nodes[idx];
        }

        @Override
        public long cost() {
          return numOfNodes;
        }

        @Override
        public int advance(int target) {
          throw new UnsupportedOperationException("Not supported");
        }
      };
    }
  }
}
