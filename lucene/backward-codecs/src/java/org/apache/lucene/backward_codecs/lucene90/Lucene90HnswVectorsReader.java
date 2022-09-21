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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.SplittableRandom;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.RandomAccessVectorValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.index.VectorValues;
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
  private final long checksumSeed;

  Lucene90HnswVectorsReader(SegmentReadState state) throws IOException {
    this.fieldInfos = state.fieldInfos;

    int versionMeta = readMetadata(state);
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
      vectorIndex =
          openDataInput(
              state,
              versionMeta,
              Lucene90HnswVectorsFormat.VECTOR_INDEX_EXTENSION,
              Lucene90HnswVectorsFormat.VECTOR_INDEX_CODEC_NAME,
              checksumRef);
      success = true;
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
    checksumSeed = checksumRef[0];
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
      SegmentReadState state,
      int versionMeta,
      String fileExtension,
      String codecName,
      long[] checksumRef)
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
    return getOffHeapVectorValues(fieldEntry);
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

    OffHeapVectorValues vectorValues = getOffHeapVectorValues(fieldEntry);
    // use a seed that is fixed for the index so we get reproducible results for the same query
    final SplittableRandom random = new SplittableRandom(checksumSeed);
    NeighborQueue results =
        Lucene90OnHeapHnswGraph.search(
            target,
            k,
            k,
            vectorValues,
            fieldEntry.similarityFunction,
            getGraphValues(fieldEntry),
            getAcceptOrds(acceptDocs, fieldEntry),
            visitedLimit,
            random);
    int i = 0;
    ScoreDoc[] scoreDocs = new ScoreDoc[Math.min(results.size(), k)];
    while (results.size() > 0) {
      int node = results.topNode();
      float minSimilarity = results.topScore();
      results.pop();
      scoreDocs[scoreDocs.length - ++i] = new ScoreDoc(fieldEntry.ordToDoc[node], minSimilarity);
    }
    TotalHits.Relation relation =
        results.incomplete()
            ? TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO
            : TotalHits.Relation.EQUAL_TO;
    return new TopDocs(new TotalHits(results.visitedCount(), relation), scoreDocs);
  }

  private OffHeapVectorValues getOffHeapVectorValues(FieldEntry fieldEntry) throws IOException {
    IndexInput bytesSlice =
        vectorData.slice("vector-data", fieldEntry.vectorDataOffset, fieldEntry.vectorDataLength);
    return new OffHeapVectorValues(fieldEntry.dimension, fieldEntry.ordToDoc, bytesSlice);
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

  /** Get knn graph values; used for testing */
  public HnswGraph getGraphValues(String field) throws IOException {
    FieldInfo info = fieldInfos.fieldInfo(field);
    if (info == null) {
      throw new IllegalArgumentException("No such field '" + field + "'");
    }
    FieldEntry entry = fields.get(field);
    if (entry != null && entry.indexDataLength > 0) {
      return getGraphValues(entry);
    } else {
      return HnswGraph.EMPTY;
    }
  }

  private HnswGraph getGraphValues(FieldEntry entry) throws IOException {
    IndexInput bytesSlice =
        vectorIndex.slice("graph-data", entry.indexDataOffset, entry.indexDataLength);
    return new OffHeapHnswGraph(entry, bytesSlice);
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(vectorData, vectorIndex);
  }

  private static class FieldEntry {

    final int dimension;
    final VectorSimilarityFunction similarityFunction;

    final long vectorDataOffset;
    final long vectorDataLength;
    final long indexDataOffset;
    final long indexDataLength;
    final int[] ordToDoc;
    final long[] ordOffsets;

    FieldEntry(DataInput input, VectorSimilarityFunction similarityFunction) throws IOException {
      this.similarityFunction = similarityFunction;
      vectorDataOffset = input.readVLong();
      vectorDataLength = input.readVLong();
      indexDataOffset = input.readVLong();
      indexDataLength = input.readVLong();
      dimension = input.readInt();
      int size = input.readInt();
      ordToDoc = new int[size];
      for (int i = 0; i < size; i++) {
        int doc = input.readVInt();
        ordToDoc[i] = doc;
      }
      ordOffsets = new long[size()];
      long offset = 0;
      for (int i = 0; i < ordOffsets.length; i++) {
        offset += input.readVLong();
        ordOffsets[i] = offset;
      }
    }

    int size() {
      return ordToDoc.length;
    }
  }

  /** Read the vector values from the index input. This supports both iterated and random access. */
  static class OffHeapVectorValues extends VectorValues implements RandomAccessVectorValues {

    final int dimension;
    final int[] ordToDoc;
    final IndexInput dataIn;

    final BytesRef binaryValue;
    final ByteBuffer byteBuffer;
    final int byteSize;
    final float[] value;

    int ord = -1;
    int doc = -1;

    OffHeapVectorValues(int dimension, int[] ordToDoc, IndexInput dataIn) {
      this.dimension = dimension;
      this.ordToDoc = ordToDoc;
      this.dataIn = dataIn;

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
      return ordToDoc.length;
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
        doc = ordToDoc[ord];
      }
      return doc;
    }

    @Override
    public int advance(int target) {
      assert docID() < target;
      ord = Arrays.binarySearch(ordToDoc, ord + 1, ordToDoc.length, target);
      if (ord < 0) {
        ord = -(ord + 1);
      }
      assert ord <= ordToDoc.length;
      if (ord == ordToDoc.length) {
        doc = NO_MORE_DOCS;
      } else {
        doc = ordToDoc[ord];
      }
      return doc;
    }

    @Override
    public long cost() {
      return ordToDoc.length;
    }

    @Override
    public RandomAccessVectorValues copy() {
      return new OffHeapVectorValues(dimension, ordToDoc, dataIn.clone());
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
  private static final class OffHeapHnswGraph extends HnswGraph {

    final FieldEntry entry;
    final IndexInput dataIn;

    int arcCount;
    int arcUpTo;
    int arc;

    OffHeapHnswGraph(FieldEntry entry, IndexInput dataIn) {
      this.entry = entry;
      this.dataIn = dataIn;
    }

    @Override
    public void seek(int level, int targetOrd) throws IOException {
      // unsafe; no bounds checking
      dataIn.seek(entry.ordOffsets[targetOrd]);
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
    public int numLevels() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int entryNode() {
      throw new UnsupportedOperationException();
    }

    @Override
    public NodesIterator getNodesOnLevel(int level) {
      throw new UnsupportedOperationException();
    }
  }
}
