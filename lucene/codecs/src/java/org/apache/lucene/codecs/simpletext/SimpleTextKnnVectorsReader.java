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

package org.apache.lucene.codecs.simpletext;

import static org.apache.lucene.codecs.simpletext.SimpleTextKnnVectorsWriter.FIELD_NAME;
import static org.apache.lucene.codecs.simpletext.SimpleTextKnnVectorsWriter.FIELD_NUMBER;
import static org.apache.lucene.codecs.simpletext.SimpleTextKnnVectorsWriter.SIZE;
import static org.apache.lucene.codecs.simpletext.SimpleTextKnnVectorsWriter.VECTOR_DATA_LENGTH;
import static org.apache.lucene.codecs.simpletext.SimpleTextKnnVectorsWriter.VECTOR_DATA_OFFSET;
import static org.apache.lucene.codecs.simpletext.SimpleTextKnnVectorsWriter.VECTOR_DIMENSION;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.internal.hppc.IntObjectHashMap;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.StringHelper;

/**
 * Reads vector values from a simple text format. All vectors are read up front and cached in RAM in
 * order to support random access. <b>FOR RECREATIONAL USE ONLY</b>
 *
 * @lucene.experimental
 */
public class SimpleTextKnnVectorsReader extends KnnVectorsReader {
  // shallowSizeOfInstance for fieldEntries map is included in ramBytesUsed() calculation

  private static final BytesRef EMPTY = new BytesRef("");

  private final SegmentReadState readState;
  private final IndexInput dataIn;
  private final BytesRefBuilder scratch = new BytesRefBuilder();
  private final IntObjectHashMap<FieldEntry> fieldEntries = new IntObjectHashMap<>();

  SimpleTextKnnVectorsReader(SegmentReadState readState) throws IOException {
    this.readState = readState;
    String metaFileName =
        IndexFileNames.segmentFileName(
            readState.segmentInfo.name,
            readState.segmentSuffix,
            SimpleTextKnnVectorsFormat.META_EXTENSION);
    String vectorFileName =
        IndexFileNames.segmentFileName(
            readState.segmentInfo.name,
            readState.segmentSuffix,
            SimpleTextKnnVectorsFormat.VECTOR_EXTENSION);

    try (ChecksumIndexInput in = readState.directory.openChecksumInput(metaFileName)) {
      int fieldNumber = readInt(in, FIELD_NUMBER);
      while (fieldNumber != -1) {
        String fieldName = readString(in, FIELD_NAME);
        long vectorDataOffset = readLong(in, VECTOR_DATA_OFFSET);
        long vectorDataLength = readLong(in, VECTOR_DATA_LENGTH);
        int dimension = readInt(in, VECTOR_DIMENSION);
        int size = readInt(in, SIZE);
        int[] docIds = new int[size];
        for (int i = 0; i < size; i++) {
          docIds[i] = readInt(in, EMPTY);
        }
        assert fieldEntries.containsKey(fieldNumber) == false;
        fieldEntries.put(
            fieldNumber,
            new FieldEntry(
                dimension,
                vectorDataOffset,
                vectorDataLength,
                docIds,
                readState.fieldInfos.fieldInfo(fieldName).getVectorSimilarityFunction()));
        fieldNumber = readInt(in, FIELD_NUMBER);
      }
      SimpleTextUtil.checkFooter(in);

      dataIn = readState.directory.openInput(vectorFileName, IOContext.DEFAULT);
    } catch (Throwable t) {
      IOUtils.closeWhileSuppressingExceptions(t, this);
      throw t;
    }
  }

  @Override
  public FloatVectorValues getFloatVectorValues(String field) throws IOException {
    FieldInfo info = readState.fieldInfos.fieldInfo(field);
    if (info == null) {
      // mirror the handling in Lucene90VectorReader#getVectorValues
      // needed to pass TestSimpleTextKnnVectorsFormat#testDeleteAllVectorDocs
      return null;
    }
    int dimension = info.getVectorDimension();
    if (dimension == 0) {
      throw new IllegalStateException(
          "KNN vectors readers should not be called on fields that don't enable KNN vectors");
    }
    FieldEntry fieldEntry = fieldEntries.get(info.number);
    if (fieldEntry == null) {
      // mirror the handling in Lucene90VectorReader#getVectorValues
      // needed to pass TestSimpleTextKnnVectorsFormat#testDeleteAllVectorDocs
      return null;
    }
    if (dimension != fieldEntry.dimension) {
      throw new IllegalStateException(
          "Inconsistent vector dimension for field=\""
              + field
              + "\"; "
              + dimension
              + " != "
              + fieldEntry.dimension);
    }
    IndexInput bytesSlice =
        dataIn.slice("vector-data", fieldEntry.vectorDataOffset, fieldEntry.vectorDataLength);
    return new SimpleTextFloatVectorValues(fieldEntry, bytesSlice);
  }

  @Override
  public ByteVectorValues getByteVectorValues(String field) throws IOException {
    FieldInfo info = readState.fieldInfos.fieldInfo(field);
    if (info == null) {
      // mirror the handling in Lucene90VectorReader#getVectorValues
      // needed to pass TestSimpleTextKnnVectorsFormat#testDeleteAllVectorDocs
      return null;
    }
    int dimension = info.getVectorDimension();
    if (dimension == 0) {
      throw new IllegalStateException(
          "KNN vectors readers should not be called on fields that don't enable KNN vectors");
    }
    FieldEntry fieldEntry = fieldEntries.get(info.number);
    if (fieldEntry == null) {
      // mirror the handling in Lucene90VectorReader#getVectorValues
      // needed to pass TestSimpleTextKnnVectorsFormat#testDeleteAllVectorDocs
      return null;
    }
    if (dimension != fieldEntry.dimension) {
      throw new IllegalStateException(
          "Inconsistent vector dimension for field=\""
              + field
              + "\"; "
              + dimension
              + " != "
              + fieldEntry.dimension);
    }
    IndexInput bytesSlice =
        dataIn.slice("vector-data", fieldEntry.vectorDataOffset, fieldEntry.vectorDataLength);
    return new SimpleTextByteVectorValues(fieldEntry, bytesSlice);
  }

  @Override
  public void search(String field, float[] target, KnnCollector knnCollector, Bits acceptDocs)
      throws IOException {
    FloatVectorValues values = getFloatVectorValues(field);
    if (target.length != values.dimension()) {
      throw new IllegalArgumentException(
          "vector query dimension: "
              + target.length
              + " differs from field dimension: "
              + values.dimension());
    }
    FieldInfo info = readState.fieldInfos.fieldInfo(field);
    VectorSimilarityFunction vectorSimilarity = info.getVectorSimilarityFunction();
    for (int ord = 0; ord < values.size(); ord++) {
      int doc = values.ordToDoc(ord);
      if (acceptDocs != null && acceptDocs.get(doc) == false) {
        continue;
      }

      if (knnCollector.earlyTerminated()) {
        break;
      }

      float[] vector = values.vectorValue(ord);
      float score = vectorSimilarity.compare(vector, target);
      knnCollector.collect(doc, score);
      knnCollector.incVisitedCount(1);
    }
  }

  @Override
  public void search(String field, byte[] target, KnnCollector knnCollector, Bits acceptDocs)
      throws IOException {
    ByteVectorValues values = getByteVectorValues(field);
    if (target.length != values.dimension()) {
      throw new IllegalArgumentException(
          "vector query dimension: "
              + target.length
              + " differs from field dimension: "
              + values.dimension());
    }
    FieldInfo info = readState.fieldInfos.fieldInfo(field);
    VectorSimilarityFunction vectorSimilarity = info.getVectorSimilarityFunction();

    for (int ord = 0; ord < values.size(); ord++) {
      int doc = values.ordToDoc(ord);
      if (acceptDocs != null && acceptDocs.get(doc) == false) {
        continue;
      }

      if (knnCollector.earlyTerminated()) {
        break;
      }

      byte[] vector = values.vectorValue(ord);
      float score = vectorSimilarity.compare(vector, target);
      knnCollector.collect(doc, score);
      knnCollector.incVisitedCount(1);
    }
  }

  @Override
  public void checkIntegrity() throws IOException {
    IndexInput clone = dataIn.clone();
    clone.seek(0);

    // checksum is fixed-width encoded with 20 bytes, plus 1 byte for newline (the space is included
    // in SimpleTextUtil.CHECKSUM):
    long footerStartPos = dataIn.length() - (SimpleTextUtil.CHECKSUM.length + 21);
    ChecksumIndexInput input = new BufferedChecksumIndexInput(clone);

    // when there's no actual vector data written (e.g. tested in
    // TestSimpleTextKnnVectorsFormat#testDeleteAllVectorDocs)
    // the first line in dataInput will be, checksum 00000000000000000000
    if (footerStartPos == 0) {
      SimpleTextUtil.checkFooter(input);
      return;
    }

    while (true) {
      SimpleTextUtil.readLine(input, scratch);
      if (input.getFilePointer() >= footerStartPos) {
        // Make sure we landed at precisely the right location:
        if (input.getFilePointer() != footerStartPos) {
          throw new CorruptIndexException(
              "SimpleText failure: footer does not start at expected position current="
                  + input.getFilePointer()
                  + " vs expected="
                  + footerStartPos,
              input);
        }
        SimpleTextUtil.checkFooter(input);
        break;
      }
    }
  }

  @Override
  public Map<String, Long> getOffHeapByteSize(FieldInfo fieldInfo) {
    Objects.requireNonNull(fieldInfo);
    FieldEntry fieldEntry = fieldEntries.get(fieldInfo.number);
    if (fieldEntry == null) {
      return null;
    }
    return Map.of(); // all in-heap
  }

  @Override
  public void close() throws IOException {
    dataIn.close();
  }

  private record FieldEntry(
      int dimension,
      long vectorDataOffset,
      long vectorDataLength,
      int[] ordToDoc,
      VectorSimilarityFunction similarityFunction) {
    int size() {
      return ordToDoc.length;
    }
  }

  private static class SimpleTextFloatVectorValues extends FloatVectorValues {

    private final BytesRefBuilder scratch = new BytesRefBuilder();
    private final FieldEntry entry;
    private final IndexInput in;
    private final float[][] values;

    int curOrd;

    SimpleTextFloatVectorValues(FieldEntry entry, IndexInput in) throws IOException {
      this.entry = entry;
      this.in = in;
      values = new float[entry.size()][entry.dimension];
      curOrd = -1;
      readAllVectors();
    }

    private SimpleTextFloatVectorValues(SimpleTextFloatVectorValues other) {
      this.entry = other.entry;
      this.in = other.in.clone();
      this.values = other.values;
      this.curOrd = other.curOrd;
    }

    @Override
    public int dimension() {
      return entry.dimension;
    }

    @Override
    public int size() {
      return entry.size();
    }

    @Override
    public float[] vectorValue(int ord) {
      return values[ord];
    }

    @Override
    public int ordToDoc(int ord) {
      return entry.ordToDoc[ord];
    }

    @Override
    public DocIndexIterator iterator() {
      return createSparseIterator();
    }

    @Override
    public VectorScorer scorer(float[] target) {
      if (size() == 0) {
        return null;
      }
      SimpleTextFloatVectorValues simpleTextFloatVectorValues =
          new SimpleTextFloatVectorValues(this);
      DocIndexIterator iterator = simpleTextFloatVectorValues.iterator();
      return new VectorScorer() {
        @Override
        public float score() throws IOException {
          int ord = iterator.index();
          return entry
              .similarityFunction()
              .compare(simpleTextFloatVectorValues.vectorValue(ord), target);
        }

        @Override
        public DocIdSetIterator iterator() {
          return iterator;
        }
      };
    }

    private void readAllVectors() throws IOException {
      for (float[] value : values) {
        readVector(value);
      }
    }

    private void readVector(float[] value) throws IOException {
      SimpleTextUtil.readLine(in, scratch);
      // skip leading "[" and strip trailing "]"
      String s = new BytesRef(scratch.bytes(), 1, scratch.length() - 2).utf8ToString();
      String[] floatStrings = s.split(",");
      assert floatStrings.length == value.length
          : " read " + s + " when expecting " + value.length + " floats";
      for (int i = 0; i < floatStrings.length; i++) {
        value[i] = Float.parseFloat(floatStrings[i]);
      }
    }

    @Override
    public SimpleTextFloatVectorValues copy() {
      return this;
    }
  }

  private static class SimpleTextByteVectorValues extends ByteVectorValues {

    private final BytesRefBuilder scratch = new BytesRefBuilder();
    private final FieldEntry entry;
    private final IndexInput in;
    private final BytesRef binaryValue;
    private final byte[][] values;

    int curOrd;

    SimpleTextByteVectorValues(FieldEntry entry, IndexInput in) throws IOException {
      this.entry = entry;
      this.in = in;
      values = new byte[entry.size()][entry.dimension];
      binaryValue = new BytesRef(entry.dimension);
      binaryValue.length = binaryValue.bytes.length;
      curOrd = -1;
      readAllVectors();
    }

    private SimpleTextByteVectorValues(SimpleTextByteVectorValues other) {
      this.entry = other.entry;
      this.in = other.in.clone();
      this.values = other.values;
      this.binaryValue = new BytesRef(entry.dimension);
      this.binaryValue.length = binaryValue.bytes.length;
      this.curOrd = other.curOrd;
    }

    @Override
    public int dimension() {
      return entry.dimension;
    }

    @Override
    public int size() {
      return entry.size();
    }

    @Override
    public byte[] vectorValue(int ord) {
      binaryValue.bytes = values[ord];
      return binaryValue.bytes;
    }

    @Override
    public int ordToDoc(int ord) {
      return entry.ordToDoc[ord];
    }

    @Override
    public DocIndexIterator iterator() {
      return createSparseIterator();
    }

    @Override
    public VectorScorer scorer(byte[] target) {
      if (size() == 0) {
        return null;
      }
      SimpleTextByteVectorValues simpleTextByteVectorValues = new SimpleTextByteVectorValues(this);
      return new VectorScorer() {
        DocIndexIterator it = simpleTextByteVectorValues.iterator();

        @Override
        public float score() throws IOException {
          int ord = it.index();
          return entry
              .similarityFunction()
              .compare(simpleTextByteVectorValues.vectorValue(ord), target);
        }

        @Override
        public DocIdSetIterator iterator() {
          return it;
        }
      };
    }

    private void readAllVectors() throws IOException {
      for (byte[] value : values) {
        readVector(value);
      }
    }

    private void readVector(byte[] value) throws IOException {
      SimpleTextUtil.readLine(in, scratch);
      // skip leading "[" and strip trailing "]"
      String s = new BytesRef(scratch.bytes(), 1, scratch.length() - 2).utf8ToString();
      String[] floatStrings = s.split(",");
      assert floatStrings.length == value.length
          : " read " + s + " when expecting " + value.length + " floats";
      for (int i = 0; i < floatStrings.length; i++) {
        value[i] = (byte) Float.parseFloat(floatStrings[i]);
      }
    }

    @Override
    public SimpleTextByteVectorValues copy() {
      return this;
    }
  }

  private int readInt(IndexInput in, BytesRef field) throws IOException {
    SimpleTextUtil.readLine(in, scratch);
    return parseInt(field);
  }

  private long readLong(IndexInput in, BytesRef field) throws IOException {
    SimpleTextUtil.readLine(in, scratch);
    return parseLong(field);
  }

  private String readString(IndexInput in, BytesRef field) throws IOException {
    SimpleTextUtil.readLine(in, scratch);
    return stripPrefix(field);
  }

  private boolean startsWith(BytesRef prefix) {
    return StringHelper.startsWith(scratch.get(), prefix);
  }

  private int parseInt(BytesRef prefix) {
    assert startsWith(prefix);
    return Integer.parseInt(stripPrefix(prefix));
  }

  private long parseLong(BytesRef prefix) {
    assert startsWith(prefix);
    return Long.parseLong(stripPrefix(prefix));
  }

  private String stripPrefix(BytesRef prefix) {
    int prefixLen = prefix.length;
    return new String(
        scratch.bytes(), prefixLen, scratch.length() - prefixLen, StandardCharsets.UTF_8);
  }
}
