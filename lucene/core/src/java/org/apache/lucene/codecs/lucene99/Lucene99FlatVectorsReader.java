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

package org.apache.lucene.codecs.lucene99;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FlatVectorsReader;
import org.apache.lucene.codecs.lucene95.OffHeapByteVectorValues;
import org.apache.lucene.codecs.lucene95.OffHeapFloatVectorValues;
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.RandomVectorScorer;

/**
 * Reads vectors from the index segments.
 *
 * @lucene.experimental
 */
public final class Lucene99FlatVectorsReader extends FlatVectorsReader {

  private static final long SHALLOW_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(Lucene99FlatVectorsFormat.class);

  private final Map<String, FieldEntry> fields = new HashMap<>();
  private final IndexInput vectorData;

  Lucene99FlatVectorsReader(SegmentReadState state) throws IOException {
    int versionMeta = readMetadata(state);
    boolean success = false;
    try {
      vectorData =
          openDataInput(
              state,
              versionMeta,
              Lucene99FlatVectorsFormat.VECTOR_DATA_EXTENSION,
              Lucene99FlatVectorsFormat.VECTOR_DATA_CODEC_NAME);
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
            state.segmentInfo.name, state.segmentSuffix, Lucene99FlatVectorsFormat.META_EXTENSION);
    int versionMeta = -1;
    try (ChecksumIndexInput meta = state.directory.openChecksumInput(metaFileName, state.context)) {
      Throwable priorE = null;
      try {
        versionMeta =
            CodecUtil.checkIndexHeader(
                meta,
                Lucene99FlatVectorsFormat.META_CODEC_NAME,
                Lucene99FlatVectorsFormat.VERSION_START,
                Lucene99FlatVectorsFormat.VERSION_CURRENT,
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
              Lucene99FlatVectorsFormat.VERSION_START,
              Lucene99FlatVectorsFormat.VERSION_CURRENT,
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

    final int byteSize;
    switch (info.getVectorEncoding()) {
      case BYTE:
        byteSize = Byte.BYTES;
        break;
      case FLOAT32:
        byteSize = Float.BYTES;
        break;
      default:
        throw new IllegalArgumentException(
            "Unsupported vector encoding: " + info.getVectorEncoding());
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
    return Lucene99FlatVectorsReader.SHALLOW_SIZE
        + RamUsageEstimator.sizeOfMap(
            fields, RamUsageEstimator.shallowSizeOfInstance(FieldEntry.class));
  }

  @Override
  public void checkIntegrity() throws IOException {
    CodecUtil.checksumEntireFile(vectorData);
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
    return OffHeapFloatVectorValues.load(
        fieldEntry.ordToDoc,
        fieldEntry.vectorEncoding,
        fieldEntry.dimension,
        fieldEntry.vectorDataOffset,
        fieldEntry.vectorDataLength,
        vectorData);
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
    return OffHeapByteVectorValues.load(
        fieldEntry.ordToDoc,
        fieldEntry.vectorEncoding,
        fieldEntry.dimension,
        fieldEntry.vectorDataOffset,
        fieldEntry.vectorDataLength,
        vectorData);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(String field, float[] target) throws IOException {
    FieldEntry fieldEntry = fields.get(field);
    if (fieldEntry == null || fieldEntry.vectorEncoding != VectorEncoding.FLOAT32) {
      return null;
    }
    return RandomVectorScorer.createFloats(
        OffHeapFloatVectorValues.load(
            fieldEntry.ordToDoc,
            fieldEntry.vectorEncoding,
            fieldEntry.dimension,
            fieldEntry.vectorDataOffset,
            fieldEntry.vectorDataLength,
            vectorData),
        fieldEntry.similarityFunction,
        target);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(String field, byte[] target) throws IOException {
    FieldEntry fieldEntry = fields.get(field);
    if (fieldEntry == null || fieldEntry.vectorEncoding != VectorEncoding.BYTE) {
      return null;
    }
    return RandomVectorScorer.createBytes(
        OffHeapByteVectorValues.load(
            fieldEntry.ordToDoc,
            fieldEntry.vectorEncoding,
            fieldEntry.dimension,
            fieldEntry.vectorDataOffset,
            fieldEntry.vectorDataLength,
            vectorData),
        fieldEntry.similarityFunction,
        target);
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(vectorData);
  }

  private static class FieldEntry implements Accountable {
    private static final long SHALLOW_SIZE =
        RamUsageEstimator.shallowSizeOfInstance(FieldEntry.class);
    final VectorSimilarityFunction similarityFunction;
    final VectorEncoding vectorEncoding;
    final int dimension;
    final long vectorDataOffset;
    final long vectorDataLength;
    final int size;
    final OrdToDocDISIReaderConfiguration ordToDoc;

    FieldEntry(
        IndexInput input,
        VectorEncoding vectorEncoding,
        VectorSimilarityFunction similarityFunction)
        throws IOException {
      this.similarityFunction = similarityFunction;
      this.vectorEncoding = vectorEncoding;
      vectorDataOffset = input.readVLong();
      vectorDataLength = input.readVLong();
      dimension = input.readVInt();
      size = input.readInt();
      ordToDoc = OrdToDocDISIReaderConfiguration.fromStoredMeta(input, size);
    }

    @Override
    public long ramBytesUsed() {
      return SHALLOW_SIZE + RamUsageEstimator.sizeOf(ordToDoc);
    }
  }
}
