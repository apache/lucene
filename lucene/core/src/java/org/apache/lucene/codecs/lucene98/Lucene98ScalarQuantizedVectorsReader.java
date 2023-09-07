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

package org.apache.lucene.codecs.lucene98;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.codecs.CodecUtil;
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
import org.apache.lucene.util.ScalarQuantizer;
import org.apache.lucene.util.packed.DirectMonotonicReader;

/**
 * Reads Scalar Quantized vectors from the index segments along with index data structures.
 *
 * @lucene.experimental
 */
public final class Lucene98ScalarQuantizedVectorsReader implements Closeable, Accountable {

  private static final long SHALLOW_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(Lucene98ScalarQuantizedVectorsFormat.class);

  private final Map<String, FieldEntry> fields = new HashMap<>();
  private final IndexInput quantizedVectorData;

  Lucene98ScalarQuantizedVectorsReader(SegmentReadState state) throws IOException {
    int versionMeta = readMetadata(state);
    boolean success = false;
    try {
      quantizedVectorData =
          openDataInput(
              state,
              versionMeta,
              Lucene98ScalarQuantizedVectorsFormat.QUANTIZED_VECTOR_DATA_EXTENSION,
              Lucene98ScalarQuantizedVectorsFormat.QUANTIZED_VECTOR_DATA_CODEC_NAME);
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
            state.segmentInfo.name,
            state.segmentSuffix,
            Lucene98ScalarQuantizedVectorsFormat.QUANTIZED_VECTOR_META_EXTENSION);
    int versionMeta = -1;
    try (ChecksumIndexInput meta = state.directory.openChecksumInput(metaFileName)) {
      Throwable priorE = null;
      try {
        versionMeta =
            CodecUtil.checkIndexHeader(
                meta,
                Lucene98ScalarQuantizedVectorsFormat.META_CODEC_NAME,
                Lucene98ScalarQuantizedVectorsFormat.VERSION_START,
                Lucene98ScalarQuantizedVectorsFormat.VERSION_CURRENT,
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
              Lucene98ScalarQuantizedVectorsFormat.VERSION_START,
              Lucene98ScalarQuantizedVectorsFormat.VERSION_CURRENT,
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

    // int8 quantized and calculated stored offset.
    long quantizedVectorBytes = dimension + Float.BYTES;
    long numQuantizedVectorBytes = Math.multiplyExact(quantizedVectorBytes, fieldEntry.size);
    if (numQuantizedVectorBytes != fieldEntry.quantizedVectorDataLength) {
      throw new IllegalStateException(
          "Quantized vector data length "
              + fieldEntry.quantizedVectorDataLength
              + " not matching size="
              + fieldEntry.size
              + " * dim="
              + dimension
              + " * byteSize="
              + quantizedVectorBytes
              + " = "
              + numQuantizedVectorBytes);
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
    return Lucene98ScalarQuantizedVectorsReader.SHALLOW_SIZE
        + RamUsageEstimator.sizeOfMap(
            fields, RamUsageEstimator.shallowSizeOfInstance(FieldEntry.class));
  }

  public void checkIntegrity() throws IOException {
    CodecUtil.checksumEntireFile(quantizedVectorData);
  }

  public FloatVectorValues getFloatVectorValues(String field) throws IOException {
    FieldEntry fieldEntry = fields.get(field);
    OffHeapQuantizedByteVectorValues vectorValues =
        OffHeapQuantizedByteVectorValues.load(fieldEntry, quantizedVectorData);
    final ScalarQuantizer scalarQuantizer = fieldEntry.scalarQuantizer;
    // TODO transform byte quantized back into float[] as iterated
    return null;
  }

  public QuantizedByteVectorValues getQuantizedVectorValues(String field) throws IOException {
    FieldEntry fieldEntry = fields.get(field);

    if (fieldEntry.size() == 0 || fieldEntry.vectorEncoding != VectorEncoding.FLOAT32) {
      return null;
    }

    return OffHeapQuantizedByteVectorValues.load(fieldEntry, quantizedVectorData);
  }

  public VectorScorer getQuantizedVectorScorer(String field, float[] target) throws IOException {
    RandomAccessQuantizedByteVectorValues values =
        (OffHeapQuantizedByteVectorValues) getQuantizedVectorValues(field);
    FieldEntry fieldEntry = fields.get(field);
    if (values == null || fieldEntry == null) {
      return null;
    }
    return QuantizedVectorScorer.fromFieldEntry(fieldEntry, values, target);
  }

  public float[] getQuantiles(String field) {
    FieldEntry fieldEntry = fields.get(field);
    return new float[] {fieldEntry.lowerQuantile, fieldEntry.upperQuantile};
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(quantizedVectorData);
  }

  static class FieldEntry implements Accountable {
    private static final long SHALLOW_SIZE =
        RamUsageEstimator.shallowSizeOfInstance(FieldEntry.class);
    final VectorSimilarityFunction similarityFunction;
    final VectorEncoding vectorEncoding;
    final long quantizedVectorDataOffset;
    final long quantizedVectorDataLength;
    final int dimension;
    final int size;

    // Every field has a calculated quantile for quantization
    final float lowerQuantile;
    final float upperQuantile;
    final ScalarQuantizer scalarQuantizer;

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
      quantizedVectorDataOffset = input.readVLong();
      quantizedVectorDataLength = input.readVLong();
      dimension = input.readVInt();
      lowerQuantile = Float.intBitsToFloat(input.readInt());
      upperQuantile = Float.intBitsToFloat(input.readInt());
      scalarQuantizer = new ScalarQuantizer(new float[] {lowerQuantile, upperQuantile});
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
    }

    int size() {
      return size;
    }

    @Override
    public long ramBytesUsed() {
      return SHALLOW_SIZE + RamUsageEstimator.sizeOf(meta);
    }
  }
}
