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

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader.readSimilarityFunction;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader.readVectorEncoding;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
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
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.ReadAdvice;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.apache.lucene.util.quantization.QuantizedVectorsReader;
import org.apache.lucene.util.quantization.ScalarQuantizer;

/**
 * Reads Scalar Quantized vectors from the index segments along with index data structures.
 *
 * @lucene.experimental
 */
public final class Lucene99ScalarQuantizedVectorsReader extends FlatVectorsReader
    implements QuantizedVectorsReader {

  private static final long SHALLOW_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(Lucene99ScalarQuantizedVectorsReader.class);

  private final Map<String, FieldEntry> fields = new HashMap<>();
  private final IndexInput quantizedVectorData;
  private final FlatVectorsReader rawVectorsReader;

  public Lucene99ScalarQuantizedVectorsReader(
      SegmentReadState state, FlatVectorsReader rawVectorsReader, FlatVectorsScorer scorer)
      throws IOException {
    super(scorer);
    this.rawVectorsReader = rawVectorsReader;
    int versionMeta = -1;
    String metaFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            Lucene99ScalarQuantizedVectorsFormat.META_EXTENSION);
    boolean success = false;
    try (ChecksumIndexInput meta = state.directory.openChecksumInput(metaFileName)) {
      Throwable priorE = null;
      try {
        versionMeta =
            CodecUtil.checkIndexHeader(
                meta,
                Lucene99ScalarQuantizedVectorsFormat.META_CODEC_NAME,
                Lucene99ScalarQuantizedVectorsFormat.VERSION_START,
                Lucene99ScalarQuantizedVectorsFormat.VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix);
        readFields(meta, versionMeta, state.fieldInfos);
      } catch (Throwable exception) {
        priorE = exception;
      } finally {
        CodecUtil.checkFooter(meta, priorE);
      }
      quantizedVectorData =
          openDataInput(
              state,
              versionMeta,
              Lucene99ScalarQuantizedVectorsFormat.VECTOR_DATA_EXTENSION,
              Lucene99ScalarQuantizedVectorsFormat.VECTOR_DATA_CODEC_NAME,
              // Quantized vectors are accessed randomly from their node ID stored in the HNSW
              // graph.
              state.context.withReadAdvice(ReadAdvice.RANDOM));
      success = true;
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }

  private void readFields(ChecksumIndexInput meta, int versionMeta, FieldInfos infos)
      throws IOException {
    for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) {
      FieldInfo info = infos.fieldInfo(fieldNumber);
      if (info == null) {
        throw new CorruptIndexException("Invalid field number: " + fieldNumber, meta);
      }
      FieldEntry fieldEntry = readField(meta, versionMeta, info);
      validateFieldEntry(info, fieldEntry);
      fields.put(info.name, fieldEntry);
    }
  }

  static void validateFieldEntry(FieldInfo info, FieldEntry fieldEntry) {
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

    final long quantizedVectorBytes;
    if (fieldEntry.bits <= 4 && fieldEntry.compress) {
      quantizedVectorBytes = ((dimension + 1) >> 1) + Float.BYTES;
    } else {
      // int8 quantized and calculated stored offset.
      quantizedVectorBytes = dimension + Float.BYTES;
    }
    long numQuantizedVectorBytes = Math.multiplyExact(quantizedVectorBytes, fieldEntry.size);
    if (numQuantizedVectorBytes != fieldEntry.vectorDataLength) {
      throw new IllegalStateException(
          "Quantized vector data length "
              + fieldEntry.vectorDataLength
              + " not matching size="
              + fieldEntry.size
              + " * (dim="
              + dimension
              + " + 4)"
              + " = "
              + numQuantizedVectorBytes);
    }
  }

  @Override
  public void checkIntegrity() throws IOException {
    rawVectorsReader.checkIntegrity();
    CodecUtil.checksumEntireFile(quantizedVectorData);
  }

  @Override
  public FloatVectorValues getFloatVectorValues(String field) throws IOException {
    return rawVectorsReader.getFloatVectorValues(field);
  }

  @Override
  public ByteVectorValues getByteVectorValues(String field) throws IOException {
    return rawVectorsReader.getByteVectorValues(field);
  }

  private static IndexInput openDataInput(
      SegmentReadState state,
      int versionMeta,
      String fileExtension,
      String codecName,
      IOContext context)
      throws IOException {
    String fileName =
        IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, fileExtension);
    IndexInput in = state.directory.openInput(fileName, context);
    boolean success = false;
    try {
      int versionVectorData =
          CodecUtil.checkIndexHeader(
              in,
              codecName,
              Lucene99ScalarQuantizedVectorsFormat.VERSION_START,
              Lucene99ScalarQuantizedVectorsFormat.VERSION_CURRENT,
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

  @Override
  public RandomVectorScorer getRandomVectorScorer(String field, float[] target) throws IOException {
    FieldEntry fieldEntry = fields.get(field);
    if (fieldEntry == null || fieldEntry.vectorEncoding != VectorEncoding.FLOAT32) {
      return null;
    }
    if (fieldEntry.scalarQuantizer == null) {
      return rawVectorsReader.getRandomVectorScorer(field, target);
    }
    OffHeapQuantizedByteVectorValues vectorValues =
        OffHeapQuantizedByteVectorValues.load(
            fieldEntry.ordToDoc,
            fieldEntry.dimension,
            fieldEntry.size,
            fieldEntry.scalarQuantizer,
            fieldEntry.compress,
            fieldEntry.vectorDataOffset,
            fieldEntry.vectorDataLength,
            quantizedVectorData);
    return vectorScorer.getRandomVectorScorer(fieldEntry.similarityFunction, vectorValues, target);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(String field, byte[] target) throws IOException {
    return rawVectorsReader.getRandomVectorScorer(field, target);
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(quantizedVectorData, rawVectorsReader);
  }

  @Override
  public long ramBytesUsed() {
    long size = SHALLOW_SIZE;
    size +=
        RamUsageEstimator.sizeOfMap(
            fields, RamUsageEstimator.shallowSizeOfInstance(FieldEntry.class));
    size += rawVectorsReader.ramBytesUsed();
    return size;
  }

  private FieldEntry readField(IndexInput input, int versionMeta, FieldInfo info)
      throws IOException {
    VectorEncoding vectorEncoding = readVectorEncoding(input);
    VectorSimilarityFunction similarityFunction = readSimilarityFunction(input);
    if (similarityFunction != info.getVectorSimilarityFunction()) {
      throw new IllegalStateException(
          "Inconsistent vector similarity function for field=\""
              + info.name
              + "\"; "
              + similarityFunction
              + " != "
              + info.getVectorSimilarityFunction());
    }
    return FieldEntry.create(
        input, versionMeta, vectorEncoding, info.getVectorSimilarityFunction());
  }

  @Override
  public QuantizedByteVectorValues getQuantizedVectorValues(String fieldName) throws IOException {
    FieldEntry fieldEntry = fields.get(fieldName);
    if (fieldEntry == null || fieldEntry.vectorEncoding != VectorEncoding.FLOAT32) {
      return null;
    }
    return OffHeapQuantizedByteVectorValues.load(
        fieldEntry.ordToDoc,
        fieldEntry.dimension,
        fieldEntry.size,
        fieldEntry.scalarQuantizer,
        fieldEntry.compress,
        fieldEntry.vectorDataOffset,
        fieldEntry.vectorDataLength,
        quantizedVectorData);
  }

  @Override
  public ScalarQuantizer getQuantizationState(String fieldName) {
    FieldEntry fieldEntry = fields.get(fieldName);
    if (fieldEntry == null || fieldEntry.vectorEncoding != VectorEncoding.FLOAT32) {
      return null;
    }
    return fieldEntry.scalarQuantizer;
  }

  private record FieldEntry(
      VectorSimilarityFunction similarityFunction,
      VectorEncoding vectorEncoding,
      int dimension,
      long vectorDataOffset,
      long vectorDataLength,
      ScalarQuantizer scalarQuantizer,
      int size,
      byte bits,
      boolean compress,
      OrdToDocDISIReaderConfiguration ordToDoc)
      implements Accountable {
    private static final long SHALLOW_SIZE =
        RamUsageEstimator.shallowSizeOfInstance(FieldEntry.class);

    static FieldEntry create(
        IndexInput input,
        int versionMeta,
        VectorEncoding vectorEncoding,
        VectorSimilarityFunction similarityFunction)
        throws IOException {
      final var vectorDataOffset = input.readVLong();
      final var vectorDataLength = input.readVLong();
      final var dimension = input.readVInt();
      final var size = input.readInt();
      final ScalarQuantizer scalarQuantizer;
      final byte bits;
      final boolean compress;
      if (size > 0) {
        if (versionMeta < Lucene99ScalarQuantizedVectorsFormat.VERSION_ADD_BITS) {
          int floatBits = input.readInt(); // confidenceInterval, unused
          if (floatBits == -1) {
            throw new CorruptIndexException(
                "Missing confidence interval for scalar quantizer", input);
          }
          bits = (byte) 7;
          compress = false;
          float minQuantile = Float.intBitsToFloat(input.readInt());
          float maxQuantile = Float.intBitsToFloat(input.readInt());
          scalarQuantizer = new ScalarQuantizer(minQuantile, maxQuantile, (byte) 7);
        } else {
          input.readInt(); // confidenceInterval, unused
          bits = input.readByte();
          compress = input.readByte() == 1;
          float minQuantile = Float.intBitsToFloat(input.readInt());
          float maxQuantile = Float.intBitsToFloat(input.readInt());
          scalarQuantizer = new ScalarQuantizer(minQuantile, maxQuantile, bits);
        }
      } else {
        scalarQuantizer = null;
        bits = (byte) 7;
        compress = false;
      }
      final var ordToDoc = OrdToDocDISIReaderConfiguration.fromStoredMeta(input, size);
      return new FieldEntry(
          similarityFunction,
          vectorEncoding,
          dimension,
          vectorDataOffset,
          vectorDataLength,
          scalarQuantizer,
          size,
          bits,
          compress,
          ordToDoc);
    }

    @Override
    public long ramBytesUsed() {
      return SHALLOW_SIZE + RamUsageEstimator.sizeOf(ordToDoc);
    }
  }
}
