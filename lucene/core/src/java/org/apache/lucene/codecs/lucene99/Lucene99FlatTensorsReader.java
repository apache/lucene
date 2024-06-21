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

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.hnsw.FlatTensorsScorer;
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
import org.apache.lucene.index.TensorSimilarityFunction;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.ReadAdvice;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;
import org.apache.lucene.util.hnsw.RandomVectorScorer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader.readSimilarityFunction;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader.readVectorEncoding;

/**
 * Reads tensors from the index segments.
 *
 * @lucene.experimental
 */
public final class Lucene99FlatTensorsReader extends FlatVectorsReader {

  private static final long SHALLOW_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(Lucene99FlatTensorsFormat.class);

  private final Map<String, FieldEntry> fields = new HashMap<>();
  private final IndexInput tensorData;
  private final FlatTensorsScorer tensorScorer;

  public Lucene99FlatTensorsReader(SegmentReadState state, FlatTensorsScorer scorer)
      throws IOException {
    super(null);
    tensorScorer = scorer;
    int versionMeta = readMetadata(state);
    boolean success = false;
    try {
      tensorData =
          openDataInput(
              state,
              versionMeta,
              Lucene99FlatTensorsFormat.VECTOR_DATA_EXTENSION,
              Lucene99FlatTensorsFormat.VECTOR_DATA_CODEC_NAME,
              // Flat formats are used to randomly access vectors from their node ID that is stored
              // in the HNSW graph.
              state.context.withReadAdvice(ReadAdvice.RANDOM));
      success = true;
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }

  @Override
  public FlatVectorsScorer getFlatVectorScorer() {
    throw new UnsupportedOperationException("Tensor reader does not work on vector scorer");
  }

  private int readMetadata(SegmentReadState state) throws IOException {
    String metaFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, Lucene99FlatTensorsFormat.META_EXTENSION);
    int versionMeta = -1;
    try (ChecksumIndexInput meta = state.directory.openChecksumInput(metaFileName)) {
      Throwable priorE = null;
      try {
        versionMeta =
            CodecUtil.checkIndexHeader(
                meta,
                Lucene99FlatTensorsFormat.META_CODEC_NAME,
                Lucene99FlatTensorsFormat.VERSION_START,
                Lucene99FlatTensorsFormat.VERSION_CURRENT,
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
              Lucene99FlatTensorsFormat.VERSION_START,
              Lucene99FlatTensorsFormat.VERSION_CURRENT,
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
      FieldEntry fieldEntry = FieldEntry.create(meta, info);
      fields.put(info.name, fieldEntry);
    }
  }

  @Override
  public long ramBytesUsed() {
    return Lucene99FlatTensorsReader.SHALLOW_SIZE
        + RamUsageEstimator.sizeOfMap(
            fields, RamUsageEstimator.shallowSizeOfInstance(FieldEntry.class));
  }

  @Override
  public void checkIntegrity() throws IOException {
    CodecUtil.checksumEntireFile(tensorData);
  }

  @Override
  public FloatVectorValues getFloatVectorValues(String field) throws IOException {
    FieldEntry fieldEntry = fields.get(field);
    if (fieldEntry.tensorEncoding != VectorEncoding.FLOAT32) {
      throw new IllegalArgumentException(
          "field=\""
              + field
              + "\" is encoded as: "
              + fieldEntry.tensorEncoding
              + " expected: "
              + VectorEncoding.FLOAT32);
    }
    return OffHeapFloatTensorValues.load(
        fieldEntry.similarityFunction,
        tensorScorer,
        fieldEntry.ordToDoc,
        fieldEntry.dataOffsets,
        fieldEntry.tensorEncoding,
        fieldEntry.dimension,
        fieldEntry.tensorDataOffset,
        fieldEntry.tensorDataLength,
        tensorData);
  }

  @Override
  public ByteVectorValues getByteVectorValues(String field) throws IOException {
    FieldEntry fieldEntry = fields.get(field);
    if (fieldEntry.tensorEncoding != VectorEncoding.BYTE) {
      throw new IllegalArgumentException(
          "field=\""
              + field
              + "\" is encoded as: "
              + fieldEntry.tensorEncoding
              + " expected: "
              + VectorEncoding.BYTE);
    }
    return OffHeapByteTensorValues.load(
        fieldEntry.similarityFunction,
        tensorScorer,
        fieldEntry.ordToDoc,
        fieldEntry.dataOffsets,
        fieldEntry.tensorEncoding,
        fieldEntry.dimension,
        fieldEntry.tensorDataOffset,
        fieldEntry.tensorDataLength,
        tensorData);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(String field, float[] target) throws IOException {
    FieldEntry fieldEntry = fields.get(field);
    if (fieldEntry == null || fieldEntry.tensorEncoding != VectorEncoding.FLOAT32) {
      return null;
    }
    // target tensor should consist of vectors with the same dimension as field
    if (target.length % fieldEntry.dimension != 0) {
      return null;
    }
    return tensorScorer.getRandomTensorScorer(
        fieldEntry.similarityFunction,
        (RandomAccessVectorValues.Floats) getFloatVectorValues(field),
        target);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(String field, byte[] target) throws IOException {
    FieldEntry fieldEntry = fields.get(field);
    if (fieldEntry == null || fieldEntry.tensorEncoding != VectorEncoding.BYTE) {
      return null;
    }
    // target tensor should consist of vectors with the same dimension as field
    if (target.length % fieldEntry.dimension != 0) {
      return null;
    }
    return tensorScorer.getRandomTensorScorer(
        fieldEntry.similarityFunction,
        (RandomAccessVectorValues.Bytes) getByteVectorValues(field),
        target);
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(tensorData);
  }

  private record FieldEntry(
      FieldInfo info,
      VectorEncoding tensorEncoding,
      TensorSimilarityFunction similarityFunction,
      long tensorDataOffset,
      long tensorDataLength,
      int dimension,
      int size,
      OrdToDocDISIReaderConfiguration ordToDoc,
      TensorDataOffsetsReaderConfiguration dataOffsets) {

    FieldEntry {
      if (similarityFunction != info.getTensorSimilarityFunction()) {
        throw new IllegalStateException(
            "Inconsistent tensor similarity function for field=\""
                + info.name
                + "\"; "
                + similarityFunction
                + " != "
                + info.getTensorSimilarityFunction());
      }
      int infoDimension = info.getTensorDimension();
      if (infoDimension != dimension) {
        throw new IllegalStateException(
            "Inconsistent tensor dimension for field=\""
                + info.name
                + "\"; "
                + infoDimension
                + " != "
                + dimension);
      }
    }

    static FieldEntry create(IndexInput input, FieldInfo info) throws IOException {
      final VectorEncoding tensorEncoding = readVectorEncoding(input);
      final TensorSimilarityFunction similarityFunction = readSimilarityFunction(input);
      final var tensorDataOffset = input.readVLong();
      final var tensorDataLength = input.readVLong();
      final var dimension = input.readVInt();
      final var size = input.readInt();
      final var ordToDoc = OrdToDocDISIReaderConfiguration.fromStoredMeta(input, size);
      final var dataOffsets = TensorDataOffsetsReaderConfiguration.fromStoredMeta(input);
      return new FieldEntry(
          info,
          tensorEncoding,
          similarityFunction,
          tensorDataOffset,
          tensorDataLength,
          dimension,
          size,
          ordToDoc,
          dataOffsets);
    }
  }

  public static TensorSimilarityFunction readSimilarityFunction(DataInput input)
      throws IOException {
    int fnId = input.readInt();
    if (fnId < 0 || fnId >= TensorSimilarityFunction.values().length) {
      throw new CorruptIndexException("Invalid tensor similarity function id: " + fnId, input);
    }
    return TensorSimilarityFunction.values()[fnId];
  }
}
