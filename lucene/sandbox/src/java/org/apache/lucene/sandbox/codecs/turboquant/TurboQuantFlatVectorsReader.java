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
package org.apache.lucene.sandbox.codecs.turboquant;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.quantization.BaseQuantizedByteVectorValues;
import org.apache.lucene.util.quantization.QuantizedVectorsReader;
import org.apache.lucene.util.quantization.ScalarQuantizer;

/**
 * Reader for TurboQuant quantized vectors. Reads quantized data from {@code .vetq} and metadata
 * from {@code .vemtq}, delegating raw vector access to the underlying {@link
 * org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat} reader.
 */
public class TurboQuantFlatVectorsReader extends FlatVectorsReader
    implements QuantizedVectorsReader {

  private static final long SHALLOW_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TurboQuantFlatVectorsReader.class);

  private final Map<String, FieldEntry> fields = new HashMap<>();
  private final IndexInput quantizedVectorData;
  private final FlatVectorsReader rawVectorsReader;

  public TurboQuantFlatVectorsReader(
      SegmentReadState state, FlatVectorsReader rawVectorsReader, FlatVectorsScorer scorer)
      throws IOException {
    super(scorer);
    this.rawVectorsReader = rawVectorsReader;

    String metaFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            TurboQuantFlatVectorsFormat.META_EXTENSION);
    try (ChecksumIndexInput meta = state.directory.openChecksumInput(metaFileName)) {
      Throwable priorE = null;
      try {
        CodecUtil.checkIndexHeader(
            meta,
            TurboQuantFlatVectorsFormat.META_CODEC_NAME,
            TurboQuantFlatVectorsFormat.VERSION_START,
            TurboQuantFlatVectorsFormat.VERSION_CURRENT,
            state.segmentInfo.getId(),
            state.segmentSuffix);
        readFields(meta, state.fieldInfos);
      } catch (Throwable exception) {
        priorE = exception;
      } finally {
        CodecUtil.checkFooter(meta, priorE);
      }
    }

    String vectorDataFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            TurboQuantFlatVectorsFormat.VECTOR_DATA_EXTENSION);
    try {
      quantizedVectorData =
          state.directory.openInput(vectorDataFileName, state.context);
      CodecUtil.checkIndexHeader(
          quantizedVectorData,
          TurboQuantFlatVectorsFormat.VECTOR_DATA_CODEC_NAME,
          TurboQuantFlatVectorsFormat.VERSION_START,
          TurboQuantFlatVectorsFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
    } catch (Throwable t) {
      IOUtils.closeWhileSuppressingExceptions(t, this);
      throw t;
    }
  }

  private void readFields(ChecksumIndexInput meta, FieldInfos infos) throws IOException {
    for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) {
      FieldInfo info = infos.fieldInfo(fieldNumber);
      if (info == null) {
        throw new CorruptIndexException("Invalid field number: " + fieldNumber, meta);
      }
      int dimension = meta.readInt();
      int vectorCount = meta.readInt();
      int encodingWire = meta.readInt();
      int simOrdinal = meta.readInt();
      long rotationSeed = meta.readLong();
      long vectorDataOffset = meta.readLong();
      long vectorDataLength = meta.readLong();

      TurboQuantEncoding encoding =
          TurboQuantEncoding.fromWireNumber(encodingWire)
              .orElseThrow(
                  () ->
                      new CorruptIndexException(
                          "Unknown TurboQuant encoding wire number: " + encodingWire, meta));

      VectorSimilarityFunction similarityFunction =
          VectorSimilarityFunction.values()[simOrdinal];

      fields.put(
          info.name,
          new FieldEntry(
              dimension,
              vectorCount,
              encoding,
              similarityFunction,
              rotationSeed,
              vectorDataOffset,
              vectorDataLength));
    }
  }

  @Override
  public FloatVectorValues getFloatVectorValues(String field) throws IOException {
    return rawVectorsReader.getFloatVectorValues(field);
  }

  @Override
  public org.apache.lucene.index.ByteVectorValues getByteVectorValues(String field)
      throws IOException {
    return rawVectorsReader.getByteVectorValues(field);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(String field, float[] target) throws IOException {
    FieldEntry entry = fields.get(field);
    if (entry == null) {
      return null;
    }
    OffHeapTurboQuantVectorValues quantizedValues = getQuantizedValues(field, entry);
    return vectorScorer.getRandomVectorScorer(
        entry.similarityFunction, quantizedValues, target);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(String field, byte[] target) throws IOException {
    return rawVectorsReader.getRandomVectorScorer(field, target);
  }

  @Override
  public BaseQuantizedByteVectorValues getQuantizedVectorValues(String fieldName)
      throws IOException {
    FieldEntry entry = fields.get(fieldName);
    if (entry == null) {
      return null;
    }
    return getQuantizedValues(fieldName, entry);
  }

  @Override
  public ScalarQuantizer getQuantizationState(String fieldName) {
    // TurboQuant doesn't use ScalarQuantizer
    return null;
  }

  private OffHeapTurboQuantVectorValues getQuantizedValues(String field, FieldEntry entry)
      throws IOException {
    HadamardRotation rotation = HadamardRotation.create(entry.dimension, entry.rotationSeed);
    float[] centroids = BetaCodebook.centroids(entry.dimension, entry.encoding.bitsPerCoordinate);
    return new OffHeapTurboQuantVectorValues(
        entry.dimension,
        entry.vectorCount,
        entry.encoding,
        entry.vectorDataOffset,
        quantizedVectorData.clone(),
        centroids,
        rotation);
  }

  @Override
  public void checkIntegrity() throws IOException {
    rawVectorsReader.checkIntegrity();
    CodecUtil.checksumEntireFile(quantizedVectorData);
  }

  @Override
  public long ramBytesUsed() {
    long total = SHALLOW_SIZE;
    total += RamUsageEstimator.sizeOfMap(fields);
    total += rawVectorsReader.ramBytesUsed();
    return total;
  }

  @Override
  public Map<String, Long> getOffHeapByteSize(FieldInfo fieldInfo) {
    Map<String, Long> result = new HashMap<>(rawVectorsReader.getOffHeapByteSize(fieldInfo));
    FieldEntry entry = fields.get(fieldInfo.name);
    if (entry != null) {
      result.put(TurboQuantFlatVectorsFormat.VECTOR_DATA_EXTENSION, entry.vectorDataLength);
    }
    return result;
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(quantizedVectorData, rawVectorsReader);
  }

  /** Per-field metadata read from .vemtq. */
  private record FieldEntry(
      int dimension,
      int vectorCount,
      TurboQuantEncoding encoding,
      VectorSimilarityFunction similarityFunction,
      long rotationSeed,
      long vectorDataOffset,
      long vectorDataLength) {}
}
