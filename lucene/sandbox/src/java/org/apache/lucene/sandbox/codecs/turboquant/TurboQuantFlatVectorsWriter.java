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
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.CloseableRandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;

/**
 * Writer for TurboQuant quantized vectors. Delegates raw vector storage to {@link
 * org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat} and writes quantized data to {@code
 * .vetq} and metadata to {@code .vemtq}.
 */
public class TurboQuantFlatVectorsWriter extends FlatVectorsWriter {

  private static final long SHALLOW_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(TurboQuantFlatVectorsWriter.class);

  private final SegmentWriteState segmentWriteState;
  private final List<FieldWriter> fields = new ArrayList<>();
  private final IndexOutput meta, quantizedVectorData;
  private final TurboQuantEncoding encoding;
  private final Long rotationSeed;
  private final FlatVectorsWriter rawVectorDelegate;
  private boolean finished;

  public TurboQuantFlatVectorsWriter(
      SegmentWriteState state,
      TurboQuantEncoding encoding,
      Long rotationSeed,
      FlatVectorsWriter rawVectorDelegate,
      FlatVectorsScorer scorer)
      throws IOException {
    super(scorer);
    this.encoding = encoding;
    this.rotationSeed = rotationSeed;
    this.segmentWriteState = state;
    this.rawVectorDelegate = rawVectorDelegate;

    String metaFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            TurboQuantFlatVectorsFormat.META_EXTENSION);
    String vectorDataFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            TurboQuantFlatVectorsFormat.VECTOR_DATA_EXTENSION);
    try {
      meta = state.directory.createOutput(metaFileName, state.context);
      quantizedVectorData = state.directory.createOutput(vectorDataFileName, state.context);
      CodecUtil.writeIndexHeader(
          meta,
          TurboQuantFlatVectorsFormat.META_CODEC_NAME,
          TurboQuantFlatVectorsFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      CodecUtil.writeIndexHeader(
          quantizedVectorData,
          TurboQuantFlatVectorsFormat.VECTOR_DATA_CODEC_NAME,
          TurboQuantFlatVectorsFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
    } catch (Throwable t) {
      IOUtils.closeWhileSuppressingExceptions(t, this);
      throw t;
    }
  }

  @Override
  public FlatFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
    FlatFieldVectorsWriter<?> rawFieldWriter = rawVectorDelegate.addField(fieldInfo);
    if (fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32)) {
      @SuppressWarnings("unchecked")
      FieldWriter fieldWriter =
          new FieldWriter(fieldInfo, (FlatFieldVectorsWriter<float[]>) rawFieldWriter);
      fields.add(fieldWriter);
      return fieldWriter;
    }
    return rawFieldWriter;
  }

  @Override
  public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
    rawVectorDelegate.flush(maxDoc, sortMap);
    for (FieldWriter field : fields) {
      int vectorCount = field.flatFieldVectorsWriter.getVectors().size();
      if (vectorCount == 0) {
        continue;
      }
      int d = field.fieldInfo.getVectorDimension();
      long seed = getRotationSeed(field.fieldInfo);
      HadamardRotation rotation = HadamardRotation.create(d, seed);
      float[] boundaries = BetaCodebook.boundaries(d, encoding.bitsPerCoordinate);

      long vectorDataOffset = quantizedVectorData.alignFilePointer(Float.BYTES);

      List<float[]> vectors = field.flatFieldVectorsWriter.getVectors();
      float[] rotated = new float[d];
      byte[] indices = new byte[d];
      byte[] packed = new byte[encoding.getPackedByteLength(d)];

      for (float[] vector : vectors) {
        writeQuantizedVector(vector, d, rotation, boundaries, indices, rotated, packed);
      }

      long vectorDataLength = quantizedVectorData.getFilePointer() - vectorDataOffset;
      writeMeta(field.fieldInfo, vectorDataOffset, vectorDataLength, vectorCount, seed);
      field.finish();
    }
  }

  private void writeQuantizedVector(
      float[] vector,
      int d,
      HadamardRotation rotation,
      float[] boundaries,
      byte[] indices,
      float[] rotated,
      byte[] packed)
      throws IOException {
    float norm = 0;
    for (int i = 0; i < d; i++) norm += vector[i] * vector[i];
    norm = (float) Math.sqrt(norm);

    float[] normalized = new float[d];
    if (norm > 0) {
      for (int i = 0; i < d; i++) normalized[i] = vector[i] / norm;
    }

    rotation.rotate(normalized, rotated);

    for (int i = 0; i < d; i++) {
      indices[i] = (byte) BetaCodebook.quantize(rotated[i], boundaries);
    }

    TurboQuantBitPacker.pack(indices, d, encoding.bitsPerCoordinate, packed);
    quantizedVectorData.writeBytes(packed, packed.length);
    quantizedVectorData.writeInt(Float.floatToIntBits(norm));
  }

  private void writeMeta(
      FieldInfo fieldInfo,
      long vectorDataOffset,
      long vectorDataLength,
      int vectorCount,
      long rotSeed)
      throws IOException {
    meta.writeInt(fieldInfo.number);
    meta.writeInt(fieldInfo.getVectorDimension());
    meta.writeInt(vectorCount);
    meta.writeInt(encoding.getWireNumber());
    meta.writeInt(fieldInfo.getVectorSimilarityFunction().ordinal());
    meta.writeLong(rotSeed);
    meta.writeLong(vectorDataOffset);
    meta.writeLong(vectorDataLength);
  }

  @Override
  public void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
    rawVectorDelegate.mergeOneField(fieldInfo, mergeState);
  }

  @Override
  public CloseableRandomVectorScorerSupplier mergeOneFieldToIndex(
      FieldInfo fieldInfo, MergeState mergeState) throws IOException {
    if (!fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32)) {
      return rawVectorDelegate.mergeOneFieldToIndex(fieldInfo, mergeState);
    }

    rawVectorDelegate.mergeOneField(fieldInfo, mergeState);

    int d = fieldInfo.getVectorDimension();
    long seed = getRotationSeed(fieldInfo);
    HadamardRotation rotation = HadamardRotation.create(d, seed);
    float[] centroids = BetaCodebook.centroids(d, encoding.bitsPerCoordinate);
    float[] boundaries = BetaCodebook.boundaries(d, encoding.bitsPerCoordinate);

    // Write quantized vectors to a temp file
    IndexOutput tempOutput =
        segmentWriteState.directory.createTempOutput(
            quantizedVectorData.getName(), "temp", segmentWriteState.context);
    String tempName = tempOutput.getName();

    int vectorCount = 0;
    float[] rotated = new float[d];
    byte[] indices = new byte[d];
    byte[] packed = new byte[encoding.getPackedByteLength(d)];

    try {
      FloatVectorValues mergedVectors =
          KnnVectorsWriter.MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState);
      KnnVectorValues.DocIndexIterator iter = mergedVectors.iterator();
      while (iter.nextDoc() != KnnVectorValues.DocIndexIterator.NO_MORE_DOCS) {
        float[] vector = mergedVectors.vectorValue(iter.index());
        float norm = 0;
        for (int i = 0; i < d; i++) norm += vector[i] * vector[i];
        norm = (float) Math.sqrt(norm);

        float[] normalized = new float[d];
        if (norm > 0) {
          for (int i = 0; i < d; i++) normalized[i] = vector[i] / norm;
        }

        rotation.rotate(normalized, rotated);
        for (int i = 0; i < d; i++) {
          indices[i] = (byte) BetaCodebook.quantize(rotated[i], boundaries);
        }
        TurboQuantBitPacker.pack(indices, d, encoding.bitsPerCoordinate, packed);
        tempOutput.writeBytes(packed, packed.length);
        tempOutput.writeInt(Float.floatToIntBits(norm));
        vectorCount++;
      }
      CodecUtil.writeFooter(tempOutput);
    } finally {
      IOUtils.close(tempOutput);
    }

    // Copy temp data to the real output
    long vectorDataOffset = quantizedVectorData.alignFilePointer(Float.BYTES);
    IndexInput tempInput =
        segmentWriteState.directory.openInput(tempName, segmentWriteState.context);
    try {
      quantizedVectorData.copyBytes(tempInput, tempInput.length() - CodecUtil.footerLength());
    } catch (Throwable t) {
      IOUtils.closeWhileSuppressingExceptions(t, tempInput);
      throw t;
    }
    long vectorDataLength = quantizedVectorData.getFilePointer() - vectorDataOffset;

    writeMeta(fieldInfo, vectorDataOffset, vectorDataLength, vectorCount, seed);

    // Use the temp file for the scorer (the real .vetq is still open for writing)
    final int finalVectorCount = vectorCount;
    OffHeapTurboQuantVectorValues quantizedValues =
        new OffHeapTurboQuantVectorValues(
            d,
            finalVectorCount,
            encoding,
            0, // temp file starts at 0
            tempInput,
            centroids,
            rotation);

    RandomVectorScorerSupplier scorerSupplier =
        vectorsScorer.getRandomVectorScorerSupplier(
            fieldInfo.getVectorSimilarityFunction(), quantizedValues);

    return new TurboQuantCloseableScorerSupplier(scorerSupplier, () -> {
      IOUtils.close(tempInput);
      segmentWriteState.directory.deleteFile(tempName);
    }, finalVectorCount);
  }

  @Override
  public void finish() throws IOException {
    if (finished) {
      throw new IllegalStateException("already finished");
    }
    finished = true;
    rawVectorDelegate.finish();
    meta.writeInt(-1); // sentinel
    CodecUtil.writeFooter(meta);
    CodecUtil.writeFooter(quantizedVectorData);
  }

  @Override
  public long ramBytesUsed() {
    long total = SHALLOW_RAM_BYTES_USED;
    for (FieldWriter field : fields) {
      total += field.ramBytesUsed();
    }
    total += rawVectorDelegate.ramBytesUsed();
    return total;
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(meta, quantizedVectorData, rawVectorDelegate);
  }

  private long getRotationSeed(FieldInfo fieldInfo) {
    if (rotationSeed != null) {
      return rotationSeed;
    }
    return murmurhash3(fieldInfo.name);
  }

  private static long murmurhash3(String key) {
    byte[] bytes = key.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    long h = 0xcafebabe;
    for (byte b : bytes) {
      h ^= b;
      h *= 0x5bd1e9955bd1e995L;
      h ^= h >>> 47;
    }
    return h;
  }

  /** Per-field writer that wraps the raw delegate. */
  private static class FieldWriter extends FlatFieldVectorsWriter<float[]> {
    final FieldInfo fieldInfo;
    final FlatFieldVectorsWriter<float[]> flatFieldVectorsWriter;
    private boolean isFinished = false;

    FieldWriter(FieldInfo fieldInfo, FlatFieldVectorsWriter<float[]> delegate) {
      this.fieldInfo = fieldInfo;
      this.flatFieldVectorsWriter = delegate;
    }

    @Override
    public void addValue(int docID, float[] vectorValue) throws IOException {
      flatFieldVectorsWriter.addValue(docID, vectorValue);
    }

    @Override
    public float[] copyValue(float[] vectorValue) {
      return flatFieldVectorsWriter.copyValue(vectorValue);
    }

    @Override
    public List<float[]> getVectors() {
      return flatFieldVectorsWriter.getVectors();
    }

    @Override
    public DocsWithFieldSet getDocsWithFieldSet() {
      return flatFieldVectorsWriter.getDocsWithFieldSet();
    }

    @Override
    public void finish() throws IOException {
      if (isFinished) {
        return;
      }
      assert flatFieldVectorsWriter.isFinished();
      isFinished = true;
    }

    @Override
    public boolean isFinished() {
      return isFinished && flatFieldVectorsWriter.isFinished();
    }

    @Override
    public long ramBytesUsed() {
      return flatFieldVectorsWriter.ramBytesUsed()
          + RamUsageEstimator.shallowSizeOfInstance(FieldWriter.class);
    }
  }

  /** Closeable scorer supplier for merge. */
  private static class TurboQuantCloseableScorerSupplier
      implements CloseableRandomVectorScorerSupplier {
    private final RandomVectorScorerSupplier delegate;
    private final java.io.Closeable toClose;
    private final int totalVectorCount;

    TurboQuantCloseableScorerSupplier(
        RandomVectorScorerSupplier delegate, java.io.Closeable toClose, int totalVectorCount) {
      this.delegate = delegate;
      this.toClose = toClose;
      this.totalVectorCount = totalVectorCount;
    }

    @Override
    public org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer scorer() throws IOException {
      return delegate.scorer();
    }

    @Override
    public RandomVectorScorerSupplier copy() throws IOException {
      return delegate.copy();
    }

    @Override
    public int totalVectorCount() {
      return totalVectorCount;
    }

    @Override
    public void close() throws IOException {
      IOUtils.close(toClose);
    }
  }
}
