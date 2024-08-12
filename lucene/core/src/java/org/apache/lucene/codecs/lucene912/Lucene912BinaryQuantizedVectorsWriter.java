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
package org.apache.lucene.codecs.lucene912;

import static org.apache.lucene.util.RamUsageEstimator.shallowSizeOfInstance;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.CloseableRandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.BinaryQuantizer;

public class Lucene912BinaryQuantizedVectorsWriter extends FlatVectorsWriter {
  private static final long SHALLOW_RAM_BYTES_USED =
      shallowSizeOfInstance(Lucene912BinaryQuantizedVectorsWriter.class);

  private final SegmentWriteState segmentWriteState;
  private final List<FieldWriter> fields = new ArrayList<>();
  private final IndexOutput meta, binarizedVectorData;
  private final FlatVectorsWriter rawVectorDelegate;
  private boolean finished;

  /**
   * Sole constructor
   *
   * @param vectorsScorer the scorer to use for scoring vectors
   */
  protected Lucene912BinaryQuantizedVectorsWriter(
      BinaryFlatVectorsScorer vectorsScorer,
      FlatVectorsWriter rawVectorDelegate,
      SegmentWriteState state)
      throws IOException {
    super(vectorsScorer);
    this.segmentWriteState = state;
    String metaFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            Lucene912BinaryQuantizedVectorsFormat.META_EXTENSION);

    String binarizedVectorDataFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            Lucene912BinaryQuantizedVectorsFormat.VECTOR_DATA_EXTENSION);
    this.rawVectorDelegate = rawVectorDelegate;
    boolean success = false;
    try {
      meta = state.directory.createOutput(metaFileName, state.context);
      binarizedVectorData =
          state.directory.createOutput(binarizedVectorDataFileName, state.context);

      CodecUtil.writeIndexHeader(
          meta,
          Lucene912BinaryQuantizedVectorsFormat.META_CODEC_NAME,
          Lucene912BinaryQuantizedVectorsFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      CodecUtil.writeIndexHeader(
          binarizedVectorData,
          Lucene912BinaryQuantizedVectorsFormat.VECTOR_DATA_CODEC_NAME,
          Lucene912BinaryQuantizedVectorsFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      success = true;
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }

  @Override
  public FlatFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
    FlatFieldVectorsWriter<?> rawVectorDelegate = this.rawVectorDelegate.addField(fieldInfo);
    if (fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32)) {
      @SuppressWarnings("unchecked")
      FieldWriter fieldWriter =
          new FieldWriter(
              fieldInfo,
              segmentWriteState.infoStream,
              (FlatFieldVectorsWriter<float[]>) rawVectorDelegate);
      fields.add(fieldWriter);
      return fieldWriter;
    }
    return rawVectorDelegate;
  }

  @Override
  public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
    rawVectorDelegate.flush(maxDoc, sortMap);
    for (FieldWriter field : fields) {
      BinaryQuantizer quantizer = field.createQuantizer();
      if (sortMap == null) {
        writeField(field, maxDoc, quantizer);
      } else {
        writeSortingField(field, maxDoc, sortMap, quantizer);
      }
      field.finish();
    }
  }

  private void writeField(FieldWriter fieldData, int maxDoc, BinaryQuantizer quantizer)
      throws IOException {
    // write vector values
    long vectorDataOffset = binarizedVectorData.alignFilePointer(Float.BYTES);
    writeBinarizedVectors(fieldData, quantizer);
    long vectorDataLength = binarizedVectorData.getFilePointer() - vectorDataOffset;

    writeMeta(
        fieldData.fieldInfo,
        maxDoc,
        vectorDataOffset,
        vectorDataLength,
        fieldData.getDocsWithFieldSet());
  }

  private void writeBinarizedVectors(FieldWriter fieldData, BinaryQuantizer scalarQuantizer)
      throws IOException {}

  private void writeSortingField(
      FieldWriter fieldData, int maxDoc, Sorter.DocMap sortMap, BinaryQuantizer scalarQuantizer)
      throws IOException {
    final int[] ordMap =
        new int[fieldData.getDocsWithFieldSet().cardinality()]; // new ord to old ord

    DocsWithFieldSet newDocsWithField = new DocsWithFieldSet();
    mapOldOrdToNewOrd(fieldData.getDocsWithFieldSet(), sortMap, null, ordMap, newDocsWithField);

    // write vector values
    long vectorDataOffset = binarizedVectorData.alignFilePointer(Float.BYTES);
    writeSortedBinarizedVectors(fieldData, ordMap, scalarQuantizer);
    long quantizedVectorLength = binarizedVectorData.getFilePointer() - vectorDataOffset;
    writeMeta(
        fieldData.fieldInfo, maxDoc, vectorDataOffset, quantizedVectorLength, newDocsWithField);
  }

  private void writeSortedBinarizedVectors(
      FieldWriter fieldData, int[] ordMap, BinaryQuantizer scalarQuantizer) throws IOException {}

  private void writeMeta(
      FieldInfo field,
      int maxDoc,
      long vectorDataOffset,
      long vectorDataLength,
      DocsWithFieldSet docsWithField)
      throws IOException {}

  @Override
  public void finish() throws IOException {
    if (finished) {
      throw new IllegalStateException("already finished");
    }
    finished = true;
    rawVectorDelegate.finish();
    if (meta != null) {
      // write end of fields marker
      meta.writeInt(-1);
      CodecUtil.writeFooter(meta);
    }
    if (binarizedVectorData != null) {
      CodecUtil.writeFooter(binarizedVectorData);
    }
  }

  @Override
  public CloseableRandomVectorScorerSupplier mergeOneFieldToIndex(
      FieldInfo fieldInfo, MergeState mergeState) throws IOException {
    return null;
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(meta, binarizedVectorData, rawVectorDelegate);
  }

  @Override
  public long ramBytesUsed() {
    long total = SHALLOW_RAM_BYTES_USED;
    for (FieldWriter field : fields) {
      // the field tracks the delegate field usage
      total += field.ramBytesUsed();
    }
    return total;
  }

  static class FieldWriter extends FlatFieldVectorsWriter<float[]> {
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(FieldWriter.class);
    private final FieldInfo fieldInfo;
    private final InfoStream infoStream;
    private boolean finished;
    private final FlatFieldVectorsWriter<float[]> flatFieldVectorsWriter;
    private final float[] dimensionSums;

    FieldWriter(
        FieldInfo fieldInfo,
        InfoStream infoStream,
        FlatFieldVectorsWriter<float[]> flatFieldVectorsWriter) {
      this.fieldInfo = fieldInfo;
      this.infoStream = infoStream;
      this.flatFieldVectorsWriter = flatFieldVectorsWriter;
      this.dimensionSums = new float[fieldInfo.getVectorDimension()];
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
      if (finished) {
        return;
      }
      assert flatFieldVectorsWriter.isFinished();
      if (flatFieldVectorsWriter.getVectors().size() > 0) {
        for (int i = 0; i < dimensionSums.length; i++) {
          dimensionSums[i] /= flatFieldVectorsWriter.getVectors().size();
        }
      }
      finished = true;
    }

    @Override
    public boolean isFinished() {
      return finished && flatFieldVectorsWriter.isFinished();
    }

    @Override
    public void addValue(int docID, float[] vectorValue) throws IOException {
      flatFieldVectorsWriter.addValue(docID, vectorValue);
      for (int i = 0; i < vectorValue.length; i++) {
        dimensionSums[i] += vectorValue[i];
      }
    }

    @Override
    public float[] copyValue(float[] vectorValue) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long ramBytesUsed() {
      long size = SHALLOW_SIZE;
      size += flatFieldVectorsWriter.ramBytesUsed();
      size += RamUsageEstimator.sizeOf(dimensionSums);
      return size;
    }

    BinaryQuantizer createQuantizer() throws IOException {
      assert isFinished();
      if (flatFieldVectorsWriter.getVectors().size() == 0) {
        return new BinaryQuantizer(new float[0]);
      }
      return new BinaryQuantizer(dimensionSums);
    }
  }
}
