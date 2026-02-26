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
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.CloseableRandomVectorScorerSupplier;

/**
 * Writes empty vector files that are format-compatible with {@link Lucene99FlatVectorsFormat} but
 * contain no actual vector data. Used as placeholders when full-precision vectors are dropped after
 * quantization.
 *
 * <p>This writer creates files with valid headers and metadata but with vector count set to zero,
 * allowing readers to handle them without errors while consuming minimal disk space.
 *
 * @lucene.experimental
 */
public class Lucene99EmptyFlatVectorWriter extends FlatVectorsWriter {

  private static final long SHALLOW_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(Lucene99EmptyFlatVectorWriter.class);

  /** Suffix appended to segment files for empty vector placeholders. */
  public static final String EMPTY_VECTOR_SUFFIX = "_empty";

  private final IndexOutput meta, vectorData;
  private final List<FieldInfo> fields = new ArrayList<>();
  private boolean finished;

  /**
   * Creates a new empty vector writer.
   *
   * @param state the segment write state
   * @throws IOException if an I/O error occurs
   */
  public Lucene99EmptyFlatVectorWriter(SegmentWriteState state) throws IOException {
    super(null);

    String metaFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix + EMPTY_VECTOR_SUFFIX,
            Lucene99FlatVectorsFormat.META_EXTENSION);

    String vectorDataFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix + EMPTY_VECTOR_SUFFIX,
            Lucene99FlatVectorsFormat.VECTOR_DATA_EXTENSION);

    try {
      meta = state.directory.createOutput(metaFileName, state.context);
      vectorData = state.directory.createOutput(vectorDataFileName, state.context);

      CodecUtil.writeIndexHeader(
          meta,
          Lucene99FlatVectorsFormat.META_CODEC_NAME,
          Lucene99FlatVectorsFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      CodecUtil.writeIndexHeader(
          vectorData,
          Lucene99FlatVectorsFormat.VECTOR_DATA_CODEC_NAME,
          Lucene99FlatVectorsFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
    } catch (Throwable t) {
      IOUtils.closeWhileSuppressingExceptions(t, this);
      throw t;
    }
  }

  @Override
  public FlatFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
    fields.add(fieldInfo);
    return NoOpFieldWriter.INSTANCE;
  }

  @Override
  public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
    for (FieldInfo field : fields) {
      writeMeta(field, maxDoc);
    }
  }

  private void writeMeta(FieldInfo field, int maxDoc) throws IOException {
    meta.writeInt(field.number);
    meta.writeInt(field.getVectorEncoding().ordinal());
    meta.writeInt(field.getVectorSimilarityFunction().ordinal());
    meta.writeVLong(0);
    meta.writeVLong(0);
    meta.writeVInt(field.getVectorDimension());
    meta.writeInt(0);

    OrdToDocDISIReaderConfiguration.writeStoredMeta(
        Lucene99FlatVectorsFormat.DIRECT_MONOTONIC_BLOCK_SHIFT, meta, null, 0, maxDoc, null);
  }

  private static class NoOpFieldWriter extends FlatFieldVectorsWriter<float[]> {
    static final NoOpFieldWriter INSTANCE = new NoOpFieldWriter();

    @Override
    public void addValue(int docID, float[] vectorValue) {}

    @Override
    public float[] copyValue(float[] vectorValue) {
      return vectorValue;
    }

    @Override
    public long ramBytesUsed() {
      return 0;
    }

    @Override
    public List<float[]> getVectors() {
      return List.of();
    }

    @Override
    public DocsWithFieldSet getDocsWithFieldSet() {
      return new DocsWithFieldSet();
    }

    @Override
    public void finish() {}

    @Override
    public boolean isFinished() {
      return true;
    }
  }

  @Override
  public void finish() throws IOException {
    if (finished) {
      throw new IllegalStateException("already finished");
    }
    finished = true;
    if (meta != null) {
      // write end of fields marker
      meta.writeInt(-1);
      CodecUtil.writeFooter(meta);
    }
    if (vectorData != null) {
      CodecUtil.writeFooter(vectorData);
    }
  }

  @Override
  public CloseableRandomVectorScorerSupplier mergeOneFieldToIndex(
      FieldInfo fieldInfo, MergeState mergeState) throws IOException {
    return null;
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(meta, vectorData);
  }

  @Override
  public long ramBytesUsed() {
    return SHALLOW_RAM_BYTES_USED;
  }
}
