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
package org.apache.lucene.sandbox.codecs.dedup;

import java.io.IOException;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Writes de-duplicated flat vectors. A single instance is used for either flushing buffered vectors
 * or merging existing segments (never both), delegating to {@link DedupFlushContext} or {@link
 * DedupMergeContext} accordingly.
 *
 * <p>Also used by {@link DedupScalarQuantizedVectorsFormat} (with a non-null {@link
 * DedupQuantizer}) to additionally write a quantized copy of each applicable group, into a separate
 * quantized vector data file.
 *
 * @lucene.experimental
 */
final class DedupFlatVectorsWriter extends FlatVectorsWriter {
  private static final long SHALLOW_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(DedupFlatVectorsWriter.class);

  private final IndexOutput meta;
  private final IndexOutput vectorData;
  private final IndexOutput quantizedVectorData;
  private boolean finished;

  private final DedupQuantizer quantizer;

  private final DedupFlushContext flushContext;
  private boolean usedForFlush;

  private final DedupMergeContext mergeContext;
  private boolean usedForMerge;

  DedupFlatVectorsWriter(
      SegmentWriteState state,
      FlatVectorsScorer vectorsScorer,
      DedupQuantizer quantizer,
      String metaCodecName,
      String metaExtension,
      String vectorDataCodecName,
      String vectorDataExtension,
      String quantizedVectorDataCodecName,
      String quantizedVectorDataExtension,
      int versionCurrent)
      throws IOException {
    super(vectorsScorer);

    this.finished = false;
    this.quantizer = quantizer;
    this.flushContext = new DedupFlushContext();
    this.usedForFlush = false;
    this.mergeContext = new DedupMergeContext();
    this.usedForMerge = false;

    String metaFileName =
        IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
    String vectorDataFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, vectorDataExtension);

    boolean success = false;
    try {
      this.meta = state.directory.createOutput(metaFileName, state.context);
      CodecUtil.writeIndexHeader(
          meta, metaCodecName, versionCurrent, state.segmentInfo.getId(), state.segmentSuffix);

      this.vectorData = state.directory.createOutput(vectorDataFileName, state.context);
      CodecUtil.writeIndexHeader(
          vectorData,
          vectorDataCodecName,
          versionCurrent,
          state.segmentInfo.getId(),
          state.segmentSuffix);

      if (quantizer != null) {
        String quantizedVectorDataFileName =
            IndexFileNames.segmentFileName(
                state.segmentInfo.name, state.segmentSuffix, quantizedVectorDataExtension);
        this.quantizedVectorData =
            state.directory.createOutput(quantizedVectorDataFileName, state.context);
        CodecUtil.writeIndexHeader(
            quantizedVectorData,
            quantizedVectorDataCodecName,
            versionCurrent,
            state.segmentInfo.getId(),
            state.segmentSuffix);
      } else {
        this.quantizedVectorData = null;
      }
      success = true;
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }

  @Override
  public FlatFieldVectorsWriter<?> addField(FieldInfo fieldInfo) {
    if (usedForMerge) {
      throw new IllegalStateException("already used for merge");
    }
    usedForFlush = true;

    return flushContext.addField(fieldInfo);
  }

  @Override
  public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
    if (usedForMerge) {
      throw new IllegalStateException("already used for merge");
    }
    usedForFlush = true;

    flushContext.flush(meta, vectorData, quantizedVectorData, maxDoc, sortMap, quantizer);
  }

  @Override
  public void finish() throws IOException {
    if (finished) {
      throw new IllegalStateException("already finished");
    }
    finished = true;

    if (usedForMerge) {
      finishMerge();
    }

    CodecUtil.writeFooter(meta);
    CodecUtil.writeFooter(vectorData);

    if (quantizedVectorData != null) {
      CodecUtil.writeFooter(quantizedVectorData);
    }
  }

  @Override
  public void mergeOneFlatVectorField(FieldInfo fieldInfo, MergeState mergeState)
      throws IOException {
    if (usedForFlush) {
      throw new IllegalStateException("already used for flush");
    }
    usedForMerge = true;

    mergeContext.addField(fieldInfo, mergeState);
  }

  private void finishMerge() throws IOException {
    mergeContext.finish(meta, vectorData, quantizedVectorData, quantizer);
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(meta, vectorData, quantizedVectorData);
  }

  @Override
  public long ramBytesUsed() {
    return SHALLOW_SIZE + flushContext.ramBytesUsed() + mergeContext.ramBytesUsed();
  }
}
