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
package org.apache.lucene.codecs.lucene106.dedup;

import static org.apache.lucene.codecs.lucene106.dedup.DedupFlatVectorsFormat.META_CODEC_NAME;
import static org.apache.lucene.codecs.lucene106.dedup.DedupFlatVectorsFormat.META_EXTENSION;
import static org.apache.lucene.codecs.lucene106.dedup.DedupFlatVectorsFormat.VECTOR_DATA_CODEC_NAME;
import static org.apache.lucene.codecs.lucene106.dedup.DedupFlatVectorsFormat.VECTOR_DATA_EXTENSION;
import static org.apache.lucene.codecs.lucene106.dedup.DedupFlatVectorsFormat.VERSION_CURRENT;

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
 * @lucene.experimental
 */
final class DedupFlatVectorsWriter extends FlatVectorsWriter {
  private static final long SHALLOW_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(DedupFlatVectorsWriter.class);

  private final IndexOutput meta;
  private final IndexOutput vectorData;
  private boolean finished;

  private final DedupFlushContext flushContext;
  private boolean usedForFlush;

  private final DedupMergeContext mergeContext;
  private boolean usedForMerge;

  DedupFlatVectorsWriter(SegmentWriteState state, FlatVectorsScorer vectorsScorer)
      throws IOException {
    super(vectorsScorer);

    this.finished = false;
    this.flushContext = new DedupFlushContext();
    this.usedForFlush = false;
    this.mergeContext = new DedupMergeContext();
    this.usedForMerge = false;

    String metaFileName =
        IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, META_EXTENSION);
    String vectorDataFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, VECTOR_DATA_EXTENSION);

    boolean success = false;
    IndexOutput m = null, v = null;
    try {
      m = state.directory.createOutput(metaFileName, state.context);
      v = state.directory.createOutput(vectorDataFileName, state.context);
      CodecUtil.writeIndexHeader(
          m, META_CODEC_NAME, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
      CodecUtil.writeIndexHeader(
          v,
          VECTOR_DATA_CODEC_NAME,
          VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      this.meta = m;
      this.vectorData = v;
      success = true;
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(m, v);
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

    flushContext.flush(meta, vectorData, maxDoc, sortMap);
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

    if (meta != null) {
      CodecUtil.writeFooter(meta);
    }

    if (vectorData != null) {
      CodecUtil.writeFooter(vectorData);
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
    mergeContext.finish(meta, vectorData);
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(meta, vectorData);
  }

  @Override
  public long ramBytesUsed() {
    return SHALLOW_SIZE + flushContext.ramBytesUsed() + mergeContext.ramBytesUsed();
  }
}
