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

package org.apache.lucene.codecs.dedup;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.util.hnsw.HnswGraphBuilder;

/**
 * HNSW vector format whose underlying flat storage is a {@link DedupFlatVectorsFormat}. The HNSW
 * graph format is the unmodified Lucene99 HNSW format — graph nodes are field-local doc-ords; the
 * dedup map indirection is hidden inside the {@link
 * org.apache.lucene.codecs.dedup.OffHeapDedupFloatVectorValues}/byte counterparts.
 *
 * @lucene.experimental
 */
public final class DedupHnswVectorsFormat extends KnnVectorsFormat {

  static final String NAME = "DedupHnswVectorsFormat";

  /** Default maxConn (M). */
  public static final int DEFAULT_MAX_CONN = HnswGraphBuilder.DEFAULT_MAX_CONN;

  /** Default beam width. */
  public static final int DEFAULT_BEAM_WIDTH = HnswGraphBuilder.DEFAULT_BEAM_WIDTH;

  private final int maxConn;
  private final int beamWidth;
  private final int numMergeWorkers;
  private final TaskExecutor mergeExec;
  private final FlatVectorsFormat flatVectorsFormat;

  /** Constructs a format with default HNSW parameters. */
  public DedupHnswVectorsFormat() {
    this(DEFAULT_MAX_CONN, DEFAULT_BEAM_WIDTH);
  }

  /** Constructs a format with the given HNSW parameters and single-threaded merge. */
  public DedupHnswVectorsFormat(int maxConn, int beamWidth) {
    this(maxConn, beamWidth, Lucene99HnswVectorsFormat.DEFAULT_NUM_MERGE_WORKER, null);
  }

  /**
   * Constructs a format with the given HNSW parameters and merge concurrency.
   *
   * @param numMergeWorkers number of workers (threads) used during HNSW graph merge. If larger than
   *     1 and {@code mergeExec} is null, the indexer's intra-merge executor is used.
   * @param mergeExec the executor for merge workers; pass {@code null} to fall back to the {@link
   *     org.apache.lucene.index.MergeScheduler}'s intra-merge executor.
   */
  public DedupHnswVectorsFormat(
      int maxConn, int beamWidth, int numMergeWorkers, ExecutorService mergeExec) {
    super(NAME);
    if (maxConn <= 0 || maxConn > Lucene99HnswVectorsFormat.MAXIMUM_MAX_CONN) {
      throw new IllegalArgumentException(
          "maxConn must be positive and <= "
              + Lucene99HnswVectorsFormat.MAXIMUM_MAX_CONN
              + "; got "
              + maxConn);
    }
    if (beamWidth <= 0 || beamWidth > Lucene99HnswVectorsFormat.MAXIMUM_BEAM_WIDTH) {
      throw new IllegalArgumentException(
          "beamWidth must be positive and <= "
              + Lucene99HnswVectorsFormat.MAXIMUM_BEAM_WIDTH
              + "; got "
              + beamWidth);
    }
    this.maxConn = maxConn;
    this.beamWidth = beamWidth;
    this.numMergeWorkers = numMergeWorkers;
    // Mirror Lucene99HnswVectorsFormat's wrapping: the writer expects a TaskExecutor.
    this.mergeExec = mergeExec == null ? null : new TaskExecutor(mergeExec);
    this.flatVectorsFormat = new DedupFlatVectorsFormat();
  }

  @Override
  public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new Lucene99HnswVectorsWriter(
        state,
        maxConn,
        beamWidth,
        flatVectorsFormat,
        flatVectorsFormat.fieldsWriter(state),
        numMergeWorkers,
        mergeExec);
  }

  @Override
  public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
    return new Lucene99HnswVectorsReader(state, flatVectorsFormat.fieldsReader(state));
  }

  @Override
  public int getMaxDimensions(String fieldName) {
    return 1024;
  }

  @Override
  public String toString() {
    return "DedupHnswVectorsFormat(maxConn="
        + maxConn
        + ", beamWidth="
        + beamWidth
        + ", numMergeWorkers="
        + numMergeWorkers
        + ", mergeExec="
        + (mergeExec == null ? "null" : mergeExec)
        + ", flatVectorsFormat="
        + flatVectorsFormat
        + ")";
  }
}
