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

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_NUM_MERGE_WORKER;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.HNSW_GRAPH_THRESHOLD;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.MAXIMUM_BEAM_WIDTH;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.MAXIMUM_MAX_CONN;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.FileDataHint;
import org.apache.lucene.store.FileTypeHint;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues.ScalarEncoding;

/**
 * An HNSW vector format that de-duplicates vectors, storing each distinct vector once in both raw
 * and scalar quantized form.
 *
 * <p>The flat vector storage ({@link DedupScalarQuantizedVectorsFormat}) stores each distinct
 * vector exactly once (in both raw and scalar quantized form), shared across all documents that
 * reference it. On top of it, {@link DedupHnswVectorsWriter} builds an HNSW graph whose node space
 * adapts per field: a node per distinct vector when a field de-duplicates well, a node per document
 * when it does not, or a hybrid of both. {@link DedupHnswVectorsReader} searches that graph against
 * the quantized vectors and expands each matched node to its referencing document(s). See {@link
 * DedupHnswVectorsFormat} for the intended multi-field usage pattern.
 *
 * <p>If you customize this format, be sure to <b>share the same instance</b> of the underlying
 * {@link DedupScalarQuantizedVectorsFormat} to de-duplicate vectors correctly.
 *
 * @lucene.experimental
 */
public final class DedupHnswScalarQuantizedVectorsFormat extends KnnVectorsFormat {
  private static final String NAME = "DedupHnswScalarQuantizedVectorsFormat";

  /**
   * Default hybrid-group threshold: {@code 0} disables the {@link DedupLayoutMode#HYBRID} layout,
   * so fields fall back to the whole-field {@link DedupLayoutMode#PLAIN}/{@link
   * DedupLayoutMode#DEDUP} selection.
   */
  public static final int DEFAULT_HYBRID_GROUP_THRESHOLD = 0;

  /**
   * Controls how many of the nearest neighbor candidates are connected to the new node. Defaults to
   * {@link org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat#DEFAULT_MAX_CONN}. See
   * {@link HnswGraph} for more details.
   */
  private final int maxConn;

  /**
   * The number of candidate neighbors to track while searching the graph for each newly inserted
   * node. Defaults to {@link
   * org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat#DEFAULT_BEAM_WIDTH}. See {@link
   * HnswGraph} for details.
   */
  private final int beamWidth;

  /** The scalar encoding for quantization. Defaults to {@link ScalarEncoding#UNSIGNED_BYTE}. */
  private final ScalarEncoding encoding;

  /** The format for storing, reading, and merging vectors on disk. */
  private final FlatVectorsFormat flatVectorsFormat;

  private final int numMergeWorkers;
  private final TaskExecutor mergeExec;

  /**
   * The threshold to use to bypass HNSW graph building for tiny segments in terms of k for a graph
   * i.e. number of docs to match the query (default is {@link
   * org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat#HNSW_GRAPH_THRESHOLD}).
   *
   * <ul>
   *   <li>0 indicates that the graph is always built.
   *   <li>Positive values require that many estimated visited nodes before a graph is built.
   *   <li>Negative values aren't allowed.
   * </ul>
   */
  private final int tinySegmentsThreshold;

  /**
   * When positive, enables the {@link DedupLayoutMode#HYBRID} layout: within a field, any distinct
   * vector (group) referenced by <b>more than</b> this many documents is promoted to a single HNSW
   * node with a posting list, while all remaining documents stay as individual HNSW nodes with no
   * postings. {@code 0} (the default) disables HYBRID and keeps the whole-field PLAIN/DEDUP
   * selection. Negative values are not allowed.
   */
  private final int hybridGroupThreshold;

  /** Constructs a format with UNSIGNED_BYTE encoding and default graph construction parameters */
  public DedupHnswScalarQuantizedVectorsFormat() {
    this(
        ScalarEncoding.UNSIGNED_BYTE,
        DEFAULT_MAX_CONN,
        DEFAULT_BEAM_WIDTH,
        DEFAULT_NUM_MERGE_WORKER,
        null,
        HNSW_GRAPH_THRESHOLD);
  }

  /**
   * Constructs a format with UNSIGNED_BYTE encoding, using the given graph construction parameters.
   *
   * @param maxConn the maximum number of connections to a node in the HNSW graph
   * @param beamWidth the size of the queue maintained during graph construction.
   */
  public DedupHnswScalarQuantizedVectorsFormat(int maxConn, int beamWidth) {
    this(
        ScalarEncoding.UNSIGNED_BYTE,
        maxConn,
        beamWidth,
        DEFAULT_NUM_MERGE_WORKER,
        null,
        HNSW_GRAPH_THRESHOLD);
  }

  /**
   * Constructs a format using the given quantization encoding and graph construction parameters.
   *
   * @param encoding the quantization encoding used to encode the vectors
   * @param maxConn the maximum number of connections to a node in the HNSW graph
   * @param beamWidth the size of the queue maintained during graph construction.
   */
  public DedupHnswScalarQuantizedVectorsFormat(
      ScalarEncoding encoding, int maxConn, int beamWidth) {
    this(encoding, maxConn, beamWidth, DEFAULT_NUM_MERGE_WORKER, null, HNSW_GRAPH_THRESHOLD);
  }

  /**
   * Constructs a format using the given quantization encoding and graph construction parameters.
   *
   * @param encoding the quantization encoding used to encode the vectors
   * @param maxConn the maximum number of connections to a node in the HNSW graph
   * @param beamWidth the size of the queue maintained during graph construction.
   * @param numMergeWorkers number of workers (threads) that will be used when doing merge. If
   *     larger than 1, a non-null {@link ExecutorService} must be passed as mergeExec
   * @param mergeExec the {@link ExecutorService} that will be used by ALL vector writers that are
   *     generated by this format to do the merge. If null, the configured {@link
   *     MergeScheduler#getIntraMergeExecutor(MergePolicy.OneMerge)} is used.
   */
  public DedupHnswScalarQuantizedVectorsFormat(
      ScalarEncoding encoding,
      int maxConn,
      int beamWidth,
      int numMergeWorkers,
      ExecutorService mergeExec) {
    this(encoding, maxConn, beamWidth, numMergeWorkers, mergeExec, HNSW_GRAPH_THRESHOLD);
  }

  /**
   * Constructs a format using the given quantization encoding and graph construction parameters.
   *
   * @param encoding the quantization encoding used to encode the vectors
   * @param maxConn the maximum number of connections to a node in the HNSW graph
   * @param beamWidth the size of the queue maintained during graph construction.
   * @param numMergeWorkers number of workers (threads) that will be used when doing merge. If
   *     larger than 1, a non-null {@link ExecutorService} must be passed as mergeExec
   * @param mergeExec the {@link ExecutorService} that will be used by ALL vector writers that are
   *     generated by this format to do the merge. If null, the configured {@link
   *     MergeScheduler#getIntraMergeExecutor(MergePolicy.OneMerge)} is used.
   * @param tinySegmentsThreshold the expected number of vector operations to return k nearest
   *     neighbors of the current graph size
   */
  public DedupHnswScalarQuantizedVectorsFormat(
      ScalarEncoding encoding,
      int maxConn,
      int beamWidth,
      int numMergeWorkers,
      ExecutorService mergeExec,
      int tinySegmentsThreshold) {
    this(
        encoding,
        maxConn,
        beamWidth,
        numMergeWorkers,
        mergeExec,
        tinySegmentsThreshold,
        DEFAULT_HYBRID_GROUP_THRESHOLD);
  }

  /**
   * Constructs a format using the given quantization encoding and graph construction parameters.
   *
   * @param encoding the quantization encoding used to encode the vectors
   * @param maxConn the maximum number of connections to a node in the HNSW graph
   * @param beamWidth the size of the queue maintained during graph construction.
   * @param numMergeWorkers number of workers (threads) that will be used when doing merge. If
   *     larger than 1, a non-null {@link ExecutorService} must be passed as mergeExec
   * @param mergeExec the {@link ExecutorService} that will be used by ALL vector writers that are
   *     generated by this format to do the merge. If null, the configured {@link
   *     MergeScheduler#getIntraMergeExecutor(MergePolicy.OneMerge)} is used.
   * @param tinySegmentsThreshold the expected number of vector operations to return k nearest
   *     neighbors of the current graph size
   * @param hybridGroupThreshold when positive, enables the {@link DedupLayoutMode#HYBRID} layout:
   *     groups referenced by more than this many documents are promoted to posting-backed graph
   *     nodes, while all other documents remain individual graph nodes. {@code 0} disables HYBRID.
   */
  public DedupHnswScalarQuantizedVectorsFormat(
      ScalarEncoding encoding,
      int maxConn,
      int beamWidth,
      int numMergeWorkers,
      ExecutorService mergeExec,
      int tinySegmentsThreshold,
      int hybridGroupThreshold) {
    super(NAME);
    if (maxConn <= 0 || maxConn > MAXIMUM_MAX_CONN) {
      throw new IllegalArgumentException(
          "maxConn must be positive and less than or equal to "
              + MAXIMUM_MAX_CONN
              + "; maxConn="
              + maxConn);
    }
    if (beamWidth <= 0 || beamWidth > MAXIMUM_BEAM_WIDTH) {
      throw new IllegalArgumentException(
          "beamWidth must be positive and less than or equal to "
              + MAXIMUM_BEAM_WIDTH
              + "; beamWidth="
              + beamWidth);
    }
    if (hybridGroupThreshold < 0) {
      throw new IllegalArgumentException(
          "hybridGroupThreshold must be non-negative; hybridGroupThreshold="
              + hybridGroupThreshold);
    }
    this.maxConn = maxConn;
    this.beamWidth = beamWidth;
    this.encoding = encoding;
    this.flatVectorsFormat = new DedupScalarQuantizedVectorsFormat(encoding);
    this.tinySegmentsThreshold = tinySegmentsThreshold;
    this.hybridGroupThreshold = hybridGroupThreshold;
    if (numMergeWorkers == 1 && mergeExec != null) {
      throw new IllegalArgumentException(
          "No executor service is needed as we'll use single thread to merge");
    }
    this.numMergeWorkers = numMergeWorkers;
    if (mergeExec != null) {
      this.mergeExec = new TaskExecutor(mergeExec);
    } else {
      this.mergeExec = null;
    }
  }

  @Override
  public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new DedupHnswVectorsWriter(
        state,
        maxConn,
        beamWidth,
        flatVectorsFormat,
        flatVectorsFormat.fieldsWriter(state),
        numMergeWorkers,
        mergeExec,
        tinySegmentsThreshold,
        hybridGroupThreshold);
  }

  @Override
  public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
    return new DedupHnswVectorsReader(
        state,
        flatVectorsFormat.fieldsReader(
            state.withHints(FileTypeHint.DATA, FileDataHint.KNN_VECTORS, DataAccessHint.RANDOM)));
  }

  @Override
  public int getMaxDimensions(String fieldName) {
    return DEFAULT_MAX_DIMENSIONS;
  }

  @Override
  public String toString() {
    return NAME
        + "(maxConn="
        + maxConn
        + ", beamWidth="
        + beamWidth
        + ", encoding="
        + encoding
        + ", tinySegmentsThreshold="
        + tinySegmentsThreshold
        + ")";
  }
}
