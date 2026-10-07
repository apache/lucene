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
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.FileDataHint;
import org.apache.lucene.store.FileTypeHint;
import org.apache.lucene.util.hnsw.HnswGraph;

/**
 * An HNSW vector format that de-duplicates raw vectors.
 *
 * <p>Unlike {@link org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat}, which builds one
 * HNSW node per document, this format builds a single graph node per <b>distinct</b> vector (see
 * {@link DedupHnswVectorsWriter}), saving graph construction time and index size when many
 * documents share the same vector. At search time the graph is traversed over distinct vectors and
 * each match is expanded back to all documents that reference it (see {@link
 * DedupHnswVectorsReader}). A {@link DedupFlatVectorsFormat} is used for the flat vector storage,
 * which stores each distinct vector exactly once, shared across all documents that reference it.
 *
 * <p>This format is suitable for high-performance filtered vector search when filter information is
 * available at indexing time. In addition to the primary vector field, the user creates separate
 * fields for each filter value (e.g. product categories in an e-commerce search engine) to build
 * dedicated HNSW graphs that share the same raw vector storage. <b>The responsibility of searching
 * the right field at query-time lies on the user</b>.
 *
 * <p>This scheme allows for more efficient search than query-time pre-filtering (i.e. {@link
 * org.apache.lucene.search.AcceptDocs} derived from a {@link org.apache.lucene.search.Query} {@code
 * filter}) at the expense of slower indexing and larger indexes due to additional HNSW graphs.
 *
 * <p>If you customize this format, be sure to <b>share the same instance</b> of the underlying
 * {@link DedupFlatVectorsFormat} to de-duplicate raw vectors correctly.
 *
 * @lucene.experimental
 */
public final class DedupHnswVectorsFormat extends KnnVectorsFormat {
  private static final String NAME = "DedupHnswVectorsFormat";

  /**
   * Default hybrid-group threshold: {@code 0} disables the {@link DedupLayoutMode#HYBRID} layout, so
   * fields fall back to the whole-field {@link DedupLayoutMode#PLAIN}/{@link DedupLayoutMode#DEDUP}
   * selection.
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

  /** The format for storing, reading, and merging vectors on disk. */
  private static final FlatVectorsFormat FORMAT = new DedupFlatVectorsFormat();

  /**
   * Number of workers (threads) used when building the HNSW graph during a merge. When {@code > 1}
   * (and an executor is available, either {@link #mergeExec} or the merge scheduler's intra-merge
   * executor) the graph is built concurrently, matching {@link
   * org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsWriter}. {@code 1} builds single-threaded.
   */
  private final int numMergeWorkers;

  /**
   * The {@link ExecutorService} used by ALL vector writers generated by this format for concurrent
   * merge. If {@code null}, the merge scheduler's {@link
   * MergeScheduler#getIntraMergeExecutor(MergePolicy.OneMerge)} is used instead (when present).
   */
  private final ExecutorService mergeExec;

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

  /** Constructs a format using default graph construction parameters */
  public DedupHnswVectorsFormat() {
    this(
        DEFAULT_MAX_CONN,
        DEFAULT_BEAM_WIDTH,
        DEFAULT_NUM_MERGE_WORKER,
        null,
        HNSW_GRAPH_THRESHOLD,
        DEFAULT_HYBRID_GROUP_THRESHOLD);
  }

  /**
   * Constructs a format using the given graph construction parameters.
   *
   * @param maxConn the maximum number of connections to a node in the HNSW graph
   * @param beamWidth the size of the queue maintained during graph construction.
   */
  public DedupHnswVectorsFormat(int maxConn, int beamWidth) {
    this(
        maxConn,
        beamWidth,
        DEFAULT_NUM_MERGE_WORKER,
        null,
        HNSW_GRAPH_THRESHOLD,
        DEFAULT_HYBRID_GROUP_THRESHOLD);
  }

  /**
   * Constructs a format using the given graph construction parameters.
   *
   * @param maxConn the maximum number of connections to a node in the HNSW graph
   * @param beamWidth the size of the queue maintained during graph construction.
   * @param tinySegmentsThreshold the expected number of vector operations to return k nearest
   *     neighbors of the current graph size
   */
  public DedupHnswVectorsFormat(int maxConn, int beamWidth, int tinySegmentsThreshold) {
    this(
        maxConn,
        beamWidth,
        DEFAULT_NUM_MERGE_WORKER,
        null,
        tinySegmentsThreshold,
        DEFAULT_HYBRID_GROUP_THRESHOLD);
  }

  /**
   * Constructs a format using the given graph construction parameters.
   *
   * @param maxConn the maximum number of connections to a node in the HNSW graph
   * @param beamWidth the size of the queue maintained during graph construction.
   * @param numMergeWorkers number of workers (threads) that will be used when doing merge. If
   *     larger than 1, a non-null {@link ExecutorService} must be passed as mergeExec
   * @param mergeExec the {@link ExecutorService} that will be used by ALL vector writers that are
   *     generated by this format to do the merge. If null, the configured {@link
   *     MergeScheduler#getIntraMergeExecutor(MergePolicy.OneMerge)} is used.
   */
  public DedupHnswVectorsFormat(
      int maxConn, int beamWidth, int numMergeWorkers, ExecutorService mergeExec) {
    this(
        maxConn,
        beamWidth,
        numMergeWorkers,
        mergeExec,
        HNSW_GRAPH_THRESHOLD,
        DEFAULT_HYBRID_GROUP_THRESHOLD);
  }

  /**
   * Constructs a format using the given graph construction parameters.
   *
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
  public DedupHnswVectorsFormat(
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
    this.tinySegmentsThreshold = tinySegmentsThreshold;
    this.hybridGroupThreshold = hybridGroupThreshold;
    if (numMergeWorkers == 1 && mergeExec != null) {
      throw new IllegalArgumentException(
          "No executor service is needed as we'll use single thread to merge");
    }
    this.numMergeWorkers = numMergeWorkers;
    this.mergeExec = mergeExec;
  }

  @Override
  public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new DedupHnswVectorsWriter(
        state,
        maxConn,
        beamWidth,
        tinySegmentsThreshold,
        hybridGroupThreshold,
        numMergeWorkers,
        mergeExec,
        FORMAT,
        FORMAT.fieldsWriter(state));
  }

  @Override
  public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
    return new DedupHnswVectorsReader(
        state,
        FORMAT.fieldsReader(
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
        + ", tinySegmentsThreshold="
        + tinySegmentsThreshold
        + ", hybridGroupThreshold="
        + hybridGroupThreshold
        + ")";
  }
}
