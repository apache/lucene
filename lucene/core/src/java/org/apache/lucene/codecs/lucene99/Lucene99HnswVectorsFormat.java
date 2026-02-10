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
import java.util.concurrent.ExecutorService;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.apache.lucene.util.hnsw.HnswGraphBuilder;

/**
 * Lucene 9.9 vector format, which encodes numeric vector values into an associated graph connecting
 * the documents having values. The graph is used to power HNSW search. The format consists of two
 * files, and requires a {@link FlatVectorsFormat} to store the actual vectors:
 *
 * <h2>.vex (vector index)</h2>
 *
 * <p>Stores graphs connecting the documents for each field organized as a list of nodes' neighbours
 * as following:
 *
 * <ul>
 *   <li>For each level:
 *       <ul>
 *         <li>For each node:
 *             <ul>
 *               <li><b>[vint]</b> the number of neighbor nodes
 *               <li><b>array[vint]</b> the delta encoded neighbor ordinals
 *             </ul>
 *       </ul>
 *   <li>After all levels are encoded, memory offsets for each node's neighbor nodes are appended to
 *       the end of the file. The offsets are encoded by {@link
 *       org.apache.lucene.util.packed.DirectMonotonicWriter}.
 * </ul>
 *
 * <h2>.vem (vector metadata) file</h2>
 *
 * <p>For each field:
 *
 * <ul>
 *   <li><b>[int32]</b> field number
 *   <li><b>[int32]</b> vector similarity function ordinal
 *   <li><b>[vlong]</b> offset to this field's index in the .vex file
 *   <li><b>[vlong]</b> length of this field's index data, in bytes
 *   <li><b>[vint]</b> dimension of this field's vectors
 *   <li><b>[int]</b> the number of documents having values for this field
 *   <li><b>[vint]</b> the maximum number of connections (neighbours) that each node can have
 *   <li><b>[vint]</b> number of levels in the graph
 *   <li>Graph nodes by level. For each level
 *       <ul>
 *         <li><b>[vint]</b> the number of nodes on this level
 *         <li><b>array[vint]</b> for levels greater than 0 list of nodes on this level, stored as
 *             the level 0th delta encoded nodes' ordinals.
 *       </ul>
 * </ul>
 *
 * @lucene.experimental
 */
public final class Lucene99HnswVectorsFormat extends KnnVectorsFormat {

  static final String META_CODEC_NAME = "Lucene99HnswVectorsFormatMeta";
  static final String VECTOR_INDEX_CODEC_NAME = "Lucene99HnswVectorsFormatIndex";
  static final String META_EXTENSION = "vem";
  static final String VECTOR_INDEX_EXTENSION = "vex";

  public static final int VERSION_START = 0;
  public static final int VERSION_GROUPVARINT = 1;
  public static final int VERSION_CURRENT = VERSION_GROUPVARINT;

  /**
   * A maximum configurable maximum max conn.
   *
   * <p>NOTE: We eagerly populate `float[MAX_CONN*2]` and `int[MAX_CONN*2]`, so exceptionally large
   * numbers here will use an inordinate amount of heap
   */
  public static final int MAXIMUM_MAX_CONN = 512;

  /** Default number of maximum connections per node */
  public static final int DEFAULT_MAX_CONN = HnswGraphBuilder.DEFAULT_MAX_CONN;

  /**
   * The maximum size of the queue to maintain while searching during graph construction. This
   * maximum value preserves the ratio of the `DEFAULT_BEAM_WIDTH`/`DEFAULT_MAX_CONN` (i.e. `6.25 *
   * 16 = 3200`).
   */
  public static final int MAXIMUM_BEAM_WIDTH = 3200;

  /**
   * Default number of the size of the queue maintained while searching during a graph construction.
   */
  public static final int DEFAULT_BEAM_WIDTH = HnswGraphBuilder.DEFAULT_BEAM_WIDTH;

  /** Default to use single-thread merge */
  public static final int DEFAULT_NUM_MERGE_WORKER = 1;

  /**
   * Minimum estimated search effort (in terms of expected visited nodes) required before building
   * an HNSW graph for a segment.
   *
   * <p>This threshold is compared against the value produced by {@link
   * org.apache.lucene.util.hnsw.HnswGraphSearcher#expectedVisitedNodes(int, int)}, which estimates
   * how many nodes would be visited during a vector search based on the current graph size and
   * {@code k} (neighbours to find).
   *
   * <p>If the estimated number of visited nodes falls below this threshold, HNSW graph construction
   * is skipped for that segment - typically for small flushes or low document count segments -
   * since the overhead of building the graph would outweigh its search benefits.
   *
   * <p>Default: {@code 100}
   */
  public static final int HNSW_GRAPH_THRESHOLD = 100;

  static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;

  /**
   * Controls how many of the nearest neighbor candidates are connected to the new node. Defaults to
   * {@link Lucene99HnswVectorsFormat#DEFAULT_MAX_CONN}. See {@link HnswGraph} for more details.
   */
  private final int maxConn;

  /**
   * The number of candidate neighbors to track while searching the graph for each newly inserted
   * node. Defaults to {@link Lucene99HnswVectorsFormat#DEFAULT_BEAM_WIDTH}. See {@link HnswGraph}
   * for details.
   */
  private final int beamWidth;

  /** The format for storing, reading, and merging vectors on disk. */
  private static final FlatVectorsFormat flatVectorsFormat =
      new Lucene99FlatVectorsFormat(FlatVectorScorerUtil.getLucene99FlatVectorsScorer());

  private final int numMergeWorkers;
  private final TaskExecutor mergeExec;

  /**
   * The threshold to use to bypass HNSW graph building for tiny segments in terms of k for a graph
   * i.e. number of docs to match the query (default is {@link
   * Lucene99HnswVectorsFormat#HNSW_GRAPH_THRESHOLD}).
   *
   * <ul>
   *   <li>0 indicates that the graph is always built.
   *   <li>0 indicates that the graph needs certain or more nodes before it starts building.
   *   <li>Negative values aren't allowed.
   * </ul>
   */
  private final int tinySegmentsThreshold;

  private final int writeVersion;

  /** Constructs a format using default graph construction parameters */
  public Lucene99HnswVectorsFormat() {
    this(
        DEFAULT_MAX_CONN,
        DEFAULT_BEAM_WIDTH,
        DEFAULT_NUM_MERGE_WORKER,
        null,
        HNSW_GRAPH_THRESHOLD,
        VERSION_CURRENT);
  }

  /**
   * Constructs a format using the given graph construction parameters.
   *
   * @param maxConn the maximum number of connections to a node in the HNSW graph
   * @param beamWidth the size of the queue maintained during graph construction.
   */
  public Lucene99HnswVectorsFormat(int maxConn, int beamWidth) {
    this(maxConn, beamWidth, DEFAULT_NUM_MERGE_WORKER, null, HNSW_GRAPH_THRESHOLD, VERSION_CURRENT);
  }

  /**
   * Constructs a format using the given graph construction parameters.
   *
   * @param maxConn the maximum number of connections to a node in the HNSW graph
   * @param beamWidth the size of the queue maintained during graph construction.
   * @param tinySegmentsThreshold the expected number of vector operations to return k nearest
   *     neighbors of the current graph size
   */
  public Lucene99HnswVectorsFormat(int maxConn, int beamWidth, int tinySegmentsThreshold) {
    this(
        maxConn, beamWidth, DEFAULT_NUM_MERGE_WORKER, null, tinySegmentsThreshold, VERSION_CURRENT);
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
  public Lucene99HnswVectorsFormat(
      int maxConn, int beamWidth, int numMergeWorkers, ExecutorService mergeExec) {
    this(maxConn, beamWidth, numMergeWorkers, mergeExec, HNSW_GRAPH_THRESHOLD, VERSION_CURRENT);
  }

  /**
   * Constructs a format using the given graph construction parameters. (This is a Test-Only
   * Constructor)
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
   */
  public Lucene99HnswVectorsFormat(
      int maxConn,
      int beamWidth,
      int numMergeWorkers,
      ExecutorService mergeExec,
      int tinySegmentsThreshold) {
    this(maxConn, beamWidth, numMergeWorkers, mergeExec, tinySegmentsThreshold, VERSION_CURRENT);
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
   * @param writeVersion the version used for the writer to encode docID's (VarInt=0, GroupVarInt=1)
   */
  Lucene99HnswVectorsFormat(
      int maxConn,
      int beamWidth,
      int numMergeWorkers,
      ExecutorService mergeExec,
      int tinySegmentsThreshold,
      int writeVersion) {
    super("Lucene99HnswVectorsFormat");
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
    this.maxConn = maxConn;
    this.beamWidth = beamWidth;
    this.tinySegmentsThreshold = tinySegmentsThreshold;
    this.writeVersion = writeVersion;
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
    return new Lucene99HnswVectorsWriter(
        state,
        maxConn,
        beamWidth,
        flatVectorsFormat.fieldsWriter(state),
        numMergeWorkers,
        mergeExec,
        tinySegmentsThreshold,
        writeVersion);
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
    return "Lucene99HnswVectorsFormat(name=Lucene99HnswVectorsFormat, maxConn="
        + maxConn
        + ", beamWidth="
        + beamWidth
        + ", tinySegmentsThreshold="
        + tinySegmentsThreshold
        + ", flatVectorFormat="
        + flatVectorsFormat
        + ")";
  }
}
