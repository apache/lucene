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
package org.apache.lucene.codecs.lucene104;

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
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.util.hnsw.HnswGraph;

/**
 * A vectors format that uses HNSW graph to store and search for vectors. But vectors are binary
 * quantized using {@link Lucene104ScalarQuantizedVectorsFormat} before being stored in the graph.
 */
public class Lucene104HnswScalarQuantizedVectorsFormat extends KnnVectorsFormat {

  public static final String NAME = "Lucene104HnswBinaryQuantizedVectorsFormat";

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

  /** The format for storing, reading, merging vectors on disk */
  private final Lucene104ScalarQuantizedVectorsFormat flatVectorsFormat;

  /**
   * The threshold to use to bypass HNSW graph building for tiny segments in terms of k for a graph
   * i.e. number of docs to match the query (default is {@link
   * Lucene99HnswVectorsFormat#HNSW_GRAPH_THRESHOLD}).
   *
   * <ul>
   *   <li>0 indicates that the graph is always built.
   *   <li>greater than 0 indicates that the graph needs a certain number of nodes before it starts
   *       building. See {@link Lucene99HnswVectorsFormat#HNSW_GRAPH_THRESHOLD} for details.
   *   <li>Negative values aren't allowed.
   * </ul>
   */
  private final int tinySegmentsThreshold;

  private final int numMergeWorkers;
  private final TaskExecutor mergeExec;

  /** Constructs a format using default graph construction parameters */
  public Lucene104HnswScalarQuantizedVectorsFormat() {
    this(
        ScalarEncoding.UNSIGNED_BYTE,
        DEFAULT_MAX_CONN,
        DEFAULT_BEAM_WIDTH,
        DEFAULT_NUM_MERGE_WORKER,
        null,
        HNSW_GRAPH_THRESHOLD);
  }

  /**
   * Constructs a format using the given graph construction parameters.
   *
   * @param maxConn the maximum number of connections to a node in the HNSW graph
   * @param beamWidth the size of the queue maintained during graph construction.
   */
  public Lucene104HnswScalarQuantizedVectorsFormat(int maxConn, int beamWidth) {
    this(
        ScalarEncoding.UNSIGNED_BYTE,
        maxConn,
        beamWidth,
        DEFAULT_NUM_MERGE_WORKER,
        null,
        HNSW_GRAPH_THRESHOLD);
  }

  /**
   * Constructs a format using the given graph construction parameters.
   *
   * @param encoding the quantization encoding used to encode the vectors
   * @param maxConn the maximum number of connections to a node in the HNSW graph
   * @param beamWidth the size of the queue maintained during graph construction.
   */
  public Lucene104HnswScalarQuantizedVectorsFormat(
      ScalarEncoding encoding, int maxConn, int beamWidth) {
    this(encoding, maxConn, beamWidth, DEFAULT_NUM_MERGE_WORKER, null, HNSW_GRAPH_THRESHOLD);
  }

  /**
   * Constructs a format using the given graph construction parameters and scalar quantization.
   *
   * @param encoding the quantization encoding used to encode the vectors
   * @param maxConn the maximum number of connections to a node in the HNSW graph
   * @param beamWidth the size of the queue maintained during graph construction.
   * @param numMergeWorkers number of workers (threads) that will be used when doing merge. If
   *     larger than 1, a non-null {@link ExecutorService} must be passed as mergeExec
   * @param mergeExec the {@link ExecutorService} that will be used by ALL vector writers that are
   *     generated by this format to do the merge
   */
  public Lucene104HnswScalarQuantizedVectorsFormat(
      ScalarEncoding encoding,
      int maxConn,
      int beamWidth,
      int numMergeWorkers,
      ExecutorService mergeExec) {
    this(encoding, maxConn, beamWidth, numMergeWorkers, mergeExec, HNSW_GRAPH_THRESHOLD);
  }

  /**
   * Constructs a format using the given graph construction parameters and scalar quantization.
   *
   * @param maxConn the maximum number of connections to a node in the HNSW graph
   * @param beamWidth the size of the queue maintained during graph construction.
   * @param numMergeWorkers number of workers (threads) that will be used when doing merge. If
   *     larger than 1, a non-null {@link ExecutorService} must be passed as mergeExec
   * @param mergeExec the {@link ExecutorService} that will be used by ALL vector writers that are
   *     generated by this format to do the merge
   */
  public Lucene104HnswScalarQuantizedVectorsFormat(
      ScalarEncoding encoding,
      int maxConn,
      int beamWidth,
      int numMergeWorkers,
      ExecutorService mergeExec,
      int tinySegmentsThreshold) {
    super(NAME);
    flatVectorsFormat = new Lucene104ScalarQuantizedVectorsFormat(encoding);
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
        tinySegmentsThreshold);
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
    return "Lucene104HnswScalarQuantizedVectorsFormat(name=Lucene104HnswScalarQuantizedVectorsFormat, maxConn="
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
