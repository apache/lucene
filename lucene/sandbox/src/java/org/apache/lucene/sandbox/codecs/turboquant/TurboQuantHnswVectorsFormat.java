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
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.TaskExecutor;

/**
 * Convenience format composing HNSW graph with TurboQuant flat vector quantization. This is the
 * primary user-facing format for TurboQuant vector search.
 */
public class TurboQuantHnswVectorsFormat extends KnnVectorsFormat {

  public static final String NAME = "TurboQuantHnswVectorsFormat";

  private final int maxConn;
  private final int beamWidth;
  private final TurboQuantFlatVectorsFormat flatVectorsFormat;
  private final int numMergeWorkers;
  private final TaskExecutor mergeExec;
  private final int tinySegmentsThreshold;

  /** Constructs with default parameters: BITS_4, maxConn=16, beamWidth=100. */
  public TurboQuantHnswVectorsFormat() {
    this(TurboQuantEncoding.BITS_4, DEFAULT_MAX_CONN, DEFAULT_BEAM_WIDTH);
  }

  /** Constructs with the given encoding and default HNSW parameters. */
  public TurboQuantHnswVectorsFormat(TurboQuantEncoding encoding, int maxConn, int beamWidth) {
    this(encoding, maxConn, beamWidth, DEFAULT_NUM_MERGE_WORKER, null, null);
  }

  /**
   * Full constructor with all parameters.
   *
   * @param encoding quantization bit-width
   * @param maxConn maximum connections per node in HNSW graph
   * @param beamWidth beam width for graph construction
   * @param numMergeWorkers number of merge workers (1 = single-threaded)
   * @param mergeExec executor for parallel merge, or null for single-threaded
   * @param rotationSeed explicit rotation seed, or null to derive from field name
   */
  public TurboQuantHnswVectorsFormat(
      TurboQuantEncoding encoding,
      int maxConn,
      int beamWidth,
      int numMergeWorkers,
      ExecutorService mergeExec,
      Long rotationSeed) {
    super(NAME);
    if (maxConn <= 0 || maxConn > MAXIMUM_MAX_CONN) {
      throw new IllegalArgumentException(
          "maxConn must be positive and <= " + MAXIMUM_MAX_CONN + "; maxConn=" + maxConn);
    }
    if (beamWidth <= 0 || beamWidth > MAXIMUM_BEAM_WIDTH) {
      throw new IllegalArgumentException(
          "beamWidth must be positive and <= " + MAXIMUM_BEAM_WIDTH + "; beamWidth=" + beamWidth);
    }
    if (numMergeWorkers == 1 && mergeExec != null) {
      throw new IllegalArgumentException(
          "No executor service is needed as we'll use single thread to merge");
    }
    this.maxConn = maxConn;
    this.beamWidth = beamWidth;
    this.flatVectorsFormat = new TurboQuantFlatVectorsFormat(encoding, rotationSeed);
    this.numMergeWorkers = numMergeWorkers;
    this.tinySegmentsThreshold = HNSW_GRAPH_THRESHOLD;
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
    return 16384;
  }

  @Override
  public String toString() {
    return "TurboQuantHnswVectorsFormat(name="
        + NAME
        + ", maxConn="
        + maxConn
        + ", beamWidth="
        + beamWidth
        + ", flatVectorsFormat="
        + flatVectorsFormat
        + ")";
  }
}
