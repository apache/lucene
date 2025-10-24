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
package org.apache.lucene.backward_codecs.lucene102;

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_NUM_MERGE_WORKER;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsWriter;
import org.apache.lucene.index.SegmentWriteState;

/**
 * A vectors format that uses HNSW graph to store and search for vectors. But vectors are binary
 * quantized using {@link Lucene102BinaryQuantizedVectorsFormat} before being stored in the graph.
 */
public class Lucene102RWHnswBinaryQuantizedVectorsFormat
    extends Lucene102HnswBinaryQuantizedVectorsFormat {

  private static final FlatVectorsFormat writeFormat =
      new Lucene102RWBinaryQuantizedVectorsFormat();

  /** Constructs a format using default graph construction parameters */
  public Lucene102RWHnswBinaryQuantizedVectorsFormat() {
    this(DEFAULT_MAX_CONN, DEFAULT_BEAM_WIDTH, DEFAULT_NUM_MERGE_WORKER, null);
  }

  public Lucene102RWHnswBinaryQuantizedVectorsFormat(int maxConn, int beamWidth) {
    this(maxConn, beamWidth, DEFAULT_NUM_MERGE_WORKER, null);
  }

  public Lucene102RWHnswBinaryQuantizedVectorsFormat(
      int maxConn, int beamWidth, int numMergeWorkers, ExecutorService mergeExec) {
    super(maxConn, beamWidth, numMergeWorkers, mergeExec);
  }

  @Override
  public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new Lucene99HnswVectorsWriter(
        state, maxConn, beamWidth, writeFormat.fieldsWriter(state), numMergeWorkers, mergeExec, 0);
  }
}
