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

package org.apache.lucene.backward_codecs.lucene92;

import java.io.IOException;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.hnsw.HnswGraph;

public final class Lucene92RWHnswVectorsFormat extends Lucene92HnswVectorsFormat {

  /** Default number of maximum connections per node */
  public static final int DEFAULT_MAX_CONN = 16;

  /**
   * Default number of the size of the queue maintained while searching during a graph construction.
   */
  public static final int DEFAULT_BEAM_WIDTH = 100;

  static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;

  /**
   * Controls how many of the nearest neighbor candidates are connected to the new node. Defaults to
   * {@link #DEFAULT_MAX_CONN}. See {@link HnswGraph} for more details.
   */
  private final int maxConn;

  /**
   * The number of candidate neighbors to track while searching the graph for each newly inserted
   * node. Defaults to to {@link #DEFAULT_BEAM_WIDTH}. See {@link HnswGraph} for details.
   */
  private final int beamWidth;

  /** Constructs a format using default graph construction parameters. */
  public Lucene92RWHnswVectorsFormat() {
    this.maxConn = DEFAULT_MAX_CONN;
    this.beamWidth = DEFAULT_BEAM_WIDTH;
  }

  @Override
  public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new Lucene92HnswVectorsWriter(state, maxConn, beamWidth);
  }

  @Override
  public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
    return new Lucene92HnswVectorsReader(state);
  }

  @Override
  public String toString() {
    return "Lucene92RWHnswVectorsFormat";
  }
}
