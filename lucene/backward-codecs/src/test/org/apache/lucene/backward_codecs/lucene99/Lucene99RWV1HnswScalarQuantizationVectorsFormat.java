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

package org.apache.lucene.backward_codecs.lucene99;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsWriter;
import org.apache.lucene.index.SegmentWriteState;

class Lucene99RWV1HnswScalarQuantizationVectorsFormat
    extends Lucene99HnswScalarQuantizedVectorsFormat {

  private final FlatVectorsFormat flatVectorsFormat;

  /** Sole constructor */
  protected Lucene99RWV1HnswScalarQuantizationVectorsFormat() {
    super();
    this.flatVectorsFormat = new Lucene99RWScalarQuantizedVectorsFormat();
  }

  public Lucene99RWV1HnswScalarQuantizationVectorsFormat(
      int maxConn,
      int beamWidth,
      int numMergeWorkers,
      int bits,
      boolean compress,
      Float confidenceInterval,
      ExecutorService mergeExec) {
    super(maxConn, beamWidth, numMergeWorkers, bits, compress, confidenceInterval, mergeExec);
    this.flatVectorsFormat =
        new Lucene99RWScalarQuantizedVectorsFormat(confidenceInterval, bits, compress);
  }

  @Override
  public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new Lucene99HnswVectorsWriter(
        state,
        Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN,
        Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH,
        flatVectorsFormat.fieldsWriter(state),
        1,
        null,
        0);
  }
}
