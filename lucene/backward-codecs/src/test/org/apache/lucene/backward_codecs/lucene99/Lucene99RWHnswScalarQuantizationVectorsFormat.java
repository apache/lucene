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
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.DefaultFlatVectorScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.hnsw.ScalarQuantizedVectorScorer;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsWriter;
import org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorsWriter;
import org.apache.lucene.index.SegmentWriteState;

class Lucene99RWHnswScalarQuantizationVectorsFormat
    extends Lucene99HnswScalarQuantizedVectorsFormat {

  private final FlatVectorsFormat flatVectorsFormat = new Lucene99RWScalarQuantizedFormat();

  /** Sole constructor */
  protected Lucene99RWHnswScalarQuantizationVectorsFormat() {
    super();
  }

  @Override
  public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new Lucene99HnswVectorsWriter(
        state,
        Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN,
        Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH,
        flatVectorsFormat.fieldsWriter(state),
        1,
        null);
  }

  @Override
  public int getMaxDimensions(String fieldName) {
    return 1024;
  }

  static class Lucene99RWScalarQuantizedFormat extends Lucene99ScalarQuantizedVectorsFormat {
    private static final FlatVectorsFormat rawVectorFormat =
        new Lucene99FlatVectorsFormat(new DefaultFlatVectorScorer());

    @Override
    public FlatVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
      return new Lucene99ScalarQuantizedVectorsWriter(
          state,
          null,
          rawVectorFormat.fieldsWriter(state),
          new ScalarQuantizedVectorScorer(new DefaultFlatVectorScorer()));
    }
  }
}
