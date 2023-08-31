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

package org.apache.lucene.codecs.lucene98;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.hnsw.HnswGraph;

import java.io.IOException;

/**
 *
 * @lucene.experimental
 */
public final class Lucene98ScalarQuantizedVectorsFormat {

  static final String META_CODEC_NAME = "Lucene98ScalarQuantizedVectorsFormatMeta";
  static final String VECTOR_DATA_CODEC_NAME = "Lucene98ScalarQuantizedVectorsFormatData";
  static final String QUANTIZED_VECTOR_DATA_CODEC_NAME = "Lucene98ScalarQuantizedVectorsFormatQuantizedData";
  static final String QUANTIZED_VECTOR_DATA_EXTENSION = "veq";

  public static final int VERSION_START = 0;
  public static final int VERSION_CURRENT = VERSION_START;

  /**
   * The minimum quantile
   */
  private static final int MINIMUM_QUANTILE = 90;

  /**
   * The maximum quantile
   */
  private static final int MAXIMUM_QUANTILE = 100;
  /**
   * Default number of the size of the queue maintained while searching during a graph construction.
   */
  public static final int DEFAULT_QUANTILE = 99;

  static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;

  /**
   * Controls the quantile used to scalar quantize the vectors
   * {@link Lucene98ScalarQuantizedVectorsFormat#DEFAULT_QUANTILE}.
   */
  private final int quantile;

  /** Constructs a format using default graph construction parameters */
  public Lucene98ScalarQuantizedVectorsFormat() {
    this(DEFAULT_QUANTILE);
  }

  /**
   * Constructs a format using the given graph construction parameters.
   *
   * @param quantile the quantile for scalar quantizing the vectors
   */
  public Lucene98ScalarQuantizedVectorsFormat(int quantile) {
    super("Lucene98QuantizedHnswVectorsFormat");
    if (quantile < MINIMUM_QUANTILE || quantile > MAXIMUM_QUANTILE) {
      throw new IllegalArgumentException(
          "quantile must be between " + MINIMUM_QUANTILE + " and " + MAXIMUM_QUANTILE
              + "; quantile="
              + quantile);
    }
    this.quantile = quantile;
  }

  @Override
  public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new Lucene98ScalarQuantizedVectorsWriter(state, quantile);
  }

  @Override
  public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
    return new Lucene98ScalarQuantizedVectorsReader(state);
  }

  @Override
  public int getMaxDimensions(String fieldName) {
    return 1024;
  }

  @Override
  public String toString() {
    return "Lucene98ScalarQuantizedVectorsFormat(name=Lucene98ScalarQuantizedVectorsFormat, quantile="
        + quantile
        + ")";
  }
}
