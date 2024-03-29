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
import org.apache.lucene.codecs.FlatVectorsFormat;
import org.apache.lucene.codecs.FlatVectorsReader;
import org.apache.lucene.codecs.FlatVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

/**
 * Format supporting vector quantization, storage, and retrieval
 *
 * @lucene.experimental
 */
public final class Lucene99ScalarQuantizedVectorsFormat extends FlatVectorsFormat {
  public static final String QUANTIZED_VECTOR_COMPONENT = "QVEC";

  static final String NAME = "Lucene99ScalarQuantizedVectorsFormat";

  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;
  static final String META_CODEC_NAME = "Lucene99ScalarQuantizedVectorsFormatMeta";
  static final String VECTOR_DATA_CODEC_NAME = "Lucene99ScalarQuantizedVectorsFormatData";
  static final String META_EXTENSION = "vemq";
  static final String VECTOR_DATA_EXTENSION = "veq";

  private static final FlatVectorsFormat rawVectorFormat = new Lucene99FlatVectorsFormat();

  /** The minimum confidence interval */
  private static final float MINIMUM_CONFIDENCE_INTERVAL = 0.9f;

  /** The maximum confidence interval */
  private static final float MAXIMUM_CONFIDENCE_INTERVAL = 1f;

  /**
   * Controls the confidence interval used to scalar quantize the vectors the default value is
   * calculated as `1-1/(vector_dimensions + 1)`
   */
  final Float confidenceInterval;

  /** Constructs a format using default graph construction parameters */
  public Lucene99ScalarQuantizedVectorsFormat() {
    this(null);
  }

  /**
   * Constructs a format using the given graph construction parameters.
   *
   * @param confidenceInterval the confidenceInterval for scalar quantizing the vectors, when `null`
   *     it is calculated based on the vector field dimensions.
   */
  public Lucene99ScalarQuantizedVectorsFormat(Float confidenceInterval) {
    if (confidenceInterval != null
        && (confidenceInterval < MINIMUM_CONFIDENCE_INTERVAL
            || confidenceInterval > MAXIMUM_CONFIDENCE_INTERVAL)) {
      throw new IllegalArgumentException(
          "confidenceInterval must be between "
              + MINIMUM_CONFIDENCE_INTERVAL
              + " and "
              + MAXIMUM_CONFIDENCE_INTERVAL
              + "; confidenceInterval="
              + confidenceInterval);
    }
    this.confidenceInterval = confidenceInterval;
  }

  public static float calculateDefaultConfidenceInterval(int vectorDimension) {
    return Math.max(MINIMUM_CONFIDENCE_INTERVAL, 1f - (1f / (vectorDimension + 1)));
  }

  @Override
  public String toString() {
    return NAME
        + "(name="
        + NAME
        + ", confidenceInterval="
        + confidenceInterval
        + ", rawVectorFormat="
        + rawVectorFormat
        + ")";
  }

  @Override
  public FlatVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new Lucene99ScalarQuantizedVectorsWriter(
        state, confidenceInterval, rawVectorFormat.fieldsWriter(state));
  }

  @Override
  public FlatVectorsReader fieldsReader(SegmentReadState state) throws IOException {
    return new Lucene99ScalarQuantizedVectorsReader(state, rawVectorFormat.fieldsReader(state));
  }
}
