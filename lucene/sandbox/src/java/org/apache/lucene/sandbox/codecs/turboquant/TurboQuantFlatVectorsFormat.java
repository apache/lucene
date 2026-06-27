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

import java.io.IOException;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

/**
 * TurboQuant flat vectors format. Stores quantized vectors using rotation-based data-oblivious
 * quantization with precomputed Beta-distribution-optimal Lloyd-Max centroids.
 *
 * <p>This format stores both raw float32 vectors (delegated to {@link Lucene99FlatVectorsFormat})
 * and quantized vectors in separate files. The quantized vectors use unique extensions {@code .vetq}
 * (data) and {@code .vemtq} (metadata).
 */
public class TurboQuantFlatVectorsFormat extends FlatVectorsFormat {

  public static final String NAME = "TurboQuantFlatVectorsFormat";

  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;
  static final String META_CODEC_NAME = "TurboQuantVectorsFormatMeta";
  static final String VECTOR_DATA_CODEC_NAME = "TurboQuantVectorsFormatData";
  static final String META_EXTENSION = "vemtq";
  static final String VECTOR_DATA_EXTENSION = "vetq";

  private static final FlatVectorsFormat rawVectorFormat =
      new Lucene99FlatVectorsFormat(FlatVectorScorerUtil.getLucene99FlatVectorsScorer());

  private final TurboQuantEncoding encoding;
  private final Long rotationSeed;
  private final FlatVectorsScorer scorer;

  /** Creates a new instance with default BITS_4 encoding. */
  public TurboQuantFlatVectorsFormat() {
    this(TurboQuantEncoding.BITS_4);
  }

  /** Creates a new instance with the given encoding. */
  public TurboQuantFlatVectorsFormat(TurboQuantEncoding encoding) {
    this(encoding, null);
  }

  /**
   * Creates a new instance with the given encoding and optional explicit rotation seed.
   *
   * @param encoding the quantization bit-width
   * @param rotationSeed explicit rotation seed, or null to derive from field name
   */
  public TurboQuantFlatVectorsFormat(TurboQuantEncoding encoding, Long rotationSeed) {
    super(NAME);
    this.encoding = encoding;
    this.rotationSeed = rotationSeed;
    this.scorer = new TurboQuantVectorsScorer(FlatVectorScorerUtil.getLucene99FlatVectorsScorer());
  }

  @Override
  public FlatVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new TurboQuantFlatVectorsWriter(
        state, encoding, rotationSeed, rawVectorFormat.fieldsWriter(state), scorer);
  }

  @Override
  public FlatVectorsReader fieldsReader(SegmentReadState state) throws IOException {
    return new TurboQuantFlatVectorsReader(state, rawVectorFormat.fieldsReader(state), scorer);
  }

  @Override
  public int getMaxDimensions(String fieldName) {
    return 16384;
  }

  @Override
  public String toString() {
    return "TurboQuantFlatVectorsFormat(name="
        + NAME
        + ", encoding="
        + encoding
        + ", rotationSeed="
        + rotationSeed
        + ")";
  }
}
