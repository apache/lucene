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
package org.apache.lucene.codecs.lucene912;

import java.io.IOException;
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

/**
 * Codec for encoding/decoding binary quantized vectors
 * The binary quantization format used here reflects RaBitQ: https://arxiv.org/abs/2405.12497
 * TODO: add more details on the format
 **/
public class Lucene912BinaryQuantizedVectorsFormat extends FlatVectorsFormat {

  public static final String BINARIZED_VECTOR_COMPONENT = "BVEC";
  public static final String NAME = "Lucene912BinaryQuantizedVectorsFormat";

  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;
  static final String META_CODEC_NAME = "Lucene912BinaryQuantizedVectorsFormatMeta";
  static final String VECTOR_DATA_CODEC_NAME = "Lucene912BinaryQuantizedVectorsFormatData";
  static final String META_EXTENSION = "vemb";
  static final String VECTOR_DATA_EXTENSION = "veb";
  static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;

  static final int DEFAULT_NUM_VECTORS_PER_CLUSTER = 100_000_000;
  // we set minimum here as we store the cluster ID in a byte, and we need to ensure that we can
  // cluster the max number of docs supported in a segment
  static final int MIN_NUM_VECTORS_PER_CLUSTER = 8_500_000;

  private static final FlatVectorsFormat rawVectorFormat =
      new Lucene99FlatVectorsFormat(FlatVectorScorerUtil.getLucene99FlatVectorsScorer());

  private final int numVectorsPerCluster;
  private final BinaryFlatVectorsScorer scorer = new Lucene912BinaryFlatVectorsScorer();

  /** Sole constructor */
  public Lucene912BinaryQuantizedVectorsFormat() {
    this(NAME, DEFAULT_NUM_VECTORS_PER_CLUSTER);
  }

  public Lucene912BinaryQuantizedVectorsFormat(int numVectorsPerCluster) {
    super(NAME);
    if (numVectorsPerCluster < MIN_NUM_VECTORS_PER_CLUSTER) {
      throw new IllegalArgumentException(
          "numVectorsPerCluster must be at least " + MIN_NUM_VECTORS_PER_CLUSTER);
    }
    this.numVectorsPerCluster = numVectorsPerCluster;
  }

  // for testing
  Lucene912BinaryQuantizedVectorsFormat(String name, int numVectorsPerCluster) {
    super(name);
    this.numVectorsPerCluster = numVectorsPerCluster;
  }

  @Override
  public FlatVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new Lucene912BinaryQuantizedVectorsWriter(
        scorer, numVectorsPerCluster, rawVectorFormat.fieldsWriter(state), state);
  }

  @Override
  public FlatVectorsReader fieldsReader(SegmentReadState state) throws IOException {
    return new Lucene912BinaryQuantizedVectorsReader(
        state, rawVectorFormat.fieldsReader(state), scorer);
  }
}
