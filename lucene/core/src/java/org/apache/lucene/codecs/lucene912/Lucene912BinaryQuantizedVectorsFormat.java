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
import org.apache.lucene.search.DocIdSetIterator;

/**
 * Codec for encoding/decoding binary quantized vectors The binary quantization format used here
 * reflects <a href="https://arxiv.org/abs/2405.12497">RaBitQ</a>. Also see {@link
 * org.apache.lucene.util.quantization.BinaryQuantizer}. Some of key features of RabitQ are:
 *
 * <ul>
 *   <li>Estimating the distance between two vectors using their centroid normalized distance. This
 *       requires some additional corrective factors, but allows for centroid normalization to occur
 *       and thus enabling binary quantization.
 *   <li>Binary quantization of centroid normalized vectors.
 *   <li>Asymmetric quantization of vectors, where query vectors are quantized to half-byte
 *       precision (normalized to the centroid) and then compared directly against the single bit
 *       quantized vectors in the index.
 *   <li>Transforming the half-byte quantized query vectors in such a way that the comparison with
 *       single bit vectors can be done with hamming distance.
 *   <li>Utilizing an error bias calculation enabled by the centroid normalization. This allows for
 *       dynamic rescoring of vectors that fall outside a certain error threshold.
 * </ul>
 *
 * The format is stored in two files:
 *
 * <h2>.veb (vector data) file</h2>
 *
 * <p>Stores the binary quantized vectors in a flat format. Additionally, it stores each vector's
 * corrective factors. At the end of the file, additional information is stored for vector ordinal
 * to centroid ordinal mapping and sparse vector information.
 *
 * <ul>
 *   <li>For each vector:
 *       <ul>
 *         <li><b>[byte]</b> the binary quantized values, each byte holds 8 bits.
 *         <li><b>[float]</b> the corrective values. Two floats for Euclidean distance. Three floats
 *             for the dot-product family of distances.
 *       </ul>
 *   <li>After all vectors are encoded, the vector ordinal to centroid ordinal mapping is stored
 *       using bit packed integers.
 *   <li>After the mapping, sparse vector information keeping track of monotonic blocks.
 * </ul>
 *
 * <h2>.vemb (vector metadata) file</h2>
 *
 * <p>Stores the metadata for the vectors. This includes the number of vectors, the number of
 * dimensions, centroids and file offset information.
 *
 * <ul>
 *   <li><b>int</b> the field number
 *   <li><b>int</b> the vector encoding ordinal
 *   <li><b>int</b> the vector similarity ordinal
 *   <li><b>vint</b> the vector dimensions
 *   <li><b>vlong</b> the offset to the vector data in the .veb file
 *   <li><b>vlong</b> the length of the vector data in the .veb file
 *   <li><b>vint</b> the number of vectors
 *   <li><b>vint</b> the number of cluster, if vector count is more than 0
 *   <li><b>[[float]]</b> the centroids of each cluster, each of size dimensions
 *   <li><b>vlong</b> the offset to the vector ordinal to centroid ordinal mapping in the .veb file
 *       (if centroid count is more than 1)
 *   <li><b>vlong</b> the length of the vector ordinal to centroid ordinal mapping in the .veb file
 *       (if centroid count is more than 1)
 *   <li>The sparse vector information, if required, mapping vector ordinal to doc ID
 * </ul>
 */
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

  public static final int DEFAULT_NUM_VECTORS_PER_CLUSTER = 10_000_000;
  // we set minimum here as we store the cluster ID in a short, and we need to ensure that we can
  // cluster the max number of docs supported in a segment
  // additionally, too many clusters puts a strain on the heap & disk space during merge
  public static final int MIN_NUM_VECTORS_PER_CLUSTER = 1_000_000;

  private static final FlatVectorsFormat rawVectorFormat =
      new Lucene99FlatVectorsFormat(FlatVectorScorerUtil.getLucene99FlatVectorsScorer());

  private final int numVectorsPerCluster;
  private final BinaryFlatVectorsScorer scorer =
      new Lucene912BinaryFlatVectorsScorer(FlatVectorScorerUtil.getLucene99FlatVectorsScorer());

  /** Creates a new instance with the default number of vectors per cluster. */
  public Lucene912BinaryQuantizedVectorsFormat() {
    this(NAME, DEFAULT_NUM_VECTORS_PER_CLUSTER);
  }

  /**
   * Creates a new instance with the specified number of vectors per cluster.
   *
   * @param numVectorsPerCluster the number of vectors per cluster, when the number of vectors
   *     exceed this value, the vectors are clustered. Note, a smaller value will result in more
   *     clusters. But this will increase heap usage, indexing time and search time at the cost of
   *     slightly improved search recall.
   */
  public Lucene912BinaryQuantizedVectorsFormat(int numVectorsPerCluster) {
    super(NAME);
    if (numVectorsPerCluster < MIN_NUM_VECTORS_PER_CLUSTER) {
      throw new IllegalArgumentException(
          "numVectorsPerCluster must be at least " + MIN_NUM_VECTORS_PER_CLUSTER);
    }
    this.numVectorsPerCluster = numVectorsPerCluster;
  }

  // for testing, purposefully bypasses the check for minimum numVectorsPerCluster & not public
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

  @Override
  public String toString() {
    return "Lucene912BinaryQuantizedVectorsFormat(name="
        + NAME
        + ", numVectorsPerCluster="
        + numVectorsPerCluster
        + ", flatVectorScorer="
        + scorer
        + ")";
  }
}
