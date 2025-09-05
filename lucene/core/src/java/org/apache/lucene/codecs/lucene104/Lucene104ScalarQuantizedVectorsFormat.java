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
package org.apache.lucene.codecs.lucene104;

import java.io.IOException;
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

/**
 * The quantization format used here is a per-vector optimized scalar quantization. These ideas are
 * evolutions of LVQ proposed in <a href="https://arxiv.org/abs/2304.04759">Similarity search in the
 * blink of an eye with compressed indices</a> by Cecilia Aguerrebere et al., the previous work on
 * globally optimized scalar quantization in Apache Lucene, and <a
 * href="https://arxiv.org/abs/1908.10396">Accelerating Large-Scale Inference with Anisotropic
 * Vector Quantization </a> by Ruiqi Guo et. al. Also see {@link
 * org.apache.lucene.util.quantization.OptimizedScalarQuantizer}. Some of key features are:
 *
 * <ul>
 *   <li>Estimating the distance between two vectors using their centroid centered distance. This
 *       requires some additional corrective factors, but allows for centroid centering to occur.
 *   <li>Optimized scalar quantization to single bit level of centroid centered vectors.
 *   <li>Asymmetric quantization of vectors, where query vectors are quantized to half-byte (4 bits)
 *       precision (normalized to the centroid) and then compared directly against the single bit
 *       quantized vectors in the index.
 *   <li>Transforming the half-byte quantized query vectors in such a way that the comparison with
 *       single bit vectors can be done with bit arithmetic.
 * </ul>
 *
 * A previous work related to improvements over regular LVQ is <a
 * href="https://arxiv.org/abs/2409.09913">Practical and Asymptotically Optimal Quantization of
 * High-Dimensional Vectors in Euclidean Space for Approximate Nearest Neighbor Search </a> by
 * Jianyang Gao, et. al.
 *
 * <p>The format is stored within two files:
 *
 * <h2>.veq (vector data) file</h2>
 *
 * <p>Stores the quantized vectors in a flat format. Additionally, it stores each vector's
 * corrective factors. At the end of the file, additional information is stored for vector ordinal
 * to centroid ordinal mapping and sparse vector information.
 *
 * <ul>
 *   <li>For each vector:
 *       <ul>
 *         <li><b>[byte]</b> the quantized values. Each dimension may be up to 8 bits, and multiple
 *             dimensions may be packed into a single byte.
 *         <li><b>[float]</b> the optimized quantiles and an additional similarity dependent
 *             corrective factor.
 *         <li><b>[int]</b> the sum of the quantized components
 *       </ul>
 *   <li>After the vectors, sparse vector information keeping track of monotonic blocks.
 * </ul>
 *
 * <h2>.vemq (vector metadata) file</h2>
 *
 * <p>Stores the metadata for the vectors. This includes the number of vectors, the number of
 * dimensions, and file offset information.
 *
 * <ul>
 *   <li><b>int</b> the field number
 *   <li><b>int</b> the vector encoding ordinal XXX wut is this?
 *   <li><b>int</b> the vector similarity ordinal
 *   <li><b>vint</b> the vector dimensions
 *   <li><b>vlong</b> the offset to the vector data in the .veb file
 *   <li><b>vlong</b> the length of the vector data in the .veb file
 *   <li><b>vint</b> the number of vectors
 *   <li><b>[float]</b> the centroid
 *   <li><b>float</b> the centroid square magnitude
 *   <li>The sparse vector information, if required, mapping vector ordinal to doc ID
 * </ul>
 */
public class Lucene104ScalarQuantizedVectorsFormat extends FlatVectorsFormat {
  public static final String QUANTIZED_VECTOR_COMPONENT = "QVEC";
  public static final String NAME = "Lucene104ScalarQuantizedVectorsFormat";

  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;
  static final String META_CODEC_NAME = "Lucene104ScalarQuantizedVectorsFormatMeta";
  static final String VECTOR_DATA_CODEC_NAME = "Lucene104ScalarQuantizedVectorsFormatData";
  static final String META_EXTENSION = "vemq";
  static final String VECTOR_DATA_EXTENSION = "veq";
  static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;

  private static final FlatVectorsFormat rawVectorFormat =
      new Lucene99FlatVectorsFormat(FlatVectorScorerUtil.getLucene99FlatVectorsScorer());

  private static final Lucene104ScalarQuantizedVectorScorer scorer =
      new Lucene104ScalarQuantizedVectorScorer(FlatVectorScorerUtil.getLucene99FlatVectorsScorer());

  /** Creates a new instance with the default number of vectors per cluster. */
  public Lucene104ScalarQuantizedVectorsFormat() {
    super(NAME);
  }

  @Override
  public FlatVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new Lucene104ScalarQuantizedVectorsWriter(
        scorer, rawVectorFormat.fieldsWriter(state), state);
  }

  @Override
  public FlatVectorsReader fieldsReader(SegmentReadState state) throws IOException {
    throw new UnsupportedOperationException("XXX TODO");
    // return new Lucene102BinaryQuantizedVectorsReader(
    //    state, rawVectorFormat.fieldsReader(state), scorer);
  }

  @Override
  public int getMaxDimensions(String fieldName) {
    return 1024;
  }

  @Override
  public String toString() {
    return "Lucene104ScalarQuantizedVectorsFormat(name="
        + NAME
        + ", flatVectorScorer="
        + scorer
        + ", rawVectorFormat="
        + rawVectorFormat
        + ")";
  }
}
