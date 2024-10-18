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
package org.apache.lucene.codecs.lucene101;

import static org.apache.lucene.util.quantization.BQSpaceUtils.constSqrt;

import java.io.IOException;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.quantization.BQSpaceUtils;
import org.apache.lucene.util.quantization.BinaryQuantizer;

/**
 * A version of {@link ByteVectorValues}, but additionally retrieving score correction values offset
 * for binarization quantization scores.
 *
 * @lucene.experimental
 */
public abstract class BinarizedByteVectorValues extends ByteVectorValues {

  /**
   * Retrieve the corrective terms for the given vector ordinal. For the dot-product family of
   * distances, the corrective terms are, in order
   *
   * <ul>
   *   <li>the dot-product of the normalized, centered vector with its binarized self
   *   <li>the norm of the centered vector
   *   <li>the dot-product of the vector with the centroid
   * </ul>
   *
   * For euclidean:
   *
   * <ul>
   *   <li>The euclidean distance to the centroid
   *   <li>The sum of the dimensions divided by the vector norm
   * </ul>
   *
   * @param vectorOrd the vector ordinal
   * @return the corrective terms
   * @throws IOException if an I/O error occurs
   */
  public abstract float[] getCorrectiveTerms(int vectorOrd) throws IOException;

  /**
   * @return the quantizer used to quantize the vectors
   */
  public abstract BinaryQuantizer getQuantizer();

  public abstract float[] getCentroid() throws IOException;

  int discretizedDimensions() {
    return BQSpaceUtils.discretize(dimension(), 64);
  }

  float sqrtDimensions() {
    return (float) constSqrt(dimension());
  }

  float maxX1() {
    return (float) (1.9 / constSqrt(discretizedDimensions() - 1.0));
  }

  /**
   * Return a {@link VectorScorer} for the given query vector.
   *
   * @param query the query vector
   * @return a {@link VectorScorer} instance or null
   */
  public abstract VectorScorer scorer(float[] query) throws IOException;

  @Override
  public abstract BinarizedByteVectorValues copy() throws IOException;

  float getCentroidDP() throws IOException {
    // this only gets executed on-merge
    float[] centroid = getCentroid();
    return VectorUtil.dotProduct(centroid, centroid);
  }
}
