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
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer;

/** Scalar quantized byte vector values */
abstract class QuantizedByteVectorValues extends ByteVectorValues {

  /**
   * Retrieve the corrective terms for the given vector ordinal. For the dot-product family of
   * distances, the corrective terms are, in order
   *
   * <ul>
   *   <li>the lower optimized interval
   *   <li>the upper optimized interval
   *   <li>the dot-product of the non-centered vector with the centroid
   *   <li>the sum of quantized components
   * </ul>
   *
   * For euclidean:
   *
   * <ul>
   *   <li>the lower optimized interval
   *   <li>the upper optimized interval
   *   <li>the l2norm of the centered vector
   *   <li>the sum of quantized components
   * </ul>
   *
   * @param vectorOrd the vector ordinal
   * @return the corrective terms
   * @throws IOException if an I/O error occurs
   */
  public abstract OptimizedScalarQuantizer.QuantizationResult getCorrectiveTerms(int vectorOrd)
      throws IOException;

  /**
   * @return the quantizer used to quantize the vectors
   */
  public abstract OptimizedScalarQuantizer getQuantizer();

  /**
   * @return the scalar encoding used to pack the stored vectors.
   */
  public abstract ScalarEncoding getScalarEncoding();

  /**
   * @return the centroid used to center the vectors prior to quantization
   */
  public abstract float[] getCentroid() throws IOException;

  /**
   * @return the dot product of the centroid.
   */
  public abstract float getCentroidDP() throws IOException;

  /**
   * Return a {@link VectorScorer} for the given query vector.
   *
   * @param query the query vector
   * @return a {@link VectorScorer} instance or null
   */
  public abstract VectorScorer scorer(float[] query) throws IOException;

  @Override
  public abstract QuantizedByteVectorValues copy() throws IOException;
}
