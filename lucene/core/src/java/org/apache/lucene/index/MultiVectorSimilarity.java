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
package org.apache.lucene.index;

/** Defines comparison functions for multi-vector similarity */
public interface MultiVectorSimilarity {

  /**
   * Calculates a similarity score between the two multi-vectors with a specified function. Higher
   * similarity scores correspond to closer vectors.
   *
   * @param t1 a multi-vector with non-empty vectors All vector values are concatenated in a single
   *     packed array.
   * @param t2 another multi-vector, vectors of the same dimension as t1. All vector values are
   *     concatenated in a single packed array.
   * @return the value of the similarity function applied to the two multi-vectors
   */
  float compare(float[] t1, float[] t2, int dimension);

  /**
   * Calculates a similarity score between the two multi-vectors with a specified function. Higher
   * similarity scores correspond to closer vectors.
   *
   * @param t1 a multi-vector with non-empty vectors. All vector values are concatenated in a single
   *     packed array.
   * @param t2 another multi-vector, vectors of the same dimension as t1. All vector values are
   *     concatenated in a single packed array.
   * @return the value of the similarity function applied to the two multi-vector
   */
  float compare(byte[] t1, byte[] t2, int dimension);
}
