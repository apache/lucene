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

package org.apache.lucene.search;

import org.apache.lucene.index.VectorSimilarityFunction;

/**
 * Interface to define the similarity function between multi-vectors
 *
 * @lucene.experimental
 */
public interface MultiVectorSimilarity {

  /**
   * Computes similarity between two multi-vectors using provided {@link VectorSimilarityFunction}
   *
   * <p>Provided multi-vectors can have varying number of composing token vectors, but their token
   * vectors should have the same dimension.
   *
   * @param outer a multi-vector
   * @param inner another multi-vector
   * @return similarity score between two multi-vectors
   */
  float compare(
      float[][] outer, float[][] inner, VectorSimilarityFunction vectorSimilarityFunction);
}
