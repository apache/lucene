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
package org.apache.lucene.search.knn;

/**
 * Provides configuration for HNSW search strategy
 *
 * @lucene.experimental
 */
public record HnswSearchStrategy(float filterRatioLimit) {

  public static final HnswSearchStrategy DEFAULT = new HnswSearchStrategy(0.0f);

  /**
   * Create a new HnswSearchStrategy.
   *
   * @param filterRatioLimit the percentage of vector ordinals that pass the filter. 0.0 means never
   *     use the filtered search strategy, 1.0 means always use the filtered search strategy.
   */
  public HnswSearchStrategy {
    if (filterRatioLimit < 0.0f || filterRatioLimit > 1.0f) {
      throw new IllegalArgumentException("filterRatioLimit must be in the range [0.0, 1.0]");
    }
  }

  /**
   * Whether specialized search heuristics should be used when executing a filtered HNSW search.
   *
   * @param filterRatio the percentage of vector ordinals that pass the filter
   * @return true if specialized filtered search heuristics should be used
   */
  public boolean shouldExecuteOptimizedFilteredSearch(float filterRatio) {
    return filterRatio < filterRatioLimit;
  }
}
