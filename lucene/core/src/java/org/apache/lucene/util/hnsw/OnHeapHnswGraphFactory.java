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

package org.apache.lucene.util.hnsw;

import java.io.IOException;
import java.util.Map;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;

/** A factory that creates OnHeapHnswGraphs. */
public class OnHeapHnswGraphFactory implements HnswGraphFactory {
  public static OnHeapHnswGraphFactory instance = new OnHeapHnswGraphFactory();

  @Override
  public <T> OnHeapHnswGraphBuilder<T> createBuilder(
      RandomAccessVectorValues<T> vectors,
      VectorEncoding vectorEncoding,
      VectorSimilarityFunction similarityFunction,
      int M,
      int beamWidth,
      long seed)
      throws IOException {
    return new OnHeapHnswGraphBuilder<>(
        vectors, vectorEncoding, similarityFunction, M, beamWidth, seed);
  }

  public <T> OnHeapHnswGraphBuilder<T> createBuilder(
      RandomAccessVectorValues<T> vectors,
      VectorEncoding vectorEncoding,
      VectorSimilarityFunction similarityFunction,
      int M,
      int beamWidth,
      long seed,
      HnswGraph initializerGraph,
      Map<Integer, Integer> oldToNewOrdinalMap)
      throws IOException {
    OnHeapHnswGraphBuilder<T> hnswGraphBuilder =
        new OnHeapHnswGraphBuilder<>(
            vectors, vectorEncoding, similarityFunction, M, beamWidth, seed);
    hnswGraphBuilder.initializeFromGraph(initializerGraph, oldToNewOrdinalMap);
    return hnswGraphBuilder;
  }

  @Override
  public HnswGraph create(int maxConnections) {
    return new OnHeapHnswGraph(maxConnections);
  }
}
