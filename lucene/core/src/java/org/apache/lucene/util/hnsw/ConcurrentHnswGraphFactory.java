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
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;

/** A factory that creates ConcurrentHnswGraphs. */
public class ConcurrentHnswGraphFactory implements HnswGraphFactory {
  public static ConcurrentHnswGraphFactory instance = new ConcurrentHnswGraphFactory();

  @Override
  public <T> ConcurrentHnswGraphBuilder<T> createBuilder(
      RandomAccessVectorValues<T> vectors,
      VectorEncoding vectorEncoding,
      VectorSimilarityFunction similarityFunction,
      int M,
      int beamWidth,
      long seed)
      throws IOException {
    return new ConcurrentHnswGraphBuilder<>(
        vectors, vectorEncoding, similarityFunction, M, beamWidth);
  }

  @Override
  public HnswGraph create(int maxConnections) {
    return new ConcurrentOnHeapHnswGraph(maxConnections);
  }
}
