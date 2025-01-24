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

import java.io.IOException;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.util.hnsw.BlockingFloatHeap;
import org.apache.lucene.util.hnsw.UpdatableScoreKnnCollector;

public class MultiVectorKnnCollectorManager implements KnnCollectorManager {

  // the number of docs to collect
  private final int k;
  // the global score queue used to track the top scores collected across all leaves
  private final BlockingFloatHeap globalScoreQueue;

  public MultiVectorKnnCollectorManager(int k, IndexSearcher indexSearcher) {
    boolean isMultiSegments = indexSearcher.getIndexReader().leaves().size() > 1;
    this.k = k;
    this.globalScoreQueue = isMultiSegments ? new BlockingFloatHeap(k) : null;
  }

  /**
   * Return a new {@link UpdatableScoreKnnCollector} instance.
   *
   * <p>This is capable of collecting multiple score values for the same document (from different
   * matching vector values), and updating score to only retain the highest score value.
   *
   * @param visitedLimit the maximum number of nodes that the search is allowed to visit
   * @param context the leaf reader context
   */
  @Override
  public KnnCollector newCollector(int visitedLimit, LeafReaderContext context) throws IOException {
    if (globalScoreQueue == null) {
      return new UpdatableScoreKnnCollector(k, visitedLimit);
    } else {
      return new MultiLeafKnnCollector(
          k, globalScoreQueue, new UpdatableScoreKnnCollector(k, visitedLimit));
    }
  }
}
