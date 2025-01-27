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

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.KnnCollector;

/**
 * HnswKnnCollector is a knn collector used for gathering kNN results and providing topDocs from the
 * gathered neighbors. This collector is focused for HNSW search.
 *
 * @lucene.experimental
 */
public class HnswKnnCollector extends KnnCollector.Decorator {
  private final HnswKnnCollector hnswCollector;

  /**
   * Constructor
   *
   * @param collector the base collector to decorate. If the collector is an instance of
   *     HnswKnnCollector, it will be used as the base collector. Otherwise sane defaults will be
   *     used.
   */
  public HnswKnnCollector(KnnCollector collector) {
    super(collector);
    if (collector instanceof HnswKnnCollector) {
      this.hnswCollector = (HnswKnnCollector) collector;
    } else {
      this.hnswCollector = null;
    }
  }

  /**
   * Iterator of valid entry points for the kNN search
   *
   * @return DocIdSetIterator of entry points, default is empty iterator
   */
  public DocIdSetIterator entryPoints() {
    if (hnswCollector != null) {
      return hnswCollector.entryPoints();
    }
    return DocIdSetIterator.empty();
  }

  /**
   * Number of valid entry points for the kNN search
   *
   * @return number of entry points, default is 0
   */
  public int numberOfEntryPoints() {
    if (hnswCollector != null) {
      return hnswCollector.numberOfEntryPoints();
    }
    return 0;
  }
}
