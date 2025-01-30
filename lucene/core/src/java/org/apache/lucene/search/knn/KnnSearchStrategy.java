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

/**
 * KnnSearchStrategy is a strategy for kNN search, providing additional search strategy
 * configuration
 *
 * @lucene.experimental
 */
public interface KnnSearchStrategy {

  /** A strategy for kNN search that uses a set of entry points to start the search */
  class Seeded implements KnnSearchStrategy {
    private final DocIdSetIterator entryPoints;
    private final int numberOfEntryPoints;

    public Seeded(DocIdSetIterator entryPoints, int numberOfEntryPoints) {
      if (numberOfEntryPoints < 0) {
        throw new IllegalArgumentException("numberOfEntryPoints must be >= 0");
      }
      this.numberOfEntryPoints = numberOfEntryPoints;
      if (numberOfEntryPoints > 0 && entryPoints == null) {
        throw new IllegalArgumentException("entryPoints must not be null");
      }
      this.entryPoints = entryPoints == null ? DocIdSetIterator.empty() : entryPoints;
    }

    /**
     * Iterator of valid entry points for the kNN search
     *
     * @return DocIdSetIterator of entry points
     */
    public DocIdSetIterator entryPoints() {
      return entryPoints;
    }

    /**
     * Number of valid entry points for the kNN search
     *
     * @return number of entry points
     */
    public int numberOfEntryPoints() {
      return numberOfEntryPoints;
    }
  }
}
