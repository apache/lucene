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

/**
 * KnnResults is a collector for gathering kNN results and providing topDocs from the gathered
 * neighbors
 */
public abstract class KnnResults {

  protected int visitedCount;
  private final int visitLimit;
  private final int k;

  protected KnnResults(int k, int visitLimit) {
    this.visitLimit = visitLimit;
    this.k = k;
  }

  /**
   * @return is the current result set marked as incomplete?
   */
  public final boolean incomplete() {
    return visitedCount >= visitLimit;
  }

  public final void incVisitedCount(int count) {
    assert count > 0;
    this.visitedCount += count;
  }

  /**
   * @return the current visited count
   */
  public final int visitedCount() {
    return visitedCount;
  }

  public final int visitLimit() {
    return visitLimit;
  }

  public final int k() {
    return k;
  }

  /**
   * Collect the provided vectorId and include in the result set.
   *
   * @param vectorId the vector to collect
   * @param similarity its calculated similarity
   * @return true if the vector is collected
   */
  public abstract boolean collect(int vectorId, float similarity);

  /**
   * @return Is the current result set considered full
   */
  public abstract boolean isFull();

  /**
   * @return the current minimum similarity in the collection
   */
  public abstract float minSimilarity();

  /**
   * This drains the collected nearest kNN results and returns them in a new {@link TopDocs}
   * collection, ordered by score descending
   *
   * @return The collected top documents
   */
  public abstract TopDocs topDocs();
}
