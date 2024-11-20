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
 * KnnCollector is a knn collector used for gathering kNN results and providing topDocs from the
 * gathered neighbors
 *
 * @lucene.experimental
 */
public interface KnnCollector {

  /**
   * If search visits too many documents, the results collector will terminate early. Usually, this
   * is due to some restricted filter on the document set.
   *
   * <p>When collection is earlyTerminated, the results are not a correct representation of k
   * nearest neighbors.
   *
   * @return is the current result set marked as incomplete?
   */
  boolean earlyTerminated();

  /**
   * @param count increments the visited vector count, must be greater than 0.
   */
  void incVisitedCount(int count);

  /**
   * @return the current visited vector count
   */
  long visitedCount();

  /**
   * @return the visited vector limit
   */
  long visitLimit();

  /**
   * @return the expected number of collected results
   */
  int k();

  /**
   * Collect the provided docId and include in the result set.
   *
   * @param docId of the vector to collect
   * @param similarity its calculated similarity
   * @return true if the vector is collected
   */
  boolean collect(int docId, float similarity);

  /**
   * This method is utilized during search to ensure only competitive results are explored.
   *
   * <p>Consequently, if this results collector wants to collect `k` results, this should return
   * {@link Float#NEGATIVE_INFINITY} when not full.
   *
   * <p>When full, the minimum score should be returned.
   *
   * @return the current minimum competitive similarity in the collection
   */
  float minCompetitiveSimilarity();

  /**
   * This drains the collected nearest kNN results and returns them in a new {@link TopDocs}
   * collection, ordered by score descending. NOTE: This is generally a destructive action and the
   * collector should not be used after topDocs() is called.
   *
   * @return The collected top documents
   */
  TopDocs topDocs();
}
