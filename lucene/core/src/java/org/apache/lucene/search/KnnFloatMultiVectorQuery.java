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

import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.search.knn.MultiVectorKnnCollectorManager;

/**
 * Adapts {@link KnnFloatVectorQuery} to run on multivalued vector fields.
 *
 * <p>Uses {@link MultiVectorKnnCollectorManager} to collect docs based on the best score from all
 * its vectors.
 */
public class KnnFloatMultiVectorQuery extends KnnFloatVectorQuery {

  // TODO: we could update KnnFloatVectorQuery to always use MultiVectorKnnCollectorManager
  //  creating a separate class for now to not impact existing query path (and make future
  // extensions simple).

  /**
   * Find the <code>k</code> nearest documents to the target vector according to the vectors in the
   * given field. <code>target</code> vector.
   *
   * @param field a field that has been indexed as a {@link KnnFloatVectorField}.
   * @param target the target of the search
   * @param k the number of documents to find
   * @throws IllegalArgumentException if <code>k</code> is less than 1
   */
  public KnnFloatMultiVectorQuery(String field, float[] target, int k) {
    super(field, target, k);
  }

  /**
   * Find the <code>k</code> nearest documents to the target vector according to the vectors in the
   * given field. <code>target</code> vector.
   *
   * @param field a field that has been indexed as a {@link KnnFloatVectorField}.
   * @param target the target of the search
   * @param k the number of documents to find
   * @param filter a filter applied before the vector search
   * @throws IllegalArgumentException if <code>k</code> is less than 1
   */
  public KnnFloatMultiVectorQuery(String field, float[] target, int k, Query filter) {
    super(field, target, k, filter);
  }

  @Override
  protected KnnCollectorManager getKnnCollectorManager(int k, IndexSearcher searcher) {
    return new MultiVectorKnnCollectorManager(k, searcher);
  }
}
