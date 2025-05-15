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
<<<<<<<< HEAD:lucene/core/src/java/org/apache/lucene/search/VectorScorer.java
package org.apache.lucene.search;

import java.io.IOException;

/**
 * Computes the similarity score between a given query vector and different document vectors. This
 * is used for exact searching and scoring
 *
 * @lucene.experimental
 */
public interface VectorScorer {

  /**
   * Compute the score for the current document ID.
   *
   * @return the score for the current document ID
   * @throws IOException if an exception occurs during score computation
   */
  float score() throws IOException;

  /**
   * @return a {@link DocIdSetIterator} over the documents.
   */
  DocIdSetIterator iterator();
========
package org.apache.lucene.sandbox.facet.labels;

import java.io.IOException;
import org.apache.lucene.facet.taxonomy.FacetLabel;

/**
 * Ordinal to label mapping interface.
 *
 * <p>TODO: move FacetLabel out of taxonomy folder to use it for any facets, not just taxonomy?
 *
 * @lucene.experimental
 */
public interface OrdToLabel {
  /** get label of one ord TODO: what do we return when ordinal is not valid? null? */
  FacetLabel getLabel(int ordinal) throws IOException;

  /** get labels for multiple ords */
  FacetLabel[] getLabels(int[] ordinals) throws IOException;
>>>>>>>> origin/branch_10_1:lucene/sandbox/src/java/org/apache/lucene/sandbox/facet/labels/OrdToLabel.java
}
