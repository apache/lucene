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
package org.apache.lucene.index;

/**
 * Classification predicates over an {@link IndexableFieldType} that name how a field participates in
 * indexing. These exist so the row-vs-column routing in {@link IndexingChain} is declared once
 * rather than re-derived inline at each call site.
 *
 * <p>The key distinction is between the two kinds of term inversion:
 *
 * <ul>
 *   <li><b>row-bound inversion</b> — a field with term vectors. Term vectors are written per
 *       document inside a {@code startDocument}/{@code finishDocument} frame, so this inversion has
 *       a genuine per-document obligation and must run in the row pass.
 *   <li><b>column-eligible inversion</b> — an indexed field without term vectors. Its only outputs
 *       are postings and norms, both of which accumulate in memory and flush at segment end with no
 *       within-document cross-field dependency, so this inversion could run column-major.
 * </ul>
 *
 * <p>{@link #columnEligibleInversion} is the seam a follow-up change will switch on to move that
 * inversion into the columnar pass; today nothing routes on it.
 */
final class IndexingFeatures {

  private IndexingFeatures() {}

  /** Whether the field is indexed (its terms are inverted into postings). */
  static boolean indexed(IndexableFieldType fieldType) {
    return fieldType.indexOptions() != IndexOptions.NONE;
  }

  /** Whether the field stores term vectors. */
  static boolean hasTermVectors(IndexableFieldType fieldType) {
    return fieldType.storeTermVectors();
  }

  /**
   * Inversion that has a per-document obligation and must run in the row pass: an indexed field with
   * term vectors.
   */
  static boolean rowBoundInversion(IndexableFieldType fieldType) {
    return indexed(fieldType) && hasTermVectors(fieldType);
  }

  /**
   * Inversion that has no per-document obligation and could run column-major: an indexed field
   * without term vectors. This is the seam for the follow-up columnar-inversion change.
   */
  static boolean columnEligibleInversion(IndexableFieldType fieldType) {
    return indexed(fieldType) && hasTermVectors(fieldType) == false;
  }

  /**
   * Whether the field has any row-oriented feature (stored fields or term inversion) and therefore
   * participates in the row pass.
   */
  static boolean rowEligible(IndexableFieldType fieldType) {
    return fieldType.stored() || indexed(fieldType);
  }
}
