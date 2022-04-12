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
package org.apache.lucene.facet.taxonomy;

/**
 * Specify aggregation logic used in {@link TaxonomyFacetIntAssociations} and {@link
 * TaxonomyFacetFloatAssociations}.
 */
public abstract class AssociationAggregationFunction {

  // TODO: Would be nice to add support for MIN as well here, but there are a number of places
  // in our facet implementations where we attribute special meaning to 0 and assume that valid
  // values are always positive. I think we'd want to break that assumption for MIN to really
  // make sense.

  /** Sole constructor. */
  protected AssociationAggregationFunction() {}

  /** Implement aggregation logic for integers */
  public abstract int aggregate(int existingVal, int newVal);

  /** Implement aggregation logic for floats */
  public abstract float aggregate(float existingVal, float newVal);

  /** Aggregation that computes the maximum value */
  public static final AssociationAggregationFunction MAX =
      new AssociationAggregationFunction() {
        @Override
        public int aggregate(int existingVal, int newVal) {
          return Math.max(existingVal, newVal);
        }

        @Override
        public float aggregate(float existingVal, float newVal) {
          return Math.max(existingVal, newVal);
        }
      };

  /** Aggregation that computes the sum */
  public static final AssociationAggregationFunction SUM =
      new AssociationAggregationFunction() {
        @Override
        public int aggregate(int existingVal, int newVal) {
          return existingVal + newVal;
        }

        @Override
        public float aggregate(float existingVal, float newVal) {
          return existingVal + newVal;
        }
      };
}
