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

import com.carrotsearch.hppc.IntFloatHashMap;
import java.io.IOException;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.FacetsConfig.DimConfig;
import org.apache.lucene.facet.TopOrdAndFloatQueue;

/** Base class for all taxonomy-based facets that aggregate to float. */
abstract class FloatTaxonomyFacets extends TaxonomyFacetAssociations {

  /** Dense ordinal values. */
  float[] values;

  /** Sparse ordinal values. */
  IntFloatHashMap sparseValues;

  /** Sole constructor. */
  FloatTaxonomyFacets(
      String indexFieldName,
      TaxonomyReader taxoReader,
      AssociationAggregationFunction aggregationFunction,
      FacetsConfig config,
      FacetsCollector fc)
      throws IOException {
    super(indexFieldName, taxoReader, config, aggregationFunction, fc);
    valueComparator = (o1, o2) -> Float.compare(o1.floatValue(), o2.floatValue());
  }

  @Override
  protected void initializeValueCounters() {
    if (initialized) {
      return;
    }
    super.initializeValueCounters();

    assert sparseValues == null && values == null;
    if (sparseCounts != null) {
      sparseValues = new IntFloatHashMap();
    } else {
      values = new float[taxoReader.getSize()];
    }
  }

  /** Set the value associated with this ordinal to {@code newValue}. */
  void setValue(int ordinal, float newValue) {
    if (sparseValues != null) {
      sparseValues.put(ordinal, newValue);
    } else {
      values[ordinal] = newValue;
    }
  }

  /** Get the value associated with this ordinal. */
  float getValue(int ordinal) {
    if (sparseValues != null) {
      return sparseValues.get(ordinal);
    } else {
      return values[ordinal];
    }
  }

  @Override
  Number getNumberValue(int ordinal) {
    return getValue(ordinal);
  }

  @Override
  protected void updateValue(int ordinal, int childOrdinal) throws IOException {
    float currentValue = getValue(ordinal);
    float newValue = aggregationFunction.aggregate(currentValue, rollup(childOrdinal));
    setValue(ordinal, newValue);
  }

  @Override
  protected Number aggregate(Number existingVal, Number newVal) {
    return aggregationFunction.aggregate(existingVal.floatValue(), newVal.floatValue());
  }

  private float rollup(int ord) throws IOException {
    int[] children = getChildren();
    int[] siblings = getSiblings();
    float aggregatedValue = 0f;
    while (ord != TaxonomyReader.INVALID_ORDINAL) {
      updateValue(ord, children[ord]);
      aggregatedValue = aggregationFunction.aggregate(aggregatedValue, getValue(ord));
      ord = siblings[ord];
    }
    return aggregatedValue;
  }

  /**
   * Determine the top-n children for a specified dimension + path. Results are in an intermediate
   * form.
   */
  @Override
  protected TopChildrenForPath getTopChildrenForPath(DimConfig dimConfig, int pathOrd, int topN)
      throws IOException {

    TopOrdAndFloatQueue q = new TopOrdAndFloatQueue(Math.min(taxoReader.getSize(), topN));
    float bottomValue = 0;
    int bottomOrd = Integer.MAX_VALUE;

    int[] children = getChildren();
    int[] siblings = getSiblings();

    int ord = children[pathOrd];
    float aggregatedValue = 0;
    int childCount = 0;

    TopOrdAndFloatQueue.OrdAndValue reuse = null;
    while (ord != TaxonomyReader.INVALID_ORDINAL) {
      float value = values[ord];
      if (value > 0) {
        aggregatedValue = aggregationFunction.aggregate(aggregatedValue, value);
        childCount++;
        if (value > bottomValue || (value == bottomValue && ord < bottomOrd)) {
          if (reuse == null) {
            reuse = new TopOrdAndFloatQueue.OrdAndValue();
          }
          reuse.ord = ord;
          reuse.value = value;
          reuse = q.insertWithOverflow(reuse);
          if (q.size() == topN) {
            bottomValue = (float) q.top().value;
            bottomOrd = q.top().ord;
          }
        }
      }

      ord = siblings[ord];
    }

    if (dimConfig.multiValued) {
      if (dimConfig.requireDimCount) {
        aggregatedValue = values[pathOrd];
      } else {
        // Our sum'd count is not correct, in general:
        aggregatedValue = -1;
      }
    }
    return new TopChildrenForPath(aggregatedValue, childCount, q);
  }
}
