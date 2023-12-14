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

import com.carrotsearch.hppc.IntIntHashMap;
import com.carrotsearch.hppc.cursors.IntIntCursor;
import java.io.IOException;
import java.util.Comparator;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.FacetsConfig.DimConfig;
import org.apache.lucene.facet.TopOrdAndIntQueue;
import org.apache.lucene.facet.TopOrdAndNumberQueue;

/** Base class for all taxonomy-based facets that aggregate to int. */
abstract class IntTaxonomyFacets extends TaxonomyFacetAssociations {

  /** Dense ordinal values. */
  int[] values;

  /** Sparse ordinal values. */
  IntIntHashMap sparseValues;

  /** Sole constructor. */
  IntTaxonomyFacets(
      String indexFieldName,
      TaxonomyReader taxoReader,
      FacetsConfig config,
      AssociationAggregationFunction aggregationFunction,
      FacetsCollector fc)
      throws IOException {
    super(indexFieldName, taxoReader, config, aggregationFunction, fc);
    valueComparator = Comparator.comparingInt(o -> (int) o);
  }

  @Override
  protected void initializeValueCounters() {
    if (initialized) {
      return;
    }
    super.initializeValueCounters();

    assert sparseValues == null && values == null;
    if (sparseCounts != null) {
      sparseValues = new IntIntHashMap();
    } else {
      values = new int[taxoReader.getSize()];
    }
  }

  /** Set the value associated with this ordinal to {@code newValue}. */
  void setValue(int ordinal, int newValue) {
    if (sparseValues != null) {
      sparseValues.put(ordinal, newValue);
    } else {
      values[ordinal] = newValue;
    }
  }

  /** Get the value associated with this ordinal. */
  int getValue(int ordinal) {
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
    int currentValue = getValue(ordinal);
    int newValue = aggregationFunction.aggregate(currentValue, rollup(childOrdinal));
    setValue(ordinal, newValue);
  }

  @Override
  protected Number aggregate(Number existingVal, Number newVal) {
    return aggregationFunction.aggregate((int) existingVal, (int) newVal);
  }

  private int rollup(int ord) throws IOException {
    int[] children = getChildren();
    int[] siblings = getSiblings();
    int aggregatedValue = 0;
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
    TopOrdAndNumberQueue q = new TopOrdAndIntQueue(Math.min(taxoReader.getSize(), topN));
    int bottomValue = 0;
    int bottomOrd = Integer.MAX_VALUE;

    int aggregatedValue = 0;
    int childCount = 0;
    TopOrdAndNumberQueue.OrdAndValue reuse = null;

    // TODO: would be faster if we had a "get the following children" API?  then we
    // can make a single pass over the hashmap
    if (sparseValues != null) {
      for (IntIntCursor c : sparseValues) {
        int value = c.value;
        int ord = c.key;
        if (parents[ord] == pathOrd && value > 0) {
          aggregatedValue = aggregationFunction.aggregate(aggregatedValue, value);
          childCount++;
          if (value > bottomValue || (value == bottomValue && ord < bottomOrd)) {
            if (reuse == null) {
              reuse = new TopOrdAndIntQueue.OrdAndValue();
            }
            reuse.ord = ord;
            reuse.value = value;
            reuse = q.insertWithOverflow(reuse);
            if (q.size() == topN) {
              bottomValue = (int) q.top().value;
              bottomOrd = q.top().ord;
            }
          }
        }
      }
    } else {
      int[] children = getChildren();
      int[] siblings = getSiblings();
      int ord = children[pathOrd];
      while (ord != TaxonomyReader.INVALID_ORDINAL) {
        int value = values[ord];
        if (value > 0) {
          aggregatedValue = aggregationFunction.aggregate(aggregatedValue, value);
          childCount++;
          if (value > bottomValue || (value == bottomValue && ord < bottomOrd)) {
            if (reuse == null) {
              reuse = new TopOrdAndIntQueue.OrdAndValue();
            }
            reuse.ord = ord;
            reuse.value = value;
            reuse = q.insertWithOverflow(reuse);
            if (q.size() == topN) {
              bottomValue = (int) q.top().value;
              bottomOrd = q.top().ord;
            }
          }
        }
        ord = siblings[ord];
      }
    }

    if (dimConfig.multiValued) {
      if (dimConfig.requireDimCount) {
        aggregatedValue = getValue(pathOrd);
      } else {
        // Our sum'd value is not correct, in general:
        aggregatedValue = -1;
      }
    }

    return new TopChildrenForPath(aggregatedValue, childCount, q);
  }
}
