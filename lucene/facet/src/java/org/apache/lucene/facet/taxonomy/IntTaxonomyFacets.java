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

import java.io.IOException;
import java.util.Comparator;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.TopOrdAndIntQueue;
import org.apache.lucene.facet.TopOrdAndNumberQueue;
import org.apache.lucene.internal.hppc.IntIntHashMap;

/** Base class for all taxonomy-based facets that aggregate to int. */
abstract class IntTaxonomyFacets extends TaxonomyFacets {

  /** Aggregation function used for combining values. */
  protected final AssociationAggregationFunction aggregationFunction;

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
    super(indexFieldName, taxoReader, config, fc);
    this.aggregationFunction = aggregationFunction;
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
  protected Number getAggregationValue(int ordinal) {
    return getValue(ordinal);
  }

  @Override
  protected Number aggregate(Number existingVal, Number newVal) {
    return aggregationFunction.aggregate((int) existingVal, (int) newVal);
  }

  @Override
  protected void updateValueFromRollup(int ordinal, int childOrdinal) throws IOException {
    super.updateValueFromRollup(ordinal, childOrdinal);
    int currentValue = getValue(ordinal);
    int newValue = aggregationFunction.aggregate(currentValue, rollup(childOrdinal));
    setValue(ordinal, newValue);
  }

  private int rollup(int ord) throws IOException {
    ParallelTaxonomyArrays.IntArray children = getChildren();
    ParallelTaxonomyArrays.IntArray siblings = getSiblings();
    int aggregatedValue = 0;
    while (ord != TaxonomyReader.INVALID_ORDINAL) {
      updateValueFromRollup(ord, children.get(ord));
      aggregatedValue = aggregationFunction.aggregate(aggregatedValue, getValue(ord));
      ord = siblings.get(ord);
    }
    return aggregatedValue;
  }

  @Override
  protected void setIncomingValue(TopOrdAndNumberQueue.OrdAndValue incomingOrdAndValue, int ord) {
    ((TopOrdAndIntQueue.OrdAndInt) incomingOrdAndValue).value = getValue(ord);
  }

  protected class IntAggregatedValue extends AggregatedValue {
    private int value;

    public IntAggregatedValue(int value) {
      this.value = value;
    }

    @Override
    public void aggregate(int ord) {
      value = aggregationFunction.aggregate(value, getValue(ord));
    }

    @Override
    public Number get() {
      return value;
    }
  }

  @Override
  protected AggregatedValue newAggregatedValue() {
    return new IntAggregatedValue(0);
  }
}
