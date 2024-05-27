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
import org.apache.lucene.facet.TopOrdAndIntNumberQueue;
import org.apache.lucene.facet.TopOrdAndNumberQueue;
import org.apache.lucene.internal.hppc.IntIntHashMap;

/**
 * Base class for all taxonomy-based facets that aggregate to a per-ords int[].
 *
 * @deprecated Visibility of this class will be reduced to pkg-private in a future version. This
 *     class is meant to host common code as an internal implementation detail to {@link
 *     FastTaxonomyFacetCounts} and {@link TaxonomyFacetIntAssociations},and is not intended as an
 *     extension point for user-created {@code Facets} implementations. If your code is relying on
 *     this, please migrate necessary functionality down into your own class.
 */
@Deprecated
public abstract class IntTaxonomyFacets extends TaxonomyFacets {

  /** Aggregation function used for combining values. */
  protected final AssociationAggregationFunction aggregationFunction;

  /**
   * Dense ordinal values.
   *
   * <p>We are making this and {@link #sparseValues} protected for some expert usage. e.g. It can be
   * checked which is being used before a loop instead of calling {@link #increment} for each
   * iteration.
   */
  protected int[] values;

  /**
   * Sparse ordinal values.
   *
   * @see #values for why protected.
   */
  protected IntIntHashMap sparseValues;

  /**
   * Constructor that defaults the aggregation function to {@link
   * AssociationAggregationFunction#SUM}.
   */
  protected IntTaxonomyFacets(
      String indexFieldName, TaxonomyReader taxoReader, FacetsConfig config, FacetsCollector fc)
      throws IOException {
    this(indexFieldName, taxoReader, config, AssociationAggregationFunction.SUM, fc);
  }

  /** Constructor that uses the provided aggregation function. */
  protected IntTaxonomyFacets(
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

  /** Increment the count for this ordinal by 1. */
  protected void increment(int ordinal) {
    increment(ordinal, 1);
  }

  /** Increment the count for this ordinal by {@code amount}.. */
  protected void increment(int ordinal, int amount) {
    if (sparseValues != null) {
      sparseValues.addTo(ordinal, amount);
    } else {
      values[ordinal] += amount;
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
    ((TopOrdAndIntNumberQueue.OrdAndInt) incomingOrdAndValue).value = getValue(ord);
  }

  /** An accumulator for an integer aggregated value. */
  protected class IntAggregatedValue extends AggregatedValue {
    private int value;

    /** Sole constructor. */
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
