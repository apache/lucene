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
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.TopOrdAndFloatNumberQueue;
import org.apache.lucene.facet.TopOrdAndNumberQueue;
import org.apache.lucene.internal.hppc.IntFloatHashMap;

/**
 * Base class for all taxonomy-based facets that aggregate to a per-ords float[].
 *
 * @deprecated Visibility of this class will be reduced to pkg-private in a future version. This
 *     class is meant to host common code as an internal implementation detail to taxonomy
 *     faceting,and is not intended as an extension point for user-created {@code Facets}
 *     implementations. If your code is relying on this, please migrate necessary functionality down
 *     into your own class.
 */
@Deprecated
public abstract class FloatTaxonomyFacets extends TaxonomyFacets {

  /** Aggregation function used for combining values. */
  protected final AssociationAggregationFunction aggregationFunction;

  /** Per-ordinal value. */
  protected float[] values;

  /** Sparse ordinal values. */
  IntFloatHashMap sparseValues;

  /**
   * Constructor that defaults the aggregation function to {@link
   * AssociationAggregationFunction#SUM}.
   */
  protected FloatTaxonomyFacets(
      String indexFieldName, TaxonomyReader taxoReader, FacetsConfig config) throws IOException {
    super(indexFieldName, taxoReader, config);
    this.aggregationFunction = AssociationAggregationFunction.SUM;
    values = new float[taxoReader.getSize()];
  }

  /** Constructor that uses the provided aggregation function. */
  protected FloatTaxonomyFacets(
      String indexFieldName,
      TaxonomyReader taxoReader,
      AssociationAggregationFunction aggregationFunction,
      FacetsConfig config,
      FacetsCollector fc)
      throws IOException {
    super(indexFieldName, taxoReader, config, fc);
    this.aggregationFunction = aggregationFunction;
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
  protected Number getAggregationValue(int ordinal) {
    return getValue(ordinal);
  }

  @Override
  protected Number aggregate(Number existingVal, Number newVal) {
    return aggregationFunction.aggregate(existingVal.floatValue(), newVal.floatValue());
  }

  @Override
  protected void updateValueFromRollup(int ordinal, int childOrdinal) throws IOException {
    super.updateValueFromRollup(ordinal, childOrdinal);
    float currentValue = getValue(ordinal);
    float newValue = aggregationFunction.aggregate(currentValue, rollup(childOrdinal));
    setValue(ordinal, newValue);
  }

  @Override
  protected TopOrdAndNumberQueue makeTopOrdAndNumberQueue(int topN) {
    return new TopOrdAndFloatNumberQueue(Math.min(taxoReader.getSize(), topN));
  }

  @Override
  protected Number missingAggregationValue() {
    return -1f;
  }

  private float rollup(int ord) throws IOException {
    ParallelTaxonomyArrays.IntArray children = getChildren();
    ParallelTaxonomyArrays.IntArray siblings = getSiblings();
    float aggregatedValue = 0f;
    while (ord != TaxonomyReader.INVALID_ORDINAL) {
      updateValueFromRollup(ord, children.get(ord));
      aggregatedValue = aggregationFunction.aggregate(aggregatedValue, getValue(ord));
      ord = siblings.get(ord);
    }
    return aggregatedValue;
  }

  @Override
  protected void setIncomingValue(TopOrdAndNumberQueue.OrdAndValue incomingOrdAndValue, int ord) {
    ((TopOrdAndFloatNumberQueue.OrdAndFloat) incomingOrdAndValue).value = getValue(ord);
  }

  /** An accumulator for a float aggregated value. */
  protected class FloatAggregatedValue extends AggregatedValue {
    private float value;

    /** Sole constructor. */
    public FloatAggregatedValue(float value) {
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
    return new FloatAggregatedValue(0f);
  }
}
