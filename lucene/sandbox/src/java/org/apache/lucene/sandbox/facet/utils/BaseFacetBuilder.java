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
package org.apache.lucene.sandbox.facet.utils;

import java.io.IOException;
import java.util.function.Supplier;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.sandbox.facet.FacetFieldCollectorManager;
import org.apache.lucene.sandbox.facet.cutters.FacetCutter;
import org.apache.lucene.sandbox.facet.iterators.ComparableSupplier;
import org.apache.lucene.sandbox.facet.iterators.LengthOrdinalIterator;
import org.apache.lucene.sandbox.facet.iterators.OrdinalIterator;
import org.apache.lucene.sandbox.facet.iterators.TopnOrdinalIterator;
import org.apache.lucene.sandbox.facet.labels.OrdToLabel;
import org.apache.lucene.sandbox.facet.recorders.CountFacetRecorder;

/**
 * Base implementation for {@link FacetBuilder}, provides common functionality that can be shared
 * between different facet types.
 *
 * @lucene.experimental
 */
abstract class BaseFacetBuilder<C extends BaseFacetBuilder<C>> extends FacetBuilder {
  final String dimension;
  final String[] path;
  CountFacetRecorder countRecorder;
  private FacetFieldCollectorManager<CountFacetRecorder> collectorManager;
  private int topN = -1;
  // Sort results by count by default
  Supplier<ComparableSupplier<?>> sortOrderSupplier = () -> ComparableUtils.byCount(countRecorder);

  BaseFacetBuilder(String dimension, String... path) {
    this.dimension = dimension;
    this.path = path;
  }

  /** Request top N results only. If not set, return all results. */
  public final C withTopN(int n) {
    this.topN = n;
    return self();
  }

  /** Sort results by count. */
  public final C withSortByCount() {
    this.sortOrderSupplier = () -> ComparableUtils.byCount(countRecorder);
    return self();
  }

  /**
   * Expert: sort results by facet ordinal. Facet ordinal meaning depends on {@link FacetCutter}
   * implementation.
   */
  final C withSortByOrdinal() {
    this.sortOrderSupplier = ComparableUtils::byOrdinal;
    return self();
  }

  /** Get {@link FacetCutter} to be used for this facet builder. */
  abstract FacetCutter getFacetCutter();

  /** Get {@link OrdToLabel} to be used for this facet builder. */
  abstract OrdToLabel getOrdToLabel();

  /**
   * Child classes should override this method to return only face ordinals that match the request
   * before topN cutoff.
   */
  OrdinalIterator getMatchingOrdinalIterator() throws IOException {
    return countRecorder.recordedOrds();
  }

  /** Get value for {@link LabelAndValue}. */
  final Number getValue(int facetOrd) {
    // TODO: support other aggregations
    return countRecorder.getCount(facetOrd);
  }

  /** Get value for {@link FacetResult#value} */
  abstract Number getOverallValue() throws IOException;

  @Override
  FacetBuilder initOrReuseCollector(FacetBuilder similar) {
    // share recorders between FacetBuilders that share CollectorManager
    // TODO: add support for other aggregation types, e.g. float/int associations
    //       and long aggregations
    if (similar instanceof BaseFacetBuilder<?> castedSimilar) {
      this.countRecorder = castedSimilar.countRecorder;
      return similar;
    } else {
      this.countRecorder = new CountFacetRecorder();
      this.collectorManager = new FacetFieldCollectorManager<>(getFacetCutter(), countRecorder);
      return this;
    }
  }

  @Override
  final FacetFieldCollectorManager<?> getCollectorManager() {
    return this.collectorManager;
  }

  @Override
  public final FacetResult getResult() {
    assert countRecorder != null : "must not be called before collect";
    OrdinalIterator ordinalIterator;
    try {
      LengthOrdinalIterator lengthOrdinalIterator =
          new LengthOrdinalIterator(getMatchingOrdinalIterator());
      ordinalIterator = lengthOrdinalIterator;

      ComparableSupplier<?> comparableSupplier = sortOrderSupplier.get();
      int[] ordinalsArray;
      if (topN != -1) {
        ordinalsArray =
            new TopnOrdinalIterator<>(ordinalIterator, comparableSupplier, topN).toArray();
      } else {
        ordinalsArray = ordinalIterator.toArray();
        ComparableUtils.sort(ordinalsArray, comparableSupplier);
      }

      OrdToLabel ordToLabel = getOrdToLabel();
      FacetLabel[] labels = ordToLabel.getLabels(ordinalsArray);

      LabelAndValue[] labelsAndValues = new LabelAndValue[labels.length];
      for (int i = 0; i < ordinalsArray.length; i++) {
        labelsAndValues[i] =
            new LabelAndValue(
                labels[i].lastComponent(),
                getValue(ordinalsArray[i]),
                countRecorder.getCount(ordinalsArray[i]));
      }
      return new FacetResult(
          dimension, path, getOverallValue(), labelsAndValues, lengthOrdinalIterator.length());

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Util method to be able to extend this class. */
  abstract C self();
}
