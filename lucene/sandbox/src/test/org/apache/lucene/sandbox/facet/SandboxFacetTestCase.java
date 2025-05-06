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
package org.apache.lucene.sandbox.facet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.sandbox.facet.iterators.CandidateSetOrdinalIterator;
import org.apache.lucene.sandbox.facet.iterators.ComparableSupplier;
import org.apache.lucene.sandbox.facet.iterators.OrdinalIterator;
import org.apache.lucene.sandbox.facet.iterators.TaxonomyChildrenOrdinalIterator;
import org.apache.lucene.sandbox.facet.iterators.TopnOrdinalIterator;
import org.apache.lucene.sandbox.facet.labels.OrdToLabel;
import org.apache.lucene.sandbox.facet.labels.TaxonomyOrdLabelBiMap;
import org.apache.lucene.sandbox.facet.recorders.CountFacetRecorder;
import org.apache.lucene.sandbox.facet.utils.ComparableUtils;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.tests.util.LuceneTestCase;

public abstract class SandboxFacetTestCase extends LuceneTestCase {
  // TODO: We don't have access to overall count for all facets from count recorder, and we can't
  // compute it as a SUM of values for each facet ordinal because we need to respect cases where the
  // same doc belongs to multiple facets (e.g. overlapping ranges or multi value fields). In most
  // cases we can already access the value. E.g. for facets with hierarchy (taxonomy or SSDV) we can
  // read value for parent facet ordinal. I believe the only  case that requires code changes is
  // range facets. To solve it we can add a parameter to range FacetCutter to assign/yeild special
  // facet ordinal for every document that matches at least one range from the list. Overall,
  // sandbox facet tests don't have to use FacetResult, so we change it to assert facet labels and
  // recorded results directly and avoid need for this constant.
  static final int VALUE_CANT_BE_COMPUTED = Integer.MIN_VALUE;

  protected void assertNumericValuesEquals(Number a, Number b) {
    assertTrue(a.getClass().isInstance(b));
    if (a instanceof Float) {
      assertEquals(a.floatValue(), b.floatValue(), a.floatValue() / 1e5);
    } else if (a instanceof Double) {
      assertEquals(a.doubleValue(), b.doubleValue(), a.doubleValue() / 1e5);
    } else {
      assertEquals(a, b);
    }
  }

  protected void assertFacetResult(
      FacetResult result,
      String expectedDim,
      String[] expectedPath,
      int expectedChildCount,
      Number expectedValue,
      LabelAndValue... expectedChildren) {
    assertEquals(expectedDim, result.dim);
    assertArrayEquals(expectedPath, result.path);
    assertEquals(expectedChildCount, result.childCount);
    assertNumericValuesEquals(expectedValue, result.value);
    assertEquals(expectedChildren.length, result.labelValues.length);
    // assert children equal with no assumption of the children ordering
    assertTrue(Arrays.asList(result.labelValues).containsAll(Arrays.asList(expectedChildren)));
  }

  FacetResult getTopChildrenByCount(
      CountFacetRecorder countFacetRecorder,
      TaxonomyReader taxoReader,
      int topN,
      String dimension,
      String... path)
      throws IOException {
    ComparableSupplier<ComparableUtils.ByCountComparable> countComparable =
        ComparableUtils.byCount(countFacetRecorder);
    TaxonomyOrdLabelBiMap ordLabels = new TaxonomyOrdLabelBiMap(taxoReader);
    FacetLabel parentLabel = new FacetLabel(dimension, path);
    OrdinalIterator childrenIterator =
        new TaxonomyChildrenOrdinalIterator(
            countFacetRecorder.recordedOrds(),
            taxoReader.getParallelTaxonomyArrays().parents(),
            ordLabels.getOrd(parentLabel));
    OrdinalIterator topByCountOrds =
        new TopnOrdinalIterator<>(childrenIterator, countComparable, topN);
    // Get array of final ordinals - we need to use all of them to get labels first, and then to get
    // counts,
    // but OrdinalIterator only allows reading ordinals once.
    int[] resultOrdinals = topByCountOrds.toArray();

    FacetLabel[] labels = ordLabels.getLabels(resultOrdinals);
    List<LabelAndValue> labelsAndValues = new ArrayList<>(labels.length);
    int childCount = 0;
    for (int i = 0; i < resultOrdinals.length; i++) {
      int count = countFacetRecorder.getCount(resultOrdinals[i]);
      labelsAndValues.add(new LabelAndValue(labels[i].lastComponent(), count));
      childCount++;
    }
    // int value = countFacetRecorder.getCount(parentOrdinal);
    return new FacetResult(
        dimension,
        path,
        VALUE_CANT_BE_COMPUTED,
        labelsAndValues.toArray(new LabelAndValue[0]),
        childCount);
  }

  FacetResult getAllChildren(
      CountFacetRecorder countFacetRecorder,
      TaxonomyReader taxoReader,
      String dimension,
      String... path)
      throws IOException {
    TaxonomyOrdLabelBiMap ordLabels = new TaxonomyOrdLabelBiMap(taxoReader);
    FacetLabel parentLabel = new FacetLabel(dimension, path);
    int parentOrdinal = ordLabels.getOrd(parentLabel);
    OrdinalIterator childrenIternator =
        new TaxonomyChildrenOrdinalIterator(
            countFacetRecorder.recordedOrds(),
            taxoReader.getParallelTaxonomyArrays().parents(),
            parentOrdinal);
    // Get array of final ordinals - we need to use all of them to get labels first, and then to get
    // counts,
    // but OrdinalIterator only allows reading ordinals once.
    int[] resultOrdinals = childrenIternator.toArray();

    FacetLabel[] labels = ordLabels.getLabels(resultOrdinals);
    List<LabelAndValue> labelsAndValues = new ArrayList<>(labels.length);
    int childCount = 0;
    for (int i = 0; i < resultOrdinals.length; i++) {
      int count = countFacetRecorder.getCount(resultOrdinals[i]);
      labelsAndValues.add(new LabelAndValue(labels[i].lastComponent(), count));
      childCount++;
    }
    // int value = countFacetRecorder.getCount(parentOrdinal);
    return new FacetResult(
        dimension,
        path,
        VALUE_CANT_BE_COMPUTED,
        labelsAndValues.toArray(new LabelAndValue[0]),
        childCount);
  }

  FacetResult getAllSortByOrd(
      int[] resultOrdinals,
      CountFacetRecorder countFacetRecorder,
      String dimension,
      OrdToLabel ordLabels)
      throws IOException {
    ComparableUtils.sort(resultOrdinals, ComparableUtils.byOrdinal());
    FacetLabel[] labels = ordLabels.getLabels(resultOrdinals);
    List<LabelAndValue> labelsAndValues = new ArrayList<>(labels.length);
    int childCount = 0;
    for (int i = 0; i < resultOrdinals.length; i++) {
      int count = countFacetRecorder.getCount(resultOrdinals[i]);
      labelsAndValues.add(new LabelAndValue(labels[i].lastComponent(), count));
      childCount++;
    }

    return new FacetResult(
        dimension,
        new String[0],
        VALUE_CANT_BE_COMPUTED,
        labelsAndValues.toArray(new LabelAndValue[0]),
        childCount);
  }

  int getSpecificValue(
      CountFacetRecorder countFacetRecorder, TaxonomyReader taxoReader, String... path)
      throws IOException {
    TaxonomyOrdLabelBiMap ordLabels = new TaxonomyOrdLabelBiMap(taxoReader);
    FacetLabel label = new FacetLabel(path);
    int facetOrd = ordLabels.getOrd(label);
    return countFacetRecorder.getCount(facetOrd);
  }

  int[] getCountsForRecordedCandidates(
      CountFacetRecorder countFacetRecorder, TaxonomyReader taxoReader, FacetLabel[] candidates)
      throws IOException {
    int[] resultOrds =
        new CandidateSetOrdinalIterator(
                countFacetRecorder, candidates, new TaxonomyOrdLabelBiMap(taxoReader))
            .toArray();
    int[] counts = new int[resultOrds.length];
    for (int i = 0; i < resultOrds.length; i++) {
      counts[i] = countFacetRecorder.getCount(resultOrds[i]);
    }
    return counts;
  }

  protected IndexSearcher getNewSearcherForDrillSideways(IndexReader reader) {
    // Do not wrap with an asserting searcher, since DrillSidewaysQuery doesn't
    // implement all the required components like Weight#scorer.
    IndexSearcher searcher = newSearcher(reader, true, false, Concurrency.INTER_SEGMENT);
    // DrillSideways requires the entire range of docs to be scored at once, so it doesn't support
    // timeouts whose implementation scores one window of doc IDs at a time.
    searcher.setTimeout(null);
    return searcher;
  }
}
