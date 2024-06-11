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

import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollector.MatchingDocs;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.taxonomy.CachedOrdinalsReader;
import org.apache.lucene.facet.taxonomy.DocValuesOrdinalsReader;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts;
import org.apache.lucene.facet.taxonomy.OrdinalsReader;
import org.apache.lucene.facet.taxonomy.TaxonomyFacetCounts;
import org.apache.lucene.facet.taxonomy.TaxonomyFacetLabels;
import org.apache.lucene.facet.taxonomy.TaxonomyFacetLabels.FacetLabelReader;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.sandbox.facet.abstracts.OrdToComparable;
import org.apache.lucene.sandbox.facet.abstracts.OrdLabelBiMap;
import org.apache.lucene.sandbox.facet.abstracts.OrdinalIterator;
import org.apache.lucene.sandbox.facet.recorders.CountFacetRecorder;
import org.apache.lucene.sandbox.facet.ordinal_iterators.TopnOrdinalIterator;
import org.apache.lucene.sandbox.facet.taxonomy.TaxonomyChildrenOrdinalIterator;
import org.apache.lucene.sandbox.facet.taxonomy.TaxonomyOrdLabelBiMap;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class SandboxFacetTestCase extends LuceneTestCase {
  // we don't have access to overall count for all facets from count recorder,
  // and we can't compute it as a SUM of values for each facet ID because we need to respect cases where
  // the same doc belongs to multiple facets (e.g. overlapping ranges and
  // multi value fields). We can add an extra range that includes everything,
  // or consider supporting overall count in CountFacetRecorder. But it is not exactly the value
  // we can get now, as this value wouldn't respect top-n cutoff. Is this value a must have facets feature?
  static final int VALUE_CANT_BE_COMPUTED = -5;

  public Facets getTaxonomyFacetCounts(
          TaxonomyReader taxoReader, FacetsConfig config, FacetsCollector c) throws IOException {
    return getTaxonomyFacetCounts(taxoReader, config, c, FacetsConfig.DEFAULT_INDEX_FIELD_NAME);
  }

  public Facets getTaxonomyFacetCounts(
      TaxonomyReader taxoReader, FacetsConfig config, FacetsCollector c, String indexFieldName)
      throws IOException {
    Facets facets;
    if (random().nextBoolean()) {
      facets = new FastTaxonomyFacetCounts(indexFieldName, taxoReader, config, c);
    } else {
      OrdinalsReader ordsReader = new DocValuesOrdinalsReader(indexFieldName);
      if (random().nextBoolean()) {
        ordsReader = new CachedOrdinalsReader(ordsReader);
      }
      facets = new TaxonomyFacetCounts(ordsReader, taxoReader, config, c);
    }

    return facets;
  }

  /**
   * Utility method that uses {@link FacetLabelReader} to get facet labels for each hit in {@link
   * MatchingDocs}. The method returns {@code List<List<FacetLabel>>} where outer list has one entry
   * per document and inner list has all {@link FacetLabel} entries that belong to a document. The
   * inner list may be empty if no {@link FacetLabel} are found for a hit.
   *
   * @param taxoReader {@link TaxonomyReader} used to read taxonomy during search. This instance is
   *     expected to be open for reading.
   * @param fc {@link FacetsCollector} A collector with matching hits.
   * @param dimension facet dimension for which labels are requested. A null value fetches labels
   *     for all dimensions.
   * @return {@code List<List<FacetLabel>} where outer list has one non-null entry per document. and
   *     inner list contain all {@link FacetLabel} entries that belong to a document.
   * @throws IOException when a low-level IO issue occurs.
   */
  public List<List<FacetLabel>> getAllTaxonomyFacetLabels(
      String dimension, TaxonomyReader taxoReader, FacetsCollector fc) throws IOException {
    List<List<FacetLabel>> actualLabels = new ArrayList<>();
    TaxonomyFacetLabels taxoLabels =
        new TaxonomyFacetLabels(taxoReader, FacetsConfig.DEFAULT_INDEX_FIELD_NAME);
    for (MatchingDocs m : fc.getMatchingDocs()) {
      FacetLabelReader facetLabelReader = taxoLabels.getFacetLabelReader(m.context);
      DocIdSetIterator disi = m.bits.iterator();
      while (disi.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
        actualLabels.add(allFacetLabels(disi.docID(), dimension, facetLabelReader));
      }
    }
    return actualLabels;
  }

  /**
   * Utility method to get all facet labels for an input docId and dimension using the supplied
   * {@link FacetLabelReader}.
   *
   * @param docId docId for which facet labels are needed.
   * @param dimension Retain facet labels for supplied dimension only. A null value fetches all
   *     facet labels.
   * @param facetLabelReader {@FacetLabelReader} instance use to get facet labels for input docId.
   * @return {@code List<FacetLabel>} containing matching facet labels.
   * @throws IOException when a low-level IO issue occurs while reading facet labels.
   */
  List<FacetLabel> allFacetLabels(int docId, String dimension, FacetLabelReader facetLabelReader)
      throws IOException {
    List<FacetLabel> facetLabels = new ArrayList<>();
    FacetLabel facetLabel;
    if (dimension != null) {
      for (facetLabel = facetLabelReader.nextFacetLabel(docId, dimension); facetLabel != null; ) {
        facetLabels.add(facetLabel);
        facetLabel = facetLabelReader.nextFacetLabel(docId, dimension);
      }
    } else {
      for (facetLabel = facetLabelReader.nextFacetLabel(docId); facetLabel != null; ) {
        facetLabels.add(facetLabel);
        facetLabel = facetLabelReader.nextFacetLabel(docId);
      }
    }
    return facetLabels;
  }

  protected String[] getRandomTokens(int count) {
    String[] tokens = new String[count];
    for (int i = 0; i < tokens.length; i++) {
      tokens[i] = TestUtil.randomRealisticUnicodeString(random(), 1, 10);
      // tokens[i] = _TestUtil.randomSimpleString(random(), 1, 10);
    }
    return tokens;
  }

  protected String pickToken(String[] tokens) {
    for (int i = 0; i < tokens.length; i++) {
      if (random().nextBoolean()) {
        return tokens[i];
      }
    }

    // Move long tail onto first token:
    return tokens[0];
  }

  protected static class TestDoc {
    public String content;
    public String[] dims;
    public float value;
  }

  protected List<TestDoc> getRandomDocs(String[] tokens, int count, int numDims) {
    List<TestDoc> docs = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      TestDoc doc = new TestDoc();
      docs.add(doc);
      doc.content = pickToken(tokens);
      doc.dims = new String[numDims];
      for (int j = 0; j < numDims; j++) {
        doc.dims[j] = pickToken(tokens);
        if (random().nextInt(10) < 3) {
          break;
        }
      }
      if (VERBOSE) {
        System.out.println("  doc " + i + ": content=" + doc.content);
        for (int j = 0; j < numDims; j++) {
          if (doc.dims[j] != null) {
            System.out.println("    dim[" + j + "]=" + doc.dims[j]);
          }
        }
      }
    }

    return docs;
  }

  protected void sortTies(List<FacetResult> results) {
    for (FacetResult result : results) {
      sortTies(result.labelValues);
    }
  }

  protected void sortTies(LabelAndValue[] labelValues) {
    double lastValue = -1;
    int numInRow = 0;
    int i = 0;
    while (i <= labelValues.length) {
      if (i < labelValues.length && labelValues[i].value.doubleValue() == lastValue) {
        numInRow++;
      } else {
        if (numInRow > 1) {
          Arrays.sort(
              labelValues,
              i - numInRow,
              i,
              new Comparator<LabelAndValue>() {
                @Override
                public int compare(LabelAndValue a, LabelAndValue b) {
                  assert a.value.doubleValue() == b.value.doubleValue();
                  return new BytesRef(a.label).compareTo(new BytesRef(b.label));
                }
              });
        }
        numInRow = 1;
        if (i < labelValues.length) {
          lastValue = labelValues[i].value.doubleValue();
        }
      }
      i++;
    }
  }

  protected void sortLabelValues(List<LabelAndValue> labelValues) {
    Collections.sort(
        labelValues,
        new Comparator<LabelAndValue>() {
          @Override
          public int compare(LabelAndValue a, LabelAndValue b) {
            if (a.value.doubleValue() > b.value.doubleValue()) {
              return -1;
            } else if (a.value.doubleValue() < b.value.doubleValue()) {
              return 1;
            } else {
              return new BytesRef(a.label).compareTo(new BytesRef(b.label));
            }
          }
        });
  }

  protected void sortFacetResults(List<FacetResult> results) {
    Collections.sort(
        results,
        new Comparator<FacetResult>() {
          @Override
          public int compare(FacetResult a, FacetResult b) {
            if (a.value.doubleValue() > b.value.doubleValue()) {
              return -1;
            } else if (b.value.doubleValue() > a.value.doubleValue()) {
              return 1;
            } else {
              return a.dim.compareTo(b.dim);
            }
          }
        });
  }

  protected void assertFloatValuesEquals(List<FacetResult> a, List<FacetResult> b) {
    assertEquals(a.size(), b.size());
    float lastValue = Float.POSITIVE_INFINITY;
    Map<String, FacetResult> aByDim = new HashMap<>();
    for (int i = 0; i < a.size(); i++) {
      assertTrue(a.get(i).value.floatValue() <= lastValue);
      lastValue = a.get(i).value.floatValue();
      aByDim.put(a.get(i).dim, a.get(i));
    }
    lastValue = Float.POSITIVE_INFINITY;
    Map<String, FacetResult> bByDim = new HashMap<>();
    for (int i = 0; i < b.size(); i++) {
      bByDim.put(b.get(i).dim, b.get(i));
      assertTrue(b.get(i).value.floatValue() <= lastValue);
      lastValue = b.get(i).value.floatValue();
    }
    for (String dim : aByDim.keySet()) {
      assertFloatValuesEquals(aByDim.get(dim), bByDim.get(dim));
    }
  }

  protected void assertFloatValuesEquals(FacetResult a, FacetResult b) {
    assertEquals(a.dim, b.dim);
    assertTrue(Arrays.equals(a.path, b.path));
    assertEquals(a.childCount, b.childCount);
    assertNumericValuesEquals(a.value, b.value);
    assertEquals(a.labelValues.length, b.labelValues.length);
    for (int i = 0; i < a.labelValues.length; i++) {
      assertEquals(a.labelValues[i].label, b.labelValues[i].label);
      assertNumericValuesEquals(a.labelValues[i].value, b.labelValues[i].value);
    }
  }

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

  FacetResult getTopChildrenByCount(CountFacetRecorder countFacetRecorder, TaxonomyReader taxoReader, int topN,
                                    String dimension, String... path) throws IOException {
    OrdToComparable<ComparableUtils.IntOrdComparable> countComparable = ComparableUtils.countOrdToComparable(
            countFacetRecorder);
    OrdLabelBiMap ordLabels = new TaxonomyOrdLabelBiMap(taxoReader);
    FacetLabel parentLabel = new FacetLabel(dimension, path);
    int parentOrdinal = ordLabels.getOrd(parentLabel);
    OrdinalIterator childrenIternator = new TaxonomyChildrenOrdinalIterator(countFacetRecorder.recordedOrds(),
            taxoReader.getParallelTaxonomyArrays()
            .parents(), ordLabels.getOrd(new FacetLabel(dimension)));
    OrdinalIterator topByCountOrds = new TopnOrdinalIterator<>(childrenIternator, countComparable, topN);
    // Get array of final ordinals - we need to use all of them to get labels first, and then to get counts,
    // but OrdinalIterator only allows reading ordinals once.
    int[] resultOrdinals = topByCountOrds.toArray();

    FacetLabel[] labels = ordLabels.getLabels(resultOrdinals);
    List<LabelAndValue> labelsAndValues = new ArrayList<>(labels.length);
    int childCount = 0;
    for (int i = 0; i < resultOrdinals.length; i++) {
      int count = countFacetRecorder.getCount(resultOrdinals[i]);
      labelsAndValues.add(new LabelAndValue(labels[i].getLeaf(), count));
      childCount++;
    }
    // int value = countFacetRecorder.getCount(parentOrdinal);
    return new FacetResult(dimension, path, VALUE_CANT_BE_COMPUTED, labelsAndValues.toArray(new LabelAndValue[0]), childCount);
  }

  FacetResult getAllChildren(CountFacetRecorder countFacetRecorder, TaxonomyReader taxoReader, String dimension, String... path) throws IOException {
    OrdLabelBiMap ordLabels = new TaxonomyOrdLabelBiMap(taxoReader);
    FacetLabel parentLabel = new FacetLabel(dimension, path);
    int parentOrdinal = ordLabels.getOrd(parentLabel);
    OrdinalIterator childrenIternator = new TaxonomyChildrenOrdinalIterator(countFacetRecorder.recordedOrds(),
            taxoReader.getParallelTaxonomyArrays()
                    .parents(), parentOrdinal);
    // Get array of final ordinals - we need to use all of them to get labels first, and then to get counts,
    // but OrdinalIterator only allows reading ordinals once.
    int[] resultOrdinals = childrenIternator.toArray();

    FacetLabel[] labels = ordLabels.getLabels(resultOrdinals);
    List<LabelAndValue> labelsAndValues = new ArrayList<>(labels.length);
    int childCount = 0;
    for (int i = 0; i < resultOrdinals.length; i++) {
      int count = countFacetRecorder.getCount(resultOrdinals[i]);
      labelsAndValues.add(new LabelAndValue(labels[i].getLeaf(), count));
      childCount++;
    }
    // int value = countFacetRecorder.getCount(parentOrdinal);
    return new FacetResult(dimension, path, VALUE_CANT_BE_COMPUTED, labelsAndValues.toArray(new LabelAndValue[0]), childCount);
  }

  FacetResult getAllSortByOrd(int[] resultOrdinals, CountFacetRecorder countFacetRecorder, String dimension, OrdLabelBiMap ordLabels) throws IOException {
    FacetLabel[] labels = ordLabels.getLabels(resultOrdinals);
    List<LabelAndValue> labelsAndValues = new ArrayList<>(labels.length);
    int childCount = 0;
    for (int i = 0; i < resultOrdinals.length; i++) {
      int count = countFacetRecorder.getCount(resultOrdinals[i]);
      labelsAndValues.add(new LabelAndValue(labels[i].getLeaf(), count));
      childCount++;
    }

    return new FacetResult(dimension, new String[0], VALUE_CANT_BE_COMPUTED, labelsAndValues.toArray(new LabelAndValue[0]), childCount);
  }

  int getSpecificValue(CountFacetRecorder countFacetRecorder, TaxonomyReader taxoReader, String... path) throws
          IOException {
    OrdLabelBiMap ordLabels = new TaxonomyOrdLabelBiMap(taxoReader);
    FacetLabel label = new FacetLabel(path);
    int facetOrd = ordLabels.getOrd(label);
    return countFacetRecorder.getCount(facetOrd);
  }
}
