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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.FacetsConfig.DimConfig;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.TopOrdAndFloatQueue;
import org.apache.lucene.util.PriorityQueue;

/** Base class for all taxonomy-based facets that aggregate to a per-ords float[]. */
abstract class FloatTaxonomyFacets extends TaxonomyFacets {

  // TODO: also use native hash map for sparse collection, like IntTaxonomyFacets

  /** Aggregation function used for combining values. */
  final AssociationAggregationFunction aggregationFunction;

  /** Per-ordinal value. */
  final float[] values;

  /** Pass in emptyPath for getTopDims and getAllDims. */
  private static final String[] emptyPath = new String[0];

  /** Sole constructor. */
  FloatTaxonomyFacets(
      String indexFieldName,
      TaxonomyReader taxoReader,
      AssociationAggregationFunction aggregationFunction,
      FacetsConfig config)
      throws IOException {
    super(indexFieldName, taxoReader, config);
    this.aggregationFunction = aggregationFunction;
    values = new float[taxoReader.getSize()];
  }

  /** Rolls up any single-valued hierarchical dimensions. */
  void rollup() throws IOException {
    // Rollup any necessary dims:
    int[] children = getChildren();
    for (Map.Entry<String, DimConfig> ent : config.getDimConfigs().entrySet()) {
      String dim = ent.getKey();
      DimConfig ft = ent.getValue();
      if (ft.hierarchical && ft.multiValued == false) {
        int dimRootOrd = taxoReader.getOrdinal(new FacetLabel(dim));
        assert dimRootOrd > 0;
        float newValue =
            aggregationFunction.aggregate(values[dimRootOrd], rollup(children[dimRootOrd]));
        values[dimRootOrd] = newValue;
      }
    }
  }

  private float rollup(int ord) throws IOException {
    int[] children = getChildren();
    int[] siblings = getSiblings();
    float aggregationValue = 0f;
    while (ord != TaxonomyReader.INVALID_ORDINAL) {
      float childValue = aggregationFunction.aggregate(values[ord], rollup(children[ord]));
      values[ord] = childValue;
      aggregationValue = aggregationFunction.aggregate(aggregationValue, childValue);
      ord = siblings[ord];
    }
    return aggregationValue;
  }

  @Override
  public Number getSpecificValue(String dim, String... path) throws IOException {
    DimConfig dimConfig = verifyDim(dim);
    if (path.length == 0) {
      if (dimConfig.hierarchical && dimConfig.multiValued == false) {
        // ok: rolled up at search time
      } else if (dimConfig.requireDimCount && dimConfig.multiValued) {
        // ok: we indexed all ords at index time
      } else {
        throw new IllegalArgumentException(
            "cannot return dimension-level value alone; use getTopChildren instead");
      }
    }
    int ord = taxoReader.getOrdinal(new FacetLabel(dim, path));
    if (ord < 0) {
      return -1;
    }
    return values[ord];
  }

  @Override
  public FacetResult getTopChildren(int topN, String dim, String... path) throws IOException {
    validateTopN(topN);
    DimConfig dimConfig = verifyDim(dim);
    FacetLabel cp = new FacetLabel(dim, path);
    int dimOrd = taxoReader.getOrdinal(cp);
    if (dimOrd == -1) {
      return null;
    }

    ChildOrdsResult childOrdsResult = getChildOrdsResult(dimConfig, dimOrd, topN);
    if (childOrdsResult.aggregatedValue == 0) {
      return null;
    }

    LabelAndValue[] labelValues = getLabelValues(childOrdsResult.q, cp.length);
    return new FacetResult(
        dim, path, childOrdsResult.aggregatedValue, labelValues, childOrdsResult.childCount);
  }

  /**
   * Return ChildOrdsResult that contains results of aggregatedValue, childCount, and the queue for
   * the dimension's top children to populate FacetResult in getPathResult.
   */
  private ChildOrdsResult getChildOrdsResult(DimConfig dimConfig, int dimOrd, int topN)
      throws IOException {

    TopOrdAndFloatQueue q = new TopOrdAndFloatQueue(Math.min(taxoReader.getSize(), topN));
    float bottomValue = 0;

    int[] children = getChildren();
    int[] siblings = getSiblings();

    int ord = children[dimOrd];
    float aggregatedValue = 0;
    int childCount = 0;

    TopOrdAndFloatQueue.OrdAndValue reuse = null;
    while (ord != TaxonomyReader.INVALID_ORDINAL) {
      if (values[ord] > 0) {
        aggregatedValue = aggregationFunction.aggregate(aggregatedValue, values[ord]);
        childCount++;
        if (values[ord] > bottomValue) {
          if (reuse == null) {
            reuse = new TopOrdAndFloatQueue.OrdAndValue();
          }
          reuse.ord = ord;
          reuse.value = values[ord];
          reuse = q.insertWithOverflow(reuse);
          if (q.size() == topN) {
            bottomValue = q.top().value;
          }
        }
      }

      ord = siblings[ord];
    }

    if (dimConfig.multiValued) {
      if (dimConfig.requireDimCount) {
        aggregatedValue = values[dimOrd];
      } else {
        // Our sum'd count is not correct, in general:
        aggregatedValue = -1;
      }
    }
    return new ChildOrdsResult(aggregatedValue, childCount, q);
  }

  /**
   * Return label and values for top dimensions and children
   *
   * @param q the queue for the dimension's top children
   * @param pathLength the length of a dimension's children paths
   */
  private LabelAndValue[] getLabelValues(TopOrdAndFloatQueue q, int pathLength) throws IOException {
    LabelAndValue[] labelValues = new LabelAndValue[q.size()];
    int[] ordinals = new int[labelValues.length];
    float[] values = new float[labelValues.length];

    for (int i = labelValues.length - 1; i >= 0; i--) {
      TopOrdAndFloatQueue.OrdAndValue ordAndValue = q.pop();
      ordinals[i] = ordAndValue.ord;
      values[i] = ordAndValue.value;
    }

    FacetLabel[] bulkPath = taxoReader.getBulkPath(ordinals);
    for (int i = 0; i < labelValues.length; i++) {
      labelValues[i] = new LabelAndValue(bulkPath[i].components[pathLength], values[i]);
    }
    return labelValues;
  }

  /** Return value of a dimension. */
  private float getDimValue(
      FacetsConfig.DimConfig dimConfig,
      String dim,
      int dimOrd,
      int topN,
      HashMap<String, ChildOrdsResult> dimToChildOrdsResult)
      throws IOException {

    // if dimConfig.hierarchical == true || dim is multiValued and dim count has been aggregated at
    // indexing time, return dimCount directly
    if (dimConfig.hierarchical == true || (dimConfig.multiValued && dimConfig.requireDimCount)) {
      return values[dimOrd];
    }

    // if dimCount was not aggregated at indexing time, iterate over childOrds to get dimCount
    ChildOrdsResult childOrdsResult = getChildOrdsResult(dimConfig, dimOrd, topN);

    // if no early termination, store dim and childOrdsResult into a hashmap to avoid calling
    // getChildOrdsResult again in getTopDims
    dimToChildOrdsResult.put(dim, childOrdsResult);
    return childOrdsResult.aggregatedValue;
  }

  @Override
  public List<FacetResult> getTopDims(int topNDims, int topNChildren) throws IOException {
    validateTopN(topNDims);
    validateTopN(topNChildren);

    // get existing children and siblings ordinal array from TaxonomyFacets
    int[] children = getChildren();
    int[] siblings = getSiblings();

    // Create priority queue to store top dimensions and sort by their aggregated values/hits and
    // string values.
    PriorityQueue<DimValueResult> pq =
        new PriorityQueue<>(topNDims) {
          @Override
          protected boolean lessThan(DimValueResult a, DimValueResult b) {
            if (a.value > b.value) {
              return false;
            } else if (a.value < b.value) {
              return true;
            } else {
              return a.dim.compareTo(b.dim) > 0;
            }
          }
        };

    // create hashMap to store the ChildOrdsResult to avoid calling getChildOrdsResult for all dims
    HashMap<String, ChildOrdsResult> dimToChildOrdsResult = new HashMap<>();

    // iterate over children and siblings ordinals for all dims
    int ord = children[TaxonomyReader.ROOT_ORDINAL];
    while (ord != TaxonomyReader.INVALID_ORDINAL) {
      String dim = taxoReader.getPath(ord).components[0];
      FacetsConfig.DimConfig dimConfig = config.getDimConfig(dim);
      if (dimConfig.indexFieldName.equals(indexFieldName)) {
        FacetLabel cp = new FacetLabel(dim, emptyPath);
        int dimOrd = taxoReader.getOrdinal(cp);
        float dimCount = 0;
        // if dimOrd = -1, we skip this dim, else call getDimValue
        if (dimOrd != -1) {
          dimCount = getDimValue(dimConfig, dim, dimOrd, topNChildren, dimToChildOrdsResult);
          if (dimCount != 0) {
            // use priority queue to store DimValueResult for topNDims
            if (pq.size() < topNDims) {
              pq.add(new DimValueResult(dim, dimOrd, dimCount));
            } else {
              if (dimCount > pq.top().value
                  || (dimCount == pq.top().value && dim.compareTo(pq.top().dim) < 0)) {
                DimValueResult bottomDim = pq.top();
                bottomDim.dim = dim;
                bottomDim.value = dimCount;
                pq.updateTop();
              }
            }
          }
        }
      }
      ord = siblings[ord];
    }

    // use fixed-size array to reduce space usage
    FacetResult[] results = new FacetResult[pq.size()];

    while (pq.size() > 0) {
      DimValueResult dimValueResult = pq.pop();
      String dim = dimValueResult.dim;
      ChildOrdsResult childOrdsResult;
      // if the childOrdsResult was stored in the map, avoid calling getChildOrdsResult again
      if (dimToChildOrdsResult.containsKey(dim)) {
        childOrdsResult = dimToChildOrdsResult.get(dim);
      } else {
        FacetsConfig.DimConfig dimConfig = config.getDimConfig(dim);
        childOrdsResult = getChildOrdsResult(dimConfig, dimValueResult.dimOrd, topNChildren);
      }
      // FacetResult requires String[] path, and path is always empty for getTopDims.
      // pathLength is always equal to 1 when FacetLabel is constructed with
      // FacetLabel(dim, emptyPath), and therefore, 1 is passed in when calling getLabelValues
      FacetResult facetResult =
          new FacetResult(
              dimValueResult.dim,
              emptyPath,
              dimValueResult.value,
              getLabelValues(childOrdsResult.q, 1),
              childOrdsResult.childCount);
      results[pq.size()] = facetResult;
    }
    return Arrays.asList(results);
  }

  /**
   * Create DimValueResult to store the label, dim ordinal and dim count of a dim in priority queue
   */
  private static class DimValueResult {
    String dim;
    int dimOrd;
    float value;

    DimValueResult(String dim, int dimOrd, float value) {
      this.dim = dim;
      this.dimOrd = dimOrd;
      this.value = value;
    }
  }

  /**
   * Create ChildOrdsResult to store dimCount, childCount, and the queue for the dimension's top
   * children
   */
  private static class ChildOrdsResult {
    final float aggregatedValue;
    final int childCount;
    final TopOrdAndFloatQueue q;

    ChildOrdsResult(float aggregatedValue, int childCount, TopOrdAndFloatQueue q) {
      this.aggregatedValue = aggregatedValue;
      this.childCount = childCount;
      this.q = q;
    }
  }
}
