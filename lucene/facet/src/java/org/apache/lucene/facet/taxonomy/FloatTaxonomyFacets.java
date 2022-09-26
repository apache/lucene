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

import com.carrotsearch.hppc.FloatArrayList;
import com.carrotsearch.hppc.IntArrayList;
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

  // TODO: also use native hash map for sparse collection, like IntTaxonomyFacets

  /** Aggregation function used for combining values. */
  protected final AssociationAggregationFunction aggregationFunction;

  /** Per-ordinal value. */
  protected final float[] values;

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
      FacetsConfig config)
      throws IOException {
    super(indexFieldName, taxoReader, config);
    this.aggregationFunction = aggregationFunction;
    values = new float[taxoReader.getSize()];
  }

  /** Rolls up any single-valued hierarchical dimensions. */
  protected void rollup() throws IOException {
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
  public FacetResult getAllChildren(String dim, String... path) throws IOException {
    DimConfig dimConfig = verifyDim(dim);
    FacetLabel cp = new FacetLabel(dim, path);
    int dimOrd = taxoReader.getOrdinal(cp);
    if (dimOrd == -1) {
      return null;
    }

    int[] children = getChildren();
    int[] siblings = getSiblings();

    int ord = children[dimOrd];
    float aggregatedValue = 0;

    IntArrayList ordinals = new IntArrayList();
    FloatArrayList ordValues = new FloatArrayList();

    while (ord != TaxonomyReader.INVALID_ORDINAL) {
      if (values[ord] > 0) {
        aggregatedValue = aggregationFunction.aggregate(aggregatedValue, values[ord]);
        ordinals.add(ord);
        ordValues.add(values[ord]);
      }
      ord = siblings[ord];
    }

    if (aggregatedValue == 0) {
      return null;
    }

    if (dimConfig.multiValued) {
      if (dimConfig.requireDimCount) {
        aggregatedValue = values[dimOrd];
      } else {
        // Our sum'd count is not correct, in general:
        aggregatedValue = -1;
      }
    } else {
      // Our sum'd dim count is accurate, so we keep it
    }

    // TODO: It would be nice if TaxonomyReader let us pass in a buffer + size so we didn't have to
    // do an array copy here:
    FacetLabel[] bulkPath = taxoReader.getBulkPath(ordinals.toArray());

    LabelAndValue[] labelValues = new LabelAndValue[ordValues.size()];
    for (int i = 0; i < labelValues.length; i++) {
      labelValues[i] = new LabelAndValue(bulkPath[i].components[cp.length], ordValues.get(i));
    }
    return new FacetResult(dim, path, aggregatedValue, labelValues, ordinals.size());
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

    TopChildrenForPath topChildrenForPath = getTopChildrenForPath(dimConfig, dimOrd, topN);
    return createFacetResult(topChildrenForPath, dim, path);
  }

  /**
   * Determine the top-n children for a specified dimension + path. Results are in an intermediate
   * form.
   */
  private TopChildrenForPath getTopChildrenForPath(DimConfig dimConfig, int pathOrd, int topN)
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
            bottomValue = q.top().value;
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

  /**
   * Create a FacetResult for the provided dim + path and intermediate results. Does the extra work
   * of resolving ordinals -> labels, etc. Will return null if there are no children.
   */
  FacetResult createFacetResult(TopChildrenForPath topChildrenForPath, String dim, String... path)
      throws IOException {
    // If the intermediate result is null or there are no children, we return null:
    if (topChildrenForPath == null || topChildrenForPath.childCount == 0) {
      return null;
    }

    TopOrdAndFloatQueue q = topChildrenForPath.childQueue;
    assert q != null;

    LabelAndValue[] labelValues = new LabelAndValue[q.size()];
    int[] ordinals = new int[labelValues.length];
    float[] values = new float[labelValues.length];

    for (int i = labelValues.length - 1; i >= 0; i--) {
      TopOrdAndFloatQueue.OrdAndValue ordAndValue = q.pop();
      assert ordAndValue != null;
      ordinals[i] = ordAndValue.ord;
      values[i] = ordAndValue.value;
    }

    FacetLabel[] bulkPath = taxoReader.getBulkPath(ordinals);
    // The path component we're interested in is the one immediately after the provided path. We
    // add 1 here to also account for the dim:
    int childComponentIdx = path.length + 1;
    for (int i = 0; i < labelValues.length; i++) {
      labelValues[i] = new LabelAndValue(bulkPath[i].components[childComponentIdx], values[i]);
    }

    return new FacetResult(
        dim, path, topChildrenForPath.pathValue, labelValues, topChildrenForPath.childCount);
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
    PriorityQueue<DimValue> pq =
        new PriorityQueue<>(topNDims) {
          @Override
          protected boolean lessThan(DimValue a, DimValue b) {
            if (a.value > b.value) {
              return false;
            } else if (a.value < b.value) {
              return true;
            } else {
              return a.dim.compareTo(b.dim) > 0;
            }
          }
        };

    // Keep track of intermediate results, if we compute them, so we can reuse them later:
    Map<String, TopChildrenForPath> intermediateResults = null;

    // iterate over children and siblings ordinals for all dims
    int ord = children[TaxonomyReader.ROOT_ORDINAL];
    while (ord != TaxonomyReader.INVALID_ORDINAL) {
      String dim = taxoReader.getPath(ord).components[0];
      FacetsConfig.DimConfig dimConfig = config.getDimConfig(dim);
      if (dimConfig.indexFieldName.equals(indexFieldName)) {
        FacetLabel cp = new FacetLabel(dim);
        int dimOrd = taxoReader.getOrdinal(cp);
        if (dimOrd != -1) {
          float dimValue;
          if (dimConfig.multiValued) {
            if (dimConfig.requireDimCount) {
              // If the dim is configured as multi-valued and requires dim counts, we can access
              // an accurate count for the dim computed at indexing time:
              dimValue = values[dimOrd];
            } else {
              // If the dim is configured as multi-valued but not requiring dim counts, we cannot
              // compute an accurate dim count, and use -1 as a place-holder:
              dimValue = -1;
            }
          } else {
            // Single-valued dims require aggregating descendant paths to get accurate dim counts
            // since we don't directly access ancestry paths:
            // TODO: We could consider indexing dim counts directly if getTopDims is a common
            // use-case.
            TopChildrenForPath topChildrenForPath =
                getTopChildrenForPath(dimConfig, dimOrd, topNChildren);
            if (intermediateResults == null) {
              intermediateResults = new HashMap<>();
            }
            intermediateResults.put(dim, topChildrenForPath);
            dimValue = topChildrenForPath.pathValue;
          }
          if (dimValue != 0) {
            if (pq.size() < topNDims) {
              pq.add(new DimValue(dim, dimOrd, dimValue));
            } else {
              if (dimValue > pq.top().value
                  || (dimValue == pq.top().value && dim.compareTo(pq.top().dim) < 0)) {
                DimValue bottomDim = pq.top();
                bottomDim.dim = dim;
                bottomDim.value = dimValue;
                pq.updateTop();
              }
            }
          }
        }
      }
      ord = siblings[ord];
    }

    FacetResult[] results = new FacetResult[pq.size()];

    while (pq.size() > 0) {
      DimValue dimValue = pq.pop();
      assert dimValue != null;
      String dim = dimValue.dim;
      TopChildrenForPath topChildrenForPath = null;
      if (intermediateResults != null) {
        topChildrenForPath = intermediateResults.get(dim);
      }
      if (topChildrenForPath == null) {
        FacetsConfig.DimConfig dimConfig = config.getDimConfig(dim);
        topChildrenForPath = getTopChildrenForPath(dimConfig, dimValue.dimOrd, topNChildren);
      }
      FacetResult facetResult = createFacetResult(topChildrenForPath, dim);
      assert facetResult != null;
      results[pq.size()] = facetResult;
    }
    return Arrays.asList(results);
  }

  private static class DimValue {
    String dim;
    int dimOrd;
    float value;

    DimValue(String dim, int dimOrd, float value) {
      this.dim = dim;
      this.dimOrd = dimOrd;
      this.value = value;
    }
  }

  /** Intermediate result to store top children for a given path before resolving labels, etc. */
  private static class TopChildrenForPath {
    private final float pathValue;
    private final int childCount;
    private final TopOrdAndFloatQueue childQueue;

    TopChildrenForPath(float pathValue, int childCount, TopOrdAndFloatQueue childQueue) {
      this.pathValue = pathValue;
      this.childCount = childCount;
      this.childQueue = childQueue;
    }
  }
}
