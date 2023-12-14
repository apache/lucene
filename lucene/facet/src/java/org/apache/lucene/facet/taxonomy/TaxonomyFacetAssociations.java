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

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.cursors.IntIntCursor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.util.PriorityQueue;

abstract class TaxonomyFacetAssociations extends TaxonomyFacets {
  private static class DimValue {
    String dim;
    int dimOrd;
    Number value;

    DimValue(String dim, int dimOrd, Number value) {
      this.dim = dim;
      this.dimOrd = dimOrd;
      this.value = value;
    }
  }

  /** Aggregation function used for combining values. */
  protected final AssociationAggregationFunction aggregationFunction;

  protected Comparator<Number> valueComparator;

  /** Sole constructor. */
  TaxonomyFacetAssociations(
      String indexFieldName,
      TaxonomyReader taxoReader,
      FacetsConfig config,
      AssociationAggregationFunction aggregationFunction,
      FacetsCollector fc)
      throws IOException {
    super(indexFieldName, taxoReader, config, fc);
    this.aggregationFunction = aggregationFunction;
  }

  /** Get the value for this ordinal. */
  abstract Number getNumberValue(int ordinal);

  protected abstract void updateValue(int ordinal, int childOrdinal) throws IOException;

  protected abstract Number aggregate(Number existingVal, Number newVal);

  /** Rolls up any single-valued hierarchical dimensions. */
  @Override
  void rollup() throws IOException {
    if (initialized == false) {
      return;
    }

    // Rollup any necessary dims:
    int[] children = null;
    for (Map.Entry<String, FacetsConfig.DimConfig> ent : config.getDimConfigs().entrySet()) {
      String dim = ent.getKey();
      FacetsConfig.DimConfig ft = ent.getValue();
      if (ft.hierarchical && ft.multiValued == false) {
        int dimRootOrd = taxoReader.getOrdinal(new FacetLabel(dim));
        // It can be -1 if this field was declared in the
        // config but never indexed:
        if (dimRootOrd > 0) {
          if (children == null) {
            // lazy init
            children = getChildren();
          }
          updateValue(dimRootOrd, children[dimRootOrd]);
        }
      }
    }
  }

  @Override
  public FacetResult getAllChildren(String dim, String... path) throws IOException {
    FacetsConfig.DimConfig dimConfig = verifyDim(dim);
    FacetLabel cp = new FacetLabel(dim, path);
    int dimOrd = taxoReader.getOrdinal(cp);
    if (dimOrd == -1) {
      return null;
    }

    if (initialized == false) {
      return null;
    }

    Number aggregatedValue = 0;
    int aggregatedCount = 0;

    IntArrayList ordinals = new IntArrayList();
    List<Number> ordValues = new ArrayList<>();

    if (sparseCounts != null) {
      for (IntIntCursor ordAndCount : sparseCounts) {
        int ord = ordAndCount.key;
        int count = ordAndCount.value;
        Number value = getNumberValue(ord);
        if (parents[ord] == dimOrd && count > 0) {
          aggregatedCount += count;
          aggregatedValue = aggregate(aggregatedValue, value);
          ordinals.add(ord);
          ordValues.add(value);
        }
      }
    } else {
      int[] children = getChildren();
      int[] siblings = getSiblings();
      int ord = children[dimOrd];
      while (ord != TaxonomyReader.INVALID_ORDINAL) {
        Number value = getNumberValue(ord);
        int count = counts[ord];
        if (count > 0) {
          aggregatedCount += count;
          aggregatedValue = aggregate(aggregatedValue, value);
          ordinals.add(ord);
          ordValues.add(value);
        }
        ord = siblings[ord];
      }
    }

    if (aggregatedCount == 0) {
      return null;
    }

    if (dimConfig.multiValued) {
      if (dimConfig.requireDimCount) {
        aggregatedValue = getNumberValue(dimOrd);
      } else {
        // Our sum'd value is not correct, in general:
        aggregatedValue = -1;
      }
    } else {
      // Our sum'd dim value is accurate, so we keep it
    }

    // TODO: It would be nice if TaxonomyReader let us pass in a buffer + size so we didn't have to
    // do an array copy here:
    FacetLabel[] bulkPath = taxoReader.getBulkPath(ordinals.toArray());

    LabelAndValue[] labelValues = new LabelAndValue[ordValues.size()];
    for (int i = 0; i < ordValues.size(); i++) {
      labelValues[i] = new LabelAndValue(bulkPath[i].components[cp.length], ordValues.get(i));
    }
    return new FacetResult(dim, path, aggregatedValue, labelValues, ordinals.size());
  }

  /**
   * Determine the top-n children for a specified dimension + path. Results are in an intermediate
   * form.
   */
  protected abstract TopChildrenForPath getTopChildrenForPath(
      FacetsConfig.DimConfig dimConfig, int pathOrd, int topN) throws IOException;

  @Override
  public FacetResult getTopChildren(int topN, String dim, String... path) throws IOException {
    validateTopN(topN);
    FacetsConfig.DimConfig dimConfig = verifyDim(dim);
    FacetLabel cp = new FacetLabel(dim, path);
    int dimOrd = taxoReader.getOrdinal(cp);
    if (dimOrd == -1) {
      return null;
    }

    if (initialized == false) {
      return null;
    }

    TopChildrenForPath topChildrenForPath = getTopChildrenForPath(dimConfig, dimOrd, topN);
    return createFacetResult(topChildrenForPath, dim, path);
  }

  @Override
  public Number getSpecificValue(String dim, String... path) throws IOException {
    FacetsConfig.DimConfig dimConfig = verifyDim(dim);
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
    return initialized ? getNumberValue(ord) : 0;
  }

  @Override
  public List<FacetResult> getTopDims(int topNDims, int topNChildren) throws IOException {
    if (topNDims <= 0 || topNChildren <= 0) {
      throw new IllegalArgumentException("topN must be > 0");
    }

    if (initialized == false) {
      return Collections.emptyList();
    }

    // get children and siblings ordinal array from TaxonomyFacets
    int[] children = getChildren();
    int[] siblings = getSiblings();

    // Create priority queue to store top dimensions and sort by their aggregated values/hits and
    // string values.
    PriorityQueue<DimValue> pq =
        new PriorityQueue<>(topNDims) {
          @Override
          protected boolean lessThan(DimValue a, DimValue b) {
            int comparison = valueComparator.compare(a.value, b.value);
            if (comparison < 0) {
              return true;
            }
            if (comparison > 0) {
              return false;
            }
            return a.dim.compareTo(b.dim) > 0;
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
          Number dimValue;
          if (dimConfig.multiValued) {
            if (dimConfig.requireDimCount) {
              // If the dim is configured as multi-valued and requires dim counts, we can access
              // an accurate count for the dim computed at indexing time:
              dimValue = getNumberValue(dimOrd);
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
            dimValue = topChildrenForPath.pathValue();
          }
          if (valueComparator.compare(dimValue, 0) != 0) {
            if (pq.size() < topNDims) {
              pq.add(new DimValue(dim, dimOrd, dimValue));
            } else {
              if (valueComparator.compare(dimValue, pq.top().value) > 0
                  || (valueComparator.compare(dimValue, pq.top().value) == 0
                      && dim.compareTo(pq.top().dim) < 0)) {
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
}
