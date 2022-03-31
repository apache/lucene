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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollector.MatchingDocs;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.FacetsConfig.DimConfig;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.TopOrdAndIntQueue;
import org.apache.lucene.util.PriorityQueue;

/** Base class for all taxonomy-based facets that aggregate to a per-ords int[]. */
abstract class IntTaxonomyFacets extends TaxonomyFacets {

  /** Aggregation function used for combining values. */
  final AssociationAggregationFunction aggregationFunction;

  /** Dense ordinal values. */
  final int[] values;

  /** Sparse ordinal values. */
  final IntIntHashMap sparseValues;

  /** Pass in emptyPath for getTopDims and getAllDims. */
  private static final String[] emptyPath = new String[0];

  /** Sole constructor. */
  IntTaxonomyFacets(
      String indexFieldName,
      TaxonomyReader taxoReader,
      FacetsConfig config,
      AssociationAggregationFunction aggregationFunction,
      FacetsCollector fc)
      throws IOException {
    super(indexFieldName, taxoReader, config);
    this.aggregationFunction = aggregationFunction;

    if (useHashTable(fc, taxoReader)) {
      sparseValues = new IntIntHashMap();
      values = null;
    } else {
      sparseValues = null;
      values = new int[taxoReader.getSize()];
    }
  }

  /** Set the count for this ordinal to {@code newValue}. */
  void setValue(int ordinal, int newValue) {
    if (sparseValues != null) {
      sparseValues.put(ordinal, newValue);
    } else {
      values[ordinal] = newValue;
    }
  }

  /** Get the count for this ordinal. */
  int getValue(int ordinal) {
    if (sparseValues != null) {
      return sparseValues.get(ordinal);
    } else {
      return values[ordinal];
    }
  }

  /** Rolls up any single-valued hierarchical dimensions. */
  void rollup() throws IOException {
    // Rollup any necessary dims:
    int[] children = null;
    for (Map.Entry<String, DimConfig> ent : config.getDimConfigs().entrySet()) {
      String dim = ent.getKey();
      DimConfig ft = ent.getValue();
      if (ft.hierarchical && ft.multiValued == false) {
        int dimRootOrd = taxoReader.getOrdinal(new FacetLabel(dim));
        // It can be -1 if this field was declared in the
        // config but never indexed:
        if (dimRootOrd > 0) {
          if (children == null) {
            // lazy init
            children = getChildren();
          }
          int currentValue = getValue(dimRootOrd);
          int newValue = aggregationFunction.aggregate(currentValue, rollup(children[dimRootOrd]));
          setValue(dimRootOrd, newValue);
        }
      }
    }
  }

  private int rollup(int ord) throws IOException {
    int[] children = getChildren();
    int[] siblings = getSiblings();
    int aggregatedValue = 0;
    while (ord != TaxonomyReader.INVALID_ORDINAL) {
      int currentValue = getValue(ord);
      int newValue = aggregationFunction.aggregate(currentValue, rollup(children[ord]));
      setValue(ord, newValue);
      aggregatedValue = aggregationFunction.aggregate(aggregatedValue, getValue(ord));
      ord = siblings[ord];
    }
    return aggregatedValue;
  }

  /** Return true if a sparse hash table should be used for counting, instead of a dense int[]. */
  private boolean useHashTable(FacetsCollector fc, TaxonomyReader taxoReader) {
    if (taxoReader.getSize() < 1024) {
      // small number of unique values: use an array
      return false;
    }

    if (fc == null) {
      // counting all docs: use an array
      return false;
    }

    int maxDoc = 0;
    int sumTotalHits = 0;
    for (MatchingDocs docs : fc.getMatchingDocs()) {
      sumTotalHits += docs.totalHits;
      maxDoc += docs.context.reader().maxDoc();
    }

    // if our result set is < 10% of the index, we collect sparsely (use hash map):
    return sumTotalHits < maxDoc / 10;
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
    return getValue(ord);
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

    ChildOrdsResult childOrdsResult = getChildOrdsResult(dimConfig, dimOrd,topN);

    if (childOrdsResult.q == null || childOrdsResult.aggregatedValue == 0) {
      return null;
    }

    LabelAndValue[] labelValues = getLabelValues(childOrdsResult.q, cp.length);
    return new FacetResult(dim, path, childOrdsResult.aggregatedValue, labelValues, childOrdsResult.childCount);
  }

  /**
   * Returns label values for dims
   * This portion of code is moved from getTopChildren because getTopDims needs to reuse it
   */
  private LabelAndValue[] getLabelValues(TopOrdAndIntQueue q, int facetLabelLength)
          throws IOException {
    LabelAndValue[] labelValues = new LabelAndValue[q.size()];
    int[] ordinals = new int[labelValues.length];
    int[] values = new int[labelValues.length];

    for (int i = labelValues.length - 1; i >= 0; i--) {
      TopOrdAndIntQueue.OrdAndValue ordAndValue = q.pop();
      ordinals[i] = ordAndValue.ord;
      values[i] = ordAndValue.value;
    }

    FacetLabel[] bulkPath = taxoReader.getBulkPath(ordinals);
    for (int i = 0; i < labelValues.length; i++) {
      labelValues[i] = new LabelAndValue(bulkPath[i].components[facetLabelLength], values[i]);
    }
    return labelValues;
  }

  /**
   * Returns ChildOrdsResult that contains results of dimCount(totalValue), childCount, and
   * the queue for the dimension's top children to populate FacetResult in getPathResult.
   * This portion of code is moved from getTopChildren because getTopDims needs to reuse it
   *
   */
  private ChildOrdsResult getChildOrdsResult(DimConfig dimConfig, int dimOrd, int topN) throws IOException {
    TopOrdAndIntQueue q = new TopOrdAndIntQueue(Math.min(taxoReader.getSize(), topN));
    int bottomValue = 0;

    int aggregatedValue = 0;
    int childCount = 0;
    TopOrdAndIntQueue.OrdAndValue reuse = null;

    if (sparseValues != null) {
      for (IntIntCursor c : sparseValues) {
        int value = c.value;
        int ord = c.key;
        if (parents[ord] == dimOrd && value > 0) {
          aggregatedValue = aggregationFunction.aggregate(aggregatedValue, value);
          childCount++;
          if (value > bottomValue) {
            if (reuse == null) {
              reuse = new TopOrdAndIntQueue.OrdAndValue();
            }
            reuse.ord = ord;
            reuse.value = value;
            reuse = q.insertWithOverflow(reuse);
            if (q.size() == topN) {
              bottomValue = q.top().value;
            }
          }
        }
      }
    } else {
      int[] children = getChildren();
      int[] siblings = getSiblings();
      int ord = children[dimOrd];
      while (ord != TaxonomyReader.INVALID_ORDINAL) {
        int value = values[ord];
        if (value > 0) {
          aggregatedValue = aggregationFunction.aggregate(aggregatedValue, value);
          childCount++;
          if (value > bottomValue) {
            if (reuse == null) {
              reuse = new TopOrdAndIntQueue.OrdAndValue();
            }
            reuse.ord = ord;
            reuse.value = value;
            reuse = q.insertWithOverflow(reuse);
            if (q.size() == topN) {
              bottomValue = q.top().value;
            }
          }
        }
        ord = siblings[ord];
      }
    }

    if (dimConfig.multiValued) {
      if (dimConfig.requireDimCount) {
        aggregatedValue = getValue(dimOrd);
      } else {
        // Our sum'd value is not correct, in general:
        aggregatedValue = -1;
      }
    }

    return new ChildOrdsResult (aggregatedValue, childCount, q);
  }


  /** Returns value/count of a dimension. */
  private int getDimValue(
          FacetsConfig.DimConfig dimConfig,
          String dim,
          int dimOrd,
          int topN,
          HashMap<String, ChildOrdsResult> dimToChildOrdsResult) throws IOException {

    // if dimConfig.hierarchical == true || dim is multiValued and dim count has been aggregated at
    // indexing time, return dimCount directly
    if (dimConfig.hierarchical == true || (dimConfig.multiValued && dimConfig.requireDimCount)) {
      return getValue(dimOrd);
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
    if (topNDims <= 0 || topNChildren <= 0) {
      throw new IllegalArgumentException("topN must be > 0");
    }

    // get existing children and siblings ordinal array from TaxonomyFacets
    int[] children = getExistingChildren();
    int[] siblings = getExistingSiblings();

    // Creates priority queue to store top dimensions and sort by their aggregated values/hits and
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
        int dimCount = 0;
        // if dimOrd = -1, we skip this dim, else call getDimValue
        if (dimOrd != -1) {
          dimCount = getDimValue(dimConfig, dim, dimOrd, topNChildren, dimToChildOrdsResult);
        }
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
        ord = siblings[ord];
      }
    }

    // get FacetResult for topNDims
    int resultSize = pq.size();
    // use fixed-size array to reduce space usage
    FacetResult[] results = new FacetResult[resultSize];

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
      // FacetResult requires String[] path, and path is always empty for getTopDims. FacetLabelLength
      // is always equal to 1 when FacetLabel is constructed with FacetLabel(dim, emptyPath), and therefore,
      // 1 is passed in when calling getLabelValues
      FacetResult facetResult = new FacetResult(dimValueResult.dim, emptyPath, dimValueResult.value,
              getLabelValues(childOrdsResult.q, 1), childOrdsResult.childCount);
      resultSize--;
      results[resultSize] = facetResult;
    }
    return Arrays.asList(results);
  }

  /**
   * Creates DimValueResult to store the label, dim ordinal and dim count of a dim in priority queue
   *
   */
  private static class DimValueResult {
    String dim;
    int dimOrd;
    int value;

    DimValueResult(String dim, int dimOrd, int value) {
      this.dim = dim;
      this.dimOrd = dimOrd;
      this.value = value;
    }
  }

  /**
   * Creates ChildOrdsResult to store dimCount, childCount, and the queue for the dimension's top
   * children
   */
  private static class ChildOrdsResult {
    final int aggregatedValue;
    final int childCount;
    final TopOrdAndIntQueue q;

    ChildOrdsResult(int aggregatedValue, int childCount, TopOrdAndIntQueue q) {
      this.aggregatedValue = aggregatedValue;
      this.childCount = childCount;
      this.q = q;
    }
  }
}
