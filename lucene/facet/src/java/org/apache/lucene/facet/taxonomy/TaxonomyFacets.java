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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.FacetsConfig.DimConfig;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.TopOrdAndFloatNumberQueue;
import org.apache.lucene.facet.TopOrdAndIntNumberQueue;
import org.apache.lucene.facet.TopOrdAndNumberQueue;
import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.internal.hppc.IntIntHashMap;
import org.apache.lucene.util.PriorityQueue;

/**
 * Base class for all taxonomy-based facets impls.
 *
 * @deprecated Visibility of this class will be reduced to pkg-private in a future version. This
 *     class is meant to host common code as an internal implementation detail to taxonomy
 *     faceting,and is not intended as an extension point for user-created {@code Facets}
 *     implementations. If your code is relying on this, please migrate necessary functionality down
 *     into your own class.
 */
@Deprecated
public abstract class TaxonomyFacets extends Facets {
  /** Intermediate result to store top children for a given path before resolving labels, etc. */
  static class TopChildrenForPath {
    Number pathValue;
    int childCount;
    TopOrdAndNumberQueue childQueue;

    public TopChildrenForPath(Number pathValue, int childCount, TopOrdAndNumberQueue childQueue) {
      this.pathValue = pathValue;
      this.childCount = childCount;
      this.childQueue = childQueue;
    }
  }

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

  private static final Comparator<FacetResult> BY_VALUE_THEN_DIM =
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
      };

  /** Index field name provided to the constructor. */
  protected final String indexFieldName;

  /** {@code TaxonomyReader} provided to the constructor. */
  protected final TaxonomyReader taxoReader;

  /** {@code FacetsConfig} provided to the constructor. */
  protected final FacetsConfig config;

  /** {@code FacetsCollector} provided to the constructor. */
  final FacetsCollector fc;

  /** Maps parent ordinal to its child, or -1 if the parent is childless. */
  private ParallelTaxonomyArrays.IntArray children;

  /** Maps an ordinal to its sibling, or -1 if there is no sibling. */
  private ParallelTaxonomyArrays.IntArray siblings;

  /** Maps an ordinal to its parent, or -1 if there is no parent (root node). */
  final ParallelTaxonomyArrays.IntArray parents;

  /**
   * Constructor without a {@link FacetsCollector} - we don't have access to the hits, so we have to
   * assume there are hits when initializing internal data structures.
   *
   * @deprecated To be removed in Lucene 10.
   */
  @Deprecated
  protected TaxonomyFacets(String indexFieldName, TaxonomyReader taxoReader, FacetsConfig config)
      throws IOException {
    this(indexFieldName, taxoReader, config, null);
  }

  /** Dense ordinal counts. */
  int[] counts;

  /** Sparse ordinal counts. */
  IntIntHashMap sparseCounts;

  /** Have value counters been initialized. */
  boolean initialized;

  /** Defines comparison between aggregated values. */
  protected Comparator<Number> valueComparator;

  /**
   * Constructor with a {@link FacetsCollector}, allowing lazy initialization of internal data
   * structures.
   */
  TaxonomyFacets(
      String indexFieldName, TaxonomyReader taxoReader, FacetsConfig config, FacetsCollector fc)
      throws IOException {
    this.indexFieldName = indexFieldName;
    this.taxoReader = taxoReader;
    this.config = config;
    this.fc = fc;
    parents = taxoReader.getParallelTaxonomyArrays().parents();
    valueComparator = Comparator.comparingInt((x) -> (int) x);
  }

  /** Return true if a sparse hash table should be used for counting, instead of a dense int[]. */
  protected boolean useHashTable(FacetsCollector fc, TaxonomyReader taxoReader) {
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
    for (FacetsCollector.MatchingDocs docs : fc.getMatchingDocs()) {
      sumTotalHits += docs.totalHits;
      maxDoc += docs.context.reader().maxDoc();
    }

    // if our result set is < 10% of the index, we collect sparsely (use hash map):
    return sumTotalHits < maxDoc / 10;
  }

  /** If not done already, initialize the data structures storing counts. */
  protected void initializeValueCounters() {
    if (initialized) {
      return;
    }
    initialized = true;
    assert sparseCounts == null && counts == null;
    if (useHashTable(fc, taxoReader)) {
      sparseCounts = new IntIntHashMap();
    } else {
      counts = new int[taxoReader.getSize()];
    }
  }

  /** Set the count for this ordinal to {@code newValue}. */
  protected void setCount(int ordinal, int newValue) {
    if (sparseCounts != null) {
      sparseCounts.put(ordinal, newValue);
    } else {
      counts[ordinal] = newValue;
    }
  }

  /** Get the count for this ordinal. */
  protected int getCount(int ordinal) {
    if (sparseCounts != null) {
      return sparseCounts.get(ordinal);
    } else {
      return counts[ordinal];
    }
  }

  /** Get the aggregation value for this ordinal. */
  protected Number getAggregationValue(int ordinal) {
    // By default, this is just the count.
    return getCount(ordinal);
  }

  /** Apply an aggregation to the two values and return the result. */
  protected Number aggregate(Number existingVal, Number newVal) {
    // By default, we are computing counts, so the values are interpreted as integers and summed.
    return (int) existingVal + (int) newVal;
  }

  /** Were any values actually aggregated during counting? */
  boolean hasValues() {
    return initialized;
  }

  /**
   * Returns int[] mapping each ordinal to its first child; this is a large array and is computed
   * (and then saved) the first time this method is invoked.
   */
  ParallelTaxonomyArrays.IntArray getChildren() throws IOException {
    if (children == null) {
      children = taxoReader.getParallelTaxonomyArrays().children();
    }
    return children;
  }

  /**
   * Returns int[] mapping each ordinal to its next sibling; this is a large array and is computed
   * (and then saved) the first time this method is invoked.
   */
  ParallelTaxonomyArrays.IntArray getSiblings() throws IOException {
    if (siblings == null) {
      siblings = taxoReader.getParallelTaxonomyArrays().siblings();
    }
    return siblings;
  }

  /**
   * Returns true if the (costly, and lazily initialized) children int[] was initialized.
   *
   * @lucene.experimental
   */
  public boolean childrenLoaded() {
    return children != null;
  }

  /**
   * Returns true if the (costly, and lazily initialized) sibling int[] was initialized.
   *
   * @lucene.experimental
   */
  public boolean siblingsLoaded() {
    return siblings != null;
  }

  /**
   * Verifies and returns {@link DimConfig} for the given dimension name.
   *
   * @return {@link DimConfig} for the given dim, or {@link FacetsConfig#DEFAULT_DIM_CONFIG} if it
   *     was never manually configured.
   * @throws IllegalArgumentException if the provided dimension was manually configured, but its
   *     {@link DimConfig#indexFieldName} does not match {@link #indexFieldName}.
   */
  protected DimConfig verifyDim(String dim) {
    FacetsConfig.DimConfig dimConfig = config.getDimConfig(dim);
    if (config.isDimConfigured(dim) == true
        && dimConfig.indexFieldName.equals(indexFieldName) == false) {
      throw new IllegalArgumentException(
          String.format(
              Locale.ROOT,
              "dimension \"%s\" cannot be found in field \"%s\", since it was configured "
                  + "to be indexed into field \"%s\"",
              dim,
              indexFieldName,
              dimConfig.indexFieldName));
    }
    return dimConfig;
  }

  /**
   * Roll-up the aggregation values from {@code childOrdinal} to {@code ordinal}. Overrides should
   * probably call this to update the counts. Overriding allows us to work with primitive types for
   * the aggregation values, keeping aggregation efficient.
   */
  protected void updateValueFromRollup(int ordinal, int childOrdinal) throws IOException {
    setCount(ordinal, getCount(ordinal) + rollup(childOrdinal));
  }

  /**
   * Return a {@link TopOrdAndNumberQueue} of the appropriate type, i.e. a {@link
   * TopOrdAndIntNumberQueue} or a {@link TopOrdAndFloatNumberQueue}.
   */
  protected TopOrdAndNumberQueue makeTopOrdAndNumberQueue(int topN) {
    return new TopOrdAndIntNumberQueue(Math.min(taxoReader.getSize(), topN));
  }

  // TODO: We don't need this if we're okay with having an integer -1 in the results even for float
  // aggregations.
  /** Return the value for a missing aggregation, i.e. {@code -1} or {@code -1f}. */
  protected Number missingAggregationValue() {
    return -1;
  }

  /** Rolls up any single-valued hierarchical dimensions. */
  protected void rollup() throws IOException {
    if (initialized == false) {
      return;
    }

    // Rollup any necessary dims:
    ParallelTaxonomyArrays.IntArray children = null;
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
          updateValueFromRollup(dimRootOrd, children.get(dimRootOrd));
        }
      }
    }
  }

  private int rollup(int ord) throws IOException {
    ParallelTaxonomyArrays.IntArray children = getChildren();
    ParallelTaxonomyArrays.IntArray siblings = getSiblings();
    int aggregatedValue = 0;
    while (ord != TaxonomyReader.INVALID_ORDINAL) {
      int currentValue = getCount(ord);
      int newValue = currentValue + rollup(children.get(ord));
      setCount(ord, newValue);
      aggregatedValue += getCount(ord);
      ord = siblings.get(ord);
    }
    return aggregatedValue;
  }

  /**
   * Create a FacetResult for the provided dim + path and intermediate results. Does the extra work
   * of resolving ordinals -> labels, etc. Will return null if there are no children.
   */
  private FacetResult createFacetResult(
      TopChildrenForPath topChildrenForPath, String dim, String... path) throws IOException {
    // If the intermediate result is null or there are no children, we return null:
    if (topChildrenForPath == null || topChildrenForPath.childCount == 0) {
      return null;
    }

    TopOrdAndNumberQueue q = topChildrenForPath.childQueue;
    assert q != null;

    LabelAndValue[] labelValues = new LabelAndValue[q.size()];
    int[] ordinals = new int[labelValues.length];
    Number[] values = new Number[labelValues.length];

    for (int i = labelValues.length - 1; i >= 0; i--) {
      TopOrdAndNumberQueue.OrdAndValue ordAndValue = q.pop();
      assert ordAndValue != null;
      ordinals[i] = ordAndValue.ord;
      values[i] = ordAndValue.getValue();
    }

    FacetLabel[] bulkPath = taxoReader.getBulkPath(ordinals);
    // The path component we're interested in is the one immediately after the provided path. We
    // add 1 here to also account for the dim:
    int childComponentIdx = path.length + 1;
    for (int i = 0; i < labelValues.length; i++) {
      labelValues[i] =
          new LabelAndValue(
              bulkPath[i].components[childComponentIdx], values[i], getCount(ordinals[i]));
    }

    return new FacetResult(
        dim, path, topChildrenForPath.pathValue, labelValues, topChildrenForPath.childCount);
  }

  @Override
  public FacetResult getAllChildren(String dim, String... path) throws IOException {
    DimConfig dimConfig = verifyDim(dim);
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
      for (IntIntHashMap.IntIntCursor ordAndCount : sparseCounts) {
        int ord = ordAndCount.key;
        int count = ordAndCount.value;
        Number value = getAggregationValue(ord);
        if (parents.get(ord) == dimOrd && count > 0) {
          aggregatedCount += count;
          aggregatedValue = aggregate(aggregatedValue, value);
          ordinals.add(ord);
          ordValues.add(value);
        }
      }
    } else {
      ParallelTaxonomyArrays.IntArray children = getChildren();
      ParallelTaxonomyArrays.IntArray siblings = getSiblings();
      int ord = children.get(dimOrd);
      while (ord != TaxonomyReader.INVALID_ORDINAL) {
        int count = counts[ord];
        Number value = getAggregationValue(ord);
        if (count > 0) {
          aggregatedCount += count;
          aggregatedValue = aggregate(aggregatedValue, value);
          ordinals.add(ord);
          ordValues.add(value);
        }
        ord = siblings.get(ord);
      }
    }

    if (aggregatedCount == 0) {
      return null;
    }

    if (dimConfig.multiValued) {
      if (dimConfig.requireDimCount) {
        aggregatedValue = getAggregationValue(dimOrd);
      } else {
        // Our aggregated value is not correct, in general:
        aggregatedValue = missingAggregationValue();
      }
    } else {
      // Our aggregateddim value is accurate, so we keep it
    }

    // TODO: It would be nice if TaxonomyReader let us pass in a buffer + size so we didn't have to
    // do an array copy here:
    FacetLabel[] bulkPath = taxoReader.getBulkPath(ordinals.toArray());

    LabelAndValue[] labelValues = new LabelAndValue[ordValues.size()];
    for (int i = 0; i < ordValues.size(); i++) {
      labelValues[i] =
          new LabelAndValue(
              bulkPath[i].components[cp.length], ordValues.get(i), getCount(ordinals.get(i)));
    }
    return new FacetResult(dim, path, aggregatedValue, labelValues, ordinals.size());
  }

  /**
   * Set the value for a {@link org.apache.lucene.facet.TopOrdAndNumberQueue.OrdAndValue} to the one
   * corresponding to the given ordinal.
   */
  protected void setIncomingValue(TopOrdAndNumberQueue.OrdAndValue incomingOrdAndValue, int ord) {
    ((TopOrdAndIntNumberQueue.OrdAndInt) incomingOrdAndValue).value = getCount(ord);
  }

  /** Insert an ordinal and the value corresponding to it into the queue. */
  protected TopOrdAndNumberQueue.OrdAndValue insertIntoQueue(
      TopOrdAndNumberQueue q, TopOrdAndNumberQueue.OrdAndValue incomingOrdAndValue, int ord) {
    if (incomingOrdAndValue == null) {
      incomingOrdAndValue = q.newOrdAndValue();
    }
    incomingOrdAndValue.ord = ord;
    setIncomingValue(incomingOrdAndValue, ord);

    incomingOrdAndValue = q.insertWithOverflow(incomingOrdAndValue);
    return incomingOrdAndValue;
  }

  /** An accumulator for an aggregated value. */
  protected abstract static class AggregatedValue {
    /** Aggregate the value corresponding to the given ordinal into this value. */
    public abstract void aggregate(int ord);

    /** Retrieve the encapsulated value. */
    public abstract Number get();

    /** Default constructor. */
    public AggregatedValue() {}
  }

  private class AggregatedCount extends AggregatedValue {
    private int count;

    private AggregatedCount(int count) {
      this.count = count;
    }

    @Override
    public void aggregate(int ord) {
      count += getCount(ord);
    }

    @Override
    public Number get() {
      return count;
    }
  }

  /** Initialize an accumulator. */
  protected AggregatedValue newAggregatedValue() {
    return new AggregatedCount(0);
  }

  /**
   * Determine the top-n children for a specified dimension + path. Results are in an intermediate
   * form.
   */
  protected TopChildrenForPath getTopChildrenForPath(DimConfig dimConfig, int pathOrd, int topN)
      throws IOException {
    TopOrdAndNumberQueue q = makeTopOrdAndNumberQueue(topN);

    AggregatedValue aggregatedValue = newAggregatedValue();
    int childCount = 0;

    TopOrdAndNumberQueue.OrdAndValue incomingOrdAndValue = null;

    // TODO: would be faster if we had a "get the following children" API?  then we
    // can make a single pass over the hashmap
    if (sparseCounts != null) {
      for (IntIntHashMap.IntIntCursor c : sparseCounts) {
        int ord = c.key;
        int count = c.value;
        if (parents.get(ord) == pathOrd && count > 0) {
          aggregatedValue.aggregate(ord);
          childCount++;

          incomingOrdAndValue = insertIntoQueue(q, incomingOrdAndValue, ord);
        }
      }
    } else {
      ParallelTaxonomyArrays.IntArray children = getChildren();
      ParallelTaxonomyArrays.IntArray siblings = getSiblings();
      int ord = children.get(pathOrd);
      while (ord != TaxonomyReader.INVALID_ORDINAL) {
        int count = counts[ord];
        if (count > 0) {
          aggregatedValue.aggregate(ord);
          childCount++;

          incomingOrdAndValue = insertIntoQueue(q, incomingOrdAndValue, ord);
        }
        ord = siblings.get(ord);
      }
    }

    Number aggregatedValueNumber = aggregatedValue.get();
    if (dimConfig.multiValued) {
      if (dimConfig.requireDimCount) {
        aggregatedValueNumber = getAggregationValue(pathOrd);
      } else {
        // Our aggregated value is not correct, in general:
        aggregatedValueNumber = missingAggregationValue();
      }
    }

    return new TopChildrenForPath(aggregatedValueNumber, childCount, q);
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

    if (initialized == false) {
      return null;
    }

    TopChildrenForPath topChildrenForPath = getTopChildrenForPath(dimConfig, dimOrd, topN);
    return createFacetResult(topChildrenForPath, dim, path);
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
    return initialized ? getAggregationValue(ord) : 0;
  }

  @Override
  public List<FacetResult> getAllDims(int topN) throws IOException {
    validateTopN(topN);

    if (hasValues() == false) {
      return Collections.emptyList();
    }

    ParallelTaxonomyArrays.IntArray children = getChildren();
    ParallelTaxonomyArrays.IntArray siblings = getSiblings();
    int ord = children.get(TaxonomyReader.ROOT_ORDINAL);
    List<FacetResult> results = new ArrayList<>();
    while (ord != TaxonomyReader.INVALID_ORDINAL) {
      String dim = taxoReader.getPath(ord).components[0];
      FacetsConfig.DimConfig dimConfig = config.getDimConfig(dim);
      if (dimConfig.indexFieldName.equals(indexFieldName)) {
        FacetResult result = getTopChildren(topN, dim);
        if (result != null) {
          results.add(result);
        }
      }
      ord = siblings.get(ord);
    }

    // Sort by highest value, tie break by dim:
    results.sort(BY_VALUE_THEN_DIM);
    return results;
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
    ParallelTaxonomyArrays.IntArray children = getChildren();
    ParallelTaxonomyArrays.IntArray siblings = getSiblings();

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
    int ord = children.get(TaxonomyReader.ROOT_ORDINAL);
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
              dimValue = getAggregationValue(dimOrd);
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
      ord = siblings.get(ord);
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
