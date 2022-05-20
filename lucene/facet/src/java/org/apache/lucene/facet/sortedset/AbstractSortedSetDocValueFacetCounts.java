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
package org.apache.lucene.facet.sortedset;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.FacetsConfig.DimConfig;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.TopOrdAndIntQueue;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState.DimTree;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState.OrdRange;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;

/** Base class for SSDV faceting implementations. */
abstract class AbstractSortedSetDocValueFacetCounts extends Facets {

  private static final Comparator<FacetResult> FACET_RESULT_COMPARATOR =
      new Comparator<>() {
        @Override
        public int compare(FacetResult a, FacetResult b) {
          if (a.value.intValue() > b.value.intValue()) {
            return -1;
          } else if (b.value.intValue() > a.value.intValue()) {
            return 1;
          } else {
            return a.dim.compareTo(b.dim);
          }
        }
      };

  final SortedSetDocValuesReaderState state;
  final FacetsConfig stateConfig;
  final SortedSetDocValues dv;
  final String field;

  AbstractSortedSetDocValueFacetCounts(SortedSetDocValuesReaderState state) throws IOException {
    this.state = state;
    this.field = state.getField();
    this.stateConfig = state.getFacetsConfig();
    this.dv = state.getDocValues();
  }

  @Override
  public FacetResult getTopChildren(int topN, String dim, String... path) throws IOException {
    validateTopN(topN);
    TopChildrenForPath topChildrenForPath = getTopChildrenForPath(topN, dim, path);
    return createFacetResult(topChildrenForPath, dim, path);
  }

  @Override
  public Number getSpecificValue(String dim, String... path) throws IOException {
    if (path.length != 1) {
      throw new IllegalArgumentException("path must be length=1");
    }
    int ord = (int) dv.lookupTerm(new BytesRef(FacetsConfig.pathToString(dim, path)));
    if (ord < 0) {
      return -1;
    }

    return getCount(ord);
  }

  @Override
  public List<FacetResult> getAllDims(int topN) throws IOException {
    validateTopN(topN);
    List<FacetResult> results = new ArrayList<>();
    for (String dim : state.getDims()) {
      TopChildrenForPath topChildrenForPath = getTopChildrenForPath(topN, dim);
      FacetResult facetResult = createFacetResult(topChildrenForPath, dim);
      if (facetResult != null) {
        results.add(facetResult);
      }
    }

    // Sort by highest count:
    results.sort(FACET_RESULT_COMPARATOR);
    return results;
  }

  @Override
  public List<FacetResult> getTopDims(int topNDims, int topNChildren) throws IOException {
    validateTopN(topNDims);
    validateTopN(topNChildren);

    // Creates priority queue to store top dimensions and sort by their aggregated values/hits and
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

    for (String dim : state.getDims()) {
      DimConfig dimConfig = stateConfig.getDimConfig(dim);
      int dimCount;
      if (dimConfig.hierarchical) {
        int dimOrd = state.getDimTree(dim).dimStartOrd;
        dimCount = getCount(dimOrd);
      } else {
        OrdRange ordRange = state.getOrdRange(dim);
        int dimOrd = ordRange.start;
        if (dimConfig.multiValued && dimConfig.requireDimCount) {
          dimCount = getCount(dimOrd);
        } else {
          PrimitiveIterator.OfInt childIt = ordRange.iterator();
          TopChildrenForPath topChildrenForPath =
              computeTopChildren(childIt, topNChildren, dimConfig, dimOrd);
          if (intermediateResults == null) {
            intermediateResults = new HashMap<>();
          }
          intermediateResults.put(dim, topChildrenForPath);
          dimCount = topChildrenForPath.pathCount;
        }
      }

      if (dimCount != 0) {
        if (pq.size() < topNDims) {
          pq.add(new DimValue(dim, dimCount));
        } else {
          if (dimCount > pq.top().value
              || (dimCount == pq.top().value && dim.compareTo(pq.top().dim) < 0)) {
            DimValue bottomDim = pq.top();
            bottomDim.dim = dim;
            bottomDim.value = dimCount;
            pq.updateTop();
          }
        }
      }
    }

    int resultSize = pq.size();
    FacetResult[] results = new FacetResult[resultSize];

    while (pq.size() > 0) {
      DimValue dimValue = pq.pop();
      assert dimValue != null;
      TopChildrenForPath topChildrenForPath = null;
      if (intermediateResults != null) {
        topChildrenForPath = intermediateResults.get(dimValue.dim);
      }
      if (topChildrenForPath == null) {
        topChildrenForPath = getTopChildrenForPath(topNChildren, dimValue.dim);
      }
      FacetResult facetResult = createFacetResult(topChildrenForPath, dimValue.dim);
      // should not be null since only dims with non-zero values were considered earlier
      assert facetResult != null;
      resultSize--;
      results[resultSize] = facetResult;
    }
    return Arrays.asList(results);
  }

  abstract int getCount(int ord);

  /**
   * Compute the top-n children for the given path and iterator of all immediate children of the
   * path. This returns an intermediate result that does the minimal required work, avoiding the
   * cost of looking up string labels, etc.
   */
  TopChildrenForPath computeTopChildren(
      PrimitiveIterator.OfInt childOrds, int topN, DimConfig dimConfig, int pathOrd) {
    TopOrdAndIntQueue q = null;
    int bottomCount = 0;
    int pathCount = 0;
    int childCount = 0;

    TopOrdAndIntQueue.OrdAndValue reuse = null;
    while (childOrds.hasNext()) {
      int ord = childOrds.next();
      int count = getCount(ord);
      if (count > 0) {
        pathCount += count;
        childCount++;
        if (count > bottomCount) {
          if (reuse == null) {
            reuse = new TopOrdAndIntQueue.OrdAndValue();
          }
          reuse.ord = ord;
          reuse.value = count;
          if (q == null) {
            // Lazy init, so we don't create this for the
            // sparse case unnecessarily
            q = new TopOrdAndIntQueue(topN);
          }
          reuse = q.insertWithOverflow(reuse);
          if (q.size() == topN) {
            bottomCount = q.top().value;
          }
        }
      }
    }

    if (dimConfig.hierarchical) {
      pathCount = getCount(pathOrd);
    } else {
      // see if pathCount is actually reliable or needs to be reset
      if (dimConfig.multiValued) {
        if (dimConfig.requireDimCount) {
          pathCount = getCount(pathOrd);
        } else {
          pathCount = -1; // pathCount is inaccurate at this point, so set it to -1
        }
      }
    }

    return new TopChildrenForPath(pathCount, childCount, q);
  }

  /**
   * Determine the top-n children for a specified dimension + path. Results are in an intermediate
   * form.
   */
  TopChildrenForPath getTopChildrenForPath(int topN, String dim, String... path)
      throws IOException {
    FacetsConfig.DimConfig dimConfig = stateConfig.getDimConfig(dim);

    // Determine the path ord and resolve an iterator to its immediate children. The logic for this
    // depends on whether-or-not the dimension is configured as hierarchical:
    final int pathOrd;
    final PrimitiveIterator.OfInt childIterator;
    if (dimConfig.hierarchical) {
      DimTree dimTree = state.getDimTree(dim);
      if (path.length > 0) {
        pathOrd = (int) dv.lookupTerm(new BytesRef(FacetsConfig.pathToString(dim, path)));
      } else {
        // If there's no path, this is a little more efficient to just look up the dim:
        pathOrd = dimTree.dimStartOrd;
      }
      if (pathOrd < 0) {
        // path was never indexed
        return null;
      }
      childIterator = dimTree.iterator(pathOrd);
    } else {
      if (path.length > 0) {
        throw new IllegalArgumentException(
            "Field is not configured as hierarchical, path should be 0 length");
      }
      OrdRange ordRange = state.getOrdRange(dim);
      if (ordRange == null) {
        // means dimension was never indexed
        return null;
      }
      pathOrd = ordRange.start;
      childIterator = ordRange.iterator();
      if (dimConfig.multiValued && dimConfig.requireDimCount) {
        // If the dim is multi-valued and requires dim counts, we know we've explicitly indexed
        // the dimension and we need to skip past it so the iterator is positioned on the first
        // child:
        childIterator.next();
      }
    }

    // Compute the actual results:
    return computeTopChildren(childIterator, topN, dimConfig, pathOrd);
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

    TopOrdAndIntQueue q = topChildrenForPath.q;
    assert q != null;

    LabelAndValue[] labelValues = new LabelAndValue[q.size()];
    for (int i = labelValues.length - 1; i >= 0; i--) {
      TopOrdAndIntQueue.OrdAndValue ordAndValue = q.pop();
      assert ordAndValue != null;
      final BytesRef term = dv.lookupOrd(ordAndValue.ord);
      String[] parts = FacetsConfig.stringToPath(term.utf8ToString());
      labelValues[i] = new LabelAndValue(parts[parts.length - 1], ordAndValue.value);
    }

    return new FacetResult(
        dim, path, topChildrenForPath.pathCount, labelValues, topChildrenForPath.childCount);
  }

  /** Intermediate result to store top children for a given path before resolving labels, etc. */
  record TopChildrenForPath(int pathCount, int childCount, TopOrdAndIntQueue q) {}

  static final class DimValue {
    String dim;
    int value;

    DimValue(String dim, int value) {
      this.dim = dim;
      this.value = value;
    }
  }
}
