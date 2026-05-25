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
package org.apache.lucene.sandbox.facet.cutters;

import java.io.IOException;
import java.util.Map;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.ParallelTaxonomyArrays;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.internal.hppc.IntHashSet;
import org.apache.lucene.sandbox.facet.iterators.OrdinalIterator;
import org.apache.lucene.util.ArrayUtil;

/**
 * {@link FacetCutter} for facets that use taxonomy side-car index.
 *
 * @lucene.experimental
 */
public final class TaxonomyFacetsCutter implements FacetCutter {

  private final FacetsConfig facetsConfig;
  private final TaxonomyReader taxoReader;
  private final String indexFieldName;
  private final boolean disableRollup;

  // Lazy; loaded on first remapOrd call (which is single-threaded, inside reduce).
  private ParallelTaxonomyArrays.IntArray parents;
  // Null until first getSingleValuedDimOrds() call; empty when disableRollup=true.
  private IntHashSet singleValuedDimOrds;

  // Reusable scratch buffer for the ancestor path (reduce is single-threaded, and the
  // returned OrdinalIterator is always fully consumed before the next remapOrd call).
  private int[] ancestorBuf = new int[8];

  /** Create {@link FacetCutter} for taxonomy facets. */
  public TaxonomyFacetsCutter(
      String indexFieldName, FacetsConfig facetsConfig, TaxonomyReader taxoReader) {
    this(indexFieldName, facetsConfig, taxoReader, false);
  }

  /**
   * Expert: Create {@link FacetCutter} for taxonomy facets.
   *
   * @param disableRollup if set to true, rollup is disabled. In most cases users should not use it.
   *     Setting it to true silently leads to incorrect results for dimensions that require rollup.
   *     At the same time, if you are sure that there are no dimensions that require rollup, setting
   *     it to true might improve performance.
   */
  public TaxonomyFacetsCutter(
      String indexFieldName,
      FacetsConfig facetsConfig,
      TaxonomyReader taxoReader,
      boolean disableRollup) {
    this.facetsConfig = facetsConfig;
    this.indexFieldName = indexFieldName;
    this.taxoReader = taxoReader;
    this.disableRollup = disableRollup;
  }

  /**
   * Returns int[] mapping each ordinal to its parent; this is a large array and is computed (and
   * then saved) the first time this method is invoked.
   */
  private ParallelTaxonomyArrays.IntArray getParents() throws IOException {
    if (parents == null) {
      parents = taxoReader.getParallelTaxonomyArrays().parents();
    }
    return parents;
  }

  /**
   * Returns the set of dimension ordinals that require rollup (single-valued dims whose index field
   * matches this cutter). Returns an empty set when rollup is disabled. Computed lazily and cached.
   */
  private IntHashSet getSingleValuedDimOrds() throws IOException {
    if (singleValuedDimOrds != null) {
      return singleValuedDimOrds;
    }
    singleValuedDimOrds = new IntHashSet();
    if (disableRollup) {
      return singleValuedDimOrds;
    }
    for (Map.Entry<String, FacetsConfig.DimConfig> ent : facetsConfig.getDimConfigs().entrySet()) {
      String dim = ent.getKey();
      FacetsConfig.DimConfig ft = ent.getValue();
      if (ft.multiValued == false && ft.indexFieldName.equals(indexFieldName)) {
        // ft.hierarchical is ignored - rollup even if path length is only two
        int dimOrd = taxoReader.getOrdinal(new FacetLabel(dim));
        if (dimOrd != TaxonomyReader.INVALID_ORDINAL) {
          singleValuedDimOrds.add(dimOrd);
        }
      }
    }
    return singleValuedDimOrds;
  }

  @Override
  public boolean needsRemapping() throws IOException {
    // Skip remapping when rollup is disabled or when all dims are multi-valued
    // (single-valued dim set is empty). In both cases every ordinal maps to itself.
    return !getSingleValuedDimOrds().isEmpty();
  }

  @Override
  public OrdinalIterator remapOrd(int ord) throws IOException {
    ParallelTaxonomyArrays.IntArray parentsArr = getParents();

    // Walk up from ord to the direct child of ROOT, collecting the ancestor path into a
    // reusable scratch buffer.
    int len = 0;
    int cur = ord;
    while (true) {
      ancestorBuf = ArrayUtil.grow(ancestorBuf, len + 1);
      ancestorBuf[len++] = cur;
      int parent = parentsArr.get(cur);
      if (parent == TaxonomyReader.ROOT_ORDINAL) {
        break;
      }
      cur = parent;
    }
    if (getSingleValuedDimOrds().contains(cur) == false) {
      // Multi-valued dim or rollup disabled: ordinal maps only to itself.
      return OrdinalIterator.fromSingleOrd(ord);
    }

    // Single-valued dim: emit the full path from the leaf ordinal up to and including the
    // dim ordinal. The returned iterator reads directly from ancestorBuf and is always fully
    // consumed before the next remapOrd call, so the shared buffer is safe to reuse.
    final int capturedLen = len;
    return new OrdinalIterator() {
      int idx = 0;

      @Override
      public int nextOrd() {
        return idx < capturedLen ? ancestorBuf[idx++] : NO_MORE_ORDS;
      }
    };
  }

  @Override
  public LeafFacetCutter createLeafCutter(LeafReaderContext context) throws IOException {
    SortedNumericDocValues multiValued =
        DocValues.getSortedNumeric(context.reader(), indexFieldName);
    // DocValues.getSortedNumeric never returns null
    assert multiValued != null;
    // TODO: if multiValued is emptySortedNumeric we can throw CollectionTerminatedException
    //       in FacetFieldLeafCollector and save some CPU cycles.
    return new TaxonomyLeafFacetCutterMultiValue(multiValued);

    // TODO: does unwrapping Single valued make things any faster? We still need to wrap it into
    //       LeafFacetCutter
    // NumericDocValues singleValued = DocValues.unwrapSingleton(multiValued);
  }

  private static class TaxonomyLeafFacetCutterMultiValue implements LeafFacetCutter {
    private final SortedNumericDocValues multiValued;
    private int ordsInDoc;

    private TaxonomyLeafFacetCutterMultiValue(SortedNumericDocValues multiValued) {
      this.multiValued = multiValued;
    }

    @Override
    public int nextOrd() throws IOException {
      if (ordsInDoc > 0) {
        ordsInDoc--;
        return (int) multiValued.nextValue();
      }
      return LeafFacetCutter.NO_MORE_ORDS;
    }

    @Override
    public boolean advanceExact(int doc) throws IOException {
      if (multiValued.advanceExact(doc)) {
        ordsInDoc = multiValued.docValueCount();
        return true;
      }
      return false;
    }
  }
}
