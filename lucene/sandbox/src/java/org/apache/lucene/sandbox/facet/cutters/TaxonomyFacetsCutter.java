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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.ParallelTaxonomyArrays;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.sandbox.facet.iterators.OrdinalIterator;

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

  private ParallelTaxonomyArrays.IntArray children;
  private ParallelTaxonomyArrays.IntArray siblings;

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

  @Override
  public LeafFacetCutter createLeafCutter(LeafReaderContext context) throws IOException {
    SortedNumericDocValues multiValued =
        DocValues.getSortedNumeric(context.reader(), indexFieldName);
    // DocValues.getSortedNumeric never returns null
    assert multiValued != null;
    // TODO: if multiValued is emptySortedNumeric we can throw CollectionTerminatedException
    //       in FacetFieldLeafCollector and save some CPU cycles.
    TaxonomyLeafFacetCutterMultiValue leafCutter =
        new TaxonomyLeafFacetCutterMultiValue(multiValued);
    return leafCutter;

    // TODO: does unwrapping Single valued make things any faster? We still need to wrap it into
    //       LeafFacetCutter
    // NumericDocValues singleValued = DocValues.unwrapSingleton(multiValued);
  }

  @Override
  public OrdinalIterator getOrdinalsToRollup() throws IOException {
    if (disableRollup) {
      return null;
    }

    // Rollup any necessary dims:
    Iterator<Map.Entry<String, FacetsConfig.DimConfig>> dimensions =
        facetsConfig.getDimConfigs().entrySet().iterator();

    ArrayList<FacetLabel> dimsToRollup = new ArrayList<>();

    while (dimensions.hasNext()) {
      Map.Entry<String, FacetsConfig.DimConfig> ent = dimensions.next();
      String dim = ent.getKey();
      FacetsConfig.DimConfig ft = ent.getValue();
      if (ft.multiValued == false && ft.indexFieldName.equals(indexFieldName)) {
        // ft.hierarchical is ignored - rollup even if path length is only two
        dimsToRollup.add(new FacetLabel(dim));
      }
    }

    int[] dimOrdToRollup = taxoReader.getBulkOrdinals(dimsToRollup.toArray(new FacetLabel[0]));

    return new OrdinalIterator() {
      int currentIndex = 0;

      @Override
      public int nextOrd() throws IOException {
        for (; currentIndex < dimOrdToRollup.length; currentIndex++) {
          // It can be invalid if this field was declared in the
          // config but never indexed
          if (dimOrdToRollup[currentIndex] != TaxonomyReader.INVALID_ORDINAL) {
            return dimOrdToRollup[currentIndex++];
          }
        }
        return NO_MORE_ORDS;
      }
    };
  }

  @Override
  public OrdinalIterator getChildrenOrds(final int parentOrd) throws IOException {
    ParallelTaxonomyArrays.IntArray children = getChildren();
    ParallelTaxonomyArrays.IntArray siblings = getSiblings();
    return new OrdinalIterator() {
      int currentChild = parentOrd;

      @Override
      public int nextOrd() {
        if (currentChild == parentOrd) {
          currentChild = children.get(currentChild);
        } else {
          currentChild = siblings.get(currentChild);
        }
        if (currentChild != TaxonomyReader.INVALID_ORDINAL) {
          return currentChild;
        }
        return NO_MORE_ORDS;
      }
    };
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
