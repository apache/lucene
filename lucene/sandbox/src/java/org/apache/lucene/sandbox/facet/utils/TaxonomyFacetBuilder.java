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
package org.apache.lucene.sandbox.facet.utils;

import java.io.IOException;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.sandbox.facet.cutters.FacetCutter;
import org.apache.lucene.sandbox.facet.cutters.TaxonomyFacetsCutter;
import org.apache.lucene.sandbox.facet.iterators.OrdinalIterator;
import org.apache.lucene.sandbox.facet.iterators.TaxonomyChildrenOrdinalIterator;
import org.apache.lucene.sandbox.facet.labels.TaxonomyOrdLabelBiMap;

/**
 * {@link FacetBuilder} for taxonomy facets.
 *
 * @lucene.experimental
 */
public final class TaxonomyFacetBuilder extends BaseFacetBuilder<TaxonomyFacetBuilder> {
  private final FacetsConfig facetsConfig;
  private final FacetsConfig.DimConfig dimConfig;
  private final TaxonomyReader taxonomyReader;
  private final String indexFieldName;
  private final CollectionKey collectionKey;

  // Post-collection vars
  private TaxonomyOrdLabelBiMap taxoOrdLabels;
  private int parentOrd = -1;

  public TaxonomyFacetBuilder(
      FacetsConfig facetsConfig, TaxonomyReader taxonomyReader, String dimension, String... path) {
    super(dimension, path);
    if (facetsConfig.isDimConfigured(dimension) == false) {
      // The reason the dimension config is required is that we want to be able to compute total
      // value for single value fields that use default config, and to do that we need to rollup
      // value for the dimension, but TaxonomyFacetsCutter uses FacetConfig#getDimConfigs
      // to find dimensions that need to be rolled up, hence it needs the dimension to be
      // configured explicitly.
      throw new IllegalArgumentException(
          "Dimension config for "
              + dimension
              + " is required."
              + " Call one of the FacetsConfig's setter with a default value.");
    }
    this.facetsConfig = facetsConfig;
    this.taxonomyReader = taxonomyReader;
    this.dimConfig = facetsConfig.getDimConfig(dimension);
    this.indexFieldName = dimConfig.indexFieldName;
    this.collectionKey = new CollectionKey(indexFieldName);
    // For taxo facets we sort by count by default
    this.withSortByCount();
  }

  private record CollectionKey(String indexFieldName) {}

  @Override
  Object collectionKey() {
    return collectionKey;
  }

  @Override
  FacetCutter getFacetCutter() {
    return new TaxonomyFacetsCutter(indexFieldName, facetsConfig, taxonomyReader);
  }

  private int getParentOrd() throws IOException {
    if (this.parentOrd < 0) {
      FacetLabel parentLabel = new FacetLabel(dimension, path);
      this.parentOrd = getOrdToLabel().getOrd(parentLabel);
    }
    return this.parentOrd;
  }

  @Override
  OrdinalIterator getMatchingOrdinalIterator() throws IOException {
    return new TaxonomyChildrenOrdinalIterator(
        super.getMatchingOrdinalIterator(),
        taxonomyReader.getParallelTaxonomyArrays().parents(),
        getParentOrd());
  }

  @Override
  Number getOverallValue() throws IOException {
    if (dimConfig.multiValued == false || dimConfig.requireDimCount) {
      return getValue(getParentOrd());
    }
    return -1; // Can't compute
  }

  @Override
  TaxonomyOrdLabelBiMap getOrdToLabel() {
    if (taxoOrdLabels == null) {
      taxoOrdLabels = new TaxonomyOrdLabelBiMap(taxonomyReader);
    }
    return taxoOrdLabels;
  }

  @Override
  TaxonomyFacetBuilder self() {
    return this;
  }
}
