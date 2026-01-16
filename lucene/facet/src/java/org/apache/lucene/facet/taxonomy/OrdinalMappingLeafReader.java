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
import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.FacetsConfig.DimConfig;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter.OrdinalMap;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.FilterSortedNumericDocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.search.DocIdSetIterator;

/**
 * A {@link org.apache.lucene.index.FilterLeafReader} for updating facets ordinal references, based
 * on an ordinal map. You should use this code in conjunction with merging taxonomies - after you
 * merge taxonomies, you receive an {@link OrdinalMap} which maps the 'old' ordinals to the 'new'
 * ones. You can use that map to re-map the doc values which contain the facets information
 * (ordinals) either before or while merging the indexes.
 *
 * <p>For re-mapping the ordinals during index merge, do the following:
 *
 * <pre><code class="language-java">
 * // merge the old taxonomy with the new one.
 * OrdinalMap map = new MemoryOrdinalMap();
 * DirectoryTaxonomyWriter.addTaxonomy(srcTaxoDir, map);
 * int[] ordmap = map.getMap();
 *
 * // Add the index and re-map ordinals on the go
 * DirectoryReader reader = DirectoryReader.open(oldDir);
 * IndexWriterConfig conf = new IndexWriterConfig(VER, ANALYZER);
 * IndexWriter writer = new IndexWriter(newDir, conf);
 * List&lt;LeafReaderContext&gt; leaves = reader.leaves();
 * LeafReader wrappedLeaves[] = new LeafReader[leaves.size()];
 * for (int i = 0; i &lt; leaves.size(); i++) {
 *   wrappedLeaves[i] = new OrdinalMappingLeafReader(leaves.get(i).reader(), ordmap);
 * }
 * writer.addIndexes(new MultiReader(wrappedLeaves));
 * writer.commit();
 * </code></pre>
 *
 * @lucene.experimental
 */
public class OrdinalMappingLeafReader extends FilterLeafReader {

  private class OrdinalMappingSortedNumericDocValues extends FilterSortedNumericDocValues {
    private final IntArrayList currentValues;
    private int currIndex;

    OrdinalMappingSortedNumericDocValues(SortedNumericDocValues in) {
      super(in);
      currentValues = new IntArrayList(32);
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      boolean result = in.advanceExact(target);
      if (result) {
        reloadValues();
      }
      return result;
    }

    @Override
    public int advance(int target) throws IOException {
      int result = in.advance(target);
      if (result != DocIdSetIterator.NO_MORE_DOCS) {
        reloadValues();
      }
      return result;
    }

    @Override
    public int nextDoc() throws IOException {
      int result = in.nextDoc();
      if (result != DocIdSetIterator.NO_MORE_DOCS) {
        reloadValues();
      }
      return result;
    }

    @Override
    public int docValueCount() {
      return currentValues.elementsCount;
    }

    private void reloadValues() throws IOException {
      currIndex = 0;
      currentValues.clear();
      for (int i = 0; i < in.docValueCount(); i++) {
        int originalOrd = Math.toIntExact(in.nextValue());
        currentValues.add(ordinalMap[originalOrd]);
      }
      Arrays.sort(currentValues.buffer, 0, currentValues.elementsCount);
    }

    @Override
    public long nextValue() {
      assert currIndex < currentValues.size();
      int actual = currentValues.get(currIndex);
      currIndex++;
      return actual;
    }
  }

  private final int[] ordinalMap;
  private final Set<String> facetFields;

  /**
   * Wraps an LeafReader, mapping ordinals according to the ordinalMap, using the provided {@link
   * FacetsConfig} which was used to build the wrapped reader.
   */
  public OrdinalMappingLeafReader(LeafReader in, int[] ordinalMap, FacetsConfig srcConfig) {
    super(in);
    this.ordinalMap = ordinalMap;
    facetFields = new HashSet<>();
    for (DimConfig dc : srcConfig.getDimConfigs().values()) {
      facetFields.add(dc.indexFieldName);
    }
    // always add the default indexFieldName. This is because FacetsConfig does
    // not explicitly record dimensions that were indexed under the default
    // DimConfig, unless they have a custom DimConfig.
    facetFields.add(FacetsConfig.DEFAULT_DIM_CONFIG.indexFieldName);
  }

  @Override
  public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
    SortedNumericDocValues original = in.getSortedNumericDocValues(field);
    if (original != null && facetFields.contains(field)) {
      // The requested field is a facet ordinals field _and_ it's non-null, so move forward with
      // mapping:
      return new OrdinalMappingSortedNumericDocValues(original);
    } else {
      // The requested field either isn't present (null) or isn't a facet ordinals field. Either
      // way, just return the original:
      return original;
    }
  }

  @Override
  public CacheHelper getCoreCacheHelper() {
    return null;
  }

  @Override
  public CacheHelper getReaderCacheHelper() {
    return null;
  }
}
