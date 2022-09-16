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
import java.util.List;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollector.MatchingDocs;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.util.Bits;

/**
 * Computes facets counts, assuming the default encoding into DocValues was used.
 *
 * @lucene.experimental
 */
public class FastTaxonomyFacetCounts extends IntTaxonomyFacets {

  /** Create {@code FastTaxonomyFacetCounts}, which also counts all facet labels. */
  public FastTaxonomyFacetCounts(TaxonomyReader taxoReader, FacetsConfig config, FacetsCollector fc)
      throws IOException {
    this(FacetsConfig.DEFAULT_INDEX_FIELD_NAME, taxoReader, config, fc);
  }

  /**
   * Create {@code FastTaxonomyFacetCounts}, using the specified {@code indexFieldName} for
   * ordinals. Use this if you had set {@link FacetsConfig#setIndexFieldName} to change the index
   * field name for certain dimensions.
   */
  public FastTaxonomyFacetCounts(
      String indexFieldName, TaxonomyReader taxoReader, FacetsConfig config, FacetsCollector fc)
      throws IOException {
    super(indexFieldName, taxoReader, config, AssociationAggregationFunction.SUM, fc);
    count(fc.getMatchingDocs());
  }

  /**
   * Create {@code FastTaxonomyFacetCounts}, using the specified {@code indexFieldName} for
   * ordinals, and counting all non-deleted documents in the index. This is the same result as
   * searching on {@link MatchAllDocsQuery}, but faster
   */
  public FastTaxonomyFacetCounts(
      String indexFieldName, IndexReader reader, TaxonomyReader taxoReader, FacetsConfig config)
      throws IOException {
    super(indexFieldName, taxoReader, config, AssociationAggregationFunction.SUM, null);
    countAll(reader);
  }

  private void count(List<MatchingDocs> matchingDocs) throws IOException {
    for (MatchingDocs hits : matchingDocs) {
      SortedNumericDocValues multiValued =
          hits.context.reader().getSortedNumericDocValues(indexFieldName);
      if (multiValued == null) {
        continue;
      }

      NumericDocValues singleValued = DocValues.unwrapSingleton(multiValued);

      DocIdSetIterator valuesIt = singleValued != null ? singleValued : multiValued;
      DocIdSetIterator it =
          ConjunctionUtils.intersectIterators(Arrays.asList(hits.bits.iterator(), valuesIt));

      if (singleValued != null) {
        if (values != null) {
          while (it.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            values[(int) singleValued.longValue()]++;
          }
        } else {
          while (it.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            sparseValues.addTo((int) singleValued.longValue(), 1);
          }
        }
      } else {
        if (values != null) {
          while (it.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            for (int i = 0; i < multiValued.docValueCount(); i++) {
              values[(int) multiValued.nextValue()]++;
            }
          }
        } else {
          while (it.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            for (int i = 0; i < multiValued.docValueCount(); i++) {
              sparseValues.addTo((int) multiValued.nextValue(), 1);
            }
          }
        }
      }
    }

    rollup();
  }

  private void countAll(IndexReader reader) throws IOException {
    assert values != null;
    for (LeafReaderContext context : reader.leaves()) {
      SortedNumericDocValues multiValued =
          context.reader().getSortedNumericDocValues(indexFieldName);
      if (multiValued == null) {
        continue;
      }

      Bits liveDocs = context.reader().getLiveDocs();

      NumericDocValues singleValued = DocValues.unwrapSingleton(multiValued);
      if (singleValued != null) {
        if (liveDocs == null) {
          for (int doc = singleValued.nextDoc();
              doc != DocIdSetIterator.NO_MORE_DOCS;
              doc = singleValued.nextDoc()) {
            values[(int) singleValued.longValue()]++;
          }
        } else {
          for (int doc = singleValued.nextDoc();
              doc != DocIdSetIterator.NO_MORE_DOCS;
              doc = singleValued.nextDoc()) {
            if (liveDocs.get(doc) == false) {
              continue;
            }
            values[(int) singleValued.longValue()]++;
          }
        }
      } else {
        if (liveDocs == null) {
          for (int doc = multiValued.nextDoc();
              doc != DocIdSetIterator.NO_MORE_DOCS;
              doc = multiValued.nextDoc()) {
            for (int i = 0; i < multiValued.docValueCount(); i++) {
              values[(int) multiValued.nextValue()]++;
            }
          }
        } else {
          for (int doc = multiValued.nextDoc();
              doc != DocIdSetIterator.NO_MORE_DOCS;
              doc = multiValued.nextDoc()) {
            if (liveDocs.get(doc) == false) {
              continue;
            }
            for (int i = 0; i < multiValued.docValueCount(); i++) {
              values[(int) multiValued.nextValue()]++;
            }
          }
        }
      }
    }

    rollup();
  }
}
