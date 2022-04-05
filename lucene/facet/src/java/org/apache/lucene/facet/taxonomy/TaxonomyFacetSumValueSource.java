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
import java.util.List;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollector.MatchingDocs;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;

/**
 * Aggregates sum of values from {@link DoubleValues#doubleValue()}, for each facet label.
 *
 * @lucene.experimental
 */
public class TaxonomyFacetSumValueSource extends FloatTaxonomyFacets {

  /**
   * Aggreggates double facet values from the provided {@link DoubleValuesSource}, pulling ordinals
   * from the default indexed facet field {@link FacetsConfig#DEFAULT_INDEX_FIELD_NAME}.
   */
  public TaxonomyFacetSumValueSource(
      TaxonomyReader taxoReader,
      FacetsConfig config,
      FacetsCollector fc,
      DoubleValuesSource valueSource)
      throws IOException {
    this(FacetsConfig.DEFAULT_INDEX_FIELD_NAME, taxoReader, config, fc, valueSource);
  }

  /**
   * Aggreggates double facet values from the provided {@link DoubleValuesSource}, pulling ordinals
   * from the specified indexed facet field.
   */
  public TaxonomyFacetSumValueSource(
      String indexField,
      TaxonomyReader taxoReader,
      FacetsConfig config,
      FacetsCollector fc,
      DoubleValuesSource valueSource)
      throws IOException {
    super(indexField, taxoReader, config);
    sumValues(fc.getMatchingDocs(), fc.getKeepScores(), valueSource);
  }

  private static DoubleValues scores(MatchingDocs hits) {
    return new DoubleValues() {

      int index = -1;

      @Override
      public double doubleValue() throws IOException {
        return hits.scores[index];
      }

      @Override
      public boolean advanceExact(int doc) throws IOException {
        index = doc;
        return true;
      }
    };
  }

  private void sumValues(
      List<MatchingDocs> matchingDocs, boolean keepScores, DoubleValuesSource valueSource)
      throws IOException {

    for (MatchingDocs hits : matchingDocs) {
      SortedNumericDocValues ordinalValues =
          DocValues.getSortedNumeric(hits.context.reader(), indexFieldName);
      DoubleValues scores = keepScores ? scores(hits) : null;
      DoubleValues functionValues = valueSource.getValues(hits.context, scores);
      DocIdSetIterator it =
          ConjunctionUtils.intersectIterators(List.of(hits.bits.iterator(), ordinalValues));

      for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
        if (functionValues.advanceExact(doc)) {
          float value = (float) functionValues.doubleValue();
          int ordinalCount = ordinalValues.docValueCount();
          for (int i = 0; i < ordinalCount; i++) {
            values[(int) ordinalValues.nextValue()] += value;
          }
        }
      }
    }

    rollup();
  }
}
