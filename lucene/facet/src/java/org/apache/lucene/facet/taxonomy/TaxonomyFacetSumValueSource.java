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
import org.apache.lucene.facet.FacetUtils;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollector.MatchingDocs;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.util.IntsRef;

/**
 * Aggregates sum of values from {@link DoubleValues#doubleValue()}, for each facet label.
 *
 * @lucene.experimental
 */
public class TaxonomyFacetSumValueSource extends FloatTaxonomyFacets {
  private final OrdinalsReader ordinalsReader;

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

    // Maintain backwards compatibility with the older binary format using an OrdinalsReader (which
    // can support both formats currently):
    // TODO: Remove this logic and set ordinalsReader to null in Lucene 11:
    MatchingDocs first = fc.getMatchingDocs().isEmpty() ? null : fc.getMatchingDocs().get(0);
    if (first != null && FacetUtils.usesOlderBinaryOrdinals(first.context.reader(), indexField)) {
      this.ordinalsReader = new DocValuesOrdinalsReader(indexField);
    } else {
      this.ordinalsReader = null;
    }

    sumValues(fc.getMatchingDocs(), fc.getKeepScores(), valueSource);
  }

  /**
   * Aggreggates float facet values from the provided {@link DoubleValuesSource}, and pulls ordinals
   * from the provided {@link OrdinalsReader}.
   */
  public TaxonomyFacetSumValueSource(
      OrdinalsReader ordinalsReader,
      TaxonomyReader taxoReader,
      FacetsConfig config,
      FacetsCollector fc,
      DoubleValuesSource vs)
      throws IOException {
    super(ordinalsReader.getIndexFieldName(), taxoReader, config);
    this.ordinalsReader = ordinalsReader;
    sumValues(fc.getMatchingDocs(), fc.getKeepScores(), vs);
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
        index++;
        return true;
      }
    };
  }

  private void sumValues(
      List<MatchingDocs> matchingDocs, boolean keepScores, DoubleValuesSource valueSource)
      throws IOException {

    if (ordinalsReader != null) {
      // If the user provided a custom ordinals reader, use it to retrieve the document ordinals:
      IntsRef scratch = new IntsRef();
      for (MatchingDocs hits : matchingDocs) {
        OrdinalsReader.OrdinalsSegmentReader ords = ordinalsReader.getReader(hits.context);
        DoubleValues scores = keepScores ? scores(hits) : null;
        DoubleValues functionValues = valueSource.getValues(hits.context, scores);
        DocIdSetIterator docs = hits.bits.iterator();

        int doc;
        while ((doc = docs.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          ords.get(doc, scratch);
          if (functionValues.advanceExact(doc)) {
            float value = (float) functionValues.doubleValue();
            for (int i = 0; i < scratch.length; i++) {
              values[scratch.ints[i]] += value;
            }
          }
        }
      }
    } else {
      // If no custom ordinals reader is provided, expect the default encoding:
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
    }

    rollup();
  }
}
