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
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollector.MatchingDocs;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;

/**
 * Aggregates float values associated with facet fields. Supports two different approaches:
 *
 * <ol>
 *   <li>Fields can be indexed with {@link FloatAssociationFacetField}, associating weights with
 *       facet values at indexing time.
 *   <li>Fields can be indexed with {@link FacetField} and a {@link DoubleValuesSource} can
 *       dynamically supply a weight from each doc. With this approach, the document's weight gets
 *       contributed to each facet value associated with the doc.
 * </ol>
 *
 * Aggregation logic is supplied by the provided {@link FloatAssociationFacetField}.
 *
 * @lucene.experimental
 */
public class TaxonomyFacetFloatAssociations extends FloatTaxonomyFacets {

  /** Create {@code TaxonomyFacetFloatAssociations} against the default index field. */
  public TaxonomyFacetFloatAssociations(
      TaxonomyReader taxoReader,
      FacetsConfig config,
      FacetsCollector fc,
      AssociationAggregationFunction aggregationFunction)
      throws IOException {
    this(FacetsConfig.DEFAULT_INDEX_FIELD_NAME, taxoReader, config, fc, aggregationFunction);
  }

  /**
   * Create {@code TaxonomyFacetFloatAssociations} against the default index field. Sources values
   * from the provided {@code valuesSource}.
   */
  public TaxonomyFacetFloatAssociations(
      TaxonomyReader taxoReader,
      FacetsConfig config,
      FacetsCollector fc,
      AssociationAggregationFunction aggregationFunction,
      DoubleValuesSource valuesSource)
      throws IOException {
    this(
        FacetsConfig.DEFAULT_INDEX_FIELD_NAME,
        taxoReader,
        config,
        fc,
        aggregationFunction,
        valuesSource);
  }

  /** Create {@code TaxonomyFacetFloatAssociations} against the specified index field. */
  public TaxonomyFacetFloatAssociations(
      String indexFieldName,
      TaxonomyReader taxoReader,
      FacetsConfig config,
      FacetsCollector fc,
      AssociationAggregationFunction aggregationFunction)
      throws IOException {
    super(indexFieldName, taxoReader, aggregationFunction, config);
    aggregateValues(aggregationFunction, fc.getMatchingDocs());
  }

  /**
   * Create {@code TaxonomyFacetFloatAssociations} against the specified index field. Sources values
   * from the provided {@code valuesSource}.
   */
  public TaxonomyFacetFloatAssociations(
      String indexFieldName,
      TaxonomyReader taxoReader,
      FacetsConfig config,
      FacetsCollector fc,
      AssociationAggregationFunction aggregationFunction,
      DoubleValuesSource valuesSource)
      throws IOException {
    super(indexFieldName, taxoReader, aggregationFunction, config);
    aggregateValues(aggregationFunction, fc.getMatchingDocs(), fc.getKeepScores(), valuesSource);
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

  /** Aggregate using the provided {@code DoubleValuesSource}. */
  private void aggregateValues(
      AssociationAggregationFunction aggregationFunction,
      List<MatchingDocs> matchingDocs,
      boolean keepScores,
      DoubleValuesSource valueSource)
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
            int ord = (int) ordinalValues.nextValue();
            float newValue = aggregationFunction.aggregate(values[ord], value);
            values[ord] = newValue;
          }
        }
      }
    }

    // Hierarchical dimensions are supported when using a value source, so we need to rollup:
    rollup();
  }

  /** Aggregate from indexed association values. */
  private void aggregateValues(
      AssociationAggregationFunction aggregationFunction, List<MatchingDocs> matchingDocs)
      throws IOException {

    for (MatchingDocs hits : matchingDocs) {
      BinaryDocValues dv = DocValues.getBinary(hits.context.reader(), indexFieldName);
      DocIdSetIterator it =
          ConjunctionUtils.intersectIterators(Arrays.asList(hits.bits.iterator(), dv));

      for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
        final BytesRef bytesRef = dv.binaryValue();
        byte[] bytes = bytesRef.bytes;
        int end = bytesRef.offset + bytesRef.length;
        int offset = bytesRef.offset;
        while (offset < end) {
          int ord = (int) BitUtil.VH_BE_INT.get(bytes, offset);
          offset += 4;
          float value = (float) BitUtil.VH_BE_FLOAT.get(bytes, offset);
          offset += 4;
          float newValue = aggregationFunction.aggregate(values[ord], value);
          values[ord] = newValue;
        }
      }
    }
  }
}
