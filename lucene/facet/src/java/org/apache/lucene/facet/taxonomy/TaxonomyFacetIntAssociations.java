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
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;

/**
 * Aggregates int values previously indexed with {@link IntAssociationFacetField}, assuming the
 * default encoding. The aggregation function is defined by a provided {@link
 * AssociationAggregationFunction}.
 *
 * @lucene.experimental
 */
public class TaxonomyFacetIntAssociations extends IntTaxonomyFacets {

  /** Create {@code TaxonomyFacetIntAssociations} against the default index field. */
  public TaxonomyFacetIntAssociations(
      TaxonomyReader taxoReader,
      FacetsConfig config,
      FacetsCollector fc,
      AssociationAggregationFunction aggregationFunction)
      throws IOException {
    this(FacetsConfig.DEFAULT_INDEX_FIELD_NAME, taxoReader, config, fc, aggregationFunction);
  }

  /** Create {@code TaxonomyFacetIntAssociations} against the specified index field. */
  public TaxonomyFacetIntAssociations(
      String indexFieldName,
      TaxonomyReader taxoReader,
      FacetsConfig config,
      FacetsCollector fc,
      AssociationAggregationFunction aggregationFunction)
      throws IOException {
    super(indexFieldName, taxoReader, config, aggregationFunction, fc);
    aggregateValues(aggregationFunction, fc.getMatchingDocs());
  }

  private void aggregateValues(
      AssociationAggregationFunction aggregationFunction, List<MatchingDocs> matchingDocs)
      throws IOException {
    for (MatchingDocs hits : matchingDocs) {
      if (hits.totalHits == 0) {
        continue;
      }
      initializeValueCounters();

      BinaryDocValues dv = DocValues.getBinary(hits.context.reader(), indexFieldName);
      DocIdSetIterator it = ConjunctionUtils.intersectIterators(List.of(hits.bits.iterator(), dv));

      for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
        final BytesRef bytesRef = dv.binaryValue();
        byte[] bytes = bytesRef.bytes;
        int end = bytesRef.offset + bytesRef.length;
        int offset = bytesRef.offset;
        while (offset < end) {
          int ord = (int) BitUtil.VH_BE_INT.get(bytes, offset);
          offset += 4;
          int value = (int) BitUtil.VH_BE_INT.get(bytes, offset);
          offset += 4;
          // TODO: Can we optimize the null check in setValue? See LUCENE-10373.
          int currentValue = getValue(ord);
          int newValue = aggregationFunction.aggregate(currentValue, value);
          setValue(ord, newValue);
          setCount(ord, getCount(ord) + 1);
        }
      }
    }
  }
}
