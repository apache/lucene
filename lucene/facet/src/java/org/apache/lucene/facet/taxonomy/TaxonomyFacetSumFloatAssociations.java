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
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;

/**
 * Aggregates sum of int values previously indexed with {@link FloatAssociationFacetField}, assuming
 * the default encoding.
 *
 * @lucene.experimental
 */
public class TaxonomyFacetSumFloatAssociations extends FloatTaxonomyFacets {

  /** Create {@code TaxonomyFacetSumFloatAssociations} against the default index field. */
  public TaxonomyFacetSumFloatAssociations(
      TaxonomyReader taxoReader, FacetsConfig config, FacetsCollector fc) throws IOException {
    this(FacetsConfig.DEFAULT_INDEX_FIELD_NAME, taxoReader, config, fc);
  }

  /** Create {@code TaxonomyFacetSumFloatAssociations} against the specified index field. */
  public TaxonomyFacetSumFloatAssociations(
      String indexFieldName, TaxonomyReader taxoReader, FacetsConfig config, FacetsCollector fc)
      throws IOException {
    super(indexFieldName, taxoReader, config);
    sumValues(fc.getMatchingDocs());
  }

  private final void sumValues(List<MatchingDocs> matchingDocs) throws IOException {
    // System.out.println("count matchingDocs=" + matchingDocs + " facetsField=" + facetsFieldName);
    for (MatchingDocs hits : matchingDocs) {
      BinaryDocValues dv = hits.context.reader().getBinaryDocValues(indexFieldName);
      if (dv == null) { // this reader does not have DocValues for the requested category list
        continue;
      }

      DocIdSetIterator docs = hits.bits.iterator();

      int doc;
      while ((doc = docs.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        // System.out.println("  doc=" + doc);
        // TODO: use OrdinalsReader?  we'd need to add a
        // BytesRef getAssociation()?
        if (dv.docID() < doc) {
          dv.advance(doc);
        }
        if (dv.docID() == doc) {
          final BytesRef bytesRef = dv.binaryValue();
          byte[] bytes = bytesRef.bytes;
          int end = bytesRef.offset + bytesRef.length;
          int offset = bytesRef.offset;
          while (offset < end) {
            int ord = (int) BitUtil.VH_BE_INT.get(bytes, offset);
            offset += 4;
            float value = (float) BitUtil.VH_BE_FLOAT.get(bytes, offset);
            offset += 4;
            values[ord] += value;
          }
        }
      }
    }
  }
}
