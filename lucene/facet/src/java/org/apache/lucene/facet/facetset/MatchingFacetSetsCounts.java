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
package org.apache.lucene.facet.facetset;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;

/**
 * Returns the counts for each given {@link FacetSet}
 *
 * @lucene.experimental
 */
public class MatchingFacetSetsCounts extends Facets {

  private final FacetSetMatcher[] facetSetMatchers;
  private final int[] counts;
  private final String field;
  private final FacetSetDecoder facetSetDecoder;
  private final int totCount;

  /**
   * Constructs a new instance of matching facet set counts which calculates the counts for each
   * given facet set matcher.
   */
  public MatchingFacetSetsCounts(
      String field,
      FacetsCollector hits,
      FacetSetDecoder facetSetDecoder,
      FacetSetMatcher... facetSetMatchers)
      throws IOException {
    if (facetSetMatchers == null || facetSetMatchers.length == 0) {
      throw new IllegalArgumentException("facetSetMatchers cannot be null or empty");
    }
    if (areFacetSetMatcherDimensionsInconsistent(facetSetMatchers)) {
      throw new IllegalArgumentException("All facet set matchers must be the same dimensionality");
    }
    this.field = field;
    this.facetSetDecoder = facetSetDecoder;
    this.facetSetMatchers = facetSetMatchers;
    this.counts = new int[facetSetMatchers.length];
    this.totCount = count(field, hits.getMatchingDocs());
  }

  /** Counts from the provided field. */
  private int count(String field, List<FacetsCollector.MatchingDocs> matchingDocs)
      throws IOException {

    int totCount = 0;
    for (FacetsCollector.MatchingDocs hits : matchingDocs) {

      BinaryDocValues binaryDocValues = DocValues.getBinary(hits.context.reader(), field);

      final DocIdSetIterator it =
          ConjunctionUtils.intersectIterators(Arrays.asList(hits.bits.iterator(), binaryDocValues));
      if (it == null) {
        continue;
      }

      long[] dimValues = null; // dimension values buffer
      int expectedNumDims = -1;
      for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
        boolean shouldCountDoc = false;
        BytesRef bytesRef = binaryDocValues.binaryValue();
        byte[] packedValue = bytesRef.bytes;
        int numDims = IntPoint.decodeDimension(packedValue, 0);
        if (expectedNumDims == -1) {
          expectedNumDims = numDims;
          dimValues = new long[numDims];
        } else {
          // Verify that the number of indexed dimensions for all matching documents is the same
          // (since we cannot verify that at indexing time).
          assert numDims == expectedNumDims
              : "Expected ("
                  + expectedNumDims
                  + ") dimensions, found ("
                  + numDims
                  + ") for doc ("
                  + doc
                  + ")";
        }

        for (int start = Integer.BYTES; start < bytesRef.length; ) {
          start += facetSetDecoder.decode(bytesRef, start, dimValues);
          for (int j = 0; j < facetSetMatchers.length; j++) { // for each facet set matcher
            if (facetSetMatchers[j].matches(dimValues)) {
              counts[j]++;
              shouldCountDoc = true;
            }
          }
        }
        if (shouldCountDoc) {
          totCount++;
        }
      }
    }
    return totCount;
  }

  @Override
  public FacetResult getAllChildren(String dim, String... path) throws IOException {
    if (field.equals(dim) == false) {
      throw new IllegalArgumentException(
          "invalid dim \"" + dim + "\"; should be \"" + field + "\"");
    }
    if (path != null && path.length != 0) {
      throw new IllegalArgumentException("path.length should be 0");
    }
    LabelAndValue[] labelValues = new LabelAndValue[counts.length];
    for (int i = 0; i < counts.length; i++) {
      labelValues[i] = new LabelAndValue(facetSetMatchers[i].label, counts[i]);
    }
    return new FacetResult(dim, path, totCount, labelValues, labelValues.length);
  }

  @Override
  public FacetResult getTopChildren(int topN, String dim, String... path) throws IOException {
    validateTopN(topN);
    return getAllChildren(dim, path);
  }

  @Override
  public Number getSpecificValue(String dim, String... path) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<FacetResult> getAllDims(int topN) throws IOException {
    validateTopN(topN);
    return Collections.singletonList(getTopChildren(topN, field));
  }

  private static boolean areFacetSetMatcherDimensionsInconsistent(
      FacetSetMatcher[] facetSetMatchers) {
    int dims = facetSetMatchers[0].dims;
    return Arrays.stream(facetSetMatchers)
        .anyMatch(facetSetMatcher -> facetSetMatcher.dims != dims);
  }
}
