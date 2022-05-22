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
package org.apache.lucene.facet.hyperrectangle;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocIdSetIterator;

/**
 * Get counts given a list of HyperRectangles
 *
 * @lucene.experimental
 */
public class HyperRectangleFacetCounts extends Facets {
  /** Hyper rectangles passed to constructor. */
  private final HyperRectangle[] hyperRectangles;

  /**
   * Holds the number of matching documents (contains intersecting point in field) for each {@link
   * HyperRectangle}
   */
  private final int[] counts;

  /** Our field name. */
  private final String field;

  /** Total number of hits. */
  private int totCount;

  /**
   * Create HyperRectangleFacetCounts using this
   *
   * @param field Field name
   * @param hits Hits to facet on
   * @param hyperRectangles List of hyper rectangle facets
   * @throws IOException If there is a problem reading the field
   */
  public HyperRectangleFacetCounts(
      String field, FacetsCollector hits, HyperRectangle... hyperRectangles) throws IOException {
    if (hyperRectangles == null || hyperRectangles.length == 0) {
      throw new IllegalArgumentException("Hyper rectangle ranges cannot be empty");
    }
    if (areHyperRectangleDimsConsistent(hyperRectangles) == false) {
      throw new IllegalArgumentException("All hyper rectangles must be the same dimensionality");
    }
    this.field = field;
    this.hyperRectangles = hyperRectangles;
    this.counts = new int[hyperRectangles.length];
    count(field, hits.getMatchingDocs());
  }

  private boolean areHyperRectangleDimsConsistent(HyperRectangle[] hyperRectangles) {
    int dims = hyperRectangles[0].dims;
    return Arrays.stream(hyperRectangles).allMatch(hyperRectangle -> hyperRectangle.dims == dims);
  }

  /** Counts from the provided field. */
  private void count(String field, List<FacetsCollector.MatchingDocs> matchingDocs)
      throws IOException {

    for (FacetsCollector.MatchingDocs hits : matchingDocs) {

      BinaryDocValues binaryDocValues = DocValues.getBinary(hits.context.reader(), field);

      final DocIdSetIterator it =
          ConjunctionUtils.intersectIterators(Arrays.asList(hits.bits.iterator(), binaryDocValues));
      if (it == null) {
        continue;
      }

      for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
        boolean shouldCountDoc = false;
        // linear scan, change this to use R trees
        for (int j = 0; j < hyperRectangles.length; j++) {
          if (hyperRectangles[j].matches(binaryDocValues.binaryValue().bytes)) {
            counts[j]++;
            shouldCountDoc = true;
          }
        }
        if (shouldCountDoc) {
          totCount++;
        }
      }
    }
  }

  // TODO: This does not really provide "top children" functionality yet but provides "all
  // children". This is being worked on in LUCENE-10550
  @Override
  public FacetResult getTopChildren(int topN, String dim, String... path) throws IOException {
    validateTopN(topN);
    if (field.equals(dim) == false) {
      throw new IllegalArgumentException(
          "invalid dim \"" + dim + "\"; should be \"" + field + "\"");
    }
    if (path != null && path.length != 0) {
      throw new IllegalArgumentException("path.length should be 0");
    }
    LabelAndValue[] labelValues = new LabelAndValue[counts.length];
    for (int i = 0; i < counts.length; i++) {
      labelValues[i] = new LabelAndValue(hyperRectangles[i].label, counts[i]);
    }
    return new FacetResult(dim, path, totCount, labelValues, labelValues.length);
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
}
