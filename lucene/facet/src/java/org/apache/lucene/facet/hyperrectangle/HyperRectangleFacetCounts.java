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
import java.util.Collections;
import java.util.List;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.search.DocIdSetIterator;

/** Get counts given a list of HyperRectangles (which must be of the same type) */
public class HyperRectangleFacetCounts extends Facets {
  /** Hypper rectangles passed to constructor. */
  protected final HyperRectangle[] hyperRectangles;

  /** Counts, initialized in by subclass. */
  protected final int[] counts;

  /** Our field name. */
  protected final String field;

  /** Number of dimensions for field */
  protected final int dims;

  /** Total number of hits. */
  protected int totCount;

  /**
   * Create HyperRectangleFacetCounts using
   *
   * @param field Field name
   * @param hits Hits to facet on
   * @param hyperRectangles List of long hyper rectangle facets
   * @throws IOException If there is a problem reading the field
   */
  public HyperRectangleFacetCounts(
      String field, FacetsCollector hits, LongHyperRectangle... hyperRectangles)
      throws IOException {
    this(true, field, hits, hyperRectangles);
  }

  /**
   * Create HyperRectangleFacetCounts using
   *
   * @param field Field name
   * @param hits Hits to facet on
   * @param hyperRectangles List of double hyper rectangle facets
   * @throws IOException If there is a problem reading the field
   */
  public HyperRectangleFacetCounts(
      String field, FacetsCollector hits, DoubleHyperRectangle... hyperRectangles)
      throws IOException {
    this(true, field, hits, hyperRectangles);
  }

  private HyperRectangleFacetCounts(
      boolean discarded, String field, FacetsCollector hits, HyperRectangle... hyperRectangles)
      throws IOException {
    assert hyperRectangles.length > 0 : "Hyper rectangle ranges cannot be empty";
    this.field = field;
    this.hyperRectangles = hyperRectangles;
    this.dims = hyperRectangles[0].dims;
    for (HyperRectangle hyperRectangle : hyperRectangles) {
      assert hyperRectangle.dims == this.dims
          : "All hyper rectangles must be the same dimensionality";
    }
    this.counts = new int[hyperRectangles.length];
    count(field, hits.getMatchingDocs());
  }

  /** Counts from the provided field. */
  private void count(String field, List<FacetsCollector.MatchingDocs> matchingDocs)
      throws IOException {

    for (int i = 0; i < matchingDocs.size(); i++) {

      FacetsCollector.MatchingDocs hits = matchingDocs.get(i);

      BinaryDocValues binaryDocValues = DocValues.getBinary(hits.context.reader(), field);

      final DocIdSetIterator it = hits.bits.iterator();
      if (it == null) {
        continue;
      }

      for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; ) {
        if (binaryDocValues.advanceExact(doc)) {
          long[] point = LongPoint.unpack(binaryDocValues.binaryValue());
          assert point.length == dims : "Point dimension is incompatible with hyper rectangle";
          // linear scan, change this to use bkd trees
          boolean docIsValid = false;
          for (int j = 0; j < hyperRectangles.length; j++) {
            boolean validPoint = true;
            for (int dim = 0; dim < dims; dim++) {
              LongHyperRectangle.LongRangePair range =
                  hyperRectangles[j].getComparableDimRange(dim);
              if (!range.accept(point[dim])) {
                validPoint = false;
                break;
              }
            }
            if (validPoint) {
              counts[j]++;
              docIsValid = true;
            }
          }
          if (docIsValid) {
            totCount++;
          }
        }
        doc = it.nextDoc();
      }
    }
  }

  @Override
  public FacetResult getTopChildren(int topN, String dim, String... path) throws IOException {
    validateTopN(topN);
    if (dim.equals(field) == false) {
      throw new IllegalArgumentException(
          "invalid dim \"" + dim + "\"; should be \"" + field + "\"");
    }
    if (path.length != 0) {
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
