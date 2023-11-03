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

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import java.io.IOException;
import java.util.Locale;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollectorManager;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.InPlaceMergeSorter;

public class TestMatchingFacetSetsCounts extends FacetTestCase {
  private static final int FORD_ORD = 100;
  private static final int TOYOTA_ORD = 101;
  private static final int CHEVY_ORD = 102;
  private static final int NISSAN_ORD = 103;
  private static final int[] MANUFACTURER_ORDS = {FORD_ORD, TOYOTA_ORD, CHEVY_ORD, NISSAN_ORD};

  public void testInvalidTopN() throws IOException {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);

    Document doc = new Document();
    doc.add(FacetSetsField.create("field", new LongFacetSet(123, 456)));
    w.addDocument(doc);

    IndexReader r = w.getReader();
    w.close();

    IndexSearcher s = newSearcher(r);
    FacetsCollector fc = s.search(new MatchAllDocsQuery(), new FacetsCollectorManager());

    Facets facets =
        new MatchingFacetSetsCounts(
            "field",
            fc,
            FacetSetDecoder::decodeLongs,
            new ExactFacetSetMatcher("Test", new LongFacetSet(123, 456)));

    expectThrows(IllegalArgumentException.class, () -> facets.getTopChildren(0, "field"));

    r.close();
    d.close();
  }

  public void testInconsistentNumOfIndexedDimensions() throws IOException {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);

    Document doc = new Document();
    doc.add(FacetSetsField.create("field", new LongFacetSet(123, 456)));
    w.addDocument(doc);

    doc = new Document();
    doc.add(FacetSetsField.create("field", new LongFacetSet(123)));
    w.addDocument(doc);

    IndexReader r = w.getReader();
    w.close();

    IndexSearcher s = newSearcher(r);
    FacetsCollector fc = s.search(new MatchAllDocsQuery(), new FacetsCollectorManager());

    expectThrows(
        AssertionError.class,
        () ->
            new MatchingFacetSetsCounts(
                "field",
                fc,
                FacetSetDecoder::decodeLongs,
                new ExactFacetSetMatcher("Test", new LongFacetSet(1))));

    r.close();
    d.close();
  }

  public void testTopChildren() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);

    // As a test scenario, we're faceting on the number of vehicles produced per day/make
    // combination over the past 30 days:
    final int numBins = 30;

    final int[] expectedCounts = new int[numBins * MANUFACTURER_ORDS.length];
    FacetSetMatcher[] facetSetMatchers = new FacetSetMatcher[numBins * MANUFACTURER_ORDS.length];

    int totalDocs = 0;
    int totalNonZeroBins = 0;
    int index = 0;
    for (int i = 0; i < numBins; i++) {
      for (int ord : MANUFACTURER_ORDS) {
        facetSetMatchers[index] =
            new ExactFacetSetMatcher(
                String.format(Locale.ROOT, "%d:%d", i, ord), new LongFacetSet(i, ord));

        int carsManufactured = RandomNumbers.randomIntBetween(random(), 0, 100);
        for (int k = 0; k < carsManufactured; k++) {
          // Create a document for every vehicle produced:
          Document doc = new Document();
          doc.add(FacetSetsField.create("field", new LongFacetSet(i, ord)));
          w.addDocument(doc);
        }

        if (carsManufactured > 0) {
          totalNonZeroBins++;
        }
        totalDocs += carsManufactured;
        expectedCounts[index] = carsManufactured;
        index++;
      }
    }

    IndexReader r = w.getReader();
    w.close();

    IndexSearcher s = newSearcher(r);
    FacetsCollector fc = s.search(new MatchAllDocsQuery(), new FacetsCollectorManager());

    Facets facets =
        new MatchingFacetSetsCounts("field", fc, FacetSetDecoder::decodeLongs, facetSetMatchers);

    // Sort by count (high-to-low) and tie-break on label, same as in
    // MatchingFacetCounts#getTopChildren:
    final int[] originalIndexes = new int[expectedCounts.length];
    for (int i = 0; i < originalIndexes.length; i++) {
      originalIndexes[i] = i;
    }
    new InPlaceMergeSorter() {
      @Override
      protected int compare(int i, int j) {
        int cmp = Integer.compare(expectedCounts[j], expectedCounts[i]);
        if (cmp == 0) {
          int dayBinI = originalIndexes[i] / MANUFACTURER_ORDS.length;
          int dayBinJ = originalIndexes[j] / MANUFACTURER_ORDS.length;
          int ordIndexI = originalIndexes[i] % MANUFACTURER_ORDS.length;
          int ordIndexJ = originalIndexes[j] % MANUFACTURER_ORDS.length;
          String labelI =
              String.format(Locale.ROOT, "%d:%d", dayBinI, MANUFACTURER_ORDS[ordIndexI]);
          String labelJ =
              String.format(Locale.ROOT, "%d:%d", dayBinJ, MANUFACTURER_ORDS[ordIndexJ]);
          cmp = new BytesRef(labelI).compareTo(new BytesRef(labelJ));
        }
        return cmp;
      }

      @Override
      protected void swap(int i, int j) {
        int tmp = expectedCounts[i];
        expectedCounts[i] = expectedCounts[j];
        expectedCounts[j] = tmp;
        tmp = originalIndexes[i];
        originalIndexes[i] = originalIndexes[j];
        originalIndexes[j] = tmp;
      }
    }.sort(0, expectedCounts.length);

    final int topN = 10;
    final LabelAndValue[] expected = new LabelAndValue[topN];
    for (int i = 0; i < topN; i++) {
      int count = expectedCounts[i];
      int dayBin = originalIndexes[i] / MANUFACTURER_ORDS.length;
      int ordIndex = originalIndexes[i] % MANUFACTURER_ORDS.length;
      expected[i] =
          new LabelAndValue(
              String.format(Locale.ROOT, "%d:%d", dayBin, MANUFACTURER_ORDS[ordIndex]), count);
    }

    final FacetResult result = facets.getTopChildren(topN, "field");
    assertFacetResult(result, "field", new String[0], totalNonZeroBins, totalDocs, expected);

    r.close();
    d.close();
  }
}
