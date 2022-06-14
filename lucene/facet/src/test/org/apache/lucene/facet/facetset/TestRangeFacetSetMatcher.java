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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.*;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;

public class TestRangeFacetSetMatcher extends FacetTestCase {

  private static final long FORD_ORD = 100;
  private static final long TOYOTA_ORD = 101;
  private static final long CHEVY_ORD = 102;
  private static final long NISSAN_ORD = 103;
  private static final long[] MANUFACTURER_ORDS = {FORD_ORD, TOYOTA_ORD, CHEVY_ORD, NISSAN_ORD};
  private static final long[] YEARS = {2010, 2011, 2012, 2013, 2014};

  public void testRangeFacetSetMatching() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);

    List<FacetSet> allSets = new ArrayList<>();
    for (long manufacturer_ord : MANUFACTURER_ORDS) {
      for (long year : YEARS) {
        allSets.add(new FacetSet(manufacturer_ord, year));
      }
    }

    int numFord2011_2013 = 0;
    int numFord2010_2014 = 0;
    int numFord2011_2014 = 0;
    int numFord2010_2013 = 0;
    int numMatchingDocs = 0;
    for (int i = 0; i < 100; i++) {
      Document doc = new Document();
      int numSets = random().nextInt(1, 4);
      Collections.shuffle(allSets, random());
      FacetSet[] facetSets = allSets.subList(0, numSets).toArray(new FacetSet[0]);
      boolean matchingDoc = false;
      for (FacetSet facetSet : facetSets) {
        if (FORD_ORD != facetSet.values[0]) {
          continue;
        }
        long year = facetSet.values[1];
        if (year > 2010 && year < 2014) {
          ++numFord2010_2013;
          ++numFord2010_2014;
          ++numFord2011_2013;
          ++numFord2011_2014;
          matchingDoc = true;
        } else if (year == 2014) {
          ++numFord2010_2014;
          ++numFord2011_2014;
          matchingDoc = true;
        } else if (year == 2010) {
          ++numFord2010_2014;
          ++numFord2010_2013;
          matchingDoc = true;
        }
      }
      numMatchingDocs += matchingDoc ? 1 : 0;
      doc.add(FacetSetsField.create("field", facetSets));
      w.addDocument(doc);
    }

    IndexReader r = w.getReader();
    w.close();

    IndexSearcher s = newSearcher(r);
    FacetsCollector fc = s.search(new MatchAllDocsQuery(), new FacetsCollectorManager());

    Facets facets =
        new MatchingFacetSetsCounts(
            "field",
            fc,
            new RangeFacetSetMatcher(
                "Ford [2010-2014]", single(FORD_ORD), range(2010, true, 2014, true)),
            new RangeFacetSetMatcher(
                "Ford (2010-2014]", single(FORD_ORD), range(2010, false, 2014, true)),
            new RangeFacetSetMatcher(
                "Ford [2010-2014)", single(FORD_ORD), range(2010, true, 2014, false)),
            new RangeFacetSetMatcher(
                "Ford (2010-2014)", single(FORD_ORD), range(2010, false, 2014, false)));

    FacetResult result = facets.getTopChildren(10, "field");

    assertEquals("field", result.dim);
    assertEquals(0, result.path.length);
    assertEquals(numMatchingDocs, result.value);
    assertEquals(4, result.childCount);

    assertEquals(new LabelAndValue("Ford [2010-2014]", numFord2010_2014), result.labelValues[0]);
    assertEquals(new LabelAndValue("Ford (2010-2014]", numFord2011_2014), result.labelValues[1]);
    assertEquals(new LabelAndValue("Ford [2010-2014)", numFord2010_2013), result.labelValues[2]);
    assertEquals(new LabelAndValue("Ford (2010-2014)", numFord2011_2013), result.labelValues[3]);

    r.close();
    d.close();
  }

  private static RangeFacetSetMatcher.LongRange single(long value) {
    return new RangeFacetSetMatcher.LongRange(value, true, value, true);
  }

  private static RangeFacetSetMatcher.LongRange range(
      long min, boolean minExclusive, long max, boolean maxExclusive) {
    return new RangeFacetSetMatcher.LongRange(min, minExclusive, max, maxExclusive);
  }
}
