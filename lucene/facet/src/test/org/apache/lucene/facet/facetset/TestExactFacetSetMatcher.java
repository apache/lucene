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

public class TestExactFacetSetMatcher extends FacetTestCase {

  private static final long FORD_ORD = 100;
  private static final long TOYOTA_ORD = 101;
  private static final long CHEVY_ORD = 102;
  private static final long NISSAN_ORD = 103;
  private static final long[] MANUFACTURER_ORDS = {FORD_ORD, TOYOTA_ORD, CHEVY_ORD, NISSAN_ORD};
  private static final long[] YEARS = {2010, 2011, 2012};

  public void testSimpleFacetSetMatching() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);

    // Ford-2010, Chevy-2011
    Document doc = new Document();
    doc.add(
        FacetSetsField.create(
            "field", new FacetSet(FORD_ORD, 2010), new FacetSet(CHEVY_ORD, 2011)));
    w.addDocument(doc);

    // Ford-2011, Chevy-2010
    doc = new Document();
    doc.add(
        FacetSetsField.create(
            "field", new FacetSet(FORD_ORD, 2011), new FacetSet(CHEVY_ORD, 2010)));
    w.addDocument(doc);

    IndexReader r = w.getReader();
    w.close();

    IndexSearcher s = newSearcher(r);
    FacetsCollector fc = s.search(new MatchAllDocsQuery(), new FacetsCollectorManager());

    Facets facets =
        new MatchingFacetSetsCounts(
            "field",
            fc,
            new ExactFacetSetMatcher("Ford 2010", new FacetSet(FORD_ORD, 2010)),
            new ExactFacetSetMatcher("Chevy 2011", new FacetSet(CHEVY_ORD, 2011)));

    FacetResult result = facets.getTopChildren(10, "field");

    assertEquals("field", result.dim);
    assertEquals(0, result.path.length);
    assertEquals(1, result.value);
    assertEquals(2, result.childCount);

    assertEquals(new LabelAndValue("Ford 2010", 1), result.labelValues[0]);
    assertEquals(new LabelAndValue("Chevy 2011", 1), result.labelValues[1]);

    r.close();
    d.close();
  }

  public void testFacetSetMatching() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);

    List<FacetSet> allSets = new ArrayList<>();
    for (long manufacturer_ord : MANUFACTURER_ORDS) {
      for (long year : YEARS) {
        allSets.add(new FacetSet(manufacturer_ord, year));
      }
    }

    int numFord2010 = 0;
    int numChevy2011 = 0;
    int numMatchingDocs = 0;
    for (int i = 0; i < 100; i++) {
      Document doc = new Document();
      int numSets = random().nextInt(1, 4);
      Collections.shuffle(allSets, random());
      FacetSet[] facetSets = allSets.subList(0, numSets).toArray(new FacetSet[0]);
      boolean matchingDoc = false;
      for (FacetSet facetSet : facetSets) {
        if (FORD_ORD == facetSet.values[0] && facetSet.values[1] == 2010) {
          ++numFord2010;
          matchingDoc = true;
        } else if (CHEVY_ORD == facetSet.values[0] && facetSet.values[1] == 2011) {
          ++numChevy2011;
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
            new ExactFacetSetMatcher("Ford 2010", new FacetSet(FORD_ORD, 2010)),
            new ExactFacetSetMatcher("Chevy 2011", new FacetSet(CHEVY_ORD, 2011)));

    FacetResult result = facets.getTopChildren(10, "field");

    assertEquals("field", result.dim);
    assertEquals(0, result.path.length);
    assertEquals(numMatchingDocs, result.value);
    assertEquals(2, result.childCount);

    assertEquals(new LabelAndValue("Ford 2010", numFord2010), result.labelValues[0]);
    assertEquals(new LabelAndValue("Chevy 2011", numChevy2011), result.labelValues[1]);

    r.close();
    d.close();
  }
}
