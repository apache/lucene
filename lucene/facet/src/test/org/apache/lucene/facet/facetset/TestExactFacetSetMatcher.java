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
import org.apache.lucene.tests.util.TestUtil;

public class TestExactFacetSetMatcher extends FacetTestCase {

  private static final int FORD_ORD = 100;
  private static final int TOYOTA_ORD = 101;
  private static final int CHEVY_ORD = 102;
  private static final int NISSAN_ORD = 103;
  private static final int[] MANUFACTURER_ORDS = {FORD_ORD, TOYOTA_ORD, CHEVY_ORD, NISSAN_ORD};
  private static final int[] YEARS = {2010, 2011, 2012};

  public void testSimpleFacetSetMatching() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);

    // Ford-2010, Chevy-2011
    Document doc = new Document();
    doc.add(
        FacetSetsField.create(
            "field", new LongFacetSet(FORD_ORD, 2010), new LongFacetSet(CHEVY_ORD, 2011)));
    w.addDocument(doc);

    // Ford-2011, Chevy-2010
    doc = new Document();
    doc.add(
        FacetSetsField.create(
            "field", new LongFacetSet(FORD_ORD, 2011), new LongFacetSet(CHEVY_ORD, 2010)));
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
            new ExactFacetSetMatcher("Ford 2010", new LongFacetSet(FORD_ORD, 2010)),
            new ExactFacetSetMatcher("Chevy 2011", new LongFacetSet(CHEVY_ORD, 2011)));

    FacetResult result = facets.getAllChildren("field");

    assertEquals("field", result.dim);
    assertEquals(0, result.path.length);
    assertEquals(1, result.value);
    assertEquals(2, result.childCount);

    assertEquals(new LabelAndValue("Ford 2010", 1), result.labelValues[0]);
    assertEquals(new LabelAndValue("Chevy 2011", 1), result.labelValues[1]);

    r.close();
    d.close();
  }

  public void testLongFacetSetMatching() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);

    List<LongFacetSet> allSets = new ArrayList<>();
    for (int manufacturerOrd : MANUFACTURER_ORDS) {
      for (int year : YEARS) {
        allSets.add(new LongFacetSet(manufacturerOrd, year));
      }
    }

    int numFord2010 = 0;
    int numChevy2011 = 0;
    int numMatchingDocs = 0;
    for (int i = 0; i < 100; i++) {
      Document doc = new Document();
      int numSets = TestUtil.nextInt(random(), 1, 4);
      Collections.shuffle(allSets, random());
      LongFacetSet[] facetSets = allSets.subList(0, numSets).toArray(LongFacetSet[]::new);
      boolean matchingDoc = false;
      for (LongFacetSet facetSet : facetSets) {
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
            FacetSetDecoder::decodeLongs,
            new ExactFacetSetMatcher("Ford 2010", new LongFacetSet(FORD_ORD, 2010)),
            new ExactFacetSetMatcher("Chevy 2011", new LongFacetSet(CHEVY_ORD, 2011)));

    FacetResult result = facets.getAllChildren("field");

    assertEquals("field", result.dim);
    assertEquals(0, result.path.length);
    assertEquals(numMatchingDocs, result.value);
    assertEquals(2, result.childCount);

    assertEquals(new LabelAndValue("Ford 2010", numFord2010), result.labelValues[0]);
    assertEquals(new LabelAndValue("Chevy 2011", numChevy2011), result.labelValues[1]);

    r.close();
    d.close();
  }

  public void testIntFacetSetMatching() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);

    List<IntFacetSet> allSets = new ArrayList<>();
    for (int manufacturerOrd : MANUFACTURER_ORDS) {
      for (int year : YEARS) {
        allSets.add(new IntFacetSet(manufacturerOrd, year));
      }
    }

    int numFord2010 = 0;
    int numChevy2011 = 0;
    int numMatchingDocs = 0;
    for (int i = 0; i < 100; i++) {
      Document doc = new Document();
      int numSets = TestUtil.nextInt(random(), 1, 4);
      Collections.shuffle(allSets, random());
      IntFacetSet[] facetSets = allSets.subList(0, numSets).toArray(IntFacetSet[]::new);
      boolean matchingDoc = false;
      for (IntFacetSet facetSet : facetSets) {
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
            FacetSetDecoder::decodeInts,
            new ExactFacetSetMatcher("Ford 2010", new IntFacetSet(FORD_ORD, 2010)),
            new ExactFacetSetMatcher("Chevy 2011", new IntFacetSet(CHEVY_ORD, 2011)));

    FacetResult result = facets.getAllChildren("field");

    assertEquals("field", result.dim);
    assertEquals(0, result.path.length);
    assertEquals(numMatchingDocs, result.value);
    assertEquals(2, result.childCount);

    assertEquals(new LabelAndValue("Ford 2010", numFord2010), result.labelValues[0]);
    assertEquals(new LabelAndValue("Chevy 2011", numChevy2011), result.labelValues[1]);

    r.close();
    d.close();
  }

  public void testDoubleFacetSetMatching() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);

    List<DoubleFacetSet> allSets = new ArrayList<>();
    for (int manufacturerOrd : MANUFACTURER_ORDS) {
      for (int year : YEARS) {
        allSets.add(new DoubleFacetSet(manufacturerOrd, year + 0.5));
      }
    }

    int numFord2010 = 0;
    int numChevy2011 = 0;
    int numMatchingDocs = 0;
    for (int i = 0; i < 100; i++) {
      Document doc = new Document();
      int numSets = TestUtil.nextInt(random(), 1, 4);
      Collections.shuffle(allSets, random());
      DoubleFacetSet[] facetSets = allSets.subList(0, numSets).toArray(DoubleFacetSet[]::new);
      boolean matchingDoc = false;
      for (DoubleFacetSet facetSet : facetSets) {
        if (FORD_ORD == facetSet.values[0] && facetSet.values[1] == 2010.5) {
          ++numFord2010;
          matchingDoc = true;
        } else if (CHEVY_ORD == facetSet.values[0] && facetSet.values[1] == 2011.5) {
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
            FacetSetDecoder::decodeLongs,
            new ExactFacetSetMatcher("Ford 2010", new DoubleFacetSet(FORD_ORD, 2010.5)),
            new ExactFacetSetMatcher("Chevy 2011", new DoubleFacetSet(CHEVY_ORD, 2011.5)));

    FacetResult result = facets.getAllChildren("field");

    assertEquals("field", result.dim);
    assertEquals(0, result.path.length);
    assertEquals(numMatchingDocs, result.value);
    assertEquals(2, result.childCount);

    assertEquals(new LabelAndValue("Ford 2010", numFord2010), result.labelValues[0]);
    assertEquals(new LabelAndValue("Chevy 2011", numChevy2011), result.labelValues[1]);

    r.close();
    d.close();
  }

  public void testFloatFacetSetMatching() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);

    List<FloatFacetSet> allSets = new ArrayList<>();
    for (int manufacturerOrd : MANUFACTURER_ORDS) {
      for (int year : YEARS) {
        allSets.add(new FloatFacetSet(manufacturerOrd, year + 0.5f));
      }
    }

    int numFord2010 = 0;
    int numChevy2011 = 0;
    int numMatchingDocs = 0;
    for (int i = 0; i < 100; i++) {
      Document doc = new Document();
      int numSets = TestUtil.nextInt(random(), 1, 4);
      Collections.shuffle(allSets, random());
      FloatFacetSet[] facetSets = allSets.subList(0, numSets).toArray(FloatFacetSet[]::new);
      boolean matchingDoc = false;
      for (FloatFacetSet facetSet : facetSets) {
        if (FORD_ORD == facetSet.values[0] && facetSet.values[1] == 2010.5f) {
          ++numFord2010;
          matchingDoc = true;
        } else if (CHEVY_ORD == facetSet.values[0] && facetSet.values[1] == 2011.5f) {
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
            FacetSetDecoder::decodeInts,
            new ExactFacetSetMatcher("Ford 2010", new FloatFacetSet(FORD_ORD, 2010.5f)),
            new ExactFacetSetMatcher("Chevy 2011", new FloatFacetSet(CHEVY_ORD, 2011.5f)));

    FacetResult result = facets.getAllChildren("field");

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
