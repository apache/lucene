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

public class TestRangeFacetSetMatcher extends FacetTestCase {

  private static final int FORD_ORD = 100;
  private static final int TOYOTA_ORD = 101;
  private static final int CHEVY_ORD = 102;
  private static final int NISSAN_ORD = 103;
  private static final int[] MANUFACTURER_ORDS = {FORD_ORD, TOYOTA_ORD, CHEVY_ORD, NISSAN_ORD};
  private static final int[] YEARS = {2010, 2011, 2012, 2013, 2014};

  public void testLongRangeFacetSetMatching() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);

    List<LongFacetSet> allSets = new ArrayList<>();
    for (int manufacturerOrd : MANUFACTURER_ORDS) {
      for (int year : YEARS) {
        allSets.add(new LongFacetSet(manufacturerOrd, year));
      }
    }

    int numFord2011_2013 = 0;
    int numFord2010_2014 = 0;
    int numFord2011_2014 = 0;
    int numFord2010_2013 = 0;
    int numMatchingDocs = 0;
    for (int i = 0; i < 100; i++) {
      Document doc = new Document();
      int numSets = TestUtil.nextInt(random(), 1, 4);
      Collections.shuffle(allSets, random());
      LongFacetSet[] facetSets = allSets.subList(0, numSets).toArray(LongFacetSet[]::new);
      boolean matchingDoc = false;
      for (LongFacetSet facetSet : facetSets) {
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
            FacetSetDecoder::decodeLongs,
            new RangeFacetSetMatcher(
                "Ford [2010-2014]", singleLong(FORD_ORD), longRange(2010, true, 2014, true)),
            new RangeFacetSetMatcher(
                "Ford (2010-2014]", singleLong(FORD_ORD), longRange(2010, false, 2014, true)),
            new RangeFacetSetMatcher(
                "Ford [2010-2014)", singleLong(FORD_ORD), longRange(2010, true, 2014, false)),
            new RangeFacetSetMatcher(
                "Ford (2010-2014)", singleLong(FORD_ORD), longRange(2010, false, 2014, false)));

    FacetResult result = facets.getAllChildren("field");

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

  public void testIntRangeFacetSetMatching() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);

    List<IntFacetSet> allSets = new ArrayList<>();
    for (int manufacturerOrd : MANUFACTURER_ORDS) {
      for (int year : YEARS) {
        allSets.add(new IntFacetSet(manufacturerOrd, year));
      }
    }

    int numFord2011_2013 = 0;
    int numFord2010_2014 = 0;
    int numFord2011_2014 = 0;
    int numFord2010_2013 = 0;
    int numMatchingDocs = 0;
    for (int i = 0; i < 100; i++) {
      Document doc = new Document();
      int numSets = TestUtil.nextInt(random(), 1, 4);
      Collections.shuffle(allSets, random());
      IntFacetSet[] facetSets = allSets.subList(0, numSets).toArray(IntFacetSet[]::new);
      boolean matchingDoc = false;
      for (IntFacetSet facetSet : facetSets) {
        if (FORD_ORD != facetSet.values[0]) {
          continue;
        }
        int year = facetSet.values[1];
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
            FacetSetDecoder::decodeInts,
            new RangeFacetSetMatcher(
                "Ford [2010-2014]", singleLong(FORD_ORD), longRange(2010, true, 2014, true)),
            new RangeFacetSetMatcher(
                "Ford (2010-2014]", singleLong(FORD_ORD), longRange(2010, false, 2014, true)),
            new RangeFacetSetMatcher(
                "Ford [2010-2014)", singleLong(FORD_ORD), longRange(2010, true, 2014, false)),
            new RangeFacetSetMatcher(
                "Ford (2010-2014)", singleLong(FORD_ORD), longRange(2010, false, 2014, false)));

    FacetResult result = facets.getAllChildren("field");

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

  public void testDoubleRangeFacetSetMatching() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);

    List<DoubleFacetSet> allSets = new ArrayList<>();
    for (int manufacturerOrd : MANUFACTURER_ORDS) {
      for (int year : YEARS) {
        allSets.add(new DoubleFacetSet(manufacturerOrd, year + 0.5));
      }
    }

    int numFord2011_2014 = 0;
    int numFord2010_2015 = 0;
    int numFord2011_2015 = 0;
    int numFord2010_2014 = 0;
    int numMatchingDocs = 0;
    for (int i = 0; i < 100; i++) {
      Document doc = new Document();
      int numSets = TestUtil.nextInt(random(), 1, 4);
      Collections.shuffle(allSets, random());
      DoubleFacetSet[] facetSets = allSets.subList(0, numSets).toArray(DoubleFacetSet[]::new);
      boolean matchingDoc = false;
      for (DoubleFacetSet facetSet : facetSets) {
        if (Double.compare(FORD_ORD, facetSet.values[0]) != 0) {
          continue;
        }
        double year = facetSet.values[1];
        if (year > 2010.5 && year < 2014.5) {
          ++numFord2010_2014;
          ++numFord2010_2015;
          ++numFord2011_2014;
          ++numFord2011_2015;
          matchingDoc = true;
        } else if (year == 2014.5) {
          ++numFord2010_2015;
          ++numFord2011_2015;
          matchingDoc = true;
        } else if (year == 2010.5) {
          ++numFord2010_2015;
          ++numFord2010_2014;
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
            new RangeFacetSetMatcher(
                "Ford [2010-2015]",
                singleDouble(FORD_ORD),
                doubleRange(2010.0, true, 2015.0, true)),
            new RangeFacetSetMatcher(
                "Ford (2011-2015]",
                singleDouble(FORD_ORD),
                doubleRange(2011.0, false, 2015.0, true)),
            new RangeFacetSetMatcher(
                "Ford [2010-2014)",
                singleDouble(FORD_ORD),
                doubleRange(2010.0, true, 2014.0, false)),
            new RangeFacetSetMatcher(
                "Ford (2011-2014)",
                singleDouble(FORD_ORD),
                doubleRange(2011.0, false, 2014.0, false)));

    FacetResult result = facets.getAllChildren("field");

    assertEquals("field", result.dim);
    assertEquals(0, result.path.length);
    assertEquals(numMatchingDocs, result.value);
    assertEquals(4, result.childCount);

    assertEquals(new LabelAndValue("Ford [2010-2015]", numFord2010_2015), result.labelValues[0]);
    assertEquals(new LabelAndValue("Ford (2011-2015]", numFord2011_2015), result.labelValues[1]);
    assertEquals(new LabelAndValue("Ford [2010-2014)", numFord2010_2014), result.labelValues[2]);
    assertEquals(new LabelAndValue("Ford (2011-2014)", numFord2011_2014), result.labelValues[3]);

    r.close();
    d.close();
  }

  public void testFloatRangeFacetSetMatching() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);

    List<FloatFacetSet> allSets = new ArrayList<>();
    for (int manufacturerOrd : MANUFACTURER_ORDS) {
      for (int year : YEARS) {
        allSets.add(new FloatFacetSet(manufacturerOrd, year + 0.5f));
      }
    }

    int numFord2011_2014 = 0;
    int numFord2010_2015 = 0;
    int numFord2011_2015 = 0;
    int numFord2010_2014 = 0;
    int numMatchingDocs = 0;
    for (int i = 0; i < 100; i++) {
      Document doc = new Document();
      int numSets = TestUtil.nextInt(random(), 1, 4);
      Collections.shuffle(allSets, random());
      FloatFacetSet[] facetSets = allSets.subList(0, numSets).toArray(FloatFacetSet[]::new);
      boolean matchingDoc = false;
      for (FloatFacetSet facetSet : facetSets) {
        if (Double.compare(FORD_ORD, facetSet.values[0]) != 0) {
          continue;
        }
        double year = facetSet.values[1];
        if (year > 2010.5f && year < 2014.5f) {
          ++numFord2010_2014;
          ++numFord2010_2015;
          ++numFord2011_2014;
          ++numFord2011_2015;
          matchingDoc = true;
        } else if (year == 2014.5f) {
          ++numFord2010_2015;
          ++numFord2011_2015;
          matchingDoc = true;
        } else if (year == 2010.5f) {
          ++numFord2010_2015;
          ++numFord2010_2014;
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
            new RangeFacetSetMatcher(
                "Ford [2010-2015]",
                singleFloat(FORD_ORD),
                floatRange(2010.0f, true, 2015.0f, true)),
            new RangeFacetSetMatcher(
                "Ford (2010-2015]",
                singleFloat(FORD_ORD),
                floatRange(2010.5f, false, 2015.0f, true)),
            new RangeFacetSetMatcher(
                "Ford [2010-2014)",
                singleFloat(FORD_ORD),
                floatRange(2010.0f, true, 2014.0f, false)),
            new RangeFacetSetMatcher(
                "Ford (2011-2014)",
                singleFloat(FORD_ORD),
                floatRange(2011.0f, false, 2014.0f, false)));

    FacetResult result = facets.getAllChildren("field");

    assertEquals("field", result.dim);
    assertEquals(0, result.path.length);
    assertEquals(numMatchingDocs, result.value);
    assertEquals(4, result.childCount);

    assertEquals(new LabelAndValue("Ford [2010-2015]", numFord2010_2015), result.labelValues[0]);
    assertEquals(new LabelAndValue("Ford (2010-2015]", numFord2011_2015), result.labelValues[1]);
    assertEquals(new LabelAndValue("Ford [2010-2014)", numFord2010_2014), result.labelValues[2]);
    assertEquals(new LabelAndValue("Ford (2011-2014)", numFord2011_2014), result.labelValues[3]);

    r.close();
    d.close();
  }

  private static DimRange singleLong(long value) {
    return DimRange.fromLongs(value, true, value, true);
  }

  private static DimRange longRange(
      long min, boolean minExclusive, long max, boolean maxExclusive) {
    return DimRange.fromLongs(min, minExclusive, max, maxExclusive);
  }

  private static DimRange singleDouble(double value) {
    return DimRange.fromDoubles(value, true, value, true);
  }

  private static DimRange doubleRange(
      double min, boolean minExclusive, double max, boolean maxExclusive) {
    return DimRange.fromDoubles(min, minExclusive, max, maxExclusive);
  }

  private static DimRange singleFloat(float value) {
    return DimRange.fromFloats(value, true, value, true);
  }

  private static DimRange floatRange(
      float min, boolean minExclusive, float max, boolean maxExclusive) {
    return DimRange.fromFloats(min, minExclusive, max, maxExclusive);
  }
}
