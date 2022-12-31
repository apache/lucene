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
package org.apache.lucene.facet.rangeonrange;

import static org.apache.lucene.document.DoubleRange.verifyAndEncode;
import static org.apache.lucene.document.LongRange.verifyAndEncode;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleRangeDocValuesField;
import org.apache.lucene.document.LongRangeDocValuesField;
import org.apache.lucene.document.RangeFieldQuery;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.DrillSideways;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollectorManager;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.MultiFacets;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

public class TestRangeOnRangeFacetCounts extends FacetTestCase {

  public void testBasicSingleDimLong() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    Document doc = new Document();
    LongRangeDocValuesField field =
        new LongRangeDocValuesField("field", longArray(0L), longArray(10L));
    doc.add(field);
    for (long l = 0; l < 100; l++) {
      setLongRangeValue(field, l, 10L + l);
      w.addDocument(doc);
    }

    // Also add Long.MAX_VALUE
    setLongRangeValue(field, Long.MAX_VALUE - 10L, Long.MAX_VALUE);
    w.addDocument(doc);

    IndexReader r = w.getReader();
    w.close();

    IndexSearcher s = newSearcher(r);
    FacetsCollector fc = s.search(new MatchAllDocsQuery(), new FacetsCollectorManager());

    Facets facets =
        new LongRangeOnRangeFacetCounts(
            "field",
            fc,
            RangeFieldQuery.QueryType.INTERSECTS,
            new LongRange("less than 10", 0L, true, 10L, false),
            new LongRange("less than or equal to 10", 0L, true, 10L, true),
            new LongRange("over 90", 90L, false, 100L, false),
            new LongRange("90 or above", 90L, true, 100L, false),
            new LongRange("over 1000", 1000L, false, Long.MAX_VALUE, true));

    FacetResult result = facets.getAllChildren("field");
    assertEquals(
        "dim=field path=[] value=32 childCount=5\n  less than 10 (10)\n  less than or equal to 10 (11)\n  over 90 (19)\n  90 or above (20)\n  over 1000 (1)\n",
        result.toString());

    result = facets.getTopChildren(4, "field");
    assertEquals(
        "dim=field path=[] value=32 childCount=5\n  90 or above (20)\n  over 90 (19)\n  less than or equal to 10 (11)\n  less than 10 (10)\n",
        result.toString());

    // test getTopChildren(0, dim)
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          facets.getTopChildren(0, "field");
        });

    r.close();
    d.close();
  }

  public void testBasicMultiDimLong() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    Document doc = new Document();
    long[] min = longArray(0L, 1L, 2L);
    long[] max = longArray(10L, 11L, 12L);
    LongRangeDocValuesField field = new LongRangeDocValuesField("field", min, max);
    doc.add(field);
    for (long l = 0; l < 100; l++) {
      setLongRangeValue(field, longArray(l, l + 1L, l + 2L), longArray(l + 10L, l + 11L, l + 12L));
      w.addDocument(doc);
    }

    // Also add Long.MAX_VALUE
    long[] infMin = longArray(Long.MAX_VALUE - 12L, Long.MAX_VALUE - 11L, Long.MAX_VALUE - 10L);
    long[] infMax = longArray(Long.MAX_VALUE - 2L, Long.MAX_VALUE - 1L, Long.MAX_VALUE);
    setLongRangeValue(field, infMin, infMax);
    w.addDocument(doc);

    IndexReader r = w.getReader();
    w.close();

    IndexSearcher s = newSearcher(r);
    FacetsCollector fc = s.search(new MatchAllDocsQuery(), new FacetsCollectorManager());

    Facets facets =
        new LongRangeOnRangeFacetCounts(
            "field",
            fc,
            RangeFieldQuery.QueryType.INTERSECTS,
            new LongRange("less than 10", longArray(0L, 1L, 2L), longArray(8L, 9L, 10L)),
            new LongRange("a bit over 10", longArray(0L, 1L, 2L), longArray(10L, 11L, 12L)),
            new LongRange("a bit under 90", longArray(88L, 89L, 90L), longArray(100L, 101L, 102L)),
            new LongRange("over 90", longArray(90L, 91L, 92L), longArray(100L, 101L, 102L)),
            new LongRange(
                "over 1000",
                longArray(1000L, 1001L, 1002L),
                longArray(Long.MAX_VALUE - 2L, Long.MAX_VALUE - 1L, Long.MAX_VALUE)));

    FacetResult result = facets.getAllChildren("field");
    assertEquals(
        "dim=field path=[] value=34 childCount=5\n  less than 10 (9)\n  a bit over 10 (11)\n  a bit under 90 (22)\n  over 90 (20)\n  over 1000 (1)\n",
        result.toString());

    result = facets.getTopChildren(4, "field");
    assertEquals(
        "dim=field path=[] value=34 childCount=5\n  a bit under 90 (22)\n  over 90 (20)\n  a bit over 10 (11)\n  less than 10 (9)\n",
        result.toString());

    // test getTopChildren(0, dim)
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          facets.getTopChildren(0, "field");
        });

    r.close();
    d.close();
  }

  public void testLongGetAllDimsSingleDim() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    Document doc = new Document();
    LongRangeDocValuesField field =
        new LongRangeDocValuesField("field", longArray(0L), longArray(10L));
    doc.add(field);
    for (long l = 0; l < 100; l++) {
      setLongRangeValue(field, l, 10L + l);
      w.addDocument(doc);
    }

    // Also add Long.MAX_VALUE
    setLongRangeValue(field, Long.MAX_VALUE - 10L, Long.MAX_VALUE);
    w.addDocument(doc);

    IndexReader r = w.getReader();
    w.close();

    IndexSearcher s = newSearcher(r);
    FacetsCollector fc = s.search(new MatchAllDocsQuery(), new FacetsCollectorManager());

    Facets facets =
        new LongRangeOnRangeFacetCounts(
            "field",
            fc,
            RangeFieldQuery.QueryType.INTERSECTS,
            new LongRange("less than 10", 0L, true, 10L, false),
            new LongRange("less than or equal to 10", 0L, true, 10L, true),
            new LongRange("over 90", 90L, false, 100L, false),
            new LongRange("90 or above", 90L, true, 100L, false),
            new LongRange("over 1000", 1000L, false, Long.MAX_VALUE, true));

    List<FacetResult> result = facets.getAllDims(10);
    assertEquals(1, result.size());
    assertEquals(
        "dim=field path=[] value=32 childCount=5\n  90 or above (20)\n  over 90 (19)\n  less than or equal to 10 (11)\n  less than 10 (10)\n  over 1000 (1)\n",
        result.get(0).toString());

    // test getAllDims(1)
    result = facets.getAllDims(1);
    assertEquals(1, result.size());
    assertEquals(
        "dim=field path=[] value=32 childCount=5\n  90 or above (20)\n", result.get(0).toString());

    // test default implementation of getTopDims
    List<FacetResult> topNDimsResult = facets.getTopDims(1, 1);
    assertEquals(result, topNDimsResult);

    // test getTopDims(0, 1)
    topNDimsResult = facets.getTopDims(0, 1);
    assertEquals(0, topNDimsResult.size());

    // test getAllDims(0)
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          facets.getAllDims(0);
        });

    r.close();
    d.close();
  }

  public void testGetAllDimsMultiDim() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    Document doc = new Document();
    long[] min = longArray(0L, 1L, 2L);
    long[] max = longArray(10L, 11L, 12L);
    LongRangeDocValuesField field = new LongRangeDocValuesField("field", min, max);
    doc.add(field);
    for (long l = 0; l < 100; l++) {
      long[] newMin = longArray(l, l + 1L, l + 2L);
      long[] newMax = longArray(l + 10L, l + 11L, l + 12L);
      setLongRangeValue(field, newMin, newMax);
      w.addDocument(doc);
    }

    // Also add Long.MAX_VALUE
    long[] infMin = longArray(Long.MAX_VALUE - 12L, Long.MAX_VALUE - 11L, Long.MAX_VALUE - 10L);
    long[] infMax = longArray(Long.MAX_VALUE - 2L, Long.MAX_VALUE - 1L, Long.MAX_VALUE);
    setLongRangeValue(field, infMin, infMax);
    w.addDocument(doc);

    IndexReader r = w.getReader();
    w.close();

    IndexSearcher s = newSearcher(r);
    FacetsCollector fc = s.search(new MatchAllDocsQuery(), new FacetsCollectorManager());

    Facets facets =
        new LongRangeOnRangeFacetCounts(
            "field",
            fc,
            RangeFieldQuery.QueryType.INTERSECTS,
            new LongRange("less than 10", longArray(0L, 1L, 2L), longArray(8L, 9L, 10L)),
            new LongRange("a bit over 10", longArray(0L, 1L, 2L), longArray(10L, 11L, 12L)),
            new LongRange("a bit under 90", longArray(88L, 89L, 90L), longArray(100L, 101L, 102L)),
            new LongRange("over 90", longArray(90L, 91L, 92L), longArray(100L, 101L, 102L)),
            new LongRange(
                "over 1000",
                longArray(1000L, 1001L, 1002L),
                longArray(Long.MAX_VALUE - 2L, Long.MAX_VALUE - 1L, Long.MAX_VALUE)));

    List<FacetResult> result = facets.getAllDims(10);
    assertEquals(1, result.size());
    assertEquals(
        "dim=field path=[] value=34 childCount=5\n  a bit under 90 (22)\n  over 90 (20)\n  a bit over 10 (11)\n  less than 10 (9)\n  over 1000 (1)\n",
        result.get(0).toString());

    // test getAllDims(1)
    result = facets.getAllDims(1);
    assertEquals(1, result.size());
    assertEquals(
        "dim=field path=[] value=34 childCount=5\n  a bit under 90 (22)\n",
        result.get(0).toString());

    // test getTopChildren(0, dim)
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          facets.getTopChildren(0, "field");
        });

    r.close();
    d.close();
  }

  public void testUselessRangeSingleDim() {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          new LongRange("useless", 7, true, 6, true);
        });
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          new LongRange("useless", 7, true, 7, false);
        });
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          new DoubleRange("useless", 7.0, true, 6.0, true);
        });
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          new DoubleRange("useless", 7.0, true, 7.0, false);
        });
  }

  public void testUselessMultiDimRange() {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          new LongRange("useless", longArray(7L, 7L), longArray(6L, 6L));
        });
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          new LongRange("useless", longArray(7L, 7L), longArray(7L, 6L));
        });
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          new DoubleRange("useless", doubleArray(7.0, 7.0), doubleArray(6.0, 6.0));
        });
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          new DoubleRange("useless", doubleArray(7.0, 7.0), doubleArray(7.0, 6.0));
        });
  }

  public void testSingleDimLongMinMax() throws Exception {

    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    Document doc = new Document();
    LongRangeDocValuesField field =
        new LongRangeDocValuesField("field", longArray(0L), longArray(10L));
    doc.add(field);
    setLongRangeValue(field, Long.MIN_VALUE, Long.MIN_VALUE + 10L);
    w.addDocument(doc);
    setLongRangeValue(field, 0L, 10L);
    w.addDocument(doc);
    setLongRangeValue(field, Long.MAX_VALUE - 10L, Long.MAX_VALUE);
    w.addDocument(doc);

    IndexReader r = w.getReader();
    w.close();

    IndexSearcher s = newSearcher(r);
    FacetsCollector fc = s.search(new MatchAllDocsQuery(), new FacetsCollectorManager());

    Facets facets =
        new LongRangeOnRangeFacetCounts(
            "field",
            fc,
            RangeFieldQuery.QueryType.INTERSECTS,
            new LongRange("min", Long.MIN_VALUE, true, Long.MIN_VALUE, true),
            new LongRange("max", Long.MAX_VALUE, true, Long.MAX_VALUE, true),
            new LongRange("all0", Long.MIN_VALUE, true, Long.MAX_VALUE, true),
            new LongRange("all1", Long.MIN_VALUE, false, Long.MAX_VALUE, true),
            new LongRange("all2", Long.MIN_VALUE, true, Long.MAX_VALUE, false),
            new LongRange("all3", Long.MIN_VALUE, false, Long.MAX_VALUE, false));

    FacetResult result = facets.getAllChildren("field");
    assertEquals(
        "dim=field path=[] value=3 childCount=6\n  min (1)\n  max (1)\n  all0 (3)\n  all1 (3)\n  all2 (3)\n  all3 (3)\n",
        result.toString());

    result = facets.getTopChildren(5, "field");
    assertEquals(
        "dim=field path=[] value=3 childCount=6\n  all0 (3)\n  all1 (3)\n  all2 (3)\n  all3 (3)\n  max (1)\n",
        result.toString());

    r.close();
    d.close();
  }

  public void testMultiDimLongMinMax() throws Exception {

    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    Document doc = new Document();
    LongRangeDocValuesField field =
        new LongRangeDocValuesField("field", longArray(0L, 1L), longArray(1L, 2L));
    doc.add(field);
    setLongRangeValue(
        field,
        longArray(Long.MIN_VALUE, Long.MIN_VALUE),
        longArray(Long.MIN_VALUE + 10L, Long.MIN_VALUE + 11L));
    w.addDocument(doc);
    setLongRangeValue(field, longArray(0L, 1L), longArray(10L, 11L));
    w.addDocument(doc);
    setLongRangeValue(
        field,
        longArray(Long.MAX_VALUE - 11L, Long.MAX_VALUE - 10L),
        longArray(Long.MAX_VALUE, Long.MAX_VALUE));
    w.addDocument(doc);

    IndexReader r = w.getReader();
    w.close();

    IndexSearcher s = newSearcher(r);
    FacetsCollector fc = s.search(new MatchAllDocsQuery(), new FacetsCollectorManager());

    Facets facets =
        new LongRangeOnRangeFacetCounts(
            "field",
            fc,
            RangeFieldQuery.QueryType.INTERSECTS,
            new LongRange(
                "min",
                longArray(Long.MIN_VALUE, Long.MIN_VALUE),
                longArray(Long.MIN_VALUE, Long.MIN_VALUE)),
            new LongRange(
                "max",
                longArray(Long.MAX_VALUE, Long.MAX_VALUE),
                longArray(Long.MAX_VALUE, Long.MAX_VALUE)),
            new LongRange(
                "all0",
                longArray(Long.MIN_VALUE, Long.MIN_VALUE),
                longArray(Long.MIN_VALUE, Long.MAX_VALUE)),
            new LongRange(
                "all1",
                longArray(Long.MIN_VALUE, Long.MIN_VALUE),
                longArray(Long.MAX_VALUE, Long.MIN_VALUE)),
            new LongRange(
                "all2",
                longArray(Long.MIN_VALUE, Long.MIN_VALUE),
                longArray(Long.MAX_VALUE, Long.MAX_VALUE)));

    FacetResult result = facets.getAllChildren("field");
    assertEquals(
        "dim=field path=[] value=3 childCount=5\n  min (1)\n  max (1)\n  all0 (1)\n  all1 (1)\n  all2 (3)\n",
        result.toString());

    result = facets.getTopChildren(4, "field");
    assertEquals(
        "dim=field path=[] value=3 childCount=5\n  all2 (3)\n  all0 (1)\n  all1 (1)\n  max (1)\n",
        result.toString());

    r.close();
    d.close();
  }

  public void testSingleDimOverlappedEndStart() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    Document doc = new Document();
    LongRangeDocValuesField field =
        new LongRangeDocValuesField("field", longArray(0L), longArray(10L));
    doc.add(field);
    for (long l = 0; l < 100; l++) {
      setLongRangeValue(field, l, 10L + l);
      w.addDocument(doc);
    }

    setLongRangeValue(field, Long.MAX_VALUE - 10L, Long.MAX_VALUE);
    w.addDocument(doc);

    IndexReader r = w.getReader();
    w.close();

    IndexSearcher s = newSearcher(r);
    FacetsCollector fc = s.search(new MatchAllDocsQuery(), new FacetsCollectorManager());

    Facets facets =
        new LongRangeOnRangeFacetCounts(
            "field",
            fc,
            RangeFieldQuery.QueryType.INTERSECTS,
            new LongRange("0-10", 0L, true, 10L, true),
            new LongRange("10-20", 10L, true, 20L, true),
            new LongRange("20-30", 20L, true, 30L, true),
            new LongRange("30-40", 30L, true, 40L, true));

    FacetResult result = facets.getAllChildren("field");
    assertEquals(
        "dim=field path=[] value=41 childCount=4\n  0-10 (11)\n  10-20 (21)\n  20-30 (21)\n  30-40 (21)\n",
        result.toString());

    result = facets.getTopChildren(3, "field");
    assertEquals(
        "dim=field path=[] value=41 childCount=4\n  10-20 (21)\n  20-30 (21)\n  30-40 (21)\n",
        result.toString());
    r.close();
    d.close();
  }

  public void testSingleDimMixedRangeAndNonRangeTaxonomy() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    Directory td = newDirectory();
    DirectoryTaxonomyWriter tw = new DirectoryTaxonomyWriter(td, IndexWriterConfig.OpenMode.CREATE);

    FacetsConfig config = new FacetsConfig();

    for (long l = 0; l < 100; l++) {
      Document doc = new Document();
      // For computing range facet counts:
      doc.add(new LongRangeDocValuesField("field", longArray(l), longArray(l + 10L)));
      // For drill down by numeric range:
      doc.add(new org.apache.lucene.document.LongRange("field", longArray(l), longArray(l + 10L)));

      if ((l & 3) == 0) {
        doc.add(new FacetField("dim", "a"));
      } else {
        doc.add(new FacetField("dim", "b"));
      }
      w.addDocument(config.build(tw, doc));
    }

    final IndexReader r = w.getReader();

    final TaxonomyReader tr = new DirectoryTaxonomyReader(tw);

    IndexSearcher s = newSearcher(r, false, false);
    // DrillSideways requires the entire range of docs to be scored at once, so it doesn't support
    // timeouts whose implementation scores one window of doc IDs at a time.
    s.setTimeout(null);

    if (VERBOSE) {
      System.out.println("TEST: searcher=" + s);
    }

    DrillSideways ds =
        new DrillSideways(s, config, tr) {

          @Override
          protected Facets buildFacetsResult(
              FacetsCollector drillDowns,
              FacetsCollector[] drillSideways,
              String[] drillSidewaysDims)
              throws IOException {
            FacetsCollector dimFC = drillDowns;
            FacetsCollector fieldFC = drillDowns;
            if (drillSideways != null) {
              for (int i = 0; i < drillSideways.length; i++) {
                String dim = drillSidewaysDims[i];
                if (dim.equals("field")) {
                  fieldFC = drillSideways[i];
                } else {
                  dimFC = drillSideways[i];
                }
              }
            }

            Map<String, Facets> byDim = new HashMap<>();
            byDim.put(
                "field",
                new LongRangeOnRangeFacetCounts(
                    "field",
                    fieldFC,
                    RangeFieldQuery.QueryType.INTERSECTS,
                    new LongRange("less than 10", 0L, true, 10L, false),
                    new LongRange("less than or equal to 10", 0L, true, 10L, true),
                    new LongRange("over 90", 90L, false, 100L, false),
                    new LongRange("90 or above", 90L, true, 100L, false),
                    new LongRange("over 1000", 1000L, false, Long.MAX_VALUE, false)));
            byDim.put("dim", getTaxonomyFacetCounts(taxoReader, config, dimFC));
            return new MultiFacets(byDim, null);
          }

          @Override
          protected boolean scoreSubDocsAtOnce() {
            return random().nextBoolean();
          }
        };

    // First search, no drill downs:
    DrillDownQuery ddq = new DrillDownQuery(config);
    DrillSideways.DrillSidewaysResult dsr = ds.search(null, ddq, 10);

    assertEquals(100, dsr.hits.totalHits.value);
    assertEquals(
        "dim=dim path=[] value=100 childCount=2\n  b (75)\n  a (25)\n",
        dsr.facets.getTopChildren(10, "dim").toString());
    assertEquals(
        "dim=field path=[] value=31 childCount=4\n  90 or above (20)\n  over 90 (19)\n  less than or equal to 10 (11)\n  less than 10 (10)\n",
        dsr.facets.getTopChildren(10, "field").toString());

    // Second search, drill down on dim=b:
    ddq = new DrillDownQuery(config);
    ddq.add("dim", "b");
    dsr = ds.search(null, ddq, 10);

    assertEquals(75, dsr.hits.totalHits.value);
    assertEquals(
        "dim=dim path=[] value=100 childCount=2\n  b (75)\n  a (25)\n",
        dsr.facets.getTopChildren(10, "dim").toString());
    assertEquals(
        "dim=field path=[] value=23 childCount=4\n  90 or above (15)\n  over 90 (15)\n  less than or equal to 10 (8)\n  less than 10 (7)\n",
        dsr.facets.getTopChildren(10, "field").toString());

    w.close();
    IOUtils.close(tw, tr, td, r, d);
  }

  public void testMultiDimMixedRangeAndNonRangeTaxonomy() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    Directory td = newDirectory();
    DirectoryTaxonomyWriter tw = new DirectoryTaxonomyWriter(td, IndexWriterConfig.OpenMode.CREATE);

    FacetsConfig config = new FacetsConfig();

    for (long l = 0; l < 100; l++) {
      Document doc = new Document();
      // For computing range facet counts:
      doc.add(
          new LongRangeDocValuesField(
              "field", longArray(l, l + 1L, l + 2L), longArray(l + 10L, l + 11L, l + 12L)));
      // For drill down by numeric range:
      doc.add(
          new org.apache.lucene.document.LongRange(
              "field", longArray(l, l + 1L, l + 2L), longArray(l + 10L, l + 11L, l + 12L)));

      if ((l & 3) == 0) {
        doc.add(new FacetField("dim", "a"));
      } else {
        doc.add(new FacetField("dim", "b"));
      }
      w.addDocument(config.build(tw, doc));
    }

    final IndexReader r = w.getReader();

    final TaxonomyReader tr = new DirectoryTaxonomyReader(tw);

    IndexSearcher s = newSearcher(r, false, false);
    // DrillSideways requires the entire range of docs to be scored at once, so it doesn't support
    // timeouts whose implementation scores one window of doc IDs at a time.
    s.setTimeout(null);

    if (VERBOSE) {
      System.out.println("TEST: searcher=" + s);
    }

    DrillSideways ds =
        new DrillSideways(s, config, tr) {

          @Override
          protected Facets buildFacetsResult(
              FacetsCollector drillDowns,
              FacetsCollector[] drillSideways,
              String[] drillSidewaysDims)
              throws IOException {
            FacetsCollector dimFC = drillDowns;
            FacetsCollector fieldFC = drillDowns;
            if (drillSideways != null) {
              for (int i = 0; i < drillSideways.length; i++) {
                String dim = drillSidewaysDims[i];
                if (dim.equals("field")) {
                  fieldFC = drillSideways[i];
                } else {
                  dimFC = drillSideways[i];
                }
              }
            }

            Map<String, Facets> byDim = new HashMap<>();
            byDim.put(
                "field",
                new LongRangeOnRangeFacetCounts(
                    "field",
                    fieldFC,
                    RangeFieldQuery.QueryType.INTERSECTS,
                    new LongRange("less than 10", longArray(0L, 1L, 2L), longArray(8L, 9L, 10L)),
                    new LongRange("a bit over 10", longArray(0L, 1L, 2L), longArray(10L, 11L, 12L)),
                    new LongRange(
                        "a bit under 90", longArray(88L, 89L, 90L), longArray(100L, 101L, 102L)),
                    new LongRange("over 90", longArray(90L, 91L, 92L), longArray(100L, 101L, 102L)),
                    new LongRange(
                        "over 1000",
                        longArray(1000L, 1001L, 1002L),
                        longArray(Long.MAX_VALUE - 2L, Long.MAX_VALUE - 1L, Long.MAX_VALUE))));
            byDim.put("dim", getTaxonomyFacetCounts(taxoReader, config, dimFC));
            return new MultiFacets(byDim, null);
          }

          @Override
          protected boolean scoreSubDocsAtOnce() {
            return random().nextBoolean();
          }
        };

    // First search, no drill downs:
    DrillDownQuery ddq = new DrillDownQuery(config);
    DrillSideways.DrillSidewaysResult dsr = ds.search(null, ddq, 10);

    assertEquals(100, dsr.hits.totalHits.value);
    assertEquals(
        "dim=dim path=[] value=100 childCount=2\n  b (75)\n  a (25)\n",
        dsr.facets.getTopChildren(10, "dim").toString());
    assertEquals(
        "dim=field path=[] value=33 childCount=4\n  a bit under 90 (22)\n  over 90 (20)\n  a bit over 10 (11)\n  less than 10 (9)\n",
        dsr.facets.getTopChildren(10, "field").toString());

    // Second search, drill down on dim=b:
    ddq = new DrillDownQuery(config);
    ddq.add("dim", "b");
    dsr = ds.search(null, ddq, 10);

    assertEquals(75, dsr.hits.totalHits.value);
    assertEquals(
        "dim=dim path=[] value=100 childCount=2\n  b (75)\n  a (25)\n",
        dsr.facets.getTopChildren(10, "dim").toString());
    assertEquals(
        "dim=field path=[] value=25 childCount=4\n  a bit under 90 (17)\n  over 90 (15)\n  a bit over 10 (8)\n  less than 10 (6)\n",
        dsr.facets.getTopChildren(10, "field").toString());

    w.close();
    IOUtils.close(tw, tr, td, r, d);
  }

  public void testBasicSingleDimDouble() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    Document doc = new Document();
    DoubleRangeDocValuesField field =
        new DoubleRangeDocValuesField("field", doubleArray(0.0), doubleArray(10.0));
    doc.add(field);
    for (double l = 0; l < 100; l++) {
      setDoubleRangeValue(field, l, 10.0 + l);
      w.addDocument(doc);
    }

    IndexReader r = w.getReader();
    w.close();

    IndexSearcher s = newSearcher(r);
    FacetsCollector fc = s.search(new MatchAllDocsQuery(), new FacetsCollectorManager());

    Facets facets =
        new DoubleRangeOnRangeFacetCounts(
            "field",
            fc,
            RangeFieldQuery.QueryType.INTERSECTS,
            new DoubleRange("less than 10", 0.0, true, 10.0, false),
            new DoubleRange("less than or equal to 10", 0.0, true, 10.0, true),
            new DoubleRange("over 90", 90.0, false, 100.0, false),
            new DoubleRange("90 or above", 90.0, true, 100.0, false),
            new DoubleRange("over 1000", 1000.0, false, Double.POSITIVE_INFINITY, false));

    FacetResult result = facets.getAllChildren("field");
    assertEquals(
        "dim=field path=[] value=31 childCount=5\n  less than 10 (10)\n  less than or equal to 10 (11)\n  over 90 (19)\n  90 or above (20)\n  over 1000 (0)\n",
        result.toString());

    result = facets.getTopChildren(4, "field");
    assertEquals(
        "dim=field path=[] value=31 childCount=4\n  90 or above (20)\n  over 90 (19)\n  less than or equal to 10 (11)\n  less than 10 (10)\n",
        result.toString());

    // test getTopChildren(0, dim)
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          facets.getTopChildren(0, "field");
        });

    IOUtils.close(r, d);
  }

  public void testBasicMultiDimDouble() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    Document doc = new Document();
    double[] min = doubleArray(0.0, 1.0, 2.0);
    double[] max = doubleArray(10.0, 11.0, 12.0);
    DoubleRangeDocValuesField field = new DoubleRangeDocValuesField("field", min, max);
    doc.add(field);
    for (double l = 0; l < 100; l++) {
      setDoubleRangeValue(
          field, doubleArray(l, l + 1.0, l + 2.0), doubleArray(l + 10.0, l + 11.0, l + 12.0));
      w.addDocument(doc);
    }

    IndexReader r = w.getReader();
    w.close();

    IndexSearcher s = newSearcher(r);
    FacetsCollector fc = s.search(new MatchAllDocsQuery(), new FacetsCollectorManager());

    Facets facets =
        new DoubleRangeOnRangeFacetCounts(
            "field",
            fc,
            RangeFieldQuery.QueryType.INTERSECTS,
            new DoubleRange(
                "less than 10", doubleArray(0.0, 1.0, 2.0), doubleArray(8.0, 9.0, 10.0)),
            new DoubleRange(
                "a bit over 10", doubleArray(0.0, 1.0, 2.0), doubleArray(10.0, 11.0, 12.0)),
            new DoubleRange(
                "a bit under 90", doubleArray(88.0, 89.0, 90.0), doubleArray(100.0, 101.0, 102.0)),
            new DoubleRange(
                "over 90", doubleArray(90.0, 91.0, 92.0), doubleArray(100.0, 101.0, 102.0)),
            new DoubleRange(
                "over 1000",
                doubleArray(1000.0, 1001.0, 1002.0),
                doubleArray(
                    Double.POSITIVE_INFINITY - 2.0,
                    Double.POSITIVE_INFINITY - 1.0,
                    Double.POSITIVE_INFINITY)));

    FacetResult result = facets.getAllChildren("field");
    assertEquals(
        "dim=field path=[] value=33 childCount=5\n  less than 10 (9)\n  a bit over 10 (11)\n  a bit under 90 (22)\n  over 90 (20)\n  over 1000 (0)\n",
        result.toString());

    result = facets.getTopChildren(4, "field");
    assertEquals(
        "dim=field path=[] value=33 childCount=4\n  a bit under 90 (22)\n  over 90 (20)\n  a bit over 10 (11)\n  less than 10 (9)\n",
        result.toString());

    // test getTopChildren(0, dim)
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          facets.getTopChildren(0, "field");
        });

    IOUtils.close(r, d);
  }

  public void testRandomSingleDimLongs() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    int numDocs = atLeast(1000);
    if (VERBOSE) {
      System.out.println("TEST: numDocs=" + numDocs);
    }
    long[] mins = new long[numDocs];
    long[] maxes = new long[numDocs];
    long minValue = Long.MAX_VALUE;
    long maxValue = Long.MIN_VALUE;
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      long min = random().nextLong();
      long max = random().nextLong();
      if (min > max) {
        long tmp = min;
        min = max;
        max = tmp;
      }
      mins[i] = min;
      maxes[i] = max;
      doc.add(new LongRangeDocValuesField("field", longArray(min), longArray(max)));
      doc.add(new org.apache.lucene.document.LongRange("field", longArray(min), longArray(max)));
      w.addDocument(doc);
      minValue = Math.min(minValue, min);
      maxValue = Math.max(maxValue, min);
    }
    IndexReader r = w.getReader();

    IndexSearcher s = newSearcher(r, false);
    FacetsConfig config = new FacetsConfig();

    int numIters = atLeast(10);
    for (int iter = 0; iter < numIters; iter++) {
      if (VERBOSE) {
        System.out.println("TEST: iter=" + iter);
      }
      int numRange = TestUtil.nextInt(random(), 1, 100);
      LongRange[] ranges = new LongRange[numRange];
      int[] expectedCounts = new int[numRange];
      int[] expectedTopNChildrenCounts = new int[numRange];
      long minAcceptedValue = Long.MAX_VALUE;
      long maxAcceptedValue = Long.MIN_VALUE;
      for (int rangeID = 0; rangeID < numRange; rangeID++) {
        long min;
        if (rangeID > 0 && random().nextInt(10) == 7) {
          // Use an existing boundary:
          LongRange prevRange = ranges[random().nextInt(rangeID)];
          if (random().nextBoolean()) {
            min = prevRange.min[0];
          } else {
            min = prevRange.max[0];
          }
        } else {
          min = random().nextLong();
        }
        long max;
        if (rangeID > 0 && random().nextInt(10) == 7) {
          // Use an existing boundary:
          LongRange prevRange = ranges[random().nextInt(rangeID)];
          if (random().nextBoolean()) {
            max = prevRange.min[0];
          } else {
            max = prevRange.max[0];
          }
        } else {
          max = random().nextLong();
        }

        if (min > max) {
          long x = min;
          min = max;
          max = x;
        }
        boolean minIncl;
        boolean maxIncl;

        // NOTE: max - min >= 0 is here to handle the common overflow case!
        if (max - min >= 0 && max - min < 2) {
          // If max == min or max == min+1, we always do inclusive, else we might pass an empty
          // range and hit exc from LongRange's ctor:
          minIncl = true;
          maxIncl = true;
        } else {
          minIncl = random().nextBoolean();
          maxIncl = random().nextBoolean();
        }
        ranges[rangeID] = new LongRange("r" + rangeID, min, minIncl, max, maxIncl);
        if (VERBOSE) {
          System.out.println("  range " + rangeID + ": " + ranges[rangeID]);
        }

        byte[] queryPackedValue = new byte[2 * Long.BYTES];
        ArrayUtil.ByteArrayComparator comparator = ArrayUtil.getUnsignedComparator(Long.BYTES);
        verifyAndEncode(new long[] {min}, new long[] {max}, queryPackedValue);
        for (int i = 0; i < numDocs; i++) {
          byte[] packedValue = new byte[2 * Long.BYTES];
          verifyAndEncode(new long[] {mins[i]}, new long[] {maxes[i]}, packedValue);
          if (RangeFieldQuery.QueryType.INTERSECTS.matches(
              queryPackedValue, packedValue, 1, Long.BYTES, comparator)) {
            expectedCounts[rangeID]++;
            expectedTopNChildrenCounts[rangeID]++;
            minAcceptedValue = Math.min(minAcceptedValue, mins[i]);
            maxAcceptedValue = Math.max(maxAcceptedValue, maxes[i]);
          }
        }
      }

      FacetsCollector sfc = s.search(new MatchAllDocsQuery(), new FacetsCollectorManager());
      Query fastMatchQuery;
      if (random().nextBoolean()) {
        fastMatchQuery =
            org.apache.lucene.document.LongRange.newIntersectsQuery(
                "field", new long[] {minAcceptedValue}, new long[] {maxAcceptedValue});
      } else {
        fastMatchQuery = null;
      }

      Facets facets =
          new LongRangeOnRangeFacetCounts(
              "field", sfc, RangeFieldQuery.QueryType.INTERSECTS, fastMatchQuery, ranges);
      FacetResult result = facets.getAllChildren("field");
      assertEquals(numRange, result.labelValues.length);
      FacetResult topNResult = facets.getTopChildren(numRange, "field");
      Arrays.sort(expectedTopNChildrenCounts);

      for (int rangeID = 0; rangeID < numRange; rangeID++) {
        if (VERBOSE) {
          System.out.println("  range " + rangeID + " expectedCount=" + expectedCounts[rangeID]);
        }
        LabelAndValue subNode = result.labelValues[rangeID];
        assertEquals("r" + rangeID, subNode.label);
        assertEquals(expectedCounts[rangeID], subNode.value.intValue());
        // test topNChildren and assert topNResults are sorted by count
        if (rangeID < topNResult.labelValues.length) {
          LabelAndValue topNsubNode = topNResult.labelValues[rangeID];
          assertEquals(
              expectedTopNChildrenCounts[numRange - rangeID - 1], topNsubNode.value.intValue());
        }

        LongRange range = ranges[rangeID];

        // Test drill-down:
        DrillDownQuery ddq = new DrillDownQuery(config);
        ddq.add(
            "field",
            org.apache.lucene.document.LongRange.newIntersectsQuery("field", range.min, range.max));
        assertEquals(expectedCounts[rangeID], s.count(ddq));
      }
    }

    w.close();
    IOUtils.close(r, dir);
  }

  public void testRandomMultiDimLongs() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    int numDocs = atLeast(1000);
    int dims = 2 + random().nextInt(3);
    if (VERBOSE) {
      System.out.println("TEST: numDocs=" + numDocs);
    }
    long[][] mins = new long[numDocs][dims];
    long[][] maxes = new long[numDocs][dims];
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      for (int dim = 0; dim < dims; dim++) {
        long min = random().nextLong();
        long max = random().nextLong();
        if (min > max) {
          long tmp = min;
          min = max;
          max = tmp;
        }
        mins[i][dim] = min;
        maxes[i][dim] = max;
      }
      doc.add(new LongRangeDocValuesField("field", mins[i], maxes[i]));
      doc.add(new org.apache.lucene.document.LongRange("field", mins[i], maxes[i]));
      w.addDocument(doc);
    }
    IndexReader r = w.getReader();

    IndexSearcher s = newSearcher(r, false);
    FacetsConfig config = new FacetsConfig();

    int numIters = atLeast(10);
    for (int iter = 0; iter < numIters; iter++) {
      if (VERBOSE) {
        System.out.println("TEST: iter=" + iter);
      }
      int numRange = TestUtil.nextInt(random(), 1, 100);
      LongRange[] ranges = new LongRange[numRange];
      int[] expectedCounts = new int[numRange];
      int[] expectedTopNChildrenCounts = new int[numRange];
      long[] minAcceptedValue = new long[dims];
      Arrays.fill(minAcceptedValue, Long.MAX_VALUE);
      long[] maxAcceptedValue = new long[dims];
      Arrays.fill(maxAcceptedValue, Long.MIN_VALUE);
      for (int rangeID = 0; rangeID < numRange; rangeID++) {
        long[] min = new long[dims];
        if (rangeID > 0 && random().nextInt(10) == 7) {
          // Use an existing boundary:
          LongRange prevRange = ranges[random().nextInt(rangeID)];
          for (int dim = 0; dim < dims; dim++) {
            if (random().nextBoolean()) {
              min[dim] = prevRange.min[dim];
            } else {
              min[dim] = prevRange.max[dim];
            }
          }
        } else {
          for (int dim = 0; dim < dims; dim++) {
            min[dim] = random().nextLong();
          }
        }
        long[] max = new long[dims];
        if (rangeID > 0 && random().nextInt(10) == 7) {
          // Use an existing boundary:
          LongRange prevRange = ranges[random().nextInt(rangeID)];
          for (int dim = 0; dim < dims; dim++) {
            if (random().nextBoolean()) {
              max[dim] = prevRange.min[dim];
            } else {
              max[dim] = prevRange.max[dim];
            }
          }
        } else {
          for (int dim = 0; dim < dims; dim++) {
            max[dim] = random().nextLong();
          }
        }

        for (int dim = 0; dim < dims; dim++) {
          if (min[dim] > max[dim]) {
            long x = min[dim];
            min[dim] = max[dim];
            max[dim] = x;
          }
        }

        ranges[rangeID] = new LongRange("r" + rangeID, min, max);
        if (VERBOSE) {
          System.out.println("  range " + rangeID + ": " + ranges[rangeID]);
        }

        byte[] queryPackedValue = new byte[2 * Long.BYTES * dims];
        ArrayUtil.ByteArrayComparator comparator = ArrayUtil.getUnsignedComparator(Long.BYTES);
        verifyAndEncode(min, max, queryPackedValue);
        for (int i = 0; i < numDocs; i++) {
          byte[] packedValue = new byte[2 * Long.BYTES * dims];
          verifyAndEncode(mins[i], maxes[i], packedValue);
          if (RangeFieldQuery.QueryType.INTERSECTS.matches(
              queryPackedValue, packedValue, dims, Long.BYTES, comparator)) {
            expectedCounts[rangeID]++;
            expectedTopNChildrenCounts[rangeID]++;
            for (int dim = 0; dim < dims; dim++) {
              minAcceptedValue[dim] = Math.min(minAcceptedValue[dim], mins[i][dim]);
              maxAcceptedValue[dim] = Math.max(maxAcceptedValue[dim], maxes[i][dim]);
            }
          }
        }
      }

      FacetsCollector sfc = s.search(new MatchAllDocsQuery(), new FacetsCollectorManager());
      Query fastMatchQuery;
      if (random().nextBoolean()) {
        fastMatchQuery =
            org.apache.lucene.document.LongRange.newIntersectsQuery(
                "field", minAcceptedValue, maxAcceptedValue);
      } else {
        fastMatchQuery = null;
      }

      Facets facets =
          new LongRangeOnRangeFacetCounts(
              "field", sfc, RangeFieldQuery.QueryType.INTERSECTS, fastMatchQuery, ranges);
      FacetResult result = facets.getAllChildren("field");
      assertEquals(numRange, result.labelValues.length);
      FacetResult topNResult = facets.getTopChildren(numRange, "field");
      Arrays.sort(expectedTopNChildrenCounts);

      for (int rangeID = 0; rangeID < numRange; rangeID++) {
        if (VERBOSE) {
          System.out.println("  range " + rangeID + " expectedCount=" + expectedCounts[rangeID]);
        }
        LabelAndValue subNode = result.labelValues[rangeID];
        assertEquals("r" + rangeID, subNode.label);
        assertEquals(expectedCounts[rangeID], subNode.value.intValue());
        // test topNChildren and assert topNResults are sorted by count
        if (rangeID < topNResult.labelValues.length) {
          LabelAndValue topNsubNode = topNResult.labelValues[rangeID];
          assertEquals(
              expectedTopNChildrenCounts[numRange - rangeID - 1], topNsubNode.value.intValue());
        }

        LongRange range = ranges[rangeID];

        // Test drill-down:
        DrillDownQuery ddq = new DrillDownQuery(config);
        ddq.add(
            "field",
            org.apache.lucene.document.LongRange.newIntersectsQuery("field", range.min, range.max));
        assertEquals(expectedCounts[rangeID], s.count(ddq));
      }
    }

    w.close();
    IOUtils.close(r, dir);
  }

  public void testRandomSingleDimDoubles() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    int numDocs = atLeast(1000);
    if (VERBOSE) {
      System.out.println("TEST: numDocs=" + numDocs);
    }
    double[] mins = new double[numDocs];
    double[] maxes = new double[numDocs];
    double minValue = Double.POSITIVE_INFINITY;
    double maxValue = Double.NEGATIVE_INFINITY;
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      double min = random().nextDouble();
      double max = random().nextDouble();
      if (min > max) {
        double tmp = min;
        min = max;
        max = tmp;
      }
      mins[i] = min;
      maxes[i] = max;
      doc.add(new DoubleRangeDocValuesField("field", doubleArray(min), doubleArray(max)));
      doc.add(
          new org.apache.lucene.document.DoubleRange("field", doubleArray(min), doubleArray(max)));
      w.addDocument(doc);
      minValue = Math.min(minValue, min);
      maxValue = Math.max(maxValue, min);
    }
    IndexReader r = w.getReader();

    IndexSearcher s = newSearcher(r, false);
    FacetsConfig config = new FacetsConfig();

    int numIters = atLeast(10);
    for (int iter = 0; iter < numIters; iter++) {
      if (VERBOSE) {
        System.out.println("TEST: iter=" + iter);
      }
      int numRange = TestUtil.nextInt(random(), 1, 100);
      DoubleRange[] ranges = new DoubleRange[numRange];
      int[] expectedCounts = new int[numRange];
      int[] expectedTopNChildrenCounts = new int[numRange];
      double minAcceptedValue = Double.POSITIVE_INFINITY;
      double maxAcceptedValue = Double.NEGATIVE_INFINITY;
      for (int rangeID = 0; rangeID < numRange; rangeID++) {
        double min;
        if (rangeID > 0 && random().nextInt(10) == 7) {
          // Use an existing boundary:
          DoubleRange prevRange = ranges[random().nextInt(rangeID)];
          if (random().nextBoolean()) {
            min = prevRange.min[0];
          } else {
            min = prevRange.max[0];
          }
        } else {
          min = random().nextDouble();
        }
        double max;
        if (rangeID > 0 && random().nextInt(10) == 7) {
          // Use an existing boundary:
          DoubleRange prevRange = ranges[random().nextInt(rangeID)];
          if (random().nextBoolean()) {
            max = prevRange.min[0];
          } else {
            max = prevRange.max[0];
          }
        } else {
          max = random().nextDouble();
        }

        if (min > max) {
          double x = min;
          min = max;
          max = x;
        }
        boolean minIncl;
        boolean maxIncl;

        // NOTE: max - min >= 0 is here to handle the common overflow case!
        if (max - min >= 0 && max - min < 2) {
          // If max == min or max == min+1, we always do inclusive, else we might pass an empty
          // range and hit exc from LongRange's ctor:
          minIncl = true;
          maxIncl = true;
        } else {
          minIncl = random().nextBoolean();
          maxIncl = random().nextBoolean();
        }
        ranges[rangeID] = new DoubleRange("r" + rangeID, min, minIncl, max, maxIncl);
        if (VERBOSE) {
          System.out.println("  range " + rangeID + ": " + ranges[rangeID]);
        }

        byte[] queryPackedValue = new byte[2 * Double.BYTES];
        verifyAndEncode(new double[] {min}, new double[] {max}, queryPackedValue);
        ArrayUtil.ByteArrayComparator comparator = ArrayUtil.getUnsignedComparator(Double.BYTES);
        for (int i = 0; i < numDocs; i++) {
          byte[] packedValue = new byte[2 * Double.BYTES];
          verifyAndEncode(new double[] {mins[i]}, new double[] {maxes[i]}, packedValue);
          if (RangeFieldQuery.QueryType.INTERSECTS.matches(
              queryPackedValue, packedValue, 1, Double.BYTES, comparator)) {
            expectedCounts[rangeID]++;
            expectedTopNChildrenCounts[rangeID]++;
            minAcceptedValue = Math.min(minAcceptedValue, mins[i]);
            maxAcceptedValue = Math.max(maxAcceptedValue, maxes[i]);
          }
        }
      }

      FacetsCollector sfc = s.search(new MatchAllDocsQuery(), new FacetsCollectorManager());
      Query fastMatchQuery;
      if (random().nextBoolean()) {
        fastMatchQuery =
            org.apache.lucene.document.DoubleRange.newIntersectsQuery(
                "field", new double[] {minAcceptedValue}, new double[] {maxAcceptedValue});
      } else {
        fastMatchQuery = null;
      }

      Facets facets =
          new DoubleRangeOnRangeFacetCounts(
              "field", sfc, RangeFieldQuery.QueryType.INTERSECTS, fastMatchQuery, ranges);
      FacetResult result = facets.getAllChildren("field");
      assertEquals(numRange, result.labelValues.length);
      FacetResult topNResult = facets.getTopChildren(numRange, "field");
      Arrays.sort(expectedTopNChildrenCounts);

      for (int rangeID = 0; rangeID < numRange; rangeID++) {
        if (VERBOSE) {
          System.out.println("  range " + rangeID + " expectedCount=" + expectedCounts[rangeID]);
        }
        LabelAndValue subNode = result.labelValues[rangeID];
        assertEquals("r" + rangeID, subNode.label);
        assertEquals(expectedCounts[rangeID], subNode.value.intValue());
        // test topNChildren and assert topNResults are sorted by count
        if (rangeID < topNResult.labelValues.length) {
          LabelAndValue topNsubNode = topNResult.labelValues[rangeID];
          assertEquals(
              expectedTopNChildrenCounts[numRange - rangeID - 1], topNsubNode.value.intValue());
        }

        DoubleRange range = ranges[rangeID];

        // Test drill-down:
        DrillDownQuery ddq = new DrillDownQuery(config);
        ddq.add(
            "field",
            org.apache.lucene.document.DoubleRange.newIntersectsQuery(
                "field", range.min, range.max));
        assertEquals(expectedCounts[rangeID], s.count(ddq));
      }
    }

    w.close();
    IOUtils.close(r, dir);
  }

  public void testRandomMultiDimDoubles() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    int numDocs = atLeast(1000);
    int dims = 2 + random().nextInt(3);
    if (VERBOSE) {
      System.out.println("TEST: numDocs=" + numDocs);
    }
    double[][] mins = new double[numDocs][dims];
    double[][] maxes = new double[numDocs][dims];
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      for (int dim = 0; dim < dims; dim++) {
        double min = random().nextDouble();
        double max = random().nextDouble();
        if (min > max) {
          double tmp = min;
          min = max;
          max = tmp;
        }
        mins[i][dim] = min;
        maxes[i][dim] = max;
      }
      doc.add(new DoubleRangeDocValuesField("field", mins[i], maxes[i]));
      doc.add(new org.apache.lucene.document.DoubleRange("field", mins[i], maxes[i]));
      w.addDocument(doc);
    }
    IndexReader r = w.getReader();

    IndexSearcher s = newSearcher(r, false);
    FacetsConfig config = new FacetsConfig();

    int numIters = atLeast(10);
    for (int iter = 0; iter < numIters; iter++) {
      if (VERBOSE) {
        System.out.println("TEST: iter=" + iter);
      }
      int numRange = TestUtil.nextInt(random(), 1, 100);
      DoubleRange[] ranges = new DoubleRange[numRange];
      int[] expectedCounts = new int[numRange];
      int[] expectedTopNChildrenCounts = new int[numRange];
      double[] minAcceptedValue = new double[dims];
      Arrays.fill(minAcceptedValue, Double.POSITIVE_INFINITY);
      double[] maxAcceptedValue = new double[dims];
      Arrays.fill(maxAcceptedValue, Double.NEGATIVE_INFINITY);
      for (int rangeID = 0; rangeID < numRange; rangeID++) {
        double[] min = new double[dims];
        if (rangeID > 0 && random().nextInt(10) == 7) {
          // Use an existing boundary:
          DoubleRange prevRange = ranges[random().nextInt(rangeID)];
          for (int dim = 0; dim < dims; dim++) {
            if (random().nextBoolean()) {
              min[dim] = prevRange.min[dim];
            } else {
              min[dim] = prevRange.max[dim];
            }
          }
        } else {
          for (int dim = 0; dim < dims; dim++) {
            min[dim] = random().nextDouble();
          }
        }
        double[] max = new double[dims];
        if (rangeID > 0 && random().nextInt(10) == 7) {
          // Use an existing boundary:
          DoubleRange prevRange = ranges[random().nextInt(rangeID)];
          for (int dim = 0; dim < dims; dim++) {
            if (random().nextBoolean()) {
              max[dim] = prevRange.min[dim];
            } else {
              max[dim] = prevRange.max[dim];
            }
          }
        } else {
          for (int dim = 0; dim < dims; dim++) {
            max[dim] = random().nextDouble();
          }
        }

        for (int dim = 0; dim < dims; dim++) {
          if (min[dim] > max[dim]) {
            double x = min[dim];
            min[dim] = max[dim];
            max[dim] = x;
          }
        }

        ranges[rangeID] = new DoubleRange("r" + rangeID, min, max);
        if (VERBOSE) {
          System.out.println("  range " + rangeID + ": " + ranges[rangeID]);
        }

        byte[] queryPackedValue = new byte[2 * Double.BYTES * dims];
        ArrayUtil.ByteArrayComparator comparator = ArrayUtil.getUnsignedComparator(Double.BYTES);
        verifyAndEncode(min, max, queryPackedValue);
        for (int i = 0; i < numDocs; i++) {
          byte[] packedValue = new byte[2 * Double.BYTES * dims];
          verifyAndEncode(mins[i], maxes[i], packedValue);
          if (RangeFieldQuery.QueryType.INTERSECTS.matches(
              queryPackedValue, packedValue, dims, Double.BYTES, comparator)) {
            expectedCounts[rangeID]++;
            expectedTopNChildrenCounts[rangeID]++;
            for (int dim = 0; dim < dims; dim++) {
              minAcceptedValue[dim] = Math.min(minAcceptedValue[dim], mins[i][dim]);
              maxAcceptedValue[dim] = Math.max(maxAcceptedValue[dim], maxes[i][dim]);
            }
          }
        }
      }

      FacetsCollector sfc = s.search(new MatchAllDocsQuery(), new FacetsCollectorManager());
      Query fastMatchQuery;
      if (random().nextBoolean()) {
        fastMatchQuery =
            org.apache.lucene.document.DoubleRange.newIntersectsQuery(
                "field", minAcceptedValue, maxAcceptedValue);
      } else {
        fastMatchQuery = null;
      }

      Facets facets =
          new DoubleRangeOnRangeFacetCounts(
              "field", sfc, RangeFieldQuery.QueryType.INTERSECTS, fastMatchQuery, ranges);
      FacetResult result = facets.getAllChildren("field");
      assertEquals(numRange, result.labelValues.length);
      FacetResult topNResult = facets.getTopChildren(numRange, "field");
      Arrays.sort(expectedTopNChildrenCounts);

      for (int rangeID = 0; rangeID < numRange; rangeID++) {
        if (VERBOSE) {
          System.out.println("  range " + rangeID + " expectedCount=" + expectedCounts[rangeID]);
        }
        LabelAndValue subNode = result.labelValues[rangeID];
        assertEquals("r" + rangeID, subNode.label);
        assertEquals(expectedCounts[rangeID], subNode.value.intValue());
        // test topNChildren and assert topNResults are sorted by count
        if (rangeID < topNResult.labelValues.length) {
          LabelAndValue topNsubNode = topNResult.labelValues[rangeID];
          assertEquals(
              expectedTopNChildrenCounts[numRange - rangeID - 1], topNsubNode.value.intValue());
        }

        DoubleRange range = ranges[rangeID];

        // Test drill-down:
        DrillDownQuery ddq = new DrillDownQuery(config);
        ddq.add(
            "field",
            org.apache.lucene.document.DoubleRange.newIntersectsQuery(
                "field", range.min, range.max));
        assertEquals(expectedCounts[rangeID], s.count(ddq));
      }
    }

    w.close();
    IOUtils.close(r, dir);
  }

  public void testMissingValues() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    Document doc = new Document();
    LongRangeDocValuesField field =
        new LongRangeDocValuesField("field", longArray(0L), longArray(10L));
    doc.add(field);
    for (long l = 0; l < 100; l++) {
      if (l % 5 == 0) {
        // Every 5th doc is missing the value:
        w.addDocument(new Document());
        continue;
      }
      setLongRangeValue(field, l, l + 10L);
      w.addDocument(doc);
    }

    IndexReader r = w.getReader();

    IndexSearcher s = newSearcher(r);
    FacetsCollector fc = s.search(new MatchAllDocsQuery(), new FacetsCollectorManager());
    Facets facets =
        new LongRangeOnRangeFacetCounts(
            "field",
            fc,
            RangeFieldQuery.QueryType.INTERSECTS,
            new LongRange("less than 10", 0L, true, 10L, false),
            new LongRange("less than or equal to 10", 0L, true, 10L, true),
            new LongRange("over 90", 90L, false, 100L, false),
            new LongRange("90 or above", 90L, true, 100L, false),
            new LongRange("over 1000", 1000L, false, Long.MAX_VALUE, false));

    assertEquals(
        "dim=field path=[] value=24 childCount=5\n  less than 10 (8)\n  less than or equal to 10 (8)\n  over 90 (16)\n  90 or above (16)\n  over 1000 (0)\n",
        facets.getAllChildren("field").toString());
    assertEquals(
        "dim=field path=[] value=24 childCount=4\n  90 or above (16)\n  over 90 (16)\n  less than 10 (8)\n  less than or equal to 10 (8)\n",
        facets.getTopChildren(4, "field").toString());

    w.close();
    IOUtils.close(r, d);
  }

  public void testLongRangeEquals() throws Exception {
    assertEquals(
        new LongRange("field", -7, true, 17, false), new LongRange("field", -7, true, 17, false));
    assertEquals(
        new LongRange("field", -7, true, 17, false).hashCode(),
        new LongRange("field", -7, true, 17, false).hashCode());
    assertNotEquals(
        new LongRange("field", -7, true, 17, false), new LongRange("field", -7, true, 17, true));
    assertNotEquals(
        new LongRange("field", -7, true, 17, false).hashCode(),
        new LongRange("field", -7, true, 17, true).hashCode());
    assertNotEquals(
        new LongRange("field", -7, true, 17, false), new LongRange("field", -7, true, 18, false));
    assertNotEquals(
        new LongRange("field", -7, true, 17, false).hashCode(),
        new LongRange("field", -7, true, 18, false).hashCode());
    // mutlidim tests
    assertEquals(
        new LongRange("field", longArray(-7, -8), longArray(17, 18)),
        new LongRange("field", longArray(-7, -8), longArray(17, 18)));
    assertEquals(
        new LongRange("field", longArray(-7, -8), longArray(17, 18)).hashCode(),
        new LongRange("field", longArray(-7, -8), longArray(17, 18)).hashCode());
    assertNotEquals(
        new LongRange("field", longArray(-7, -8, -9), longArray(17, 18, 19)).hashCode(),
        new LongRange("field", longArray(-7, -8), longArray(17, 18)).hashCode());
    assertNotEquals(
        new LongRange("field", longArray(-7, -9), longArray(17, 18)).hashCode(),
        new LongRange("field", longArray(-7, -8), longArray(17, 18)).hashCode());
    assertNotEquals(
        new LongRange("field", longArray(-7, -8), longArray(17, 19)).hashCode(),
        new LongRange("field", longArray(-7, -8), longArray(17, 18)).hashCode());
  }

  public void testDoubleRangeEquals() throws Exception {
    assertEquals(
        new DoubleRange("field", -7d, true, 17d, false),
        new DoubleRange("field", -7d, true, 17d, false));
    assertEquals(
        new DoubleRange("field", -7d, true, 17d, false).hashCode(),
        new DoubleRange("field", -7d, true, 17d, false).hashCode());
    assertNotEquals(
        new DoubleRange("field", -7d, true, 17d, false),
        new DoubleRange("field", -7d, true, 17d, true));
    assertNotEquals(
        new DoubleRange("field", -7d, true, 17d, false).hashCode(),
        new DoubleRange("field", -7d, true, 17d, true).hashCode());
    assertNotEquals(
        new DoubleRange("field", -7d, true, 17d, false),
        new DoubleRange("field", -7d, true, 18d, false));
    assertNotEquals(
        new DoubleRange("field", -7d, true, 17d, false).hashCode(),
        new DoubleRange("field", -7d, true, 18, false).hashCode());
    // multidim tests
    assertEquals(
        new DoubleRange("field", doubleArray(-7d, -8d), doubleArray(17d, 18d)),
        new DoubleRange("field", doubleArray(-7d, -8d), doubleArray(17d, 18d)));
    assertEquals(
        new DoubleRange("field", doubleArray(-7d, -8d), doubleArray(17d, 18d)).hashCode(),
        new DoubleRange("field", doubleArray(-7d, -8d), doubleArray(17d, 18d)).hashCode());
    assertNotEquals(
        new DoubleRange("field", doubleArray(-7d, -8d, -9d), doubleArray(17d, 18d, 19d)),
        new DoubleRange("field", doubleArray(-7d, -8d), doubleArray(17d, 18d)));
    assertNotEquals(
        new DoubleRange("field", doubleArray(-7d, -9d), doubleArray(17d, 18d)),
        new DoubleRange("field", doubleArray(-7d, -8d), doubleArray(17d, 18d)));
    assertNotEquals(
        new DoubleRange("field", doubleArray(-7d, -8d), doubleArray(17d, 19d)),
        new DoubleRange("field", doubleArray(-7d, -8d), doubleArray(17d, 18d)));
  }

  private void setLongRangeValue(LongRangeDocValuesField field, long min, long max) {
    setLongRangeValue(field, longArray(min), longArray(max));
  }

  private void setLongRangeValue(LongRangeDocValuesField field, long[] min, long[] max) {
    byte[] encodedValue = new byte[2 * min.length * Long.BYTES];
    org.apache.lucene.document.LongRange.verifyAndEncode(min, max, encodedValue);
    field.setBytesValue(new BytesRef(encodedValue));
  }

  private void setDoubleRangeValue(DoubleRangeDocValuesField field, double min, double max) {
    setDoubleRangeValue(field, doubleArray(min), doubleArray(max));
  }

  private void setDoubleRangeValue(DoubleRangeDocValuesField field, double[] min, double[] max) {
    byte[] encodedValue = new byte[2 * min.length * Double.BYTES];
    org.apache.lucene.document.DoubleRange.verifyAndEncode(min, max, encodedValue);
    field.setBytesValue(new BytesRef(encodedValue));
  }

  private static long[] longArray(long... longs) {
    long[] ret = new long[longs.length];
    System.arraycopy(longs, 0, ret, 0, longs.length);
    return ret;
  }

  private static double[] doubleArray(double... doubles) {
    double[] ret = new double[doubles.length];
    System.arraycopy(doubles, 0, ret, 0, doubles.length);
    return ret;
  }
}
