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
package org.apache.lucene.sandbox.facet.utils;

import java.io.IOException;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.facet.*;
import org.apache.lucene.facet.range.DoubleRange;
import org.apache.lucene.facet.range.LongRange;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.sandbox.facet.SandboxFacetTestCase;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.IOUtils;

public class TestFacetOrchestrators extends SandboxFacetTestCase {
  public void testTaxonomyFacets() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();

    // Writes facet ords to a separate directory from the
    // main index:
    DirectoryTaxonomyWriter taxoWriter =
        new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);

    FacetsConfig config = new FacetsConfig();
    config.setHierarchical("Publish Date", true);
    config.setHierarchical("Author", false);

    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    doc.add(new FacetField("Author", "Bob"));
    doc.add(new FacetField("Publish Date", "2010", "10", "15"));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Lisa"));
    doc.add(new FacetField("Publish Date", "2010", "10", "20"));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Lisa"));
    doc.add(new FacetField("Publish Date", "2012", "1", "1"));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Susan"));
    doc.add(new FacetField("Publish Date", "2012", "1", "7"));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Frank"));
    doc.add(new FacetField("Publish Date", "1999", "5", "5"));
    writer.addDocument(config.build(taxoWriter, doc));

    // NRT open
    IndexSearcher searcher = getNewSearcherForDrillSideways(writer.getReader());

    // NRT open
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);

    Query query = new MatchAllDocsQuery();

    FacetBuilder authorTop0Builder =
        new TaxonomyFacetBuilder(config, taxoReader, "Author").withTopN(0);
    FacetBuilder authorTop10Builder =
        new TaxonomyFacetBuilder(config, taxoReader, "Author").withTopN(10);
    FacetBuilder authorAllBuilder =
        new TaxonomyFacetBuilder(config, taxoReader, "Author").withTopN(10);
    FacetBuilder publishDateTop10Builder =
        new TaxonomyFacetBuilder(config, taxoReader, "Publish Date").withTopN(10);
    FacetBuilder publishDateAllBuilder =
        new TaxonomyFacetBuilder(config, taxoReader, "Publish Date");
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          new TaxonomyFacetBuilder(config, taxoReader, "Not configured dimension");
        });

    assertEquals(
        "There must be only one collector because all TaxonomyFacetBuilder use the same index field",
        1,
        FacetOrchestrator.collectorManagerForBuilders(
                List.of(
                    authorTop0Builder,
                    authorTop10Builder,
                    authorAllBuilder,
                    publishDateTop10Builder,
                    publishDateAllBuilder))
            .size());

    new FacetOrchestrator()
        .addBuilder(authorTop0Builder)
        .addBuilder(authorTop10Builder)
        .addBuilder(authorAllBuilder)
        .addBuilder(publishDateTop10Builder)
        .addBuilder(publishDateAllBuilder)
        .collect(query, searcher);

    expectThrows(
        IllegalArgumentException.class,
        () -> {
          authorTop0Builder.getResult();
        });

    // Retrieve & verify results:
    assertEquals(
        "dim=Publish Date path=[] value=5 childCount=3\n  2010 (2)\n  2012 (2)\n  1999 (1)\n",
        publishDateTop10Builder.getResult().toString());
    assertEquals(
        "dim=Author path=[] value=5 childCount=4\n  Lisa (2)\n  Bob (1)\n  Susan (1)\n  Frank (1)\n",
        authorTop10Builder.getResult().toString());

    assertFacetResult(
        publishDateAllBuilder.getResult(),
        "Publish Date",
        new String[0],
        3,
        5,
        new LabelAndValue("1999", 1),
        new LabelAndValue("2010", 2),
        new LabelAndValue("2012", 2));

    assertFacetResult(
        authorAllBuilder.getResult(),
        "Author",
        new String[0],
        4,
        5,
        new LabelAndValue("Bob", 1),
        new LabelAndValue("Frank", 1),
        new LabelAndValue("Lisa", 2),
        new LabelAndValue("Susan", 1));

    // Now use drill sideways
    authorTop10Builder = new TaxonomyFacetBuilder(config, taxoReader, "Author").withTopN(10);
    publishDateTop10Builder =
        new TaxonomyFacetBuilder(config, taxoReader, "Publish Date").withTopN(10);
    DrillSidewaysFacetOrchestrator drillSidewaysFacetOrchestrator =
        new DrillSidewaysFacetOrchestrator();
    drillSidewaysFacetOrchestrator.addDrillDownBuilder(authorTop10Builder);
    DrillDownQuery q2 = new DrillDownQuery(config);
    q2.add("Publish Date", "2010");
    drillSidewaysFacetOrchestrator.addDrillSidewaysBuilder("Publish Date", publishDateTop10Builder);

    DrillSideways ds =
        new DrillSideways(searcher, config, taxoReader) {
          @Override
          protected boolean scoreSubDocsAtOnce() {
            return random().nextBoolean();
          }
        };
    drillSidewaysFacetOrchestrator.collect(q2, ds);

    // For Author it is drill down
    assertEquals(
        "dim=Author path=[] value=2 childCount=2\n" + "  Bob (1)\n" + "  Lisa (1)\n",
        authorTop10Builder.getResult().toString());
    // For Publish Date it is drill sideways
    assertEquals(
        "dim=Publish Date path=[] value=5 childCount=3\n"
            + "  2010 (2)\n"
            + "  2012 (2)\n"
            + "  1999 (1)\n",
        publishDateTop10Builder.getResult().toString());

    writer.close();
    IOUtils.close(taxoWriter, searcher.getIndexReader(), taxoReader, taxoDir, dir);
  }

  /** Tests mix of long range, double range and taxonomy facets. */
  public void testMixedRangeAndNonRangeTaxonomy() throws IOException {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    Directory td = newDirectory();
    DirectoryTaxonomyWriter tw = new DirectoryTaxonomyWriter(td, IndexWriterConfig.OpenMode.CREATE);

    FacetsConfig config = new FacetsConfig();
    config.setRequireDimCount("dim", false);

    for (long l = 0; l < 100; l++) {
      Document doc = new Document();
      // For computing range facet counts:
      doc.add(new NumericDocValuesField("longField", l));
      // For drill down by numeric range:
      doc.add(new LongPoint("longField", l));
      // For double range facets
      doc.add(new DoubleDocValuesField("doubleField", (double) l / 10));

      if ((l & 3) == 0) {
        doc.add(new FacetField("dim", "a"));
      } else {
        doc.add(new FacetField("dim", "b"));
      }
      w.addDocument(config.build(tw, doc));
    }

    final IndexReader r = w.getReader();
    final TaxonomyReader tr = new DirectoryTaxonomyReader(tw);

    IndexSearcher s = getNewSearcherForDrillSideways(r);
    // DrillSideways requires the entire range of docs to be scored at once, so it doesn't support
    // timeouts whose implementation scores one window of doc IDs at a time.
    s.setTimeout(null);

    if (VERBOSE) {
      System.out.println("TEST: searcher=" + s);
    }

    DrillSideways ds =
        new DrillSideways(s, config, tr) {
          @Override
          protected boolean scoreSubDocsAtOnce() {
            return random().nextBoolean();
          }
        };

    // Data for range facets
    LongRange[] longRanges =
        new LongRange[] {
          new LongRange("less than 10", 0L, true, 10L, false),
          new LongRange("less than or equal to 10", 0L, true, 10L, true),
          new LongRange("over 90", 90L, false, 100L, false),
          new LongRange("90 or above", 90L, true, 100L, false),
          new LongRange("over 1000", 1000L, false, Long.MAX_VALUE, false)
        };
    DoubleRange[] doubleRanges =
        new DoubleRange[] {
          new DoubleRange("less than 1.0", 0.0, true, 1.0, false),
          new DoubleRange("less than or equal to 1.0", 0.0, true, 1.0, true),
          new DoubleRange("over 9.0", 9.0, false, 10.0, false),
          new DoubleRange("9.0 or above", 9.0, true, 10.0, false),
          new DoubleRange("over 10.0", 10.0, false, Double.POSITIVE_INFINITY, false)
        };

    // Search with IndexSearcher
    FacetBuilder longRangeFacetBuilder =
        RangeFacetBuilderFactory.forLongRanges("longField", longRanges);
    FacetBuilder doubleRangeFacetBuilder =
        RangeFacetBuilderFactory.forDoubleRanges("doubleField", doubleRanges)
            .withSortByCount()
            .withTopN(3);
    FacetBuilder taxonomyFacetBuilder = new TaxonomyFacetBuilder(config, tr, "dim").withTopN(10);

    new FacetOrchestrator()
        .addBuilder(taxonomyFacetBuilder)
        .addBuilder(longRangeFacetBuilder)
        .addBuilder(doubleRangeFacetBuilder)
        .collect(new MatchAllDocsQuery(), s);
    assertEquals(
        "dim=dim path=[] value=100 childCount=2\n  b (75)\n  a (25)\n",
        taxonomyFacetBuilder.getResult().toString());
    assertEquals(
        "dim=longField path=[] value=-1 childCount=4\n"
            + "  less than 10 (10)\n"
            + "  less than or equal to 10 (11)\n"
            + "  over 90 (9)\n"
            + "  90 or above (10)\n",
        longRangeFacetBuilder.getResult().toString());
    assertEquals(
        "dim=doubleField path=[] value=-1 childCount=4\n"
            + "  less than or equal to 1.0 (11)\n"
            + "  less than 1.0 (10)\n"
            + "  9.0 or above (10)\n",
        doubleRangeFacetBuilder.getResult().toString());

    // Now search with DrillDownQuery
    longRangeFacetBuilder = RangeFacetBuilderFactory.forLongRanges("longField", longRanges);
    doubleRangeFacetBuilder =
        RangeFacetBuilderFactory.forDoubleRanges("doubleField", doubleRanges)
            .withSortByCount()
            .withTopN(3);
    taxonomyFacetBuilder = new TaxonomyFacetBuilder(config, tr, "dim").withTopN(10);

    new DrillSidewaysFacetOrchestrator()
        .addDrillDownBuilder(taxonomyFacetBuilder)
        .addDrillDownBuilder(longRangeFacetBuilder)
        .addDrillDownBuilder(doubleRangeFacetBuilder)
        .collect(new DrillDownQuery(config), ds);
    assertEquals(
        "dim=dim path=[] value=100 childCount=2\n  b (75)\n  a (25)\n",
        taxonomyFacetBuilder.getResult().toString());
    assertEquals(
        "dim=longField path=[] value=-1 childCount=4\n"
            + "  less than 10 (10)\n"
            + "  less than or equal to 10 (11)\n"
            + "  over 90 (9)\n"
            + "  90 or above (10)\n",
        longRangeFacetBuilder.getResult().toString());
    assertEquals(
        "dim=doubleField path=[] value=-1 childCount=4\n"
            + "  less than or equal to 1.0 (11)\n"
            + "  less than 1.0 (10)\n"
            + "  9.0 or above (10)\n",
        doubleRangeFacetBuilder.getResult().toString());

    // Now search with DrillDownQuery, with drill down on dim=b
    longRangeFacetBuilder = RangeFacetBuilderFactory.forLongRanges("longField", longRanges);
    doubleRangeFacetBuilder =
        RangeFacetBuilderFactory.forDoubleRanges("doubleField", doubleRanges)
            .withSortByCount()
            .withTopN(3);
    taxonomyFacetBuilder = new TaxonomyFacetBuilder(config, tr, "dim").withTopN(10);

    DrillDownQuery query = new DrillDownQuery(config);
    query.add("dim", "b");
    new DrillSidewaysFacetOrchestrator()
        .addDrillDownBuilder(longRangeFacetBuilder)
        .addDrillDownBuilder(doubleRangeFacetBuilder)
        .addDrillSidewaysBuilder("dim", taxonomyFacetBuilder)
        .collect(query, ds);
    assertEquals(
        "dim=dim path=[] value=100 childCount=2\n  b (75)\n  a (25)\n",
        taxonomyFacetBuilder.getResult().toString());
    assertEquals(
        "dim=longField path=[] value=-1 childCount=4\n"
            + "  less than 10 (7)\n"
            + "  less than or equal to 10 (8)\n"
            + "  over 90 (7)\n"
            + "  90 or above (8)\n",
        longRangeFacetBuilder.getResult().toString());
    assertEquals(
        "dim=doubleField path=[] value=-1 childCount=4\n"
            + "  less than or equal to 1.0 (8)\n"
            + "  9.0 or above (8)\n"
            + "  less than 1.0 (7)\n",
        doubleRangeFacetBuilder.getResult().toString());

    // Now search with DrillDownQuery, with drill down on longField range "less than or equal to
    // 10":
    longRangeFacetBuilder = RangeFacetBuilderFactory.forLongRanges("longField", longRanges);
    doubleRangeFacetBuilder =
        RangeFacetBuilderFactory.forDoubleRanges("doubleField", doubleRanges)
            .withSortByCount()
            .withTopN(3);
    taxonomyFacetBuilder = new TaxonomyFacetBuilder(config, tr, "dim").withTopN(10);

    query = new DrillDownQuery(config);
    query.add("longField", LongPoint.newRangeQuery("longField", 0L, 10L));
    new DrillSidewaysFacetOrchestrator()
        .addDrillDownBuilder(taxonomyFacetBuilder)
        .addDrillDownBuilder(doubleRangeFacetBuilder)
        .addDrillSidewaysBuilder("longField", longRangeFacetBuilder)
        .collect(query, ds);
    assertEquals(
        "dim=dim path=[] value=11 childCount=2\n" + "  b (8)\n" + "  a (3)\n",
        taxonomyFacetBuilder.getResult().toString());
    assertEquals(
        "dim=longField path=[] value=-1 childCount=4\n"
            + "  less than 10 (10)\n"
            + "  less than or equal to 10 (11)\n"
            + "  over 90 (9)\n"
            + "  90 or above (10)\n",
        longRangeFacetBuilder.getResult().toString());
    assertEquals(
        "dim=doubleField path=[] value=-1 childCount=2\n"
            + "  less than or equal to 1.0 (11)\n"
            + "  less than 1.0 (10)\n",
        doubleRangeFacetBuilder.getResult().toString());

    w.close();
    IOUtils.close(tw, tr, td, r, d);
  }

  public void testLongValues() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    for (long l = 0; l < 5; l++) {
      for (long ll = 0; ll < l + 10; ll++) {
        Document doc = new Document();
        doc.add(new NumericDocValuesField("field", l));
        w.addDocument(doc);
      }
    }

    // Also add Long.MAX_VALUE
    Document doc = new Document();
    doc.add(new NumericDocValuesField("field", Long.MAX_VALUE));
    w.addDocument(doc);

    IndexReader r = w.getReader();
    w.close();

    IndexSearcher s = newSearcher(r);
    FacetBuilder allChildrenSortByValue = new LongValueFacetBuilder("field");
    FacetBuilder allChildrenSortByCount = new LongValueFacetBuilder("field").withSortByCount();
    FacetBuilder topChildrenSortByCount =
        new LongValueFacetBuilder("field").withSortByCount().withTopN(2);

    assertEquals(
        "There must be only one collector for all builders since they all use the same field",
        1,
        FacetOrchestrator.collectorManagerForBuilders(
                List.of(allChildrenSortByCount, allChildrenSortByValue, topChildrenSortByCount))
            .size());

    new FacetOrchestrator()
        .addBuilder(allChildrenSortByValue)
        .addBuilder(allChildrenSortByCount)
        .addBuilder(topChildrenSortByCount)
        .collect(new MatchAllDocsQuery(), s);

    assertEquals(
        "dim=field path=[] value=-1 childCount=6\n"
            + "  0 (10)\n"
            + "  1 (11)\n"
            + "  2 (12)\n"
            + "  3 (13)\n"
            + "  4 (14)\n"
            + "  9223372036854775807 (1)\n",
        allChildrenSortByValue.getResult().toString());

    assertEquals(
        "dim=field path=[] value=-1 childCount=6\n" + "  4 (14)\n" + "  3 (13)\n",
        topChildrenSortByCount.getResult().toString());

    assertEquals(
        "dim=field path=[] value=-1 childCount=6\n"
            + "  4 (14)\n"
            + "  3 (13)\n"
            + "  2 (12)\n"
            + "  1 (11)\n"
            + "  0 (10)\n"
            + "  9223372036854775807 (1)\n",
        allChildrenSortByCount.getResult().toString());

    r.close();
    d.close();
  }
}
