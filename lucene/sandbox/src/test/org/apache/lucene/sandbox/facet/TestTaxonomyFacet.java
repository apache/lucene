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
package org.apache.lucene.sandbox.facet;

import static org.apache.lucene.facet.FacetsConfig.DEFAULT_INDEX_FIELD_NAME;

import org.apache.lucene.document.Document;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.sandbox.facet.cutters.TaxonomyFacetsCutter;
import org.apache.lucene.sandbox.facet.labels.TaxonomyOrdLabelBiMap;
import org.apache.lucene.sandbox.facet.recorders.CountFacetRecorder;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.IOUtils;

/** Test for associations */
public class TestTaxonomyFacet extends SandboxFacetTestCase {

  public void testConstants() {
    // It is essential for TaxonomyOrdLabelBiMap that invalid ordinal is the same as for
    // TaxonomyReader
    assertEquals(TaxonomyOrdLabelBiMap.INVALID_ORD, TaxonomyReader.INVALID_ORDINAL);
  }

  public void testBasic() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();

    // Writes facet ords to a separate directory from the
    // main index:
    DirectoryTaxonomyWriter taxoWriter =
        new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);

    FacetsConfig config = new FacetsConfig();
    config.setHierarchical("Publish Date", true);

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
    IndexSearcher searcher = newSearcher(writer.getReader());

    // NRT open
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);

    Query query = MatchAllDocsQuery.INSTANCE;

    TaxonomyFacetsCutter defaultTaxoCutter =
        new TaxonomyFacetsCutter(DEFAULT_INDEX_FIELD_NAME, config, taxoReader);
    final CountFacetRecorder countRecorder = new CountFacetRecorder();
    FacetFieldCollectorManager<CountFacetRecorder> collectorManager =
        new FacetFieldCollectorManager<>(defaultTaxoCutter, countRecorder);
    searcher.search(query, collectorManager);

    expectThrows(
        IllegalArgumentException.class,
        () -> {
          getTopChildrenByCount(countRecorder, taxoReader, 0, "Author");
        });

    // Retrieve & verify results:
    assertEquals(
        "dim=Publish Date path=[] value=-2147483648 childCount=3\n  2010 (2)\n  2012 (2)\n  1999 (1)\n",
        getTopChildrenByCount(countRecorder, taxoReader, 10, "Publish Date").toString());
    assertEquals(
        "dim=Author path=[] value=-2147483648 childCount=4\n  Lisa (2)\n  Bob (1)\n  Susan (1)\n  Frank (1)\n",
        getTopChildrenByCount(countRecorder, taxoReader, 10, "Author").toString());

    assertFacetResult(
        getAllChildren(countRecorder, taxoReader, "Publish Date"),
        "Publish Date",
        new String[0],
        3,
        VALUE_CANT_BE_COMPUTED,
        new LabelAndValue[] {
          new LabelAndValue("1999", 1), new LabelAndValue("2010", 2), new LabelAndValue("2012", 2),
        });

    assertFacetResult(
        getAllChildren(countRecorder, taxoReader, "Author"),
        "Author",
        new String[0],
        4,
        VALUE_CANT_BE_COMPUTED,
        new LabelAndValue[] {
          new LabelAndValue("Bob", 1),
          new LabelAndValue("Frank", 1),
          new LabelAndValue("Lisa", 2),
          new LabelAndValue("Susan", 1),
        });

    // Now user drills down on Publish Date/2010:
    DrillDownQuery q2 = new DrillDownQuery(config);
    q2.add("Publish Date", "2010");
    final CountFacetRecorder countRecorder2 = new CountFacetRecorder();
    collectorManager = new FacetFieldCollectorManager<>(defaultTaxoCutter, countRecorder2);
    searcher.search(q2, collectorManager);

    assertEquals(
        "dim=Author path=[] value=-2147483648 childCount=2\n  Bob (1)\n  Lisa (1)\n",
        getTopChildrenByCount(countRecorder2, taxoReader, 10, "Author").toString());

    assertEquals(1, getSpecificValue(countRecorder2, taxoReader, "Author", "Lisa"));

    assertArrayEquals(
        new int[] {1, 1},
        getCountsForRecordedCandidates(
            countRecorder2,
            taxoReader,
            new FacetLabel[] {
              new FacetLabel("Author", "Lisa"),
              new FacetLabel("Author", "Susan"), // 0 count, filtered out
              new FacetLabel("Author", "DoesNotExist"), // Doesn't exist in the index, filtered out
              new FacetLabel("Author", "Bob"),
            }));

    if (TEST_ASSERTS_ENABLED) {
      expectThrows(
          AssertionError.class,
          () -> {
            getTopChildrenByCount(countRecorder2, taxoReader, 10, "Non exitent dim");
          });
    }

    writer.close();
    IOUtils.close(taxoWriter, searcher.getIndexReader(), taxoReader, taxoDir, dir);
  }

  public void testTaxonomyCutterExpertModeDisableRollup() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();

    DirectoryTaxonomyWriter taxoWriter =
        new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);

    FacetsConfig config = new FacetsConfig();
    config.setHierarchical("Publish Date", true);

    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    doc.add(new FacetField("Publish Date", "2010", "10", "15"));
    writer.addDocument(config.build(taxoWriter, doc));

    IndexSearcher searcher = newSearcher(writer.getReader());
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);
    Query query = MatchAllDocsQuery.INSTANCE;

    TaxonomyFacetsCutter defaultTaxoCutter =
        new TaxonomyFacetsCutter(DEFAULT_INDEX_FIELD_NAME, config, taxoReader, true);
    final CountFacetRecorder countRecorder = new CountFacetRecorder();
    FacetFieldCollectorManager<CountFacetRecorder> collectorManager =
        new FacetFieldCollectorManager<>(defaultTaxoCutter, countRecorder);
    searcher.search(query, collectorManager);

    assertEquals(
        "Only leaf value should have been counted when rollup is disabled",
        1,
        countRecorder.recordedOrds().toArray().length);

    writer.close();
    IOUtils.close(taxoWriter, searcher.getIndexReader(), taxoReader, taxoDir, dir);
  }
}
