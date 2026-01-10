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
package org.apache.lucene.facet;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.IOUtils;
import org.junit.Test;

public class TestMultipleIndexFields extends FacetTestCase {

  private static final FacetField[] CATEGORIES =
      new FacetField[] {
        new FacetField("Author", "Mark Twain"),
        new FacetField("Author", "Stephen King"),
        new FacetField("Author", "Kurt Vonnegut"),
        new FacetField("Band", "Rock & Pop", "The Beatles"),
        new FacetField("Band", "Punk", "The Ramones"),
        new FacetField("Band", "Rock & Pop", "U2"),
        new FacetField("Band", "Rock & Pop", "REM"),
        new FacetField("Band", "Rock & Pop", "Dave Matthews Band"),
        new FacetField("Composer", "Bach"),
      };

  private FacetsConfig getConfig() {
    FacetsConfig config = new FacetsConfig();
    config.setHierarchical("Band", true);
    return config;
  }

  @Test
  public void testDefault() throws Exception {
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();

    // create and open an index writer
    RandomIndexWriter iw =
        new RandomIndexWriter(
            random(),
            indexDir,
            newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)));
    // create and open a taxonomy writer
    TaxonomyWriter tw = new DirectoryTaxonomyWriter(taxoDir, OpenMode.CREATE);
    FacetsConfig config = getConfig();

    seedIndex(tw, iw, config);

    IndexReader ir = iw.getReader();
    tw.commit();

    // prepare index reader and taxonomy.
    TaxonomyReader tr = new DirectoryTaxonomyReader(taxoDir);

    // prepare searcher to search against
    IndexSearcher searcher = newSearcher(ir);

    FacetsCollector sfc = performSearch(searcher);

    // Obtain facets results and hand-test them
    assertCorrectResults(getTaxonomyFacetCounts(tr, config, sfc));

    assertOrdinalsExist("$facets", ir);

    iw.close();
    IOUtils.close(tr, ir, tw, indexDir, taxoDir);
  }

  @Test
  public void testCustom() throws Exception {
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();

    // create and open an index writer
    RandomIndexWriter iw =
        new RandomIndexWriter(
            random(),
            indexDir,
            newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)));
    // create and open a taxonomy writer
    TaxonomyWriter tw = new DirectoryTaxonomyWriter(taxoDir, OpenMode.CREATE);

    FacetsConfig config = getConfig();
    config.setIndexFieldName("Author", "$author");
    seedIndex(tw, iw, config);

    IndexReader ir = iw.getReader();
    tw.commit();

    // prepare index reader and taxonomy.
    TaxonomyReader tr = new DirectoryTaxonomyReader(taxoDir);

    // prepare searcher to search against
    IndexSearcher searcher = newSearcher(ir);

    FacetsCollector sfc = performSearch(searcher);

    Map<String, Facets> facetsMap = new HashMap<>();
    facetsMap.put("Author", getTaxonomyFacetCounts(tr, config, sfc, "$author"));
    Facets facets = new MultiFacets(facetsMap, getTaxonomyFacetCounts(tr, config, sfc));

    // Obtain facets results and hand-test them
    assertCorrectResults(facets);

    assertOrdinalsExist("$facets", ir);
    assertOrdinalsExist("$author", ir);

    iw.close();
    IOUtils.close(tr, ir, tw, indexDir, taxoDir);
  }

  @Test
  public void testTwoCustomsSameField() throws Exception {
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();

    // create and open an index writer
    RandomIndexWriter iw =
        new RandomIndexWriter(
            random(),
            indexDir,
            newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)));
    // create and open a taxonomy writer
    TaxonomyWriter tw = new DirectoryTaxonomyWriter(taxoDir, OpenMode.CREATE);

    FacetsConfig config = getConfig();
    config.setIndexFieldName("Band", "$music");
    config.setIndexFieldName("Composer", "$music");
    seedIndex(tw, iw, config);

    IndexReader ir = iw.getReader();
    tw.commit();

    // prepare index reader and taxonomy.
    TaxonomyReader tr = new DirectoryTaxonomyReader(taxoDir);

    // prepare searcher to search against
    IndexSearcher searcher = newSearcher(ir);

    FacetsCollector sfc = performSearch(searcher);

    Map<String, Facets> facetsMap = new HashMap<>();
    Facets facets2 = getTaxonomyFacetCounts(tr, config, sfc, "$music");
    facetsMap.put("Band", facets2);
    facetsMap.put("Composer", facets2);
    Facets facets = new MultiFacets(facetsMap, getTaxonomyFacetCounts(tr, config, sfc));

    // Obtain facets results and hand-test them
    assertCorrectResults(facets);

    assertOrdinalsExist("$facets", ir);
    assertOrdinalsExist("$music", ir);
    assertOrdinalsExist("$music", ir);

    iw.close();
    IOUtils.close(tr, ir, tw, indexDir, taxoDir);
  }

  private void assertOrdinalsExist(String field, IndexReader ir) throws IOException {
    for (LeafReaderContext context : ir.leaves()) {
      LeafReader r = context.reader();
      if (r.getSortedNumericDocValues(field) != null) {
        return; // not all segments must have this DocValues
      }
    }
    fail("no ordinals found for " + field);
  }

  @Test
  public void testDifferentFieldsAndText() throws Exception {
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();

    // create and open an index writer
    RandomIndexWriter iw =
        new RandomIndexWriter(
            random(),
            indexDir,
            newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)));
    // create and open a taxonomy writer
    TaxonomyWriter tw = new DirectoryTaxonomyWriter(taxoDir, OpenMode.CREATE);

    FacetsConfig config = getConfig();
    config.setIndexFieldName("Band", "$bands");
    config.setIndexFieldName("Composer", "$composers");
    seedIndex(tw, iw, config);

    IndexReader ir = iw.getReader();
    tw.commit();

    // prepare index reader and taxonomy.
    TaxonomyReader tr = new DirectoryTaxonomyReader(taxoDir);

    // prepare searcher to search against
    IndexSearcher searcher = newSearcher(ir);

    FacetsCollector sfc = performSearch(searcher);

    Map<String, Facets> facetsMap = new HashMap<>();
    facetsMap.put("Band", getTaxonomyFacetCounts(tr, config, sfc, "$bands"));
    facetsMap.put("Composer", getTaxonomyFacetCounts(tr, config, sfc, "$composers"));
    Facets facets = new MultiFacets(facetsMap, getTaxonomyFacetCounts(tr, config, sfc));

    // Obtain facets results and hand-test them
    assertCorrectResults(facets);
    assertOrdinalsExist("$facets", ir);
    assertOrdinalsExist("$bands", ir);
    assertOrdinalsExist("$composers", ir);

    iw.close();
    IOUtils.close(tr, ir, tw, indexDir, taxoDir);
  }

  @Test
  public void testSomeSameSomeDifferent() throws Exception {
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();

    // create and open an index writer
    RandomIndexWriter iw =
        new RandomIndexWriter(
            random(),
            indexDir,
            newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)));
    // create and open a taxonomy writer
    TaxonomyWriter tw = new DirectoryTaxonomyWriter(taxoDir, OpenMode.CREATE);

    FacetsConfig config = getConfig();
    config.setIndexFieldName("Band", "$music");
    config.setIndexFieldName("Composer", "$music");
    config.setIndexFieldName("Author", "$literature");
    seedIndex(tw, iw, config);

    IndexReader ir = iw.getReader();
    tw.commit();

    // prepare index reader and taxonomy.
    TaxonomyReader tr = new DirectoryTaxonomyReader(taxoDir);

    // prepare searcher to search against
    IndexSearcher searcher = newSearcher(ir);

    FacetsCollector sfc = performSearch(searcher);

    Map<String, Facets> facetsMap = new HashMap<>();
    Facets facets2 = getTaxonomyFacetCounts(tr, config, sfc, "$music");
    facetsMap.put("Band", facets2);
    facetsMap.put("Composer", facets2);
    facetsMap.put("Author", getTaxonomyFacetCounts(tr, config, sfc, "$literature"));
    Facets facets = new MultiFacets(facetsMap, getTaxonomyFacetCounts(tr, config, sfc));

    // Obtain facets results and hand-test them
    assertCorrectResults(facets);
    assertOrdinalsExist("$music", ir);
    assertOrdinalsExist("$literature", ir);

    iw.close();
    IOUtils.close(tr, ir, iw, tw, indexDir, taxoDir);
  }

  private void assertCorrectResults(Facets facets) throws IOException {
    assertEquals(5, facets.getSpecificValue("Band"));
    assertEquals(
        "dim=Band path=[] value=5 childCount=2\n  Rock & Pop (4)\n  Punk (1)\n",
        facets.getTopChildren(10, "Band").toString());
    assertFacetResult(
        facets.getAllChildren("Band"),
        "Band",
        new String[0],
        2,
        5,
        new LabelAndValue("Punk", 1),
        new LabelAndValue("Rock & Pop", 4));
    assertEquals(
        "dim=Band path=[Rock & Pop] value=4 childCount=4\n  The Beatles (1)\n  U2 (1)\n  REM (1)\n  Dave Matthews Band (1)\n",
        facets.getTopChildren(10, "Band", "Rock & Pop").toString());
    assertFacetResult(
        facets.getAllChildren("Band", "Rock & Pop"),
        "Band",
        new String[] {"Rock & Pop"},
        4,
        4,
        new LabelAndValue("Dave Matthews Band", 1),
        new LabelAndValue("REM", 1),
        new LabelAndValue("The Beatles", 1),
        new LabelAndValue("U2", 1));

    assertEquals(
        "dim=Author path=[] value=3 childCount=3\n  Mark Twain (1)\n  Stephen King (1)\n  Kurt Vonnegut (1)\n",
        facets.getTopChildren(10, "Author").toString());
    assertFacetResult(
        facets.getAllChildren("Author"),
        "Author",
        new String[0],
        3,
        3,
        new LabelAndValue("Kurt Vonnegut", 1),
        new LabelAndValue("Mark Twain", 1),
        new LabelAndValue("Stephen King", 1));
  }

  private FacetsCollector performSearch(IndexSearcher searcher) throws IOException {
    return FacetsCollectorManager.search(
            searcher, MatchAllDocsQuery.INSTANCE, 10, new FacetsCollectorManager())
        .facetsCollector();
  }

  private void seedIndex(TaxonomyWriter tw, RandomIndexWriter iw, FacetsConfig config)
      throws IOException {
    for (FacetField ff : CATEGORIES) {
      Document doc = new Document();
      doc.add(ff);
      doc.add(new TextField("content", "alpha", Field.Store.YES));
      iw.addDocument(config.build(tw, doc));
    }
  }
}
