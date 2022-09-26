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
package org.apache.lucene.facet.taxonomy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollectorManager;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.IOUtils;

public class TestTaxonomyFacetCounts extends FacetTestCase {

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

    Facets facets =
        getAllFacets(FacetsConfig.DEFAULT_INDEX_FIELD_NAME, searcher, taxoReader, config);

    // Publish Date is hierarchical, so we should have loaded all 3 int[]:
    assertTrue(((TaxonomyFacets) facets).siblingsLoaded());
    assertTrue(((TaxonomyFacets) facets).childrenLoaded());

    Facets finalFacets = facets;
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          finalFacets.getTopChildren(0, "Author");
        });

    // Retrieve & verify results:
    assertEquals(
        "dim=Publish Date path=[] value=5 childCount=3\n  2010 (2)\n  2012 (2)\n  1999 (1)\n",
        facets.getTopChildren(10, "Publish Date").toString());
    assertEquals(
        "dim=Author path=[] value=5 childCount=4\n  Lisa (2)\n  Bob (1)\n  Susan (1)\n  Frank (1)\n",
        facets.getTopChildren(10, "Author").toString());

    assertFacetResult(
        facets.getAllChildren("Publish Date"),
        "Publish Date",
        new String[0],
        3,
        5,
        new LabelAndValue[] {
          new LabelAndValue("1999", 1), new LabelAndValue("2010", 2), new LabelAndValue("2012", 2),
        });

    assertFacetResult(
        facets.getAllChildren("Author"),
        "Author",
        new String[0],
        4,
        5,
        new LabelAndValue[] {
          new LabelAndValue("Bob", 1),
          new LabelAndValue("Frank", 1),
          new LabelAndValue("Lisa", 2),
          new LabelAndValue("Susan", 1),
        });

    // test getAllDims
    List<FacetResult> results = facets.getAllDims(10);
    // test getTopDims(10, 10) and expect same results from getAllDims(10)
    List<FacetResult> allTopDimsResults = facets.getTopDims(10, 10);
    assertEquals(results, allTopDimsResults);

    // Now user drills down on Publish Date/2010:
    DrillDownQuery q2 = new DrillDownQuery(config);
    q2.add("Publish Date", "2010");
    FacetsCollector c = searcher.search(q2, new FacetsCollectorManager());
    facets = new FastTaxonomyFacetCounts(taxoReader, config, c);
    assertEquals(
        "dim=Author path=[] value=2 childCount=2\n  Bob (1)\n  Lisa (1)\n",
        facets.getTopChildren(10, "Author").toString());

    assertEquals(1, facets.getSpecificValue("Author", "Lisa"));

    assertNull(facets.getTopChildren(10, "Non exitent dim"));

    // Smoke test PrintTaxonomyStats:
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    PrintTaxonomyStats.printStats(taxoReader, new PrintStream(bos, false, IOUtils.UTF_8), true);
    String result = bos.toString(IOUtils.UTF_8);
    assertTrue(result.indexOf("/Author: 4 immediate children; 5 total categories") != -1);
    assertTrue(result.indexOf("/Publish Date: 3 immediate children; 12 total categories") != -1);
    // Make sure at least a few nodes of the tree came out:
    assertTrue(result.indexOf("  /1999") != -1);
    assertTrue(result.indexOf("  /2012") != -1);
    assertTrue(result.indexOf("      /20") != -1);

    writer.close();
    IOUtils.close(taxoWriter, searcher.getIndexReader(), taxoReader, taxoDir, dir);
  }

  // LUCENE-5333
  public void testSparseFacets() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();

    // Writes facet ords to a separate directory from the
    // main index:
    DirectoryTaxonomyWriter taxoWriter =
        new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);

    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    FacetsConfig config = new FacetsConfig();

    Document doc = new Document();
    doc.add(new FacetField("a", "foo1"));
    doc.add(new FacetField("b", "aar1"));
    writer.addDocument(config.build(taxoWriter, doc));

    if (random().nextBoolean()) {
      writer.commit();
    }

    doc = new Document();
    doc.add(new FacetField("a", "foo2"));
    doc.add(new FacetField("b", "bar1"));
    writer.addDocument(config.build(taxoWriter, doc));

    if (random().nextBoolean()) {
      writer.commit();
    }

    doc = new Document();
    doc.add(new FacetField("a", "foo3"));
    doc.add(new FacetField("b", "bar2"));
    doc.add(new FacetField("c", "baz1"));
    writer.addDocument(config.build(taxoWriter, doc));

    // NRT open
    IndexSearcher searcher = newSearcher(writer.getReader());

    // NRT open
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);

    Facets facets =
        getAllFacets(FacetsConfig.DEFAULT_INDEX_FIELD_NAME, searcher, taxoReader, config);

    // test getAllDims(0)
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          facets.getAllDims(0);
        });

    // Ask for top 10 labels for any dims that have counts:
    List<FacetResult> results = facets.getAllDims(10);

    assertEquals(3, results.size());
    assertEquals(
        "dim=a path=[] value=3 childCount=3\n  foo1 (1)\n  foo2 (1)\n  foo3 (1)\n",
        results.get(0).toString());
    assertEquals(
        "dim=b path=[] value=3 childCount=3\n  aar1 (1)\n  bar1 (1)\n  bar2 (1)\n",
        results.get(1).toString());
    assertEquals("dim=c path=[] value=1 childCount=1\n  baz1 (1)\n", results.get(2).toString());

    // test getAllDims with topN = 1, sort by dim names when values are equal
    List<FacetResult> top1results = facets.getAllDims(1);

    assertEquals(3, results.size());
    assertEquals("dim=a path=[] value=3 childCount=3\n  foo1 (1)\n", top1results.get(0).toString());
    assertEquals("dim=b path=[] value=3 childCount=3\n  aar1 (1)\n", top1results.get(1).toString());
    assertEquals("dim=c path=[] value=1 childCount=1\n  baz1 (1)\n", top1results.get(2).toString());

    // test default implementation of getTopDims
    List<FacetResult> topNDimsResult = facets.getTopDims(2, 1);
    assertEquals(2, topNDimsResult.size());
    assertEquals(
        "dim=a path=[] value=3 childCount=3\n  foo1 (1)\n", topNDimsResult.get(0).toString());
    assertEquals(
        "dim=b path=[] value=3 childCount=3\n  aar1 (1)\n", topNDimsResult.get(1).toString());

    // test getTopDims(10, 10) and expect same results from getAllDims(10)
    List<FacetResult> allDimsResults = facets.getTopDims(10, 10);
    assertEquals(results, allDimsResults);

    // test getTopDims(0, 1)
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          facets.getTopDims(0, 1);
        });

    // test getTopDims(1, 0) with topNChildren = 0
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          facets.getTopDims(1, 0);
        });

    writer.close();
    IOUtils.close(taxoWriter, searcher.getIndexReader(), taxoReader, taxoDir, dir);
  }

  public void testWrongIndexFieldName() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();

    // Writes facet ords to a separate directory from the
    // main index:
    DirectoryTaxonomyWriter taxoWriter =
        new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);

    FacetsConfig config = new FacetsConfig();
    config.setIndexFieldName("a", "$facets2");
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    doc.add(new FacetField("a", "foo1"));
    writer.addDocument(config.build(taxoWriter, doc));

    // NRT open
    IndexSearcher searcher = newSearcher(writer.getReader());

    // NRT open
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);

    FacetsCollector c = searcher.search(new MatchAllDocsQuery(), new FacetsCollectorManager());

    // Uses default $facets field:
    Facets facets;
    if (random().nextBoolean()) {
      facets = new FastTaxonomyFacetCounts(taxoReader, config, c);
    } else {
      OrdinalsReader ordsReader = new DocValuesOrdinalsReader();
      if (random().nextBoolean()) {
        ordsReader = new CachedOrdinalsReader(ordsReader);
      }
      facets = new TaxonomyFacetCounts(ordsReader, taxoReader, config, c);
    }

    // Ask for top 10 labels for any dims that have counts:
    List<FacetResult> results = facets.getAllDims(10);
    assertTrue(results.isEmpty());

    // test getTopDims(10, 10) and expect same results from getAllDims(10)
    List<FacetResult> allTopDimsResults = facets.getTopDims(10, 10);
    assertEquals(results, allTopDimsResults);

    expectThrows(
        IllegalArgumentException.class,
        () -> {
          facets.getSpecificValue("a");
        });

    expectThrows(
        IllegalArgumentException.class,
        () -> {
          facets.getTopChildren(10, "a");
        });
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          facets.getAllChildren("a");
        });

    writer.close();
    IOUtils.close(taxoWriter, searcher.getIndexReader(), taxoReader, taxoDir, dir);
  }

  public void testNonExistentDimension() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();

    DirectoryTaxonomyWriter taxoWriter =
        new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);

    FacetsConfig config = new FacetsConfig();
    config.setIndexFieldName("foo", "$custom");

    Document doc = new Document();
    doc.add(new FacetField("foo", "bar"));

    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    writer.addDocument(config.build(taxoWriter, doc));

    IndexSearcher searcher = newSearcher(writer.getReader());
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);

    Facets facets = getAllFacets("$custom", searcher, taxoReader, config);

    // get facets for the dimension, which was never configured or indexed before
    FacetResult result = facets.getTopChildren(5, "non-existent dimension");

    // make sure the result is null (and no exception was thrown)
    assertNull(result);

    // get facets for the dimension, which was never configured or indexed before
    FacetResult allChildrenResult = facets.getAllChildren("non-existent dimension");

    // make sure the result is null (and no exception was thrown)
    assertNull(allChildrenResult);

    writer.close();
    IOUtils.close(taxoWriter, searcher.getIndexReader(), taxoReader, taxoDir, dir);
  }

  public void testReallyNoNormsForDrillDown() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setSimilarity(
        new PerFieldSimilarityWrapper() {
          final Similarity sim = new ClassicSimilarity();

          @Override
          public Similarity get(String name) {
            assertEquals("field", name);
            return sim;
          }
        });
    TaxonomyWriter taxoWriter =
        new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
    FacetsConfig config = new FacetsConfig();

    Document doc = new Document();
    doc.add(newTextField("field", "text", Field.Store.NO));
    doc.add(new FacetField("a", "path"));
    writer.addDocument(config.build(taxoWriter, doc));
    writer.close();
    IOUtils.close(taxoWriter, dir, taxoDir);
  }

  public void testMultiValuedHierarchy() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();
    DirectoryTaxonomyWriter taxoWriter =
        new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);
    FacetsConfig config = new FacetsConfig();
    config.setHierarchical("a", true);
    config.setMultiValued("a", true);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    doc.add(newTextField("field", "text", Field.Store.NO));
    doc.add(new FacetField("a", "path", "x"));
    doc.add(new FacetField("a", "path", "y"));
    writer.addDocument(config.build(taxoWriter, doc));

    // NRT open
    IndexSearcher searcher = newSearcher(writer.getReader());

    // NRT open
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);

    Facets facets =
        getAllFacets(FacetsConfig.DEFAULT_INDEX_FIELD_NAME, searcher, taxoReader, config);

    expectThrows(
        IllegalArgumentException.class,
        () -> {
          facets.getSpecificValue("a");
        });

    FacetResult result = facets.getTopChildren(10, "a");
    assertEquals(1, result.labelValues.length);
    assertEquals(1, result.labelValues[0].value.intValue());

    FacetResult allChildren = facets.getAllChildren("a");
    assertEquals(1, allChildren.labelValues.length);
    assertEquals(1, allChildren.labelValues[0].value.intValue());

    writer.close();
    IOUtils.close(taxoWriter, searcher.getIndexReader(), taxoReader, dir, taxoDir);
  }

  public void testLabelWithDelimiter() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    DirectoryTaxonomyWriter taxoWriter =
        new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);

    FacetsConfig config = new FacetsConfig();
    config.setMultiValued("dim", true);

    Document doc = new Document();
    doc.add(newTextField("field", "text", Field.Store.NO));
    doc.add(new FacetField("dim", "test\u001Fone"));
    doc.add(new FacetField("dim", "test\u001Etwo"));
    writer.addDocument(config.build(taxoWriter, doc));

    // NRT open
    IndexSearcher searcher = newSearcher(writer.getReader());

    // NRT open
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);

    Facets facets =
        getAllFacets(FacetsConfig.DEFAULT_INDEX_FIELD_NAME, searcher, taxoReader, config);

    assertEquals(1, facets.getSpecificValue("dim", "test\u001Fone"));
    assertEquals(1, facets.getSpecificValue("dim", "test\u001Etwo"));

    // no hierarchy
    assertFalse(((TaxonomyFacets) facets).siblingsLoaded());
    assertFalse(((TaxonomyFacets) facets).childrenLoaded());

    FacetResult result = facets.getTopChildren(10, "dim");
    assertEquals(
        "dim=dim path=[] value=-1 childCount=2\n  test\u001Fone (1)\n  test\u001Etwo (1)\n",
        result.toString());

    assertFacetResult(
        facets.getAllChildren("dim"),
        "dim",
        new String[0],
        2,
        -1,
        new LabelAndValue[] {
          new LabelAndValue("test\u001Etwo", 1), new LabelAndValue("test\u001Fone", 1),
        });
    writer.close();
    IOUtils.close(taxoWriter, searcher.getIndexReader(), taxoReader, dir, taxoDir);
  }

  public void testRequireDimCount() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    DirectoryTaxonomyWriter taxoWriter =
        new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);

    FacetsConfig config = new FacetsConfig();
    config.setRequireDimCount("dim", true);

    config.setMultiValued("dim2", true);
    config.setRequireDimCount("dim2", true);

    config.setMultiValued("dim3", true);
    config.setHierarchical("dim3", true);
    config.setRequireDimCount("dim3", true);

    Document doc = new Document();
    doc.add(newTextField("field", "text", Field.Store.NO));
    doc.add(new FacetField("dim", "a"));
    doc.add(new FacetField("dim2", "a"));
    doc.add(new FacetField("dim2", "b"));
    doc.add(new FacetField("dim3", "a", "b"));
    doc.add(new FacetField("dim3", "a", "c"));
    writer.addDocument(config.build(taxoWriter, doc));

    // NRT open
    IndexSearcher searcher = newSearcher(writer.getReader());

    // NRT open
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);
    Facets facets =
        getAllFacets(FacetsConfig.DEFAULT_INDEX_FIELD_NAME, searcher, taxoReader, config);

    assertEquals(1, facets.getTopChildren(10, "dim").value);
    assertEquals(1, facets.getAllChildren("dim").value);
    assertEquals(1, facets.getTopChildren(10, "dim2").value);
    assertEquals(1, facets.getAllChildren("dim2").value);
    assertEquals(1, facets.getTopChildren(10, "dim3").value);
    assertEquals(1, facets.getAllChildren("dim3").value);
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          facets.getSpecificValue("dim");
        });

    assertEquals(1, facets.getSpecificValue("dim2"));
    assertEquals(1, facets.getSpecificValue("dim3"));
    writer.close();
    IOUtils.close(taxoWriter, searcher.getIndexReader(), taxoReader, dir, taxoDir);
  }

  // LUCENE-4583: make sure if we require > 32 KB for one
  // document, we don't hit exc when using Facet42DocValuesFormat
  public void testManyFacetsInOneDocument() throws Exception {
    assumeTrue(
        "default Codec doesn't support huge BinaryDocValues",
        TestUtil.fieldSupportsHugeBinaryDocValues(FacetsConfig.DEFAULT_INDEX_FIELD_NAME));
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
    DirectoryTaxonomyWriter taxoWriter =
        new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);

    FacetsConfig config = new FacetsConfig();
    config.setMultiValued("dim", true);

    int numLabels =
        TEST_NIGHTLY
            ? TestUtil.nextInt(random(), 40000, 100000)
            : TestUtil.nextInt(random(), 4000, 10000);

    Document doc = new Document();
    doc.add(newTextField("field", "text", Field.Store.NO));
    for (int i = 0; i < numLabels; i++) {
      doc.add(new FacetField("dim", "" + i));
    }
    writer.addDocument(config.build(taxoWriter, doc));

    // NRT open
    IndexSearcher searcher = newSearcher(writer.getReader());

    // NRT open
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);

    Facets facets =
        getAllFacets(FacetsConfig.DEFAULT_INDEX_FIELD_NAME, searcher, taxoReader, config);

    FacetResult result = facets.getTopChildren(Integer.MAX_VALUE, "dim");
    assertEquals(numLabels, result.labelValues.length);
    Set<String> allLabels = new HashSet<>();
    for (LabelAndValue labelValue : result.labelValues) {
      allLabels.add(labelValue.label);
      assertEquals(1, labelValue.value.intValue());
    }
    assertEquals(numLabels, allLabels.size());

    FacetResult allChildrenResult = facets.getAllChildren("dim");
    assertEquals(numLabels, result.labelValues.length);
    Set<String> allChildrenLabels = new HashSet<>();
    for (LabelAndValue labelValue : allChildrenResult.labelValues) {
      allChildrenLabels.add(labelValue.label);
      assertEquals(1, labelValue.value.intValue());
    }
    assertEquals(numLabels, allChildrenLabels.size());

    writer.close();
    IOUtils.close(searcher.getIndexReader(), taxoWriter, taxoReader, dir, taxoDir);
  }

  // Make sure we catch when app didn't declare field as
  // hierarchical but it was:
  public void testDetectHierarchicalField() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();
    TaxonomyWriter taxoWriter =
        new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    FacetsConfig config = new FacetsConfig();

    Document doc = new Document();
    doc.add(newTextField("field", "text", Field.Store.NO));
    doc.add(new FacetField("a", "path", "other"));
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          config.build(taxoWriter, doc);
        });

    writer.close();
    IOUtils.close(taxoWriter, dir, taxoDir);
  }

  // Make sure we catch when app didn't declare field as
  // multi-valued but it was:
  public void testDetectMultiValuedField() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();
    TaxonomyWriter taxoWriter =
        new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    FacetsConfig config = new FacetsConfig();

    Document doc = new Document();
    doc.add(newTextField("field", "text", Field.Store.NO));
    doc.add(new FacetField("a", "path"));
    doc.add(new FacetField("a", "path2"));
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          config.build(taxoWriter, doc);
        });

    writer.close();
    IOUtils.close(taxoWriter, dir, taxoDir);
  }

  public void testSeparateIndexedFields() throws Exception {
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();

    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    IndexWriter iw = new IndexWriter(indexDir, newIndexWriterConfig(new MockAnalyzer(random())));
    FacetsConfig config = new FacetsConfig();
    config.setIndexFieldName("b", "$b");

    for (int i = atLeast(30); i > 0; --i) {
      Document doc = new Document();
      doc.add(new StringField("f", "v", Field.Store.NO));
      doc.add(new FacetField("a", "1"));
      doc.add(new FacetField("b", "1"));
      iw.addDocument(config.build(taxoWriter, doc));
    }

    DirectoryReader r = DirectoryReader.open(iw);
    DirectoryTaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);

    FacetsCollector sfc =
        newSearcher(r).search(new MatchAllDocsQuery(), new FacetsCollectorManager());
    Facets facets1 = getTaxonomyFacetCounts(taxoReader, config, sfc);
    Facets facets2 = getTaxonomyFacetCounts(taxoReader, config, sfc, "$b");
    assertEquals(r.maxDoc(), facets1.getTopChildren(10, "a").value.intValue());
    assertEquals(r.maxDoc(), facets1.getAllChildren("a").value.intValue());
    assertEquals(r.maxDoc(), facets2.getTopChildren(10, "b").value.intValue());
    assertEquals(r.maxDoc(), facets2.getAllChildren("b").value.intValue());
    iw.close();
    IOUtils.close(taxoWriter, taxoReader, taxoDir, r, indexDir);
  }

  public void testCountRoot() throws Exception {
    // LUCENE-4882: FacetsAccumulator threw NPE if a FacetRequest was defined on CP.EMPTY
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();

    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    IndexWriter iw = new IndexWriter(indexDir, newIndexWriterConfig(new MockAnalyzer(random())));
    FacetsConfig config = new FacetsConfig();
    for (int i = atLeast(30); i > 0; --i) {
      Document doc = new Document();
      doc.add(new FacetField("a", "1"));
      doc.add(new FacetField("b", "1"));
      iw.addDocument(config.build(taxoWriter, doc));
    }

    DirectoryReader r = DirectoryReader.open(iw);
    DirectoryTaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);

    Facets facets =
        getAllFacets(FacetsConfig.DEFAULT_INDEX_FIELD_NAME, newSearcher(r), taxoReader, config);

    List<FacetResult> allDimsResult = facets.getAllDims(10);
    for (FacetResult result : allDimsResult) {
      assertEquals(r.numDocs(), result.value.intValue());
    }

    // test override implementation of getTopDims
    if (allDimsResult.size() > 0) {
      List<FacetResult> topNDimsResult = facets.getTopDims(1, 10);
      assertEquals(allDimsResult.get(0), topNDimsResult.get(0));
    }

    // test getTopDims(0, 1)
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          facets.getTopDims(0, 1);
        });

    // test getTopDims(1, 0) with topNChildren = 0
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          facets.getTopDims(1, 0);
        });

    // test getAllDims(0)
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          facets.getAllDims(0);
        });

    iw.close();
    IOUtils.close(taxoWriter, taxoReader, taxoDir, r, indexDir);
  }

  public void testGetFacetResultsTwice() throws Exception {
    // LUCENE-4893: counts were multiplied as many times as getFacetResults was called.
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();

    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    IndexWriter iw = new IndexWriter(indexDir, newIndexWriterConfig(new MockAnalyzer(random())));
    FacetsConfig config = new FacetsConfig();

    Document doc = new Document();
    doc.add(new FacetField("a", "1"));
    doc.add(new FacetField("b", "1"));
    iw.addDocument(config.build(taxoWriter, doc));

    DirectoryReader r = DirectoryReader.open(iw);
    DirectoryTaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);

    Facets facets =
        getAllFacets(FacetsConfig.DEFAULT_INDEX_FIELD_NAME, newSearcher(r), taxoReader, config);

    List<FacetResult> res1 = facets.getAllDims(10);
    List<FacetResult> res2 = facets.getAllDims(10);
    assertEquals(
        "calling getFacetResults twice should return the .equals()=true result", res1, res2);

    // test getTopDims(n, 10)
    if (res1.size() > 0) {
      for (int i = 1; i < res1.size(); i++) {
        assertEquals(res1.subList(0, i), facets.getTopDims(i, 10));
      }
    }

    iw.close();
    IOUtils.close(taxoWriter, taxoReader, taxoDir, r, indexDir);
  }

  public void testChildCount() throws Exception {
    // LUCENE-4885: FacetResult.numValidDescendants was not set properly by FacetsAccumulator
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();

    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    IndexWriter iw = new IndexWriter(indexDir, newIndexWriterConfig(new MockAnalyzer(random())));
    FacetsConfig config = new FacetsConfig();
    for (int i = 0; i < 10; i++) {
      Document doc = new Document();
      doc.add(new FacetField("a", Integer.toString(i)));
      iw.addDocument(config.build(taxoWriter, doc));
    }

    DirectoryReader r = DirectoryReader.open(iw);
    DirectoryTaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);

    Facets facets =
        getAllFacets(FacetsConfig.DEFAULT_INDEX_FIELD_NAME, newSearcher(r), taxoReader, config);

    assertEquals(10, facets.getTopChildren(2, "a").childCount);
    assertEquals(10, facets.getAllChildren("a").childCount);

    iw.close();
    IOUtils.close(taxoWriter, taxoReader, taxoDir, r, indexDir);
  }

  public void testMultiValuedFieldCountAll() throws Exception {

    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();

    DirectoryTaxonomyWriter taxoWriter =
        new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);

    FacetsConfig config = new FacetsConfig();
    config.setHierarchical("Publish Date", true);

    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    doc.add(new StringField("id", "0", Field.Store.NO));
    doc.add(new FacetField("Author", "Bob"));
    doc.add(new FacetField("Publish Date", "2010", "10", "15"));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.NO));
    doc.add(new FacetField("Author", "Lisa"));
    doc.add(new FacetField("Publish Date", "2010", "10", "20"));
    writer.addDocument(config.build(taxoWriter, doc));

    IndexSearcher searcher1 = newSearcher(writer.getReader());
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);
    Facets facets =
        new FastTaxonomyFacetCounts(
            FacetsConfig.DEFAULT_INDEX_FIELD_NAME, searcher1.getIndexReader(), taxoReader, config);

    assertEquals(
        "dim=Author path=[] value=2 childCount=2\n  Bob (1)\n  Lisa (1)",
        facets.getTopChildren(10, "Author").toString().trim());

    assertFacetResult(
        facets.getAllChildren("Author"),
        "Author",
        new String[0],
        2,
        2,
        new LabelAndValue[] {
          new LabelAndValue("Bob", 1), new LabelAndValue("Lisa", 1),
        });

    // -- delete to trigger liveDocs != null
    writer.deleteDocuments(new Term("id", "0"));
    IndexSearcher searcher2 = newSearcher(writer.getReader());
    facets =
        new FastTaxonomyFacetCounts(
            FacetsConfig.DEFAULT_INDEX_FIELD_NAME, searcher2.getIndexReader(), taxoReader, config);

    assertEquals(
        "dim=Author path=[] value=1 childCount=1\n  Lisa (1)",
        facets.getTopChildren(10, "Author").toString().trim());

    assertFacetResult(
        facets.getAllChildren("Author"),
        "Author",
        new String[0],
        1,
        1,
        new LabelAndValue[] {
          new LabelAndValue("Lisa", 1),
        });

    IOUtils.close(
        writer,
        taxoWriter,
        searcher1.getIndexReader(),
        searcher2.getIndexReader(),
        taxoReader,
        taxoDir,
        dir);
  }

  public void testCountAllSingleValuedField() throws Exception {

    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();

    DirectoryTaxonomyWriter taxoWriter =
        new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);

    FacetsConfig config = new FacetsConfig();
    config.setHierarchical("Author", true);

    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    doc.add(new StringField("id", "0", Field.Store.NO));
    doc.add(new FacetField("Author", "Bob"));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.NO));
    doc.add(new FacetField("Author", "Lisa"));
    writer.addDocument(config.build(taxoWriter, doc));

    IndexSearcher searcher1 = newSearcher(writer.getReader());
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);
    Facets facets =
        new FastTaxonomyFacetCounts(
            FacetsConfig.DEFAULT_INDEX_FIELD_NAME, searcher1.getIndexReader(), taxoReader, config);

    assertEquals(
        "dim=Author path=[] value=2 childCount=2\n  Bob (1)\n  Lisa (1)",
        facets.getTopChildren(10, "Author").toString().trim());

    assertFacetResult(
        facets.getAllChildren("Author"),
        "Author",
        new String[0],
        2,
        2,
        new LabelAndValue[] {
          new LabelAndValue("Bob", 1), new LabelAndValue("Lisa", 1),
        });

    // -- delete to trigger liveDocs != null
    writer.deleteDocuments(new Term("id", "0"));
    IndexSearcher searcher2 = newSearcher(writer.getReader());
    facets =
        new FastTaxonomyFacetCounts(
            FacetsConfig.DEFAULT_INDEX_FIELD_NAME, searcher2.getIndexReader(), taxoReader, config);

    assertEquals(
        "dim=Author path=[] value=1 childCount=1\n  Lisa (1)",
        facets.getTopChildren(10, "Author").toString().trim());

    assertFacetResult(
        facets.getAllChildren("Author"),
        "Author",
        new String[0],
        1,
        1,
        new LabelAndValue[] {
          new LabelAndValue("Lisa", 1),
        });

    IOUtils.close(
        writer,
        taxoWriter,
        searcher1.getIndexReader(),
        searcher2.getIndexReader(),
        taxoReader,
        taxoDir,
        dir);
  }

  private void indexTwoDocs(
      TaxonomyWriter taxoWriter, IndexWriter indexWriter, FacetsConfig config, boolean withContent)
      throws Exception {
    for (int i = 0; i < 2; i++) {
      Document doc = new Document();
      if (withContent) {
        doc.add(new StringField("f", "a", Field.Store.NO));
      }
      if (config != null) {
        doc.add(new FacetField("A", Integer.toString(i)));
        indexWriter.addDocument(config.build(taxoWriter, doc));
      } else {
        indexWriter.addDocument(doc);
      }
    }

    indexWriter.commit();
  }

  public void testSegmentsWithoutCategoriesOrResults() throws Exception {
    // tests the accumulator when there are segments with no results
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();

    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setMergePolicy(NoMergePolicy.INSTANCE); // prevent merges
    IndexWriter indexWriter = new IndexWriter(indexDir, iwc);

    TaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    FacetsConfig config = new FacetsConfig();
    indexTwoDocs(
        taxoWriter, indexWriter, config, false); // 1st segment, no content, with categories
    indexTwoDocs(taxoWriter, indexWriter, null, true); // 2nd segment, with content, no categories
    indexTwoDocs(taxoWriter, indexWriter, config, true); // 3rd segment ok
    indexTwoDocs(taxoWriter, indexWriter, null, false); // 4th segment, no content, or categories
    indexTwoDocs(taxoWriter, indexWriter, null, true); // 5th segment, with content, no categories
    indexTwoDocs(
        taxoWriter, indexWriter, config, true); // 6th segment, with content, with categories
    indexTwoDocs(taxoWriter, indexWriter, null, true); // 7th segment, with content, no categories
    indexWriter.close();
    IOUtils.close(taxoWriter);

    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);
    IndexSearcher indexSearcher = newSearcher(indexReader);

    // search for "f:a", only segments 1 and 3 should match results
    Query q = new TermQuery(new Term("f", "a"));
    FacetsCollector sfc = indexSearcher.search(q, new FacetsCollectorManager());
    Facets facets = getTaxonomyFacetCounts(taxoReader, config, sfc);
    FacetResult result = facets.getTopChildren(10, "A");
    assertEquals("wrong number of children", 2, result.labelValues.length);
    for (LabelAndValue labelValue : result.labelValues) {
      assertEquals("wrong weight for child " + labelValue.label, 2, labelValue.value.intValue());
    }
    FacetResult allChildrenResult = facets.getAllChildren("A");
    assertEquals("wrong number of children", 2, allChildrenResult.labelValues.length);
    for (LabelAndValue labelValue : allChildrenResult.labelValues) {
      assertEquals("wrong weight for child " + labelValue.label, 2, labelValue.value.intValue());
    }

    IOUtils.close(indexReader, taxoReader, indexDir, taxoDir);
  }

  public void testRandom() throws Exception {
    String[] tokens = getRandomTokens(10);
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();

    RandomIndexWriter w = new RandomIndexWriter(random(), indexDir);
    DirectoryTaxonomyWriter tw = new DirectoryTaxonomyWriter(taxoDir);
    FacetsConfig config = new FacetsConfig();
    int numDocs = atLeast(1000);
    int numDims = TestUtil.nextInt(random(), 1, 7);
    List<TestDoc> testDocs = getRandomDocs(tokens, numDocs, numDims);
    for (TestDoc testDoc : testDocs) {
      Document doc = new Document();
      doc.add(newStringField("content", testDoc.content, Field.Store.NO));
      for (int j = 0; j < numDims; j++) {
        if (testDoc.dims[j] != null) {
          doc.add(new FacetField("dim" + j, testDoc.dims[j]));
        }
      }
      w.addDocument(config.build(tw, doc));
    }

    // NRT open
    IndexSearcher searcher = newSearcher(w.getReader());

    // NRT open
    TaxonomyReader tr = new DirectoryTaxonomyReader(tw);

    int iters = atLeast(100);
    for (int iter = 0; iter < iters; iter++) {
      String searchToken = tokens[random().nextInt(tokens.length)];
      if (VERBOSE) {
        System.out.println("\nTEST: iter content=" + searchToken);
      }
      FacetsCollector fc = new FacetsCollector();
      FacetsCollector.search(searcher, new TermQuery(new Term("content", searchToken)), 10, fc);
      Facets facets = getTaxonomyFacetCounts(tr, config, fc);

      // Slow, yet hopefully bug-free, faceting:
      @SuppressWarnings({"rawtypes", "unchecked"})
      Map<String, Integer>[] expectedCounts = new HashMap[numDims];
      List<List<FacetLabel>> expectedLabels = new ArrayList<>();

      for (int i = 0; i < numDims; i++) {
        expectedCounts[i] = new HashMap<>();
      }

      for (TestDoc doc : testDocs) {
        if (doc.content.equals(searchToken)) {
          List<FacetLabel> facetLabels = new ArrayList<>();
          for (int j = 0; j < numDims; j++) {
            if (doc.dims[j] != null) {
              Integer v = expectedCounts[j].get(doc.dims[j]);
              if (v == null) {
                expectedCounts[j].put(doc.dims[j], 1);
              } else {
                expectedCounts[j].put(doc.dims[j], v.intValue() + 1);
              }
              // Add document facet labels
              facetLabels.add(new FacetLabel("dim" + j, doc.dims[j]));
            }
          }
          expectedLabels.add(facetLabels);
        }
      }

      List<FacetResult> expected = new ArrayList<>();
      for (int i = 0; i < numDims; i++) {
        List<LabelAndValue> labelValues = new ArrayList<>();
        int totCount = 0;
        for (Map.Entry<String, Integer> ent : expectedCounts[i].entrySet()) {
          labelValues.add(new LabelAndValue(ent.getKey(), ent.getValue()));
          totCount += ent.getValue();
        }
        sortLabelValues(labelValues);
        if (totCount > 0) {
          expected.add(
              new FacetResult(
                  "dim" + i,
                  new String[0],
                  totCount,
                  labelValues.toArray(new LabelAndValue[labelValues.size()]),
                  labelValues.size()));
        }
      }

      // Sort by highest value, tie break by value:
      sortFacetResults(expected);

      List<FacetResult> actual = facets.getAllDims(10);
      // Messy: fixup ties
      sortTies(actual);

      assertEquals(expected, actual);

      // test getTopDims
      if (actual.size() > 0) {
        List<FacetResult> topNDimsResult = facets.getTopDims(actual.size(), 10);
        sortTies(topNDimsResult);
        assertEquals(actual, topNDimsResult);
      }

      // Test facet labels for each matching test doc
      List<List<FacetLabel>> actualLabels = getAllTaxonomyFacetLabels(null, tr, fc);
      assertEquals(expectedLabels.size(), actualLabels.size());
      assertTrue(sortedFacetLabels(expectedLabels).equals(sortedFacetLabels(actualLabels)));

      // Test facet labels for each matching test doc, given a specific dimension chosen randomly
      final String dimension = "dim" + random().nextInt(numDims);
      expectedLabels.forEach(
          list -> list.removeIf(f -> f.components[0].equals(dimension) == false));

      actualLabels = getAllTaxonomyFacetLabels(dimension, tr, fc);
      assertTrue(sortedFacetLabels(expectedLabels).equals(sortedFacetLabels(actualLabels)));
    }

    w.close();
    IOUtils.close(tw, searcher.getIndexReader(), tr, indexDir, taxoDir);
  }

  private static List<List<FacetLabel>> sortedFacetLabels(List<List<FacetLabel>> allFacetLabels) {
    // Sort each inner list since there is no guaranteed order in which
    // FacetLabels are expected to be retrieved for each document.
    for (List<FacetLabel> facetLabels : allFacetLabels) {
      Collections.sort(facetLabels);
    }

    Collections.sort(
        allFacetLabels,
        (o1, o2) -> {
          int diff = o1.size() - o2.size();
          if (diff != 0) {
            return diff;
          }

          // the lists are equal in size and sorted
          for (int i = 0; i < o1.size(); i++) {
            int comp = o1.get(i).compareTo(o2.get(i));
            if (comp != 0) {
              return comp;
            }
          }
          // all elements are equal
          return 0;
        });

    return allFacetLabels;
  }

  private static Facets getAllFacets(
      String indexFieldName, IndexSearcher searcher, TaxonomyReader taxoReader, FacetsConfig config)
      throws IOException {
    if (random().nextBoolean()) {
      // MatchAllDocsQuery is for "browsing" (counts facets
      // for all non-deleted docs in the index); normally
      // you'd use a "normal" query, and use MultiCollector to
      // wrap collecting the "normal" hits and also facets:
      FacetsCollector c = searcher.search(new MatchAllDocsQuery(), new FacetsCollectorManager());

      return new FastTaxonomyFacetCounts(taxoReader, config, c);
    } else {
      return new FastTaxonomyFacetCounts(
          indexFieldName, searcher.getIndexReader(), taxoReader, config);
    }
  }
}
