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
package org.apache.lucene.facet.taxonomy.directory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.DocValuesOrdinalsReader;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts;
import org.apache.lucene.facet.taxonomy.TaxonomyFacetCounts;
import org.apache.lucene.facet.taxonomy.TaxonomyFacetLabels;
import org.apache.lucene.facet.taxonomy.TaxonomyFacetSumValueSource;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.IOUtils;
import org.junit.Ignore;

/*
  Verify we can read previous versions' taxonomy indexes, do searches
  against them, and add documents to them.
*/
// See: https://issues.apache.org/jira/browse/SOLR-12028 Tests cannot remove files on Windows
// machines occasionally
public class TestBackwardsCompatibility extends LuceneTestCase {

  // To generate backcompat indexes with the current default codec, run the following gradle
  // command:
  //  gradlew test -Dtestcase=TestBackwardsCompatibility -Dtests.bwcdir=/path/to/store/indexes
  //           -Dtests.codec=default -Dtests.useSecurityManager=false
  // Also add testmethod with one of the index creation methods below, for example:
  //    -Dtestmethod=testCreateOldTaxonomy
  //
  // Zip up the generated indexes:
  //
  //    cd /path/to/store/indexes/index.cfs   ; zip index.<VERSION>-cfs.zip *
  //
  // Then move the zip file to your trunk checkout and use it in your test cases

  private static final String OLD_TAXONOMY_INDEX_NAME = "taxonomy.8.11.0-cfs";
  private static final String OLD_INDEX_NAME = "index.8.11.0-cfs";

  public void testCreateNewTaxonomy() throws IOException {
    createNewTaxonomyIndex(OLD_TAXONOMY_INDEX_NAME, OLD_INDEX_NAME);
  }

  /**
   * This test exercises a bunch of different faceting operations and directly taxonomy index
   * reading to make sure more modern faceting formats introduced in 9.0 are backwards-compatible
   * with 8.x indexes. It requires an "older" 8.x index to be in place with assumed docs/categories
   * already present. It makes sure it can still run a number of different "read" operations against
   * the old index, then it writes new content, forces a merge and does a bunch more "read"
   * operations. It may seem a bit chaotic, but it's trying to test a number of different
   * faceting-related implementations that require specific back-compat support.
   */
  private void createNewTaxonomyIndex(String taxoDirName, String indexDirName) throws IOException {
    Path taxoPath = createTempDir(taxoDirName);
    Path indexPath = createTempDir(indexDirName);

    TestUtil.unzip(getDataInputStream(taxoDirName + ".zip"), taxoPath);
    TestUtil.unzip(getDataInputStream(indexDirName + ".zip"), indexPath);

    Directory taxoDir = newFSDirectory(taxoPath);
    Directory indexDir = newFSDirectory(indexPath);

    // Open the existing indexes (explicitly open in APPEND mode and fail if they don't exist since
    // we're trying to test
    // back-compat with existing indexes):
    DirectoryTaxonomyWriter taxoWriter =
        new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.APPEND);
    IndexWriterConfig indexWriterConfig = newIndexWriterConfig();
    indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
    RandomIndexWriter indexWriter =
        new RandomIndexWriter(random(), indexDir, indexWriterConfig, random().nextBoolean());

    // Use a default FacetsConfig. This assumes that we didn't need to register anything interesting
    // when creating
    // the older format index. If that changes, we need a way to ensure we re-use the same facet
    // configuration used
    // in created the old format taxonomy index:
    FacetsConfig facetsConfig = new FacetsConfig();

    // At this point we should have a taxonomy index and "regular" index containing some taxonomy
    // categories and
    // documents with facet ordinals indexed. Confirm that we can facet and search against it as-is
    // before adding
    // anything new. Of course these tests are highly dependent on the index we're starting with, so
    // they will
    // need to be updated accordingly if the "old" test index changes:
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);
    IndexSearcher searcher = newSearcher(indexWriter.getReader());
    FacetsCollector facetsCollector = new FacetsCollector();
    searcher.search(new MatchAllDocsQuery(), facetsCollector);
    // Test a few different facet implementations that we know have back-compat implications:
    Facets facets = new FastTaxonomyFacetCounts(taxoReader, facetsConfig, facetsCollector);
    FacetResult facetResult = facets.getTopChildren(10, "f1");
    assertEquals(2, facetResult.value);
    facets =
        new TaxonomyFacetCounts(
            new DocValuesOrdinalsReader(), taxoReader, facetsConfig, facetsCollector);
    facetResult = facets.getTopChildren(10, "f1");
    assertEquals(2, facetResult.value);
    facets =
        new TaxonomyFacetSumValueSource(
            taxoReader, facetsConfig, facetsCollector, DoubleValuesSource.constant(1d));
    facetResult = facets.getTopChildren(10, "f1");
    assertEquals(2.0f, facetResult.value);
    // Test that we can drill-down as expected (and read facet labels from matching docs):
    TaxonomyFacetLabels facetLabels =
        new TaxonomyFacetLabels(taxoReader, FacetsConfig.DEFAULT_INDEX_FIELD_NAME);
    assert (searcher.getIndexReader().leaves().size() == 1);
    TaxonomyFacetLabels.FacetLabelReader labelReader =
        facetLabels.getFacetLabelReader(searcher.getIndexReader().leaves().get(0));
    DrillDownQuery query = new DrillDownQuery(facetsConfig, new MatchAllDocsQuery());
    query.add("f1", "foo");
    TopDocs docResults = searcher.search(query, 10);
    assertEquals(1, docResults.totalHits.value);
    int docId = docResults.scoreDocs[0].doc;
    Set<FacetLabel> labels = new HashSet<>();
    for (FacetLabel label = labelReader.nextFacetLabel(docId);
        label != null;
        label = labelReader.nextFacetLabel(docId)) {
      labels.add(label);
    }
    assertEquals(2, labels.size());
    assertTrue(
        labels.containsAll(List.of(new FacetLabel("f1", "foo"), new FacetLabel("f2", "foo"))));
    assertEquals(0, docResults.scoreDocs[0].doc);
    // And make sure we can read directly from the taxonomy like we'd expect:
    int ord = taxoReader.getOrdinal(new FacetLabel("f1", "foo"));
    assertNotEquals(TaxonomyReader.INVALID_ORDINAL, ord);
    assertNotNull(taxoReader.getPath(ord));

    // Now we'll add some new docs and taxonomy categories, force merge (to make sure that goes
    // well) and then do
    // some more searches, etc.:
    Document doc = new Document();
    doc.add(new FacetField("f1", "zed"));
    indexWriter.addDocument(facetsConfig.build(taxoWriter, doc));

    FacetLabel cp_c = new FacetLabel("c");
    taxoWriter.addCategory(cp_c);

    indexWriter.forceMerge(1);
    taxoWriter.getInternalIndexWriter().forceMerge(1);
    indexWriter.commit();
    taxoWriter.commit();

    IOUtils.close(taxoReader, searcher.getIndexReader());
    taxoReader = new DirectoryTaxonomyReader(taxoWriter);
    searcher = newSearcher(indexWriter.getReader());
    IOUtils.close(indexWriter, taxoWriter);

    // Re-test a number of different use-cases, which should now "see" the newly added content:
    facetsCollector = new FacetsCollector();
    searcher.search(new MatchAllDocsQuery(), facetsCollector);
    facets = new FastTaxonomyFacetCounts(taxoReader, facetsConfig, facetsCollector);
    facetResult = facets.getTopChildren(10, "f1");
    assertEquals(3, facetResult.value);
    facets =
        new TaxonomyFacetCounts(
            new DocValuesOrdinalsReader(), taxoReader, facetsConfig, facetsCollector);
    facetResult = facets.getTopChildren(10, "f1");
    assertEquals(3, facetResult.value);
    facets =
        new TaxonomyFacetSumValueSource(
            taxoReader, facetsConfig, facetsCollector, DoubleValuesSource.constant(1d));
    facetResult = facets.getTopChildren(10, "f1");
    assertEquals(3.0f, facetResult.value);
    // Test that we can drill-down as expected, and access facet labels:
    facetLabels = new TaxonomyFacetLabels(taxoReader, FacetsConfig.DEFAULT_INDEX_FIELD_NAME);
    assert (searcher.getIndexReader().leaves().size() == 1);
    labelReader = facetLabels.getFacetLabelReader(searcher.getIndexReader().leaves().get(0));
    query = new DrillDownQuery(facetsConfig, new MatchAllDocsQuery());
    query.add("f1", "foo");
    docResults = searcher.search(query, 10);
    assertEquals(1, docResults.totalHits.value);
    docId = docResults.scoreDocs[0].doc;
    labels = new HashSet<>();
    for (FacetLabel label = labelReader.nextFacetLabel(docId);
        label != null;
        label = labelReader.nextFacetLabel(docId)) {
      labels.add(label);
    }
    assertEquals(2, labels.size());
    assertTrue(
        labels.containsAll(List.of(new FacetLabel("f1", "foo"), new FacetLabel("f2", "foo"))));
    labelReader = facetLabels.getFacetLabelReader(searcher.getIndexReader().leaves().get(0));
    query = new DrillDownQuery(facetsConfig, new MatchAllDocsQuery());
    query.add("f1", "zed");
    docResults = searcher.search(query, 10);
    assertEquals(1, docResults.totalHits.value);
    docId = docResults.scoreDocs[0].doc;
    labels = new HashSet<>();
    for (FacetLabel label = labelReader.nextFacetLabel(docId);
        label != null;
        label = labelReader.nextFacetLabel(docId)) {
      labels.add(label);
    }
    assertEquals(1, labels.size());
    assertTrue(labels.contains(new FacetLabel("f1", "zed")));
    // And make sure we can read directly from the taxonomy like we'd expect:
    ord = taxoReader.getOrdinal(new FacetLabel("f1", "foo"));
    assertNotEquals(TaxonomyReader.INVALID_ORDINAL, ord);
    assertNotNull(taxoReader.getPath(ord));
    ord = taxoReader.getOrdinal(new FacetLabel("f1", "zed"));
    assertNotEquals(TaxonomyReader.INVALID_ORDINAL, ord);
    assertNotNull(taxoReader.getPath(ord));
    // And check a few more direct reads from the taxonomy:
    ord = taxoReader.getOrdinal(new FacetLabel("a"));
    assertNotEquals(TaxonomyReader.INVALID_ORDINAL, ord);
    assertNotNull(taxoReader.getPath(ord));

    ord = taxoReader.getOrdinal(new FacetLabel("b"));
    assertNotEquals(TaxonomyReader.INVALID_ORDINAL, ord);
    // Just asserting ord2 != TaxonomyReader.INVALID_ORDINAL is not enough to check compatibility
    assertNotNull(taxoReader.getPath(ord));

    ord = taxoReader.getOrdinal(cp_c);
    assertNotEquals(TaxonomyReader.INVALID_ORDINAL, ord);
    assertNotNull(taxoReader.getPath(ord));

    IOUtils.close(taxoReader, searcher.getIndexReader(), taxoDir, indexDir);
  }

  // Opens up a pre-existing index and tries to run getBulkPath on it
  public void testGetBulkPathOnOlderCodec() throws Exception {
    Path indexDir = createTempDir(OLD_TAXONOMY_INDEX_NAME);
    TestUtil.unzip(getDataInputStream(OLD_TAXONOMY_INDEX_NAME + ".zip"), indexDir);
    Directory dir = newFSDirectory(indexDir);

    DirectoryTaxonomyWriter writer = new DirectoryTaxonomyWriter(dir);
    DirectoryTaxonomyReader reader = new DirectoryTaxonomyReader(writer);

    FacetLabel[] facetLabels = {
      new FacetLabel("a"), new FacetLabel("b"),
    };
    int[] ordinals =
        new int[] {reader.getOrdinal(facetLabels[0]), reader.getOrdinal(facetLabels[1])};
    // The zip file already contains a category "a" and a category "b" stored with the older
    // StoredFields codec
    assertArrayEquals(facetLabels, reader.getBulkPath(ordinals));
    reader.close();
    writer.close();
    dir.close();
  }

  // Used to create a fresh taxonomy index with StoredFields
  @Ignore
  public void testCreateOldTaxonomy() throws IOException {
    createOldTaxonomyIndex(OLD_TAXONOMY_INDEX_NAME, OLD_INDEX_NAME);
  }

  private void createOldTaxonomyIndex(String taxoDirName, String indexDirName) throws IOException {
    Path taxoPath = getIndexDir().resolve(taxoDirName);
    Path indexPath = getIndexDir().resolve(indexDirName);
    Files.deleteIfExists(taxoPath);
    Files.deleteIfExists(indexPath);

    Directory taxoDir = newFSDirectory(taxoPath);
    Directory indexDir = newFSDirectory(indexPath);

    TaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    FacetsConfig facetsConfig = new FacetsConfig();
    RandomIndexWriter indexWriter = new RandomIndexWriter(random(), indexDir);

    Document doc = new Document();
    doc.add(new FacetField("f1", "foo"));
    doc.add(new FacetField("f2", "foo"));
    indexWriter.addDocument(facetsConfig.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("f1", "bar"));
    indexWriter.addDocument(facetsConfig.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("f2", "bar"));
    indexWriter.addDocument(facetsConfig.build(taxoWriter, doc));

    taxoWriter.addCategory(new FacetLabel("a"));
    taxoWriter.addCategory(new FacetLabel("b"));

    indexWriter.commit();
    taxoWriter.commit();
    IOUtils.close(indexWriter, taxoWriter, indexDir, taxoDir);
  }

  private Path getIndexDir() {
    String path = System.getProperty("tests.bwcdir");
    assumeTrue(
        "backcompat creation tests must be run with -Dtests.bwcdir=/path/to/write/indexes",
        path != null);
    return Paths.get(path);
  }
}
