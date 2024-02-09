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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.ReindexingEnrichedDirectoryTaxonomyWriter;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.junit.After;
import org.junit.Before;

public class TestOrdinalData extends FacetTestCase {
  Directory taxoDir;
  DirectoryTaxonomyReader taxoReader;
  IndexReader taxoIndexReader;
  ReindexingEnrichedDirectoryTaxonomyWriter taxoWriter;

  private static final Map<String, Long> labelToScore =
      Map.of(
          "Bob", 42L,
          "Lisa", 35L);

  private static class OrdinalDataAppender implements BiConsumer<FacetLabel, Document> {
    private final Map<String, Long> scores;

    private OrdinalDataAppender(Map<String, Long> scores) {
      this.scores = scores;
    }

    @Override
    public void accept(FacetLabel facetLabel, Document doc) {
      if (facetLabel.length == 0) {
        return;
      }
      Long score = scores.get(facetLabel.components[facetLabel.length - 1]);
      if (score != null) {
        doc.add(new NumericDocValuesField("score", score));
        doc.add(new StringField("hasScore?", "yes", Field.Store.NO));
      } else {
        doc.add(new StringField("hasScore?", "no", Field.Store.NO));
      }
    }
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();

    Directory indexDir = newDirectory();
    taxoDir = newDirectory();

    IndexWriter indexWriter = new IndexWriter(indexDir, new IndexWriterConfig());
    taxoWriter =
        new ReindexingEnrichedDirectoryTaxonomyWriter(
            taxoDir, new OrdinalDataAppender(labelToScore));

    FacetsConfig facetsConfig = new FacetsConfig();
    facetsConfig.setHierarchical("Author", true);
    facetsConfig.setMultiValued("Author", true);
    facetsConfig.setHierarchical("Publish Date", true);
    facetsConfig.setMultiValued("Publish Date", true);

    Document doc;

    doc = new Document();
    doc.add(new FacetField("Author", "Bob"));
    doc.add(new FacetField("Publish Date", "2010", "10", "15"));
    indexWriter.addDocument(facetsConfig.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Lisa"));
    doc.add(new FacetField("Publish Date", "2010", "10", "20"));
    indexWriter.addDocument(facetsConfig.build(taxoWriter, doc));

    IOUtils.close(indexWriter, indexDir);
    taxoWriter.commit();

    taxoReader = new DirectoryTaxonomyReader(taxoDir);
    taxoIndexReader = taxoReader.getInternalIndexReader();
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();

    IOUtils.close(taxoWriter);
    IOUtils.close(taxoReader);
    IOUtils.close(taxoDir);
  }

  public void testDocValue() throws IOException {
    // Each unique label will have been assigned a doc.
    // Additionally, we have the root node of the taxonomy.
    assertEquals(9, taxoIndexReader.maxDoc());
    for (LeafReaderContext ctx : taxoIndexReader.leaves()) {
      LeafReader leafReader = ctx.reader();
      NumericDocValues scores = leafReader.getNumericDocValues("score");
      if (scores == null) {
        continue;
      }
      for (int ord = 0; ord < leafReader.maxDoc(); ord++) {
        if (scores.advanceExact(ord) == false) {
          continue;
        }
        FacetLabel label = taxoReader.getPath(ctx.docBase + ord);
        Long score = labelToScore.get(label.components[label.length - 1]);
        if (score == null) {
          throw new IOException("Unexpected score for " + Arrays.toString(label.components));
        }
        assertEquals((long) score, scores.longValue());
      }
    }
  }

  private void validateSearchResults(IndexSearcher searcher, Map<Query, Integer> queriesAndCounts)
      throws IOException {
    for (Map.Entry<Query, Integer> queryAndCount : queriesAndCounts.entrySet()) {
      Query q = queryAndCount.getKey();
      int count = queryAndCount.getValue();
      TopDocs td = searcher.search(q, Integer.MAX_VALUE);
      assertEquals(count, td.totalHits.value);
    }
  }

  public void testSearchableField() throws IOException {
    IndexSearcher taxoSearcher = newSearcher(taxoIndexReader);
    validateSearchResults(
        taxoSearcher,
        Map.of(
            new TermQuery(new Term("hasScore?", "yes")), 2,
            new TermQuery(new Term("hasScore?", "no")), 6));
  }

  public void testReindex() throws IOException {
    taxoWriter.reindexWithNewOrdinalData(new OrdinalDataAppender(new HashMap<>()));
    taxoReader.close();
    taxoReader = new DirectoryTaxonomyReader(taxoDir);
    taxoIndexReader = taxoReader.getInternalIndexReader();

    IndexSearcher taxoSearcher = newSearcher(taxoIndexReader);
    validateSearchResults(
        taxoSearcher,
        Map.of(
            new TermQuery(new Term("hasScore?", "yes")), 0,
            new TermQuery(new Term("hasScore?", "no")), 8));
  }
}
