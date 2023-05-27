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
import org.apache.lucene.facet.taxonomy.directory.Consts;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
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
  IndexReader taxoIndexReader;

  private static final Map<String, Long> labelToScore;

  static {
    labelToScore = new HashMap<>();
    labelToScore.put("Bob", 42L);
    labelToScore.put("Lisa", 35L);
  }

  static class OrdinalDataAppender implements BiConsumer<FacetLabel, Document> {
    @Override
    public void accept(FacetLabel facetLabel, Document doc) {
      if (facetLabel.length == 0) {
        return;
      }
      Long score = labelToScore.get(facetLabel.components[facetLabel.length - 1]);
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
    TaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir, new OrdinalDataAppender());

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

    IOUtils.close(taxoWriter, indexWriter, indexDir);

    taxoIndexReader = DirectoryReader.open(taxoDir);
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();

    IOUtils.close(taxoIndexReader, taxoDir);
  }

  public void testDocValue() throws IOException {
    // Each unique label will have been assigned a doc.
    // Additionally, we have the root node of the taxonomy.
    assertEquals(9, taxoIndexReader.maxDoc());
    for (LeafReaderContext ctx : taxoIndexReader.leaves()) {
      LeafReader leafReader = ctx.reader();
      BinaryDocValues fullPaths = leafReader.getBinaryDocValues(Consts.FULL);
      NumericDocValues scores = leafReader.getNumericDocValues("score");
      if (scores == null) {
        continue;
      }
      for (int ord = 0; ord < leafReader.maxDoc(); ord++) {
        if (scores.advanceExact(ord) == false) {
          continue;
        }
        if (fullPaths.advanceExact(ord) == false) {
          throw new IOException("All taxonomy docs should have a full path");
        }

        String[] pathComponents =
            fullPaths.binaryValue().utf8ToString().split(String.valueOf(FacetsConfig.DELIM_CHAR));
        Long score = labelToScore.get(pathComponents[pathComponents.length - 1]);
        if (score == null) {
          throw new IOException("Unexpected score for " + Arrays.toString(pathComponents));
        }
        assertEquals((long) score, scores.longValue());
      }
    }
  }

  public void testSearchableField() throws IOException {
    IndexSearcher taxoSearcher = newSearcher(taxoIndexReader);
    Query q;
    TopDocs td;

    q = new TermQuery(new Term("hasScore?", "yes"));
    td = taxoSearcher.search(q, 100);
    // "Bob" and "Lisa" have scores
    assertEquals(2, td.totalHits.value);

    q = new TermQuery(new Term("hasScore?", "no"));
    td = taxoSearcher.search(q, 100);
    // All the publishing date components and the dimensions do not have scores
    assertEquals(6, td.totalHits.value);
  }
}
