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
package org.apache.lucene.demo.facet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.StringDocValuesReaderState;
import org.apache.lucene.facet.StringValueFacetCounts;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

/**
 * Demonstrate the use of {@link StringValueFacetCounts} over {@link SortedDocValuesField} and
 * {@link KeywordField}.
 */
public class StringValueFacetCountsExample {

  private final Directory indexDir = new ByteBuffersDirectory();
  private final FacetsConfig config = new FacetsConfig();

  /** Empty constructor */
  public StringValueFacetCountsExample() {}

  /** Build the example index. */
  private void index() throws IOException {
    IndexWriter indexWriter =
        new IndexWriter(
            indexDir, new IndexWriterConfig(new WhitespaceAnalyzer()).setOpenMode(OpenMode.CREATE));

    Document doc = new Document();
    doc.add(new KeywordField("Author", "Bob", Field.Store.NO));
    doc.add(new SortedDocValuesField("Publish Year", new BytesRef("2010")));
    indexWriter.addDocument(config.build(doc));

    doc = new Document();
    doc.add(new KeywordField("Author", "Lisa", Field.Store.NO));
    doc.add(new SortedDocValuesField("Publish Year", new BytesRef("2010")));
    indexWriter.addDocument(config.build(doc));

    doc = new Document();
    doc.add(new KeywordField("Author", "Lisa", Field.Store.NO));
    doc.add(new SortedDocValuesField("Publish Year", new BytesRef("2012")));
    indexWriter.addDocument(config.build(doc));

    doc = new Document();
    doc.add(new KeywordField("Author", "Susan", Field.Store.NO));
    doc.add(new SortedDocValuesField("Publish Year", new BytesRef("2012")));
    indexWriter.addDocument(config.build(doc));

    doc = new Document();
    doc.add(new KeywordField("Author", "Frank", Field.Store.NO));
    doc.add(new SortedDocValuesField("Publish Year", new BytesRef("1999")));
    indexWriter.addDocument(config.build(doc));

    indexWriter.close();
  }

  /** User runs a query and counts facets. */
  private List<FacetResult> search() throws IOException {
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    IndexSearcher searcher = new IndexSearcher(indexReader);

    StringDocValuesReaderState authorState = new StringDocValuesReaderState(indexReader, "Author");
    StringDocValuesReaderState publishState =
        new StringDocValuesReaderState(indexReader, "Publish Year");

    // Aggregatses the facet counts
    FacetsCollector fc = new FacetsCollector();

    // MatchAllDocsQuery is for "browsing" (counts facets
    // for all non-deleted docs in the index); normally
    // you'd use a "normal" query:
    FacetsCollector.search(searcher, new MatchAllDocsQuery(), 10, fc);

    // Retrieve results
    Facets authorFacets = new StringValueFacetCounts(authorState, fc);
    Facets publishFacets = new StringValueFacetCounts(publishState, fc);

    List<FacetResult> results = new ArrayList<>();
    results.add(authorFacets.getTopChildren(10, "Author"));
    results.add(publishFacets.getTopChildren(10, "Publish Year"));
    indexReader.close();

    return results;
  }

  /** Runs the search example. */
  public List<FacetResult> runSearch() throws IOException {
    index();
    return search();
  }

  /** Runs the search and drill-down examples and prints the results. */
  public static void main(String[] args) throws Exception {
    System.out.println("Facet counting example:");
    System.out.println("-----------------------");
    StringValueFacetCountsExample example = new StringValueFacetCountsExample();
    List<FacetResult> results = example.runSearch();
    System.out.println("Author: " + results.get(0));
    System.out.println("Publish Year: " + results.get(1));
  }
}
