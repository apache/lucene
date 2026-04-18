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
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollectorManager;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.range.DynamicRangeUtil;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LongValuesSource;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.NamedThreadFactory;

/**
 * Demo dynamic range faceting.
 *
 * <p>The results look like so: min: 63 max: 75 centroid: 69.000000 count: 2 weight: 137 min: 79
 * max: 96 centroid: 86.000000 count: 3 weight: 83
 *
 * <p>We've computed dynamic ranges over popularity weighted by number of books. We can read the
 * results as so: There are 137 books written by authors in the 63 to 75 popularity range.
 *
 * <p>How it works: We collect all the values (popularity) and their weights (book counts). We sort
 * the values and find the approximate weight per range. In this case the total weight is 220 (total
 * books by all authors) and we want 2 ranges, so we're aiming for 110 books in each range. We add
 * Chesterton to the first range, since he is the least popular author. He's written a lot of books,
 * the range's weight is 90. We add Tolstoy to the first range, since he is next in line of
 * popularity. He's written another 47 books, which brings the total weight to 137. We're over the
 * 110 target weight, so we stop and add everyone left to the second range.
 */
public class DynamicRangeFacetsExample {

  private final Directory indexDir = new ByteBuffersDirectory();
  private final FacetsConfig config = new FacetsConfig();

  /** Empty constructor */
  public DynamicRangeFacetsExample() {}

  /** Build the example index. */
  private void index() throws IOException {
    IndexWriter indexWriter =
        new IndexWriter(
            indexDir,
            new IndexWriterConfig(new WhitespaceAnalyzer())
                .setOpenMode(IndexWriterConfig.OpenMode.CREATE));

    Document doc = new Document();
    doc.add(new StringField("Author", "J. R. R. Tolkien", Field.Store.NO));
    doc.add(new NumericDocValuesField("Popularity", 96));
    doc.add(new NumericDocValuesField("Books", 24));
    indexWriter.addDocument(config.build(doc));

    doc = new Document();
    doc.add(new StringField("Author", "C. S. Lewis", Field.Store.NO));
    doc.add(new NumericDocValuesField("Popularity", 83));
    doc.add(new NumericDocValuesField("Books", 48));
    indexWriter.addDocument(config.build(doc));

    doc = new Document();
    doc.add(new StringField("Author", "G. K. Chesterton", Field.Store.NO));
    doc.add(new NumericDocValuesField("Popularity", 63));
    doc.add(new NumericDocValuesField("Books", 90));
    indexWriter.addDocument(config.build(doc));
    indexWriter.commit();

    doc = new Document();
    doc.add(new StringField("Author", "Fyodor Dostoevsky", Field.Store.NO));
    doc.add(new NumericDocValuesField("Popularity", 79));
    doc.add(new NumericDocValuesField("Books", 11));
    indexWriter.addDocument(config.build(doc));

    doc = new Document();
    doc.add(new StringField("Author", "Leo Tolstoy", Field.Store.NO));
    doc.add(new NumericDocValuesField("Popularity", 75));
    doc.add(new NumericDocValuesField("Books", 47));
    indexWriter.addDocument(config.build(doc));

    indexWriter.close();
  }

  /** User runs a query and counts facets. */
  private List<DynamicRangeUtil.DynamicRangeInfo> search() throws IOException {
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    IndexSearcher searcher = new IndexSearcher(indexReader);

    LongValuesSource valuesSource = LongValuesSource.fromLongField("Popularity");
    LongValuesSource weightsSource = LongValuesSource.fromLongField("Books");

    // Aggregates the facet counts
    FacetsCollectorManager fcm = new FacetsCollectorManager();

    // MatchAllDocsQuery is for "browsing" (counts facets
    // for all non-deleted docs in the index); normally
    // you'd use a "normal" query:
    FacetsCollector fc =
        FacetsCollectorManager.search(searcher, MatchAllDocsQuery.INSTANCE, 10, fcm)
            .facetsCollector();

    try (ExecutorService executor =
        Executors.newFixedThreadPool(2, new NamedThreadFactory("dynamic-ranges"))) {
      // We ask for 2 ranges over popularity weighted by book count
      return DynamicRangeUtil.computeDynamicRanges(
          "Books", weightsSource, valuesSource, fc, 2, executor);
    }
  }

  /** Runs the search example. */
  public List<DynamicRangeUtil.DynamicRangeInfo> runSearch() throws IOException {
    index();
    return search();
  }

  /** Runs the search example and prints the results. */
  public static void main(String[] args) throws Exception {
    System.out.println("Dynamic range facets example:");
    System.out.println("-----------------------");
    DynamicRangeFacetsExample example = new DynamicRangeFacetsExample();
    List<DynamicRangeUtil.DynamicRangeInfo> results = example.runSearch();
    for (DynamicRangeUtil.DynamicRangeInfo range : results) {
      System.out.printf(
          Locale.ROOT,
          "min: %d max: %d centroid: %f count: %d weight: %d%n",
          range.min(),
          range.max(),
          range.centroid(),
          range.count(),
          range.weight());
    }
  }
}
