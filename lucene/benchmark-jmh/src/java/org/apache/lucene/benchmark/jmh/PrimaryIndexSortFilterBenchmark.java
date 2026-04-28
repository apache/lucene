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
package org.apache.lucene.benchmark.jmh;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.search.TopScoreDocCollectorManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 3)
@Measurement(iterations = 10, time = 5)
@Fork(
    value = 3,
    jvmArgsAppend = {"-Xmx2g", "-Xms2g"})
public class PrimaryIndexSortFilterBenchmark {

  private static final int NUM_DOCS = 1_000_000;
  private static final int FILTERED_DOCS = 10_000;
  private static final int NUM_DESCRIPTION_TERMS = 20;
  private static final int NUM_HITS = 10;

  private Directory sortedDir;
  private Directory unsortedDir;
  private IndexReader sortedReader;
  private IndexReader unsortedReader;
  private Path sortedPath;
  private Path unsortedPath;
  private IndexSearcher sortedSearcher;
  private IndexSearcher unsortedSearcher;
  private Query sortedQuery;
  private Query unsortedQuery;

  @Setup(Level.Trial)
  public void setUp() throws IOException {
    sortedPath = Files.createTempDirectory("primary-sort-filter-sorted");
    unsortedPath = Files.createTempDirectory("primary-sort-filter-unsorted");
    sortedDir = new MMapDirectory(sortedPath);
    unsortedDir = new MMapDirectory(unsortedPath);

    buildIndex(sortedDir, true);
    buildIndex(unsortedDir, false);

    sortedReader = DirectoryReader.open(sortedDir);
    unsortedReader = DirectoryReader.open(unsortedDir);
    sortedSearcher = new IndexSearcher(sortedReader);
    unsortedSearcher = new IndexSearcher(unsortedReader);

    sortedQuery = sortedSearcher.rewrite(buildQuery());
    unsortedQuery = unsortedSearcher.rewrite(buildQuery());
  }

  private static void buildIndex(Directory dir, boolean sorted) throws IOException {
    IndexWriterConfig config = new IndexWriterConfig();
    if (sorted) {
      config.setIndexSort(
          new Sort(
              KeywordField.newSortField(
                  "category", false, SortedSetSelector.Type.MIN, SortField.STRING_LAST)));
    }

    try (IndexWriter writer = new IndexWriter(dir, config)) {
      for (int i = 0; i < NUM_DOCS; i++) {
        Document doc = new Document();
        String category = i < FILTERED_DOCS ? "books" : (i < NUM_DOCS / 2 ? "music" : "software");
        doc.add(new KeywordField("category", category, Field.Store.NO));
        for (int term = 0; term < NUM_DESCRIPTION_TERMS; term++) {
          if ((i + term) % 3 == 0) {
            doc.add(new StringField("description", "t" + term, Field.Store.NO));
          }
        }
        writer.addDocument(doc);
      }
      writer.forceMerge(1);
    }
  }

  private static Query buildQuery() {
    BooleanQuery.Builder description = new BooleanQuery.Builder();
    for (int term = 0; term < NUM_DESCRIPTION_TERMS; term++) {
      description.add(
          new TermQuery(new Term("description", "t" + term)), BooleanClause.Occur.SHOULD);
    }

    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add(description.build(), BooleanClause.Occur.MUST);
    query.add(new TermQuery(new Term("category", "books")), BooleanClause.Occur.FILTER);
    return query.build();
  }

  @TearDown(Level.Trial)
  public void tearDown() throws IOException {
    sortedReader.close();
    unsortedReader.close();
    sortedDir.close();
    unsortedDir.close();
    deleteDirectory(sortedPath);
    deleteDirectory(unsortedPath);
  }

  private static void deleteDirectory(Path path) throws IOException {
    if (path != null && Files.exists(path)) {
      try (Stream<Path> walk = Files.walk(path)) {
        walk.sorted(Comparator.reverseOrder())
            .forEach(
                p -> {
                  try {
                    Files.delete(p);
                  } catch (IOException _) {
                  }
                });
      }
    }
  }

  @Benchmark
  public TopDocs benchmarkSortedTopScores() throws IOException {
    return sortedSearcher.search(sortedQuery, NUM_HITS);
  }

  @Benchmark
  public TopDocs benchmarkUnsortedTopScores() throws IOException {
    return unsortedSearcher.search(unsortedQuery, NUM_HITS);
  }

  @Benchmark
  public TopDocs benchmarkSortedComplete() throws IOException {
    return sortedSearcher.search(
        sortedQuery, new TopScoreDocCollectorManager(NUM_HITS, Integer.MAX_VALUE));
  }

  @Benchmark
  public TopDocs benchmarkSortedCompleteNoScores() throws IOException {
    return sortedSearcher.search(
        sortedQuery, new TopFieldCollectorManager(Sort.INDEXORDER, NUM_HITS, Integer.MAX_VALUE));
  }
}
