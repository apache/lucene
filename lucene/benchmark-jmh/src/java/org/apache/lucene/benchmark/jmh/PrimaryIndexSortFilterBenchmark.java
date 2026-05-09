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
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
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
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.search.TopScoreDocCollectorManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
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

/**
 * Primary-sort-aligned FILTER coverage for bulk-scoring narrowing:
 *
 * <ul>
 *   <li>{@link TermQuery} on {@link KeywordField} / primary {@link
 *       org.apache.lucene.search.SortedSetSortField}
 *   <li>Slow sorted-numeric range ({@link SortedNumericDocValuesField#newSlowRangeQuery}) on
 *       numeric primary sort
 *   <li>Slow sorted-set range ({@link SortedSetDocValuesField#newSlowRangeQuery}) on the same
 *       keyword index (exact books..books span)
 *   <li>{@link LongPoint#newRangeQuery} on {@link LongField} primary sort (points + DV)
 * </ul>
 *
 * <p>Within each group, {@code Unsorted*} benchmarks serve as baseline (no primary-sort rewrite on
 * the FILTER); slow numeric / slow set FILTERs use realistic doc-values RANGE skips on the sort
 * column. Unsorted indexes that still index long points may use {@link LongField#newRangeQuery} for
 * the FILTER ({@code slowNumeric_unsortedTopScores_longFieldRange}) as the fast default independent
 * of index sort.
 *
 * <p><b>Comparing {@code main} vs a branch with primary-sort FILTER narrowing:</b> use the <em>same
 * </em> {@code .java} file on both revisions (this benchmark is not on {@code main} yet — copy it
 * into a clean {@code main} worktree before building). Build {@code :lucene:benchmark-jmh:assemble}
 * each time, then run JMH with identical CLI. For a focused run that hits every sorted/unsorted
 * top-10 path without complete traversal variants:
 *
 * <pre>{@code
 * BENCH=lucene/benchmark-jmh/build/benchmarks
 * java --add-modules jdk.unsupported --module-path "$BENCH" \
 *     --module org.apache.lucene.benchmark.jmh/org.apache.lucene.benchmark.jmh.Main \
 *     "PrimaryIndexSortFilterBenchmark.*TopScores" -f 3
 * }</pre>
 *
 * <p>Omit {@code -f 3} to use the class default fork count; pass smaller {@code -wi}/{@code -i}
 * when iterating locally.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 3)
@Measurement(iterations = 10, time = 5)
// G1 + heap pre-touch for stable timings on bulk-scorer / DV heavy workloads (same JVM on main vs
// branch).
@Fork(
    value = 3,
    jvmArgsAppend = {"-Xmx2g", "-Xms2g", "-XX:+UseG1GC", "-XX:+AlwaysPreTouch"})
public class PrimaryIndexSortFilterBenchmark {

  static final int NUM_DOCS = 1_000_000;
  static final int FILTERED_DOCS = 10_000;
  static final int NUM_DESCRIPTION_TERMS = 20;
  static final int NUM_HITS = 10;

  /**
   * Like {@link KeywordField} but with {@link DocValuesSkipIndexType#RANGE}; required so {@link
   * SortedSetDocValuesField#newSlowRangeQuery} can use skipper-based primary-sort narrowing the
   * same way production indexes often enable for sort fields.
   */
  private static final FieldType CATEGORY_FIELD_WITH_DV_SKIP;

  static {
    FieldType ft = new FieldType();
    ft.setIndexOptions(IndexOptions.DOCS);
    ft.setOmitNorms(true);
    ft.setTokenized(false);
    ft.setDocValuesType(DocValuesType.SORTED_SET);
    ft.setDocValuesSkipIndexType(DocValuesSkipIndexType.RANGE);
    ft.freeze();
    CATEGORY_FIELD_WITH_DV_SKIP = ft;
  }

  /**
   * Same indexing as {@link LongField} (BKD + sorted numeric DV) with {@link
   * DocValuesSkipIndexType#RANGE}, matching common production setup for sort / range filters.
   */
  private static final class SortvalLongField extends Field {
    private static final FieldType TYPE = new FieldType();

    static {
      TYPE.setDimensions(1, Long.BYTES);
      TYPE.setDocValuesType(DocValuesType.SORTED_NUMERIC);
      TYPE.setDocValuesSkipIndexType(DocValuesSkipIndexType.RANGE);
      TYPE.freeze();
    }

    SortvalLongField(String name, long value) {
      super(name, TYPE);
      fieldsData = value;
    }

    @Override
    public BytesRef binaryValue() {
      var bytes = new byte[Long.BYTES];
      NumericUtils.longToSortableBytes((Long) fieldsData, bytes, 0);
      return new BytesRef(bytes);
    }
  }

  static IndexSearcher newSearcher(IndexReader reader) {
    IndexSearcher searcher = new IndexSearcher(reader);
    searcher.setQueryCache(null);
    return searcher;
  }

  /** MUST: disjunction over description terms (same shape across scenarios). */
  static BooleanQuery descriptionMust() {
    BooleanQuery.Builder description = new BooleanQuery.Builder();
    for (int term = 0; term < NUM_DESCRIPTION_TERMS; term++) {
      description.add(
          new TermQuery(new Term("description", "t" + term)), BooleanClause.Occur.SHOULD);
    }
    return description.build();
  }

  static void addDescriptionFields(Document doc, int i) {
    for (int term = 0; term < NUM_DESCRIPTION_TERMS; term++) {
      if ((i + term) % 3 == 0) {
        doc.add(new StringField("description", "t" + term, Field.Store.NO));
      }
    }
  }

  static void deleteDir(Path path) throws IOException {
    if (path != null && Files.exists(path)) {
      try (Stream<Path> walk = Files.walk(path)) {
        for (Path p : walk.sorted(Comparator.reverseOrder()).toList()) {
          Files.delete(p);
        }
      }
    }
  }

  static void buildCategoryIndex(Directory dir, boolean sorted) throws IOException {
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
        doc.add(new Field("category", new BytesRef(category), CATEGORY_FIELD_WITH_DV_SKIP));
        addDescriptionFields(doc, i);
        writer.addDocument(doc);
      }
      writer.forceMerge(1);
    }
  }

  static void buildLongSortIndex(Directory dir, boolean sorted) throws IOException {
    IndexWriterConfig config = new IndexWriterConfig();
    if (sorted) {
      config.setIndexSort(
          new Sort(LongField.newSortField("sortval", false, SortedNumericSelector.Type.MIN)));
    }
    try (IndexWriter writer = new IndexWriter(dir, config)) {
      for (int i = 0; i < NUM_DOCS; i++) {
        Document doc = new Document();
        doc.add(new SortvalLongField("sortval", i));
        addDescriptionFields(doc, i);
        writer.addDocument(doc);
      }
      writer.forceMerge(1);
    }
  }

  // --- Term FILTER on category (original benchmark) ---

  @State(Scope.Benchmark)
  public static class TermCategoryFilter {

    private Directory sortedDir;
    private Directory unsortedDir;
    private IndexReader sortedReader;
    private IndexReader unsortedReader;
    private Path sortedPath;
    private Path unsortedPath;
    public IndexSearcher sortedSearcher;
    public IndexSearcher unsortedSearcher;
    public Query sortedQuery;
    public Query unsortedQuery;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      sortedPath = Files.createTempDirectory("pisf-term-sorted");
      unsortedPath = Files.createTempDirectory("pisf-term-unsorted");
      sortedDir = new MMapDirectory(sortedPath);
      unsortedDir = new MMapDirectory(unsortedPath);

      buildCategoryIndex(sortedDir, true);
      buildCategoryIndex(unsortedDir, false);

      sortedReader = DirectoryReader.open(sortedDir);
      unsortedReader = DirectoryReader.open(unsortedDir);
      sortedSearcher = newSearcher(sortedReader);
      unsortedSearcher = newSearcher(unsortedReader);

      sortedQuery =
          sortedSearcher.rewrite(
              buildQuery(new TermQuery(new Term("category", "books")), descriptionMust()));
      unsortedQuery =
          unsortedSearcher.rewrite(
              buildQuery(new TermQuery(new Term("category", "books")), descriptionMust()));
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
      sortedReader.close();
      unsortedReader.close();
      sortedDir.close();
      unsortedDir.close();
      deleteDir(sortedPath);
      deleteDir(unsortedPath);
    }
  }

  // --- Slow sorted-numeric FILTER on long primary sort ---

  @State(Scope.Benchmark)
  public static class SlowNumericRangeFilter {

    private Directory sortedDir;
    private Directory unsortedDir;
    private IndexReader sortedReader;
    private IndexReader unsortedReader;
    private Path sortedPath;
    private Path unsortedPath;
    public IndexSearcher sortedSearcher;
    public IndexSearcher unsortedSearcher;
    public Query sortedQuery;
    public Query unsortedQuery;

    /** Same documents as {@link #unsortedQuery} but FILTER via {@link LongField#newRangeQuery}. */
    public Query unsortedQueryLongFieldRange;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      sortedPath = Files.createTempDirectory("pisf-num-sorted");
      unsortedPath = Files.createTempDirectory("pisf-num-unsorted");
      sortedDir = new MMapDirectory(sortedPath);
      unsortedDir = new MMapDirectory(unsortedPath);

      buildLongSortIndex(sortedDir, true);
      buildLongSortIndex(unsortedDir, false);

      sortedReader = DirectoryReader.open(sortedDir);
      unsortedReader = DirectoryReader.open(unsortedDir);
      sortedSearcher = newSearcher(sortedReader);
      unsortedSearcher = newSearcher(unsortedReader);

      Query filter =
          SortedNumericDocValuesField.newSlowRangeQuery("sortval", 0, FILTERED_DOCS - 1L);
      sortedQuery = sortedSearcher.rewrite(buildQuery(filter, descriptionMust()));
      unsortedQuery = unsortedSearcher.rewrite(buildQuery(filter, descriptionMust()));
      Query longFieldFilter = LongField.newRangeQuery("sortval", 0, FILTERED_DOCS - 1L);
      unsortedQueryLongFieldRange =
          unsortedSearcher.rewrite(buildQuery(longFieldFilter, descriptionMust()));
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
      sortedReader.close();
      unsortedReader.close();
      sortedDir.close();
      unsortedDir.close();
      deleteDir(sortedPath);
      deleteDir(unsortedPath);
    }
  }

  // --- Slow sorted-set range FILTER (exact ord span books..books) ---

  @State(Scope.Benchmark)
  public static class SlowSortedSetRangeFilter {

    private Directory sortedDir;
    private Directory unsortedDir;
    private IndexReader sortedReader;
    private IndexReader unsortedReader;
    private Path sortedPath;
    private Path unsortedPath;
    public IndexSearcher sortedSearcher;
    public IndexSearcher unsortedSearcher;
    public Query sortedQuery;
    public Query unsortedQuery;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      sortedPath = Files.createTempDirectory("pisf-sset-sorted");
      unsortedPath = Files.createTempDirectory("pisf-sset-unsorted");
      sortedDir = new MMapDirectory(sortedPath);
      unsortedDir = new MMapDirectory(unsortedPath);

      buildCategoryIndex(sortedDir, true);
      buildCategoryIndex(unsortedDir, false);

      sortedReader = DirectoryReader.open(sortedDir);
      unsortedReader = DirectoryReader.open(unsortedDir);
      sortedSearcher = newSearcher(sortedReader);
      unsortedSearcher = newSearcher(unsortedReader);

      Query filter =
          SortedSetDocValuesField.newSlowRangeQuery(
              "category", new BytesRef("books"), new BytesRef("books"), true, true);
      sortedQuery = sortedSearcher.rewrite(buildQuery(filter, descriptionMust()));
      unsortedQuery = unsortedSearcher.rewrite(buildQuery(filter, descriptionMust()));
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
      sortedReader.close();
      unsortedReader.close();
      sortedDir.close();
      unsortedDir.close();
      deleteDir(sortedPath);
      deleteDir(unsortedPath);
    }
  }

  // --- Point range FILTER on long primary sort ---

  @State(Scope.Benchmark)
  public static class PointRangeFilter {

    private Directory sortedDir;
    private Directory unsortedDir;
    private IndexReader sortedReader;
    private IndexReader unsortedReader;
    private Path sortedPath;
    private Path unsortedPath;
    public IndexSearcher sortedSearcher;
    public IndexSearcher unsortedSearcher;
    public Query sortedQuery;
    public Query unsortedQuery;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      sortedPath = Files.createTempDirectory("pisf-point-sorted");
      unsortedPath = Files.createTempDirectory("pisf-point-unsorted");
      sortedDir = new MMapDirectory(sortedPath);
      unsortedDir = new MMapDirectory(unsortedPath);

      buildLongSortIndex(sortedDir, true);
      buildLongSortIndex(unsortedDir, false);

      sortedReader = DirectoryReader.open(sortedDir);
      unsortedReader = DirectoryReader.open(unsortedDir);
      sortedSearcher = newSearcher(sortedReader);
      unsortedSearcher = newSearcher(unsortedReader);

      Query filter = LongPoint.newRangeQuery("sortval", 0, FILTERED_DOCS - 1L);
      sortedQuery = sortedSearcher.rewrite(buildQuery(filter, descriptionMust()));
      unsortedQuery = unsortedSearcher.rewrite(buildQuery(filter, descriptionMust()));
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
      sortedReader.close();
      unsortedReader.close();
      sortedDir.close();
      unsortedDir.close();
      deleteDir(sortedPath);
      deleteDir(unsortedPath);
    }
  }

  private static BooleanQuery buildQuery(Query filter, BooleanQuery description) {
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add(description, BooleanClause.Occur.MUST);
    query.add(filter, BooleanClause.Occur.FILTER);
    return query.build();
  }

  // --- TermQuery category ---

  @Benchmark
  public TopDocs termFilter_sortedTopScores(TermCategoryFilter s) throws IOException {
    return s.sortedSearcher.search(s.sortedQuery, NUM_HITS);
  }

  @Benchmark
  public TopDocs termFilter_unsortedTopScores(TermCategoryFilter s) throws IOException {
    return s.unsortedSearcher.search(s.unsortedQuery, NUM_HITS);
  }

  @Benchmark
  public TopDocs termFilter_sortedComplete(TermCategoryFilter s) throws IOException {
    return s.sortedSearcher.search(
        s.sortedQuery, new TopScoreDocCollectorManager(NUM_HITS, Integer.MAX_VALUE));
  }

  @Benchmark
  public TopDocs termFilter_sortedCompleteNoScores(TermCategoryFilter s) throws IOException {
    return s.sortedSearcher.search(
        s.sortedQuery, new TopFieldCollectorManager(Sort.INDEXORDER, NUM_HITS, Integer.MAX_VALUE));
  }

  // --- Slow numeric range ---

  @Benchmark
  public TopDocs slowNumeric_sortedTopScores(SlowNumericRangeFilter s) throws IOException {
    return s.sortedSearcher.search(s.sortedQuery, NUM_HITS);
  }

  @Benchmark
  public TopDocs slowNumeric_unsortedTopScores(SlowNumericRangeFilter s) throws IOException {
    return s.unsortedSearcher.search(s.unsortedQuery, NUM_HITS);
  }

  @Benchmark
  public TopDocs slowNumeric_unsortedTopScores_longFieldRange(SlowNumericRangeFilter s)
      throws IOException {
    return s.unsortedSearcher.search(s.unsortedQueryLongFieldRange, NUM_HITS);
  }

  @Benchmark
  public TopDocs slowNumeric_sortedComplete(SlowNumericRangeFilter s) throws IOException {
    return s.sortedSearcher.search(
        s.sortedQuery, new TopScoreDocCollectorManager(NUM_HITS, Integer.MAX_VALUE));
  }

  @Benchmark
  public TopDocs slowNumeric_sortedCompleteNoScores(SlowNumericRangeFilter s) throws IOException {
    return s.sortedSearcher.search(
        s.sortedQuery, new TopFieldCollectorManager(Sort.INDEXORDER, NUM_HITS, Integer.MAX_VALUE));
  }

  // --- Slow sorted-set range ---

  @Benchmark
  public TopDocs slowSortedSet_sortedTopScores(SlowSortedSetRangeFilter s) throws IOException {
    return s.sortedSearcher.search(s.sortedQuery, NUM_HITS);
  }

  @Benchmark
  public TopDocs slowSortedSet_unsortedTopScores(SlowSortedSetRangeFilter s) throws IOException {
    return s.unsortedSearcher.search(s.unsortedQuery, NUM_HITS);
  }

  @Benchmark
  public TopDocs slowSortedSet_sortedComplete(SlowSortedSetRangeFilter s) throws IOException {
    return s.sortedSearcher.search(
        s.sortedQuery, new TopScoreDocCollectorManager(NUM_HITS, Integer.MAX_VALUE));
  }

  @Benchmark
  public TopDocs slowSortedSet_sortedCompleteNoScores(SlowSortedSetRangeFilter s)
      throws IOException {
    return s.sortedSearcher.search(
        s.sortedQuery, new TopFieldCollectorManager(Sort.INDEXORDER, NUM_HITS, Integer.MAX_VALUE));
  }

  // --- Point range ---

  @Benchmark
  public TopDocs pointRange_sortedTopScores(PointRangeFilter s) throws IOException {
    return s.sortedSearcher.search(s.sortedQuery, NUM_HITS);
  }

  @Benchmark
  public TopDocs pointRange_unsortedTopScores(PointRangeFilter s) throws IOException {
    return s.unsortedSearcher.search(s.unsortedQuery, NUM_HITS);
  }

  @Benchmark
  public TopDocs pointRange_sortedComplete(PointRangeFilter s) throws IOException {
    return s.sortedSearcher.search(
        s.sortedQuery, new TopScoreDocCollectorManager(NUM_HITS, Integer.MAX_VALUE));
  }

  @Benchmark
  public TopDocs pointRange_sortedCompleteNoScores(PointRangeFilter s) throws IOException {
    return s.sortedSearcher.search(
        s.sortedQuery, new TopFieldCollectorManager(Sort.INDEXORDER, NUM_HITS, Integer.MAX_VALUE));
  }
}
