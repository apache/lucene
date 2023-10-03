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
package org.apache.lucene.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.QueryTimeout;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.automaton.ByteRunAutomaton;

/**
 * Implements search over a single IndexReader.
 *
 * <p>Applications usually need only call the inherited {@link #search(Query,int)} method. For
 * performance reasons, if your index is unchanging, you should share a single IndexSearcher
 * instance across multiple searches instead of creating a new one per-search. If your index has
 * changed and you wish to see the changes reflected in searching, you should use {@link
 * DirectoryReader#openIfChanged(DirectoryReader)} to obtain a new reader and then create a new
 * IndexSearcher from that. Also, for low-latency turnaround it's best to use a near-real-time
 * reader ({@link DirectoryReader#open(IndexWriter)}). Once you have a new {@link IndexReader}, it's
 * relatively cheap to create a new IndexSearcher from it.
 *
 * <p><b>NOTE</b>: The {@link #search} and {@link #searchAfter} methods are configured to only count
 * top hits accurately up to {@code 1,000} and may return a {@link TotalHits.Relation lower bound}
 * of the hit count if the hit count is greater than or equal to {@code 1,000}. On queries that
 * match lots of documents, counting the number of hits may take much longer than computing the top
 * hits so this trade-off allows to get some minimal information about the hit count without slowing
 * down search too much. The {@link TopDocs#scoreDocs} array is always accurate however. If this
 * behavior doesn't suit your needs, you should create collectors manually with either {@link
 * TopScoreDocCollector#create} or {@link TopFieldCollector#create} and call {@link #search(Query,
 * Collector)}.
 *
 * <p><a id="thread-safety"></a>
 *
 * <p><b>NOTE</b>: <code>{@link
 * IndexSearcher}</code> instances are completely thread safe, meaning multiple threads can call any
 * of its methods, concurrently. If your application requires external synchronization, you should
 * <b>not</b> synchronize on the <code>IndexSearcher</code> instance; use your own (non-Lucene)
 * objects instead.
 */
public class IndexSearcher {

  static int maxClauseCount = 1024;
  private static QueryCache DEFAULT_QUERY_CACHE;
  private static QueryCachingPolicy DEFAULT_CACHING_POLICY = new UsageTrackingQueryCachingPolicy();
  private QueryTimeout queryTimeout = null;
  // partialResult may be set on one of the threads of the executor. It may be correct to not make
  // this variable volatile since joining these threads should ensure a happens-before relationship
  // that guarantees that writes become visible on the main thread, but making the variable volatile
  // shouldn't hurt either.
  private volatile boolean partialResult = false;

  static {
    final int maxCachedQueries = 1000;
    // min of 32MB or 5% of the heap size
    final long maxRamBytesUsed = Math.min(1L << 25, Runtime.getRuntime().maxMemory() / 20);
    DEFAULT_QUERY_CACHE = new LRUQueryCache(maxCachedQueries, maxRamBytesUsed);
  }
  /**
   * By default we count hits accurately up to 1000. This makes sure that we don't spend most time
   * on computing hit counts
   */
  private static final int TOTAL_HITS_THRESHOLD = 1000;

  /**
   * Thresholds for index slice allocation logic. To change the default, extend <code> IndexSearcher
   * </code> and use custom values
   */
  private static final int MAX_DOCS_PER_SLICE = 250_000;

  private static final int MAX_SEGMENTS_PER_SLICE = 5;

  final IndexReader reader; // package private for testing!

  // NOTE: these members might change in incompatible ways
  // in the next release
  protected final IndexReaderContext readerContext;
  protected final List<LeafReaderContext> leafContexts;

  /**
   * used with executor - LeafSlice supplier where each slice holds a set of leafs executed within
   * one thread. We are caching it instead of creating it eagerly to avoid calling a protected
   * method from constructor, which is a bad practice. This is {@code null} if no executor is
   * provided
   */
  private final Supplier<LeafSlice[]> leafSlicesSupplier;

  private final Executor executor;
  // Used internally for load balancing threads executing for the query
  private final TaskExecutor taskExecutor;

  // the default Similarity
  private static final Similarity defaultSimilarity = new BM25Similarity();

  private QueryCache queryCache = DEFAULT_QUERY_CACHE;
  private QueryCachingPolicy queryCachingPolicy = DEFAULT_CACHING_POLICY;

  /**
   * Expert: returns a default Similarity instance. In general, this method is only called to
   * initialize searchers and writers. User code and query implementations should respect {@link
   * IndexSearcher#getSimilarity()}.
   *
   * @lucene.internal
   */
  public static Similarity getDefaultSimilarity() {
    return defaultSimilarity;
  }

  /**
   * Expert: returns leaf contexts associated with this searcher. This is an internal method exposed
   * for tests only.
   *
   * @lucene.internal
   */
  public List<LeafReaderContext> getLeafContexts() {
    return leafContexts;
  }

  /**
   * Expert: Get the default {@link QueryCache} or {@code null} if the cache is disabled.
   *
   * @lucene.internal
   */
  public static QueryCache getDefaultQueryCache() {
    return DEFAULT_QUERY_CACHE;
  }

  /**
   * Expert: set the default {@link QueryCache} instance.
   *
   * @lucene.internal
   */
  public static void setDefaultQueryCache(QueryCache defaultQueryCache) {
    DEFAULT_QUERY_CACHE = defaultQueryCache;
  }

  /**
   * Expert: Get the default {@link QueryCachingPolicy}.
   *
   * @lucene.internal
   */
  public static QueryCachingPolicy getDefaultQueryCachingPolicy() {
    return DEFAULT_CACHING_POLICY;
  }

  /**
   * Expert: set the default {@link QueryCachingPolicy} instance.
   *
   * @lucene.internal
   */
  public static void setDefaultQueryCachingPolicy(QueryCachingPolicy defaultQueryCachingPolicy) {
    DEFAULT_CACHING_POLICY = defaultQueryCachingPolicy;
  }

  /** The Similarity implementation used by this searcher. */
  private Similarity similarity = defaultSimilarity;

  /** Creates a searcher searching the provided index. */
  public IndexSearcher(IndexReader r) {
    this(r, null);
  }

  /**
   * Runs searches for each segment separately, using the provided Executor. NOTE: if you are using
   * {@link NIOFSDirectory}, do not use the shutdownNow method of ExecutorService as this uses
   * Thread.interrupt under-the-hood which can silently close file descriptors (see <a href=
   * "https://issues.apache.org/jira/browse/LUCENE-2239">LUCENE-2239</a>).
   *
   * @lucene.experimental
   */
  public IndexSearcher(IndexReader r, Executor executor) {
    this(r.getContext(), executor);
  }

  /**
   * Creates a searcher searching the provided top-level {@link IndexReaderContext}.
   *
   * <p>Given a non-<code>null</code> {@link Executor} this method runs searches for each segment
   * separately, using the provided Executor. NOTE: if you are using {@link NIOFSDirectory}, do not
   * use the shutdownNow method of ExecutorService as this uses Thread.interrupt under-the-hood
   * which can silently close file descriptors (see <a href=
   * "https://issues.apache.org/jira/browse/LUCENE-2239">LUCENE-2239</a>).
   *
   * @see IndexReaderContext
   * @see IndexReader#getContext()
   * @lucene.experimental
   */
  public IndexSearcher(IndexReaderContext context, Executor executor) {
    assert context.isTopLevel
        : "IndexSearcher's ReaderContext must be topLevel for reader" + context.reader();
    reader = context.reader();
    this.executor = executor;
    this.taskExecutor =
        executor == null ? new TaskExecutor(Runnable::run) : new TaskExecutor(executor);
    this.readerContext = context;
    leafContexts = context.leaves();
    Function<List<LeafReaderContext>, LeafSlice[]> slicesProvider =
        executor == null
            ? leaves ->
                leaves.size() == 0
                    ? new LeafSlice[0]
                    : new LeafSlice[] {new LeafSlice(new ArrayList<>(leaves))}
            : this::slices;
    leafSlicesSupplier = new CachingLeafSlicesSupplier(slicesProvider, leafContexts);
  }

  /**
   * Creates a searcher searching the provided top-level {@link IndexReaderContext}.
   *
   * @see IndexReaderContext
   * @see IndexReader#getContext()
   * @lucene.experimental
   */
  public IndexSearcher(IndexReaderContext context) {
    this(context, null);
  }

  /**
   * Return the maximum number of clauses permitted, 1024 by default. Attempts to add more than the
   * permitted number of clauses cause {@link TooManyClauses} to be thrown.
   *
   * @see #setMaxClauseCount(int)
   */
  public static int getMaxClauseCount() {
    return maxClauseCount;
  }

  /** Set the maximum number of clauses permitted per Query. Default value is 1024. */
  public static void setMaxClauseCount(int value) {
    if (value < 1) {
      throw new IllegalArgumentException("maxClauseCount must be >= 1");
    }
    maxClauseCount = value;
  }

  /**
   * Set the {@link QueryCache} to use when scores are not needed. A value of {@code null} indicates
   * that query matches should never be cached. This method should be called <b>before</b> starting
   * using this {@link IndexSearcher}.
   *
   * <p>NOTE: When using a query cache, queries should not be modified after they have been passed
   * to IndexSearcher.
   *
   * @see QueryCache
   * @lucene.experimental
   */
  public void setQueryCache(QueryCache queryCache) {
    this.queryCache = queryCache;
  }

  /**
   * Return the query cache of this {@link IndexSearcher}. This will be either the {@link
   * #getDefaultQueryCache() default query cache} or the query cache that was last set through
   * {@link #setQueryCache(QueryCache)}. A return value of {@code null} indicates that caching is
   * disabled.
   *
   * @lucene.experimental
   */
  public QueryCache getQueryCache() {
    return queryCache;
  }

  /**
   * Set the {@link QueryCachingPolicy} to use for query caching. This method should be called
   * <b>before</b> starting using this {@link IndexSearcher}.
   *
   * @see QueryCachingPolicy
   * @lucene.experimental
   */
  public void setQueryCachingPolicy(QueryCachingPolicy queryCachingPolicy) {
    this.queryCachingPolicy = Objects.requireNonNull(queryCachingPolicy);
  }

  /**
   * Return the query cache of this {@link IndexSearcher}. This will be either the {@link
   * #getDefaultQueryCachingPolicy() default policy} or the policy that was last set through {@link
   * #setQueryCachingPolicy(QueryCachingPolicy)}.
   *
   * @lucene.experimental
   */
  public QueryCachingPolicy getQueryCachingPolicy() {
    return queryCachingPolicy;
  }

  /**
   * Expert: Creates an array of leaf slices each holding a subset of the given leaves. Each {@link
   * LeafSlice} is executed in a single thread. By default, segments with more than
   * MAX_DOCS_PER_SLICE will get their own thread
   */
  protected LeafSlice[] slices(List<LeafReaderContext> leaves) {
    return slices(leaves, MAX_DOCS_PER_SLICE, MAX_SEGMENTS_PER_SLICE);
  }

  /** Static method to segregate LeafReaderContexts amongst multiple slices */
  public static LeafSlice[] slices(
      List<LeafReaderContext> leaves, int maxDocsPerSlice, int maxSegmentsPerSlice) {
    // Make a copy so we can sort:
    List<LeafReaderContext> sortedLeaves = new ArrayList<>(leaves);

    // Sort by maxDoc, descending:
    Collections.sort(
        sortedLeaves, Collections.reverseOrder(Comparator.comparingInt(l -> l.reader().maxDoc())));

    final List<List<LeafReaderContext>> groupedLeaves = new ArrayList<>();
    long docSum = 0;
    List<LeafReaderContext> group = null;
    for (LeafReaderContext ctx : sortedLeaves) {
      if (ctx.reader().maxDoc() > maxDocsPerSlice) {
        assert group == null;
        groupedLeaves.add(Collections.singletonList(ctx));
      } else {
        if (group == null) {
          group = new ArrayList<>();
          group.add(ctx);

          groupedLeaves.add(group);
        } else {
          group.add(ctx);
        }

        docSum += ctx.reader().maxDoc();
        if (group.size() >= maxSegmentsPerSlice || docSum > maxDocsPerSlice) {
          group = null;
          docSum = 0;
        }
      }
    }

    LeafSlice[] slices = new LeafSlice[groupedLeaves.size()];
    int upto = 0;
    for (List<LeafReaderContext> currentLeaf : groupedLeaves) {
      slices[upto] = new LeafSlice(currentLeaf);
      ++upto;
    }

    return slices;
  }

  /** Return the {@link IndexReader} this searches. */
  public IndexReader getIndexReader() {
    return reader;
  }

  /**
   * Sugar for <code>.getIndexReader().document(docID)</code>
   *
   * @see IndexReader#document(int)
   * @deprecated Use {@link #storedFields()} to access fields for one or more documents
   */
  @Deprecated
  public Document doc(int docID) throws IOException {
    return reader.document(docID);
  }

  /**
   * Sugar for <code>.getIndexReader().document(docID, fieldVisitor)</code>
   *
   * @see IndexReader#document(int, StoredFieldVisitor)
   * @deprecated Use {@link #storedFields()} to access fields for one or more documents
   */
  @Deprecated
  public void doc(int docID, StoredFieldVisitor fieldVisitor) throws IOException {
    reader.document(docID, fieldVisitor);
  }

  /**
   * Sugar for <code>.getIndexReader().document(docID, fieldsToLoad)</code>
   *
   * @see IndexReader#document(int, Set)
   * @deprecated Use {@link #storedFields()} to access fields for one or more documents
   */
  @Deprecated
  public Document doc(int docID, Set<String> fieldsToLoad) throws IOException {
    return reader.document(docID, fieldsToLoad);
  }

  /**
   * Returns a {@link StoredFields} reader for the stored fields of this index.
   *
   * <p>Sugar for <code>.getIndexReader().storedFields()</code>
   *
   * <p>This call never returns {@code null}, even if no stored fields were indexed. The returned
   * instance should only be used by a single thread.
   *
   * <p>Example:
   *
   * <pre class="prettyprint">
   * TopDocs hits = searcher.search(query, 10);
   * StoredFields storedFields = searcher.storedFields();
   * for (ScoreDoc hit : hits.scoreDocs) {
   *   Document doc = storedFields.document(hit.doc);
   * }
   * </pre>
   *
   * @throws IOException If there is a low-level IO error
   * @see IndexReader#storedFields()
   */
  public StoredFields storedFields() throws IOException {
    return reader.storedFields();
  }

  /** Expert: Set the Similarity implementation used by this IndexSearcher. */
  public void setSimilarity(Similarity similarity) {
    this.similarity = similarity;
  }

  /**
   * Expert: Get the {@link Similarity} to use to compute scores. This returns the {@link
   * Similarity} that has been set through {@link #setSimilarity(Similarity)} or the default {@link
   * Similarity} if none has been set explicitly.
   */
  public Similarity getSimilarity() {
    return similarity;
  }

  /**
   * Count how many documents match the given query. May be faster than counting number of hits by
   * collecting all matches, as the number of hits is retrieved from the index statistics when
   * possible.
   */
  public int count(Query query) throws IOException {
    return search(new ConstantScoreQuery(query), new TotalHitCountCollectorManager());
  }

  /**
   * Returns the leaf slices used for concurrent searching
   *
   * @lucene.experimental
   */
  public LeafSlice[] getSlices() {
    return leafSlicesSupplier.get();
  }

  /**
   * Finds the top <code>n</code> hits for <code>query</code> where all results are after a previous
   * result (<code>after</code>).
   *
   * <p>By passing the bottom result from a previous page as <code>after</code>, this method can be
   * used for efficient 'deep-paging' across potentially large result sets.
   *
   * @throws TooManyClauses If a query would exceed {@link IndexSearcher#getMaxClauseCount()}
   *     clauses.
   */
  public TopDocs searchAfter(ScoreDoc after, Query query, int numHits) throws IOException {
    final int limit = Math.max(1, reader.maxDoc());
    if (after != null && after.doc >= limit) {
      throw new IllegalArgumentException(
          "after.doc exceeds the number of documents in the reader: after.doc="
              + after.doc
              + " limit="
              + limit);
    }

    final int cappedNumHits = Math.min(numHits, limit);

    final LeafSlice[] leafSlices = getSlices();
    final CollectorManager<TopScoreDocCollector, TopDocs> manager =
        new CollectorManager<TopScoreDocCollector, TopDocs>() {

          private final HitsThresholdChecker hitsThresholdChecker =
              leafSlices.length <= 1
                  ? HitsThresholdChecker.create(Math.max(TOTAL_HITS_THRESHOLD, numHits))
                  : HitsThresholdChecker.createShared(Math.max(TOTAL_HITS_THRESHOLD, numHits));

          private final MaxScoreAccumulator minScoreAcc =
              leafSlices.length <= 1 ? null : new MaxScoreAccumulator();

          @Override
          public TopScoreDocCollector newCollector() throws IOException {
            return TopScoreDocCollector.create(
                cappedNumHits, after, hitsThresholdChecker, minScoreAcc);
          }

          @Override
          public TopDocs reduce(Collection<TopScoreDocCollector> collectors) throws IOException {
            final TopDocs[] topDocs = new TopDocs[collectors.size()];
            int i = 0;
            for (TopScoreDocCollector collector : collectors) {
              topDocs[i++] = collector.topDocs();
            }
            return TopDocs.merge(0, cappedNumHits, topDocs);
          }
        };

    return search(query, manager);
  }

  /** Set a {@link QueryTimeout} for all searches that run through this {@link IndexSearcher}. */
  public void setTimeout(QueryTimeout queryTimeout) {
    this.queryTimeout = queryTimeout;
  }

  /**
   * Finds the top <code>n</code> hits for <code>query</code>.
   *
   * @throws TooManyClauses If a query would exceed {@link IndexSearcher#getMaxClauseCount()}
   *     clauses.
   */
  public TopDocs search(Query query, int n) throws IOException {
    return searchAfter(null, query, n);
  }

  /**
   * Lower-level search API.
   *
   * <p>{@link LeafCollector#collect(int)} is called for every matching document.
   *
   * @throws TooManyClauses If a query would exceed {@link IndexSearcher#getMaxClauseCount()}
   *     clauses.
   */
  public void search(Query query, Collector results) throws IOException {
    query = rewrite(query, results.scoreMode().needsScores());
    search(leafContexts, createWeight(query, results.scoreMode(), 1), results);
  }

  /** Returns true if any search hit the {@link #setTimeout(QueryTimeout) timeout}. */
  public boolean timedOut() {
    return partialResult;
  }

  /**
   * Search implementation with arbitrary sorting, plus control over whether hit scores and max
   * score should be computed. Finds the top <code>n</code> hits for <code>query</code>, and sorting
   * the hits by the criteria in <code>sort</code>. If <code>doDocScores</code> is <code>true</code>
   * then the score of each hit will be computed and returned. If <code>doMaxScore</code> is <code>
   * true</code> then the maximum score over all collected hits will be computed.
   *
   * @throws TooManyClauses If a query would exceed {@link IndexSearcher#getMaxClauseCount()}
   *     clauses.
   */
  public TopFieldDocs search(Query query, int n, Sort sort, boolean doDocScores)
      throws IOException {
    return searchAfter(null, query, n, sort, doDocScores);
  }

  /**
   * Search implementation with arbitrary sorting.
   *
   * @param query The query to search for
   * @param n Return only the top n results
   * @param sort The {@link org.apache.lucene.search.Sort} object
   * @return The top docs, sorted according to the supplied {@link org.apache.lucene.search.Sort}
   *     instance
   * @throws IOException if there is a low-level I/O error
   */
  public TopFieldDocs search(Query query, int n, Sort sort) throws IOException {
    return searchAfter(null, query, n, sort, false);
  }

  /**
   * Finds the top <code>n</code> hits for <code>query</code> where all results are after a previous
   * result (<code>after</code>).
   *
   * <p>By passing the bottom result from a previous page as <code>after</code>, this method can be
   * used for efficient 'deep-paging' across potentially large result sets.
   *
   * @throws TooManyClauses If a query would exceed {@link IndexSearcher#getMaxClauseCount()}
   *     clauses.
   */
  public TopDocs searchAfter(ScoreDoc after, Query query, int n, Sort sort) throws IOException {
    return searchAfter(after, query, n, sort, false);
  }

  /**
   * Finds the top <code>n</code> hits for <code>query</code> where all results are after a previous
   * result (<code>after</code>), allowing control over whether hit scores and max score should be
   * computed.
   *
   * <p>By passing the bottom result from a previous page as <code>after</code>, this method can be
   * used for efficient 'deep-paging' across potentially large result sets. If <code>doDocScores
   * </code> is <code>true</code> then the score of each hit will be computed and returned. If
   * <code>doMaxScore</code> is <code>true</code> then the maximum score over all collected hits
   * will be computed.
   *
   * @throws TooManyClauses If a query would exceed {@link IndexSearcher#getMaxClauseCount()}
   *     clauses.
   */
  public TopFieldDocs searchAfter(
      ScoreDoc after, Query query, int numHits, Sort sort, boolean doDocScores) throws IOException {
    if (after != null && !(after instanceof FieldDoc)) {
      // TODO: if we fix type safety of TopFieldDocs we can
      // remove this
      throw new IllegalArgumentException("after must be a FieldDoc; got " + after);
    }
    return searchAfter((FieldDoc) after, query, numHits, sort, doDocScores);
  }

  private TopFieldDocs searchAfter(
      FieldDoc after, Query query, int numHits, Sort sort, boolean doDocScores) throws IOException {
    final int limit = Math.max(1, reader.maxDoc());
    if (after != null && after.doc >= limit) {
      throw new IllegalArgumentException(
          "after.doc exceeds the number of documents in the reader: after.doc="
              + after.doc
              + " limit="
              + limit);
    }
    final int cappedNumHits = Math.min(numHits, limit);
    final Sort rewrittenSort = sort.rewrite(this);
    final LeafSlice[] leafSlices = getSlices();

    final CollectorManager<TopFieldCollector, TopFieldDocs> manager =
        new CollectorManager<>() {

          private final HitsThresholdChecker hitsThresholdChecker =
              leafSlices.length <= 1
                  ? HitsThresholdChecker.create(Math.max(TOTAL_HITS_THRESHOLD, numHits))
                  : HitsThresholdChecker.createShared(Math.max(TOTAL_HITS_THRESHOLD, numHits));

          private final MaxScoreAccumulator minScoreAcc =
              leafSlices.length <= 1 ? null : new MaxScoreAccumulator();

          @Override
          public TopFieldCollector newCollector() throws IOException {
            // TODO: don't pay the price for accurate hit counts by default
            return TopFieldCollector.create(
                rewrittenSort, cappedNumHits, after, hitsThresholdChecker, minScoreAcc);
          }

          @Override
          public TopFieldDocs reduce(Collection<TopFieldCollector> collectors) throws IOException {
            final TopFieldDocs[] topDocs = new TopFieldDocs[collectors.size()];
            int i = 0;
            for (TopFieldCollector collector : collectors) {
              topDocs[i++] = collector.topDocs();
            }
            return TopDocs.merge(rewrittenSort, 0, cappedNumHits, topDocs);
          }
        };

    TopFieldDocs topDocs = search(query, manager);
    if (doDocScores) {
      TopFieldCollector.populateScores(topDocs.scoreDocs, this, query);
    }
    return topDocs;
  }

  /**
   * Lower-level search API. Search all leaves using the given {@link CollectorManager}. In contrast
   * to {@link #search(Query, Collector)}, this method will use the searcher's {@link Executor} in
   * order to parallelize execution of the collection on the configured {@link #getSlices()}.
   *
   * @see CollectorManager
   * @lucene.experimental
   */
  public <C extends Collector, T> T search(Query query, CollectorManager<C, T> collectorManager)
      throws IOException {
    final C firstCollector = collectorManager.newCollector();
    query = rewrite(query, firstCollector.scoreMode().needsScores());
    final Weight weight = createWeight(query, firstCollector.scoreMode(), 1);
    return search(weight, collectorManager, firstCollector);
  }

  private <C extends Collector, T> T search(
      Weight weight, CollectorManager<C, T> collectorManager, C firstCollector) throws IOException {
    final LeafSlice[] leafSlices = getSlices();
    if (leafSlices.length == 0) {
      // there are no segments, nothing to offload to the executor, but we do need to call reduce to
      // create some kind of empty result
      assert leafContexts.size() == 0;
      return collectorManager.reduce(Collections.singletonList(firstCollector));
    } else {
      final List<C> collectors = new ArrayList<>(leafSlices.length);
      collectors.add(firstCollector);
      final ScoreMode scoreMode = firstCollector.scoreMode();
      for (int i = 1; i < leafSlices.length; ++i) {
        final C collector = collectorManager.newCollector();
        collectors.add(collector);
        if (scoreMode != collector.scoreMode()) {
          throw new IllegalStateException(
              "CollectorManager does not always produce collectors with the same score mode");
        }
      }
      final List<Callable<C>> listTasks = new ArrayList<>(leafSlices.length);
      for (int i = 0; i < leafSlices.length; ++i) {
        final LeafReaderContext[] leaves = leafSlices[i].leaves;
        final C collector = collectors.get(i);
        listTasks.add(
            () -> {
              search(Arrays.asList(leaves), weight, collector);
              return collector;
            });
      }
      List<C> results = taskExecutor.invokeAll(listTasks);
      return collectorManager.reduce(results);
    }
  }

  /**
   * Lower-level search API.
   *
   * <p>{@link LeafCollector#collect(int)} is called for every document. <br>
   *
   * <p>NOTE: this method executes the searches on all given leaves exclusively. To search across
   * all the searchers leaves use {@link #leafContexts}.
   *
   * @param leaves the searchers leaves to execute the searches on
   * @param weight to match documents
   * @param collector to receive hits
   * @throws TooManyClauses If a query would exceed {@link IndexSearcher#getMaxClauseCount()}
   *     clauses.
   */
  protected void search(List<LeafReaderContext> leaves, Weight weight, Collector collector)
      throws IOException {

    collector.setWeight(weight);

    // TODO: should we make this
    // threaded...? the Collector could be sync'd?
    // always use single thread:
    for (LeafReaderContext ctx : leaves) { // search each subreader
      final LeafCollector leafCollector;
      try {
        leafCollector = collector.getLeafCollector(ctx);
      } catch (
          @SuppressWarnings("unused")
          CollectionTerminatedException e) {
        // there is no doc of interest in this reader context
        // continue with the following leaf
        continue;
      }
      BulkScorer scorer = weight.bulkScorer(ctx);
      if (scorer != null) {
        if (queryTimeout != null) {
          scorer = new TimeLimitingBulkScorer(scorer, queryTimeout);
        }
        try {
          scorer.score(leafCollector, ctx.reader().getLiveDocs());
        } catch (
            @SuppressWarnings("unused")
            CollectionTerminatedException e) {
          // collection was terminated prematurely
          // continue with the following leaf
        } catch (
            @SuppressWarnings("unused")
            TimeLimitingBulkScorer.TimeExceededException e) {
          partialResult = true;
        }
      }
      // Note: this is called if collection ran successfully, including the above special cases of
      // CollectionTerminatedException and TimeExceededException, but no other exception.
      leafCollector.finish();
    }
  }

  /**
   * Expert: called to re-write queries into primitive queries.
   *
   * @throws TooManyClauses If a query would exceed {@link IndexSearcher#getMaxClauseCount()}
   *     clauses.
   */
  public Query rewrite(Query original) throws IOException {
    Query query = original;
    for (Query rewrittenQuery = query.rewrite(this);
        rewrittenQuery != query;
        rewrittenQuery = query.rewrite(this)) {
      query = rewrittenQuery;
    }
    query.visit(getNumClausesCheckVisitor());
    return query;
  }

  private Query rewrite(Query original, boolean needsScores) throws IOException {
    if (needsScores) {
      return rewrite(original);
    } else {
      // Take advantage of the few extra rewrite rules of ConstantScoreQuery.
      return rewrite(new ConstantScoreQuery(original));
    }
  }

  /**
   * Returns a QueryVisitor which recursively checks the total number of clauses that a query and
   * its children cumulatively have and validates that the total number does not exceed the
   * specified limit. Throws {@link TooManyNestedClauses} if the limit is exceeded.
   */
  private static QueryVisitor getNumClausesCheckVisitor() {
    return new QueryVisitor() {

      int numClauses;

      @Override
      public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
        // Return this instance even for MUST_NOT and not an empty QueryVisitor
        return this;
      }

      @Override
      public void visitLeaf(Query query) {
        if (numClauses > maxClauseCount) {
          throw new TooManyNestedClauses();
        }
        ++numClauses;
      }

      @Override
      public void consumeTerms(Query query, Term... terms) {
        if (numClauses > maxClauseCount) {
          throw new TooManyNestedClauses();
        }
        ++numClauses;
      }

      @Override
      public void consumeTermsMatching(
          Query query, String field, Supplier<ByteRunAutomaton> automaton) {
        if (numClauses > maxClauseCount) {
          throw new TooManyNestedClauses();
        }
        ++numClauses;
      }
    };
  }

  /**
   * Returns an Explanation that describes how <code>doc</code> scored against <code>query</code>.
   *
   * <p>This is intended to be used in developing Similarity implementations, and, for good
   * performance, should not be displayed with every hit. Computing an explanation is as expensive
   * as executing the query over the entire index.
   */
  public Explanation explain(Query query, int doc) throws IOException {
    query = rewrite(query);
    return explain(createWeight(query, ScoreMode.COMPLETE, 1), doc);
  }

  /**
   * Expert: low-level implementation method Returns an Explanation that describes how <code>doc
   * </code> scored against <code>weight</code>.
   *
   * <p>This is intended to be used in developing Similarity implementations, and, for good
   * performance, should not be displayed with every hit. Computing an explanation is as expensive
   * as executing the query over the entire index.
   *
   * <p>Applications should call {@link IndexSearcher#explain(Query, int)}.
   *
   * @throws TooManyClauses If a query would exceed {@link IndexSearcher#getMaxClauseCount()}
   *     clauses.
   */
  protected Explanation explain(Weight weight, int doc) throws IOException {
    int n = ReaderUtil.subIndex(doc, leafContexts);
    final LeafReaderContext ctx = leafContexts.get(n);
    int deBasedDoc = doc - ctx.docBase;
    final Bits liveDocs = ctx.reader().getLiveDocs();
    if (liveDocs != null && liveDocs.get(deBasedDoc) == false) {
      return Explanation.noMatch("Document " + doc + " is deleted");
    }
    return weight.explain(ctx, deBasedDoc);
  }

  /**
   * Creates a {@link Weight} for the given query, potentially adding caching if possible and
   * configured.
   *
   * @lucene.experimental
   */
  public Weight createWeight(Query query, ScoreMode scoreMode, float boost) throws IOException {
    final QueryCache queryCache = this.queryCache;
    Weight weight = query.createWeight(this, scoreMode, boost);
    if (scoreMode.needsScores() == false && queryCache != null) {
      weight = queryCache.doCache(weight, queryCachingPolicy);
    }
    return weight;
  }

  /**
   * Returns this searchers the top-level {@link IndexReaderContext}.
   *
   * @see IndexReader#getContext()
   */
  /* sugar for #getReader().getTopReaderContext() */
  public IndexReaderContext getTopReaderContext() {
    return readerContext;
  }

  /**
   * A class holding a subset of the {@link IndexSearcher}s leaf contexts to be executed within a
   * single thread.
   *
   * @lucene.experimental
   */
  public static class LeafSlice {

    /**
     * The leaves that make up this slice.
     *
     * @lucene.experimental
     */
    public final LeafReaderContext[] leaves;

    public LeafSlice(List<LeafReaderContext> leavesList) {
      Collections.sort(leavesList, Comparator.comparingInt(l -> l.docBase));
      this.leaves = leavesList.toArray(new LeafReaderContext[0]);
    }
  }

  @Override
  public String toString() {
    return "IndexSearcher(" + reader + "; taskExecutor=" + taskExecutor + ")";
  }

  /**
   * Returns {@link TermStatistics} for a term.
   *
   * <p>This can be overridden for example, to return a term's statistics across a distributed
   * collection.
   *
   * @param docFreq The document frequency of the term. It must be greater or equal to 1.
   * @param totalTermFreq The total term frequency.
   * @return A {@link TermStatistics} (never null).
   * @lucene.experimental
   */
  public TermStatistics termStatistics(Term term, int docFreq, long totalTermFreq)
      throws IOException {
    // This constructor will throw an exception if docFreq <= 0.
    return new TermStatistics(term.bytes(), docFreq, totalTermFreq);
  }

  /**
   * Returns {@link CollectionStatistics} for a field, or {@code null} if the field does not exist
   * (has no indexed terms)
   *
   * <p>This can be overridden for example, to return a field's statistics across a distributed
   * collection.
   *
   * @lucene.experimental
   */
  public CollectionStatistics collectionStatistics(String field) throws IOException {
    assert field != null;
    long docCount = 0;
    long sumTotalTermFreq = 0;
    long sumDocFreq = 0;
    for (LeafReaderContext leaf : reader.leaves()) {
      final Terms terms = Terms.getTerms(leaf.reader(), field);
      docCount += terms.getDocCount();
      sumTotalTermFreq += terms.getSumTotalTermFreq();
      sumDocFreq += terms.getSumDocFreq();
    }
    if (docCount == 0) {
      return null;
    }
    return new CollectionStatistics(field, reader.maxDoc(), docCount, sumTotalTermFreq, sumDocFreq);
  }

  /**
   * Returns this searchers executor or <code>null</code> if no executor was provided
   *
   * @deprecated use {@link #getTaskExecutor()} executor instead to execute concurrent tasks
   */
  @Deprecated
  public Executor getExecutor() {
    return executor;
  }

  /**
   * Returns the {@link TaskExecutor} that this searcher relies on to execute concurrent operations
   *
   * @return the task executor
   */
  public TaskExecutor getTaskExecutor() {
    return taskExecutor;
  }

  /**
   * Thrown when an attempt is made to add more than {@link #getMaxClauseCount()} clauses. This
   * typically happens if a PrefixQuery, FuzzyQuery, WildcardQuery, or TermRangeQuery is expanded to
   * many terms during search.
   */
  public static class TooManyClauses extends RuntimeException {
    private final int maxClauseCount;

    public TooManyClauses(String msg) {
      super(msg);
      this.maxClauseCount = IndexSearcher.getMaxClauseCount();
    }

    public TooManyClauses() {
      this("maxClauseCount is set to " + IndexSearcher.getMaxClauseCount());
    }
    /** The value of {@link IndexSearcher#getMaxClauseCount()} when this Exception was created */
    public int getMaxClauseCount() {
      return maxClauseCount;
    }
  }

  /**
   * Thrown when a client attempts to execute a Query that has more than {@link
   * #getMaxClauseCount()} total clauses cumulatively in all of it's children.
   *
   * @see #rewrite
   */
  public static class TooManyNestedClauses extends TooManyClauses {
    public TooManyNestedClauses() {
      super(
          "Query contains too many nested clauses; maxClauseCount is set to "
              + IndexSearcher.getMaxClauseCount());
    }
  }

  /**
   * Supplier for {@link LeafSlice} slices which computes and caches the value on first invocation
   * and returns cached value on subsequent invocation. If the passed in provider for slice
   * computation throws exception then same will be passed to the caller of this supplier on each
   * invocation. If the provider returns null then {@link NullPointerException} will be thrown to
   * the caller.
   *
   * <p>NOTE: To provide thread safe caching mechanism this class is implementing the (subtle) <a
   * href="https://shipilev.net/blog/2014/safe-public-construction/">double-checked locking
   * idiom</a>
   */
  private static class CachingLeafSlicesSupplier implements Supplier<LeafSlice[]> {
    private volatile LeafSlice[] leafSlices;

    private final Function<List<LeafReaderContext>, LeafSlice[]> sliceProvider;

    private final List<LeafReaderContext> leaves;

    private CachingLeafSlicesSupplier(
        Function<List<LeafReaderContext>, LeafSlice[]> provider, List<LeafReaderContext> leaves) {
      this.sliceProvider = Objects.requireNonNull(provider, "leaf slice provider cannot be null");
      this.leaves = Objects.requireNonNull(leaves, "list of LeafReaderContext cannot be null");
    }

    @Override
    public LeafSlice[] get() {
      if (leafSlices == null) {
        synchronized (this) {
          if (leafSlices == null) {
            leafSlices =
                Objects.requireNonNull(
                    sliceProvider.apply(leaves), "slices computed by the provider is null");
          }
        }
      }
      return leafSlices;
    }
  }
}
