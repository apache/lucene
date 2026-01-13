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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.QueryTimeout;
import org.apache.lucene.index.ReaderUtil;
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
 * behavior doesn't suit your needs, you should create collectorManagers manually with either {@link
 * TopScoreDocCollectorManager} or {@link TopFieldCollectorManager} and call {@link #search(Query,
 * CollectorManager)}.
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

  @SuppressWarnings("NonFinalStaticField")
  static int maxClauseCount = 1024;

  // Caching is disabled by default.
  @SuppressWarnings("NonFinalStaticField")
  private static QueryCache DEFAULT_QUERY_CACHE = null;

  @SuppressWarnings("NonFinalStaticField")
  private static QueryCachingPolicy DEFAULT_CACHING_POLICY = new UsageTrackingQueryCachingPolicy();

  private QueryTimeout queryTimeout = null;
  // partialResult may be set on one of the threads of the executor. It may be correct to not make
  // this variable volatile since joining these threads should ensure a happens-before relationship
  // that guarantees that writes become visible on the main thread, but making the variable volatile
  // shouldn't hurt either.
  private volatile boolean partialResult = false;

  /**
   * By default, we count hits accurately up to 1000. This makes sure that we don't spend most time
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

  private volatile LeafSlice[] leafSlices;

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
        : "IndexSearcher's ReaderContext must be topLevel for reader " + context.reader();
    reader = context.reader();
    this.taskExecutor =
        executor == null ? new TaskExecutor(Runnable::run) : new TaskExecutor(executor);
    this.readerContext = context;
    leafContexts = context.leaves();
    if (executor == null) {
      leafSlices =
          leafContexts.isEmpty()
              ? new LeafSlice[0]
              : new LeafSlice[] {LeafSlice.entireSegments(leafContexts)};
    }
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
   * MAX_DOCS_PER_SLICE will get their own thread.
   *
   * <p>It is possible to leverage intra-segment concurrency by splitting segments into multiple
   * partitions. Such behaviour is not enabled by default as there is still a performance penalty
   * for queries that require segment-level computation ahead of time, such as points/range queries.
   * This is an implementation limitation that we expect to improve in future releases, see <a
   * href="https://github.com/apache/lucene/issues/13745">the corresponding github issue</a>.
   */
  protected LeafSlice[] slices(List<LeafReaderContext> leaves) {
    return slices(leaves, MAX_DOCS_PER_SLICE, MAX_SEGMENTS_PER_SLICE, false);
  }

  /**
   * Static method to segregate LeafReaderContexts amongst multiple slices. Creates slices according
   * to the provided max number of documents per slice and max number of segments per slice. Splits
   * segments into partitions when the last argument is true.
   *
   * @param leaves the leaves to slice
   * @param maxDocsPerSlice the maximum number of documents in a single slice
   * @param maxSegmentsPerSlice the maximum number of segments in a single slice
   * @param allowSegmentPartitions whether segments may be split into partitions according to the
   *     provided maxDocsPerSlice argument. When <code>true</code>, if a segment holds more
   *     documents than the provided max docs per slice, it is split into equal size partitions that
   *     each gets its own slice assigned.
   * @return the array of slices
   */
  public static LeafSlice[] slices(
      List<LeafReaderContext> leaves,
      int maxDocsPerSlice,
      int maxSegmentsPerSlice,
      boolean allowSegmentPartitions) {

    // Make a copy so we can sort:
    List<LeafReaderContext> sortedLeaves = new ArrayList<>(leaves);

    // Sort by maxDoc, descending:
    sortedLeaves.sort(Collections.reverseOrder(Comparator.comparingInt(l -> l.reader().maxDoc())));

    if (allowSegmentPartitions) {
      return slicesWithSegmentPartitions(maxDocsPerSlice, maxSegmentsPerSlice, sortedLeaves);
    }

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
      slices[upto] = LeafSlice.entireSegments(currentLeaf);
      ++upto;
    }

    return slices;
  }

  private static LeafSlice[] slicesWithSegmentPartitions(
      int maxDocsPerSlice, int maxSegmentsPerSlice, List<LeafReaderContext> sortedLeaves) {
    final List<List<LeafReaderContextPartition>> groupedLeafPartitions = new ArrayList<>();
    int currentSliceNumDocs = 0;
    List<LeafReaderContextPartition> group = null;
    for (LeafReaderContext ctx : sortedLeaves) {
      if (ctx.reader().maxDoc() > maxDocsPerSlice) {
        assert group == null;
        // if the segment does not fit in a single slice, we split it into maximum 5 partitions of
        // equal size
        int numSlices = Math.min(5, Math.ceilDiv(ctx.reader().maxDoc(), maxDocsPerSlice));
        int numDocs = ctx.reader().maxDoc() / numSlices;
        int maxDocId = numDocs;
        int minDocId = 0;
        for (int i = 0; i < numSlices - 1; i++) {
          groupedLeafPartitions.add(
              Collections.singletonList(
                  LeafReaderContextPartition.createFromAndTo(ctx, minDocId, maxDocId)));
          minDocId = maxDocId;
          maxDocId += numDocs;
        }
        // the last slice gets all the remaining docs
        groupedLeafPartitions.add(
            Collections.singletonList(
                LeafReaderContextPartition.createFromAndTo(ctx, minDocId, ctx.reader().maxDoc())));
      } else {
        if (group == null) {
          group = new ArrayList<>();
          groupedLeafPartitions.add(group);
        }
        group.add(LeafReaderContextPartition.createForEntireSegment(ctx));

        currentSliceNumDocs += ctx.reader().maxDoc();
        // We only split a segment when it does not fit entirely in a slice. We don't partition
        // the
        // segment that makes the current slice (which holds multiple segments) go over
        // maxDocsPerSlice. This means that a slice either contains multiple entire segments, or a
        // single partition of a segment.
        if (group.size() >= maxSegmentsPerSlice || currentSliceNumDocs > maxDocsPerSlice) {
          group = null;
          currentSliceNumDocs = 0;
        }
      }
    }

    LeafSlice[] slices = new LeafSlice[groupedLeafPartitions.size()];
    int upto = 0;
    for (List<LeafReaderContextPartition> currentGroup : groupedLeafPartitions) {
      slices[upto] = new LeafSlice(currentGroup);
      ++upto;
    }
    return slices;
  }

  /** Return the {@link IndexReader} this searches. */
  public IndexReader getIndexReader() {
    return reader;
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
    // Rewrite query before optimization check
    query = rewrite(new ConstantScoreQuery(query));
    if (query instanceof ConstantScoreQuery csq) {
      query = csq.getQuery();
    }

    // Check if two clause disjunction optimization applies
    if (query instanceof BooleanQuery booleanQuery
        && this.reader.hasDeletions() == false
        && booleanQuery.isTwoClausePureDisjunctionWithTerms()) {
      Query[] queries = booleanQuery.rewriteTwoClauseDisjunctionWithTermsForCount(this);
      int countTerm1 = count(queries[0]);
      int countTerm2 = count(queries[1]);
      if (countTerm1 == 0 || countTerm2 == 0) {
        return Math.max(countTerm1, countTerm2);
        // Only apply optimization if the intersection is significantly smaller than the union
      } else if ((double) Math.min(countTerm1, countTerm2) / Math.max(countTerm1, countTerm2)
          < 0.1) {
        return countTerm1 + countTerm2 - count(queries[2]);
      }
    }
    return search(new ConstantScoreQuery(query), new TotalHitCountCollectorManager(getSlices()));
  }

  /**
   * Returns the leaf slices used for concurrent searching. Override {@link #slices(List)} to
   * customize how slices are created.
   *
   * @lucene.experimental
   */
  public final LeafSlice[] getSlices() {
    LeafSlice[] res = leafSlices;
    if (res == null) {
      res = computeAndCacheSlices();
    }
    return res;
  }

  private synchronized LeafSlice[] computeAndCacheSlices() {
    LeafSlice[] res = leafSlices;
    if (res == null) {
      res = slices(leafContexts);
      /*
       * Enforce that there aren't multiple leaf partitions within the same leaf slice pointing to the
       * same leaf context. It is a requirement that {@link Collector#getLeafCollector(LeafReaderContext)}
       * gets called once per leaf context. Also, it does not make sense to partition a segment to then search
       * those partitions as part of the same slice, because the goal of partitioning is parallel searching
       * which happens at the slice level.
       */
      for (LeafSlice leafSlice : res) {
        if (leafSlice.partitions.length <= 1) {
          continue;
        }
        enforceDistinctLeaves(leafSlice);
      }
      leafSlices = res;
    }
    return res;
  }

  private static void enforceDistinctLeaves(LeafSlice leafSlice) {
    Set<LeafReaderContext> distinctLeaves = new HashSet<>();
    for (LeafReaderContextPartition leafPartition : leafSlice.partitions) {
      if (distinctLeaves.add(leafPartition.ctx) == false) {
        throw new IllegalStateException(
            "The same slice targets multiple leaf partitions of the same leaf reader context. A physical segment should rather get partitioned to be searched concurrently from as many slices as the number of leaf partitions it is split into.");
      }
    }
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
    CollectorManager<TopScoreDocCollector, TopDocs> manager =
        new TopScoreDocCollectorManager(cappedNumHits, after, TOTAL_HITS_THRESHOLD);

    return search(query, manager);
  }

  /**
   * Get the configured {@link QueryTimeout} for all searches that run through this {@link
   * IndexSearcher}, or {@code null} if not set.
   */
  public QueryTimeout getTimeout() {
    return this.queryTimeout;
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
   * @deprecated This method is being deprecated in favor of {@link IndexSearcher#search(Query,
   *     CollectorManager)} due to its support for concurrency in IndexSearcher
   */
  @Deprecated
  public void search(Query query, Collector collector) throws IOException {
    query = rewrite(query, collector.scoreMode().needsScores());
    Weight weight = createWeight(query, collector.scoreMode(), 1);
    collector.setWeight(weight);
    for (LeafReaderContext ctx : leafContexts) { // search each subreader
      searchLeaf(ctx, 0, DocIdSetIterator.NO_MORE_DOCS, weight, collector);
    }
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

    final CollectorManager<TopFieldCollector, TopFieldDocs> manager =
        new TopFieldCollectorManager(rewrittenSort, cappedNumHits, after, TOTAL_HITS_THRESHOLD);

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
      assert leafContexts.isEmpty();
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
        final LeafReaderContextPartition[] leaves = leafSlices[i].partitions;
        final C collector = collectors.get(i);
        listTasks.add(
            () -> {
              search(leaves, weight, collector);
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
   * <p>{@link #searchLeaf(LeafReaderContext, int, int, Weight, Collector)} is called for every leaf
   * partition. <br>
   *
   * <p>NOTE: this method executes the searches on all given leaf partitions exclusively. To search
   * across all the searchers leaves use {@link #leafContexts}.
   *
   * @param partitions the leaf partitions to execute the searches on
   * @param weight to match documents
   * @param collector to receive hits
   * @throws TooManyClauses If a query would exceed {@link IndexSearcher#getMaxClauseCount()}
   *     clauses.
   */
  protected void search(LeafReaderContextPartition[] partitions, Weight weight, Collector collector)
      throws IOException {

    collector.setWeight(weight);

    for (LeafReaderContextPartition partition : partitions) { // search each subreader partition
      searchLeaf(partition.ctx, partition.minDocId, partition.maxDocId, weight, collector);
    }
  }

  /**
   * Lower-level search API
   *
   * <p>{@link LeafCollector#collect(int)} is called for every document. <br>
   *
   * @param ctx the leaf to execute the search against
   * @param minDocId the lower bound of the doc id range to search
   * @param maxDocId the upper bound of the doc id range to search
   * @param weight to match document
   * @param collector to receive hits
   * @throws TooManyClauses If a query would exceed {@link IndexSearcher#getMaxClauseCount()}
   *     clauses.
   */
  protected void searchLeaf(
      LeafReaderContext ctx, int minDocId, int maxDocId, Weight weight, Collector collector)
      throws IOException {
    final LeafCollector leafCollector;
    try {
      leafCollector = collector.getLeafCollector(ctx);
    } catch (CollectionTerminatedException _) {
      // there is no doc of interest in this reader context
      // continue with the following leaf
      return;
    }
    ScorerSupplier scorerSupplier = weight.scorerSupplier(ctx);
    if (scorerSupplier != null) {
      scorerSupplier.setTopLevelScoringClause();
      BulkScorer scorer = scorerSupplier.bulkScorer();
      if (queryTimeout != null) {
        scorer = new TimeLimitingBulkScorer(scorer, queryTimeout);
      }
      try {
        // Optimize for the case when live docs are stored in a FixedBitSet.
        Bits acceptDocs = ScorerUtil.likelyLiveDocs(ctx.reader().getLiveDocs());
        scorer.score(leafCollector, acceptDocs, minDocId, maxDocId);
      } catch (CollectionTerminatedException _) {
        // collection was terminated prematurely
        // continue with the following leaf
      } catch (TimeLimitingBulkScorer.TimeExceededException _) {
        partialResult = true;
      }
    }
    // Note: this is called if collection ran successfully, including the above special cases of
    // CollectionTerminatedException and TimeExceededException, but no other exception.
    leafCollector.finish();
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
   * Returns this searcher's top-level {@link IndexReaderContext}.
   *
   * @see IndexReader#getContext()
   */
  /* sugar for #getReader().getTopReaderContext() */
  public IndexReaderContext getTopReaderContext() {
    return readerContext;
  }

  /**
   * A class holding a subset of the {@link IndexSearcher}s leaf contexts to be executed within a
   * single thread. A leaf slice holds references to one or more {@link LeafReaderContextPartition}
   * instances. Each partition targets a specific doc id range of a {@link LeafReaderContext}.
   *
   * @lucene.experimental
   */
  public static class LeafSlice {

    private static final Comparator<LeafReaderContextPartition> COMPARATOR =
        Comparator.<LeafReaderContextPartition>comparingInt(l -> l.ctx.docBase)
            .thenComparingInt(l -> l.minDocId);

    /**
     * The leaves that make up this slice.
     *
     * @lucene.experimental
     */
    public final LeafReaderContextPartition[] partitions;

    private final int maxDocs;

    public LeafSlice(List<LeafReaderContextPartition> partitions) {
      this(partitions.toArray(new LeafReaderContextPartition[0]));
    }

    private static LeafSlice entireSegments(List<LeafReaderContext> contexts) {
      int count = contexts.size();
      LeafReaderContextPartition[] parts = new LeafReaderContextPartition[count];
      for (int i = 0; i < count; i++) {
        parts[i] = LeafReaderContextPartition.createForEntireSegment(contexts.get(i));
      }
      return new LeafSlice(parts);
    }

    private LeafSlice(LeafReaderContextPartition... leafReaderContextPartitions) {
      Arrays.sort(leafReaderContextPartitions, COMPARATOR);
      this.partitions = leafReaderContextPartitions;
      int maxDocs = 0;
      for (LeafReaderContextPartition partition : partitions) {
        maxDocs += partition.maxDocs;
      }
      this.maxDocs = maxDocs;
    }

    /**
     * Returns the total number of docs that a slice targets, by summing the number of docs that
     * each of its leaf context partitions targets.
     */
    public int getMaxDocs() {
      return maxDocs;
    }
  }

  /**
   * Holds information about a specific leaf context and the corresponding range of doc ids to
   * search within. Used to optionally search across partitions of the same segment concurrently.
   *
   * <p>A partition instance can be created via {@link #createForEntireSegment(LeafReaderContext)},
   * in which case it will target the entire provided {@link LeafReaderContext}. A true partition of
   * a segment can be created via {@link #createFromAndTo(LeafReaderContext, int, int)} providing
   * the minimum doc id (including) to search as well as the max doc id (excluding).
   *
   * @lucene.experimental
   */
  public static final class LeafReaderContextPartition {
    public final int minDocId;
    public final int maxDocId;
    public final LeafReaderContext ctx;
    // we keep track of maxDocs separately because we use NO_MORE_DOCS as upper bound when targeting
    // the entire segment. We use this only in tests.
    private final int maxDocs;

    private LeafReaderContextPartition(
        LeafReaderContext leafReaderContext, int minDocId, int maxDocId, int maxDocs) {
      if (minDocId >= maxDocId) {
        throw new IllegalArgumentException(
            "minDocId is greater than or equal to maxDocId: ["
                + minDocId
                + "] > ["
                + maxDocId
                + "]");
      }
      if (minDocId < 0) {
        throw new IllegalArgumentException("minDocId is lower than 0: [" + minDocId + "]");
      }
      if (minDocId >= leafReaderContext.reader().maxDoc()) {
        throw new IllegalArgumentException(
            "minDocId is greater than than maxDoc: ["
                + minDocId
                + "] > ["
                + leafReaderContext.reader().maxDoc()
                + "]");
      }

      this.ctx = leafReaderContext;
      this.minDocId = minDocId;
      this.maxDocId = maxDocId;
      this.maxDocs = maxDocs;
    }

    /** Creates a partition of the provided leaf context that targets the entire segment */
    public static LeafReaderContextPartition createForEntireSegment(LeafReaderContext ctx) {
      return new LeafReaderContextPartition(
          ctx, 0, DocIdSetIterator.NO_MORE_DOCS, ctx.reader().maxDoc());
    }

    /**
     * Creates a partition of the provided leaf context that targets a subset of the entire segment,
     * starting from and including the min doc id provided, until and not including the provided max
     * doc id
     */
    public static LeafReaderContextPartition createFromAndTo(
        LeafReaderContext ctx, int minDocId, int maxDocId) {
      assert maxDocId != DocIdSetIterator.NO_MORE_DOCS;
      return new LeafReaderContextPartition(ctx, minDocId, maxDocId, maxDocId - minDocId);
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
   * #getMaxClauseCount()} total clauses cumulatively in all of its children.
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
}
