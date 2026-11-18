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

package org.apache.lucene.sandbox.codecs.segmentivf;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.QueryTimeout;
import org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.SearchStrategy;
import org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsReader.Candidates;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TimeLimitingKnnCollectorManager;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;

/**
 * A {@link KnnFloatVectorQuery} for SegmentIVF fields that reranks once for the whole index.
 *
 * <p>A plain {@link KnnFloatVectorQuery} reranks each segment's shortlist separately, so fine reads
 * grow with the segment count. This query instead collects every segment's deduplicated coarse
 * shortlist, keeps the strategy's oversample per requested neighbor (by default {@value
 * SegmentIVFVectorsFormat#DEFAULT_OVERSAMPLE}, at least {@value SegmentIVFVectorsFormat#MIN_RERANK}
 * in all) nearest by coarse distance across all of them, and reads and scores only those fine
 * codes. A filter is pushed down ({@link FilterPlan.Mode#PUSHDOWN}): its matches are collected into
 * a bit set per segment, which the segment's scan tests each probed slot against.
 *
 * <p>When some segment's field is not read by SegmentIVF, for example after the field's format
 * changed, the query searches every segment on its own, like a {@link KnnFloatVectorQuery}.
 *
 * <p><b>How it runs.</b> Like every {@link KnnFloatVectorQuery}, it does its search in {@link
 * #rewrite}, which Lucene calls once before scoring; the result is a query over the top {@code k}
 * documents and their scores. Lucene's rewrite searches each leaf with {@link #searchLeaf} and
 * merges the leaves' hits. This query runs {@code rewrite} twice:
 *
 * <ol>
 *   <li>The first call does the work: every segment's coarse scan, the index-wide cut, and each
 *       segment's rerank of its share ({@code globalRerank}). It ends by making a copy of this
 *       query that holds every leaf's reranked hits, and rewriting that.
 *   <li>The copy's {@code rewrite} is Lucene's own; its {@link #searchLeaf} returns the hits the
 *       first call found instead of searching, so Lucene only merges them.
 * </ol>
 *
 * <p>The filter never reaches {@link KnnFloatVectorQuery}: the constructors pass it {@code null}
 * and keep it here, so Lucene's rewrite neither applies it again nor switches to its own exact
 * search of a small filter.
 *
 * <p>Subclasses may override {@link #mergeLeafResults}, calling it on the parent to merge, to
 * post-process the merged top {@code k}, for example to drop hits under a score threshold: the
 * second pass hands its merged hits back to the query it was copied from, so the override sees the
 * global result.
 *
 * @lucene.experimental
 */
public class SegmentIVFKnnQuery extends KnnFloatVectorQuery {
  /**
   * The caller's filter, or null. Applied by this query, not by {@link KnnFloatVectorQuery}, which
   * is given none.
   */
  private final Query denseFilter;

  /**
   * Set only once the global rerank ran: each leaf's reranked hits, by leaf ord, which {@link
   * #searchLeaf} hands to {@code super.rewrite} to merge. Lucene offers no public way to build the
   * rewritten query directly.
   */
  private final Map<Integer, TopDocs> reranked;

  /**
   * In the second-pass copy, the query it was copied from, which post-processes the merged hits
   * ({@link #mergeLeafResults}). Null in a first-pass query.
   */
  private final SegmentIVFKnnQuery firstPass;

  /** Creates a query probing {@code numProbes} cells, with an optional (possibly null) filter. */
  public SegmentIVFKnnQuery(String field, float[] target, int k, Query filter, int numProbes) {
    this(field, target, k, filter, new SearchStrategy(numProbes));
  }

  /** Creates a query with an explicit probe strategy and an optional (possibly null) filter. */
  public SegmentIVFKnnQuery(
      String field, float[] target, int k, Query filter, SearchStrategy strategy) {
    super(field, target, k, null, Objects.requireNonNull(strategy));
    this.denseFilter = filter;
    this.reranked = null;
    this.firstPass = null;
  }

  /** The second-pass copy of {@code query}, carrying the first pass's hits by leaf ord. */
  private SegmentIVFKnnQuery(SegmentIVFKnnQuery query, Map<Integer, TopDocs> reranked) {
    super(query.field, query.target, query.k, null, query.searchStrategy);
    this.denseFilter = query.denseFilter;
    this.reranked = reranked;
    this.firstPass = query;
  }

  /**
   * Merges the leaves' hits into the top {@code k}, where a subclass may post-process them. The
   * second-pass copy is always this class, so it hands the hits to the query it was copied from,
   * where a subclass's override runs.
   */
  @Override
  protected TopDocs mergeLeafResults(TopDocs[] perLeafResults) {
    if (firstPass != null) {
      return firstPass.mergeLeafResults(perLeafResults);
    }
    return super.mergeLeafResults(perLeafResults);
  }

  /** The filter this query was created with; it is applied by this class, not by its parent. */
  @Override
  public Query getFilter() {
    return denseFilter;
  }

  /**
   * Runs the search; see the class documentation for its two passes. Returns a query matching the
   * top {@code k} documents with their scores.
   */
  @Override
  public Query rewrite(IndexSearcher searcher) throws IOException {
    // Second pass: the hits are known, so Lucene's rewrite only merges what searchLeaf returns.
    if (reranked != null) {
      return super.rewrite(searcher);
    }
    // First pass. A global rerank needs every segment read by SegmentIVF.
    List<SegmentIVFVectorsReader> readers = segmentIVFReaders(searcher);
    if (readers == null) {
      // Some segment is not SegmentIVF: search segment by segment as a plain kNN query, which
      // keeps Lucene's exact-search fallback for small filters. Its merge hands back to this
      // query, so a subclass's post-processing runs on this path too.
      SegmentIVFKnnQuery self = this;
      return new KnnFloatVectorQuery(field, target, k, denseFilter, searchStrategy) {
        @Override
        protected TopDocs mergeLeafResults(TopDocs[] perLeafResults) {
          return self.mergeLeafResults(perLeafResults);
        }
      }.rewrite(searcher);
    }
    // Compile the filter once for every segment. As core does, a filter that rewrites to match
    // nothing ends the search and one that matches everything is dropped. Otherwise it is narrowed
    // to documents that have a vector, so documents without one neither count as matches, which
    // would skew the choice between an exact and a scanned search, nor are offered as hits. Scores
    // are not needed, only matches.
    // Core: AbstractKnnVectorQuery#rewrite compiles the filter inline, with no hook to reuse.
    Weight filter = null;
    Query filterQuery = denseFilter == null ? null : searcher.rewrite(denseFilter);
    if (filterQuery != null && filterQuery.getClass() == MatchNoDocsQuery.class) {
      return filterQuery;
    }
    if (filterQuery != null && filterQuery.getClass() != MatchAllDocsQuery.class) {
      Query both =
          new BooleanQuery.Builder()
              .add(denseFilter, Occur.FILTER)
              .add(new FieldExistsQuery(field), Occur.FILTER)
              .build();
      Query rewritten = searcher.rewrite(both);
      // Nothing can match, so there is nothing to scan.
      if (rewritten.getClass() == MatchNoDocsQuery.class) {
        return rewritten;
      }
      filter = rewritten.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1f);
    }
    Map<Integer, TopDocs> hits = globalRerank(searcher, readers, filter);
    // Hand the hits to Lucene's rewrite through a copy that returns them from searchLeaf:
    // Lucene has no public way to build the rewritten query directly.
    // Core: DocAndScoreQuery#createDocAndScoreQuery, package-private to lucene-core.
    return new SegmentIVFKnnQuery(this, hits).rewrite(searcher);
  }

  /**
   * A leaf's share of the finished global rerank in the second pass. In a fallback search, which
   * never sets {@code reranked}, Lucene's own per-leaf search.
   */
  @Override
  protected TopDocs searchLeaf(
      LeafReaderContext context, Weight filter, TimeLimitingKnnCollectorManager manager)
      throws IOException {
    if (reranked == null) {
      return super.searchLeaf(context, filter, manager);
    }
    return reranked.getOrDefault(context.ord, TopDocsCollector.EMPTY_TOPDOCS);
  }

  /**
   * The SegmentIVF reader of the field in each leaf, null for leaves without the field, or null
   * altogether when some leaf's field is read by another format.
   */
  private List<SegmentIVFVectorsReader> segmentIVFReaders(IndexSearcher searcher) {
    List<SegmentIVFVectorsReader> readers = new ArrayList<>();
    for (LeafReaderContext context : searcher.getIndexReader().leaves()) {
      // A segment without the field has nothing to search, but does not stop the others.
      if (context.reader().getFieldInfos().fieldInfo(field) == null) {
        readers.add(null);
        continue;
      }
      SegmentIVFVectorsReader reader = SegmentIVFVectorsReader.of(context.reader(), field);
      if (reader == null) {
        return null;
      }
      readers.add(reader);
    }
    return readers;
  }

  /**
   * Gathers every segment's coarse shortlist, keeps the index-wide nearest, and has each segment
   * rerank its share. Returns each leaf's hits, with global doc ids, by leaf ord.
   *
   * <ol>
   *   <li><b>Scan</b>, every segment in parallel: its coarse search returns a deduplicated
   *       shortlist of ordinals and coarse distances ({@link Candidates}).
   *   <li><b>Cut</b>: the {@code rerankCount(k)} candidates nearest by coarse distance across all
   *       segments are kept ({@link #nearestShares}); coarse distances compare across segments.
   *       Each segment's kept ordinals are its share.
   *   <li><b>Rerank</b>, every segment with a share in parallel: its share is scored with fine
   *       codes into a top-{@code k} collector, reusing the scan's prepared query.
   * </ol>
   */
  private Map<Integer, TopDocs> globalRerank(
      IndexSearcher searcher, List<SegmentIVFVectorsReader> readers, Weight filter)
      throws IOException {
    SearchStrategy strategy = (SearchStrategy) searchStrategy;
    List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();
    // How many candidates the whole index reranks.
    int keep = (int) Math.min(Integer.MAX_VALUE, strategy.rerankCount(k));
    // The searcher's timeout is checked as each segment's scan and rerank starts: once it expires,
    // segments not yet started are skipped and the hits are marked incomplete, as Lucene's
    // TimeLimitingKnnCollector marks them. A segment already started runs to its end.
    QueryTimeout timeout = searcher.getTimeout();
    AtomicBoolean timedOut = new AtomicBoolean();
    List<Callable<Candidates>> scans = new ArrayList<>();
    for (LeafReaderContext context : leaves) {
      SegmentIVFVectorsReader reader = readers.get(context.ord);
      scans.add(() -> {
        if (expired(timeout, timedOut)) {
          return Candidates.EMPTY;
        }
        return scan(reader, context, filter, strategy);
      });
    }
    // 1. Scan.
    List<Candidates> candidates = searcher.getTaskExecutor().invokeAll(scans);
    // 2. Cut.
    int[][] shares = nearestShares(candidates, keep);
    // 3. Rerank. A segment with no candidate in the cut has nothing to rerank. Every segment's
    // share is planned first and all are read in one batch, so the rerank waits for one device
    // round trip however many segments its candidates span; the shares are then scored from it.
    List<Integer> rerankedOrds = new ArrayList<>();
    List<FineCodeIO.Plan> plans = new ArrayList<>();
    FineCodeIO.Plan[] planOf = new FineCodeIO.Plan[leaves.size()];
    for (LeafReaderContext context : leaves) {
      int[] share = shares[context.ord];
      if (share.length == 0) {
        continue;
      }
      rerankedOrds.add(context.ord);
      planOf[context.ord] = readers.get(context.ord).planRerank(candidates.get(context.ord), share);
      if (planOf[context.ord] != null) {
        plans.add(planOf[context.ord]);
      }
    }
    // The reranks read the batch's codes in place, so the buffer is held until they are done.
    FineCodeIO.BatchBuffer batch = FineCodeIO.BatchBuffer.acquire();
    List<TopDocs> hits;
    try {
      MemorySegment read = FineCodeIO.readAll(plans, batch);
      List<Callable<TopDocs>> reranks = new ArrayList<>();
      for (int leaf : rerankedOrds) {
        LeafReaderContext context = leaves.get(leaf);
        SegmentIVFVectorsReader reader = readers.get(leaf);
        Candidates from = candidates.get(leaf);
        int[] share = shares[leaf];
        FineCodeIO.Plan plan = planOf[leaf];
        reranks.add(() -> {
          if (expired(timeout, timedOut)) {
            return TopDocsCollector.EMPTY_TOPDOCS;
          }
          // No visit limit: the share is already the bounded set to score.
          TopKnnCollector collector = new TopKnnCollector(k, Integer.MAX_VALUE, strategy);
          reader.rerank(from, share, plan, read, collector);
          TopDocs segmentHits = collector.topDocs();
          // Leaf doc ids to index-wide ones, as Lucene's merge of leaf hits expects.
          for (ScoreDoc hit : segmentHits.scoreDocs) {
            hit.doc += context.docBase;
          }
          return segmentHits;
        });
      }
      hits = searcher.getTaskExecutor().invokeAll(reranks);
    } finally {
      batch.release();
    }
    Map<Integer, TopDocs> byLeaf = new HashMap<>();
    for (int i = 0; i < hits.size(); i++) {
      TopDocs leafHits = hits.get(i);
      if (timedOut.get()) {
        leafHits =
            new TopDocs(
                new TotalHits(
                    leafHits.totalHits.value(), TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),
                leafHits.scoreDocs);
      }
      byLeaf.put(rerankedOrds.get(i), leafHits);
    }
    return byLeaf;
  }

  /** Whether {@code timeout} has expired, recording it in {@code timedOut}. */
  private static boolean expired(QueryTimeout timeout, AtomicBoolean timedOut) {
    if (timedOut.get()) {
      return true;
    }
    if (timeout != null && timeout.shouldExit()) {
      timedOut.set(true);
      return true;
    }
    return false;
  }

  /**
   * Runs {@code reader}'s coarse search of one leaf, handing it the accepted documents as bits
   * ({@link #accepted}), and returns its shortlist; empty when the leaf lacks the field or accepts
   * nothing.
   */
  private Candidates scan(
      SegmentIVFVectorsReader reader,
      LeafReaderContext context,
      Weight filter,
      SearchStrategy strategy)
      throws IOException {
    if (reader == null) {
      return Candidates.EMPTY;
    }
    AcceptDocs accept = accepted(context, filter, context.reader().getLiveDocs());
    if (accept == null) {
      return Candidates.EMPTY;
    }
    return reader.candidates(field, target, k, strategy, accept);
  }

  /**
   * Keeps the {@code keep} candidates nearest by coarse distance across all segments, and returns
   * each segment's kept ordinals. Their order does not matter: each segment reranks its own share.
   *
   * <p>Every candidate of every segment gets a global number, and is ranked as {@code distance <<
   * 32 | number}, so sorting the longs orders by distance. A selection, not a sort, moves the
   * {@code keep} smallest to the front; the numbers then lead back to each candidate's segment and
   * position, which group the kept ordinals by segment.
   */
  private static int[][] nearestShares(List<Candidates> candidates, int keep) {
    int total = 0;
    for (Candidates segment : candidates) {
      total += segment.ords().length;
    }
    // Rank (distance, candidate) pairs, numbering candidates segment by segment.
    long[] ranked = new long[total];
    int[] segmentOf = new int[total];
    int[] positionOf = new int[total];
    int at = 0;
    for (int s = 0; s < candidates.size(); s++) {
      int[] distances = candidates.get(s).distances();
      for (int i = 0; i < distances.length; i++) {
        ranked[at] = (long) distances[i] << 32 | at;
        segmentOf[at] = s;
        positionOf[at] = i;
        at++;
      }
    }
    keep = Math.min(total, keep);
    if (keep < total) {
      SegmentSearch.selectSmallest(ranked, total, keep);
    }
    // Count each segment's kept candidates to size its share, then fill the shares.
    int[] counts = new int[candidates.size()];
    for (int i = 0; i < keep; i++) {
      counts[segmentOf[(int) ranked[i]]]++;
    }
    int[][] shares = new int[candidates.size()][];
    for (int s = 0; s < shares.length; s++) {
      shares[s] = new int[counts[s]];
      counts[s] = 0;
    }
    for (int i = 0; i < keep; i++) {
      int candidate = (int) ranked[i];
      int s = segmentOf[candidate];
      int ord = candidates.get(s).ords()[positionOf[candidate]];
      shares[s][counts[s]] = ord;
      counts[s]++;
    }
    return shares;
  }

  /**
   * The segment's live documents, intersected with the filter as a bit set when there is one, or
   * null when the segment accepts nothing.
   *
   * <p>A pushdown runs the filter over the whole segment up front, so the scan can test any slot's
   * document with one bit lookup. The bit set also gives the exact match count, which decides
   * between searching the matches exactly and scanning cells.
   */
  private static AcceptDocs accepted(LeafReaderContext context, Weight filter, Bits liveDocs)
      throws IOException {
    int maxDoc = context.reader().maxDoc();
    if (filter == null) {
      return AcceptDocs.fromLiveDocs(liveDocs, maxDoc);
    }
    BulkScorer scorer = filter.bulkScorer(context);
    if (scorer == null) {
      return null;
    }
    FixedBitSet accepted = new FixedBitSet(maxDoc);
    LeafCollector collector =
        new LeafCollector() {
          @Override
          public void setScorer(Scorable scorer) {}

          @Override
          public void collect(int doc) {
            accepted.set(doc);
          }
        };
    scorer.score(collector, liveDocs, 0, maxDoc);
    int matches = accepted.cardinality();
    if (matches == 0) {
      return null;
    }
    return AcceptDocs.fromIteratorSupplier(
        () -> new BitSetIterator(accepted, matches), null, maxDoc);
  }

  @Override
  public String toString(String field) {
    return "SegmentIVFKnnQuery(" + super.toString(field) + ", filter=" + denseFilter + ")";
  }

  @Override
  public boolean equals(Object other) {
    return super.equals(other)
        && Objects.equals(denseFilter, ((SegmentIVFKnnQuery) other).denseFilter);
  }

  @Override
  public int hashCode() {
    return 31 * super.hashCode() + Objects.hashCode(denseFilter);
  }
}
