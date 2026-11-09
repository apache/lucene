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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.QueryTimeout;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.search.knn.TopKnnCollectorManager;
import org.apache.lucene.util.Bits;

/**
 * Uses {@link KnnVectorsReader#search} to perform nearest neighbour search.
 *
 * <p>This query also allows for performing a kNN search subject to a filter. In this case, it first
 * executes the filter for each leaf, then chooses a strategy dynamically:
 *
 * <ul>
 *   <li>If the filter cost is less than k, just execute an exact search
 *   <li>Otherwise run a kNN search subject to the filter
 *   <li>If the kNN search visits too many vectors without completing, stop and run an exact search
 * </ul>
 */
abstract class AbstractKnnVectorQuery extends Query {

  // Constant controlling the degree of additional result exploration done during
  // pro-rata search of segments.
  private static final int LAMBDA = 16;

  protected final String field;
  protected final int k;
  protected final Query filter;
  protected final KnnSearchStrategy searchStrategy;

  AbstractKnnVectorQuery(String field, int k, Query filter, KnnSearchStrategy searchStrategy) {
    this.field = Objects.requireNonNull(field, "field");
    this.k = k;
    if (k < 1) {
      throw new IllegalArgumentException("k must be at least 1, got: " + k);
    }
    this.filter = filter;
    this.searchStrategy = searchStrategy;
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    IndexReader reader = indexSearcher.getIndexReader();

    final Weight filterWeight;
    if (filter != null) {
      // rewrite inner filter query first to determine if it's a match all
      // or match no docs query, so we can skip the knn search
      Query rewrittenFilter = filter.rewrite(indexSearcher);
      if (rewrittenFilter.getClass() == MatchNoDocsQuery.class) {
        // If the filter is a match no docs query, we can also skip it
        return rewrittenFilter;
      }
      if (rewrittenFilter.getClass() != MatchAllDocsQuery.class) {
        BooleanQuery booleanQuery =
            new BooleanQuery.Builder()
                .add(filter, BooleanClause.Occur.FILTER)
                .add(new FieldExistsQuery(field), BooleanClause.Occur.FILTER)
                .build();
        Query rewritten = indexSearcher.rewrite(booleanQuery);
        if (rewritten.getClass() == MatchNoDocsQuery.class) {
          return rewritten;
        }
        filterWeight = rewritten.createWeight(indexSearcher, ScoreMode.COMPLETE_NO_SCORES, 1f);
      } else {
        // If the filter is a match all docs query, we can skip it
        filterWeight = null;
      }
    } else {
      filterWeight = null;
    }

    TaskExecutor taskExecutor = indexSearcher.getTaskExecutor();

    List<LeafReaderContext> leafReaderContexts = new ArrayList<>(reader.leaves());
    List<Callable<AcceptDocs>> weights = new ArrayList<>(leafReaderContexts.size());

    List<AcceptDocs> acceptDocs;
    if (filterWeight != null) {
      // Filter may be arbitrarily expensive, so parallelize the result if possible.
      for (LeafReaderContext context : leafReaderContexts) {
        weights.add(() -> getLeafWeight(context, filterWeight));
      }
      acceptDocs = taskExecutor.invokeAll(weights);
    } else {
      acceptDocs = new ArrayList<>(leafReaderContexts.size());
      for (LeafReaderContext leaf : leafReaderContexts) {
        AcceptDocs docs =
            AcceptDocs.fromLiveDocs(leaf.reader().getLiveDocs(), leaf.reader().maxDoc());
        acceptDocs.add(docs);
      }
    }

    int cumulativeCost = 0;
    for (AcceptDocs a : acceptDocs) {
      cumulativeCost += a.cost();
    }
    final int totalHits = cumulativeCost;

    // Each leaf's expected share of the global top-k is its share of total filter hits, not its
    // share of total docs.
    int[] perLeafTopKByOrd = new int[leafReaderContexts.size()];
    for (int i = 0; i < leafReaderContexts.size(); ++i) {
      LeafReaderContext context = leafReaderContexts.get(i);
      perLeafTopKByOrd[context.ord] =
          perLeafTopKCalculation(totalHits, acceptDocs.get(i).cost(), k);
    }

    KnnCollectorManager knnCollectorManager = getKnnCollectorManager(k, indexSearcher);
    OptimisticKnnCollectorManager optimisticCollectorManager =
        new OptimisticKnnCollectorManager(k, knnCollectorManager, perLeafTopKByOrd);
    TimeLimitingKnnCollectorManager timeLimitingKnnCollectorManager =
        new TimeLimitingKnnCollectorManager(optimisticCollectorManager, indexSearcher.getTimeout());

    final boolean unfiltered = filterWeight == null;
    List<Callable<KnnResult>> tasks = new ArrayList<>(leafReaderContexts.size());
    for (int i = 0; i < leafReaderContexts.size(); i++) {
      LeafReaderContext context = leafReaderContexts.get(i);
      AcceptDocs weight = acceptDocs.get(i);
      tasks.add(
          () ->
              searchLeaf(
                  context,
                  weight,
                  perLeafTopKByOrd[context.ord],
                  unfiltered,
                  timeLimitingKnnCollectorManager));
    }
    Map<Integer, KnnResult> perLeafResults = new HashMap<>();
    TopDocs topK = runSearchTasks(tasks, taskExecutor, perLeafResults, leafReaderContexts);
    int reentryCount = 0;
    if (topK.scoreDocs.length > 0
        && perLeafResults.size() > 1
        // only re-enter if we used the optimistic collection
        && knnCollectorManager.isOptimistic()
        // don't re-enter the search if we early terminated
        && topK.totalHits.relation() == TotalHits.Relation.EQUAL_TO) {
      float minTopKScore = topK.scoreDocs[topK.scoreDocs.length - 1].score;
      TimeLimitingKnnCollectorManager knnCollectorManagerPhase2 =
          new TimeLimitingKnnCollectorManager(
              new ReentrantKnnCollectorManager(
                  getKnnCollectorManager(k, indexSearcher), perLeafResults),
              indexSearcher.getTimeout());
      Iterator<LeafReaderContext> ctxIter = leafReaderContexts.iterator();
      Iterator<AcceptDocs> acceptDocsIter = acceptDocs.iterator();
      while (ctxIter.hasNext()) {
        LeafReaderContext ctx = ctxIter.next();
        AcceptDocs accept = acceptDocsIter.next();

        KnnResult perLeaf = perLeafResults.get(ctx.ord);
        if (perLeaf.hasMoreCompetitiveScores(minTopKScore)) {
          // All this leaf's hits are at or above the global topK min score; explore it further
          tasks.add(
              () ->
                  searchLeaf(
                      ctx,
                      accept,
                      perLeafTopKByOrd[ctx.ord],
                      unfiltered,
                      knnCollectorManagerPhase2));
        } else {
          // This leaf is tapped out; discard the context from the active list so we maintain
          // correspondence between tasks and leaves
          ctxIter.remove();
          acceptDocsIter.remove();
        }
      }
      assert leafReaderContexts.size() == tasks.size();
      assert acceptDocs.size() == tasks.size();
      assert perLeafResults.size() == reader.leaves().size();
      if (tasks.size() > 0) {
        topK = runSearchTasks(tasks, taskExecutor, perLeafResults, leafReaderContexts);
      }
    }
    if (topK.scoreDocs.length == 0) {
      return new MatchNoDocsQuery("No documents matched the nearest-neighbor search");
    }
    return DocAndScoreQuery.createDocAndScoreQuery(
        reader, topK, reentryCount, noMatchExplainer(topK, filterWeight));
  }

  /** Builds the explainer for documents this query did not collect, capturing minTopKScore. */
  private DocAndScoreQuery.NoMatchExplainer noMatchExplainer(TopDocs topK, Weight filterWeight) {
    // topK is score-descending, so the lowest collected score is the last entry.
    final float minTopKScore = topK.scoreDocs[topK.scoreDocs.length - 1].score;
    return (context, doc, topN) ->
        explainNotCollected(context, doc, topN, filterWeight, minTopKScore);
  }

  /** Explains why a doc was not collected, by recomputing its score. null when no vectors. */
  private Explanation explainNotCollected(
      LeafReaderContext context, int doc, int topN, Weight filterWeight, float minTopKScore)
      throws IOException {
    String prefix = "Not in top " + topN + " doc(s): ";
    FieldInfo fi = context.reader().getFieldInfos().fieldInfo(field);
    if (fi == null || fi.getVectorDimension() == 0) {
      return null;
    }
    VectorScorer vectorScorer = createVectorScorer(context, fi);
    if (vectorScorer == null) {
      return null;
    }
    if (vectorScorer.iterator().advance(doc) != doc) {
      return Explanation.noMatch(prefix + "no vector value in field \"" + field + "\"");
    }
    if (filterWeight != null && docPassesFilter(filterWeight, context, doc) == false) {
      return Explanation.noMatch(prefix + "excluded by filter");
    }
    float score = vectorScorer.score();
    if (score < minTopKScore) {
      return Explanation.noMatch(
          prefix + "score " + score + " < minimum top-K score " + minTopKScore);
    }
    // Score meets the cutoff but the doc was not collected (tie-break, recall miss, or rescoring).
    return Explanation.noMatch(
        prefix
            + "score "
            + score
            + " >= minimum top-K score "
            + minTopKScore
            + " (tie-break or approximate-search miss)");
  }

  private static boolean docPassesFilter(Weight filterWeight, LeafReaderContext context, int doc)
      throws IOException {
    Scorer scorer = filterWeight.scorer(context);
    if (scorer == null) {
      return false;
    }
    TwoPhaseIterator twoPhase = scorer.twoPhaseIterator();
    if (twoPhase != null) {
      return twoPhase.approximation().advance(doc) == doc && twoPhase.matches();
    }
    return scorer.iterator().advance(doc) == doc;
  }

  private TopDocs runSearchTasks(
      List<Callable<KnnResult>> tasks,
      TaskExecutor taskExecutor,
      Map<Integer, KnnResult> perLeafResults,
      List<LeafReaderContext> leafReaderContexts)
      throws IOException {
    List<KnnResult> taskResults = taskExecutor.invokeAll(tasks);
    for (int i = 0; i < taskResults.size(); i++) {
      perLeafResults.put(leafReaderContexts.get(i).ord, taskResults.get(i));
    }
    tasks.clear();
    // Merge sort the results
    return mergeLeafResults(perLeafResults.values());
  }

  protected KnnResult searchLeaf(
      LeafReaderContext ctx,
      AcceptDocs acceptDocs,
      int perLeafTopK,
      boolean unfiltered,
      TimeLimitingKnnCollectorManager timeLimitingKnnCollectorManager)
      throws IOException {
    KnnResult results =
        getLeafResults(ctx, acceptDocs, perLeafTopK, unfiltered, timeLimitingKnnCollectorManager);
    results.addDocBase(ctx.docBase);
    return results;
  }

  private AcceptDocs getLeafWeight(LeafReaderContext ctx, Weight filterWeight) {
    final LeafReader reader = ctx.reader();
    final Bits liveDocs = reader.getLiveDocs();

    if (filterWeight == null) {
      return AcceptDocs.fromLiveDocs(liveDocs, reader.maxDoc());
    }

    return AcceptDocs.fromIteratorSupplier(
        () -> {
          Scorer scorer = filterWeight.scorer(ctx);
          if (scorer == null) {
            return DocIdSetIterator.empty();
          } else {
            return scorer.iterator();
          }
        },
        liveDocs,
        reader.maxDoc());
  }

  private KnnResult getLeafResults(
      LeafReaderContext ctx,
      AcceptDocs acceptDocs,
      int perLeafTopK,
      boolean unfiltered,
      TimeLimitingKnnCollectorManager timeLimitingKnnCollectorManager)
      throws IOException {

    if (unfiltered) {
      // No filter: every live doc is a candidate, so there is nothing to short-circuit or fall
      // back from -- the reader's own `k >= numVectors` gate already scans tiny segments. The
      // visit limit stays unbounded, as an exhaustive fallback is pointless without a filter.
      return approximateSearch(ctx, acceptDocs, Integer.MAX_VALUE, timeLimitingKnnCollectorManager);
    }

    final int cost = acceptDocs.cost();
    QueryTimeout queryTimeout = timeLimitingKnnCollectorManager.getQueryTimeout();

    if (cost <= perLeafTopK) {
      // If there are <= perLeafTopK possible matches, short-circuit and perform exact search, since
      // HNSW must always visit at least perLeafTopK documents
      return exactSearch(ctx, acceptDocs.iterator(), queryTimeout);
    }

    // Perform the approximate kNN search
    // We pass cost + 1 here to account for the edge case when we explore exactly cost vectors
    KnnResult knnResult =
        approximateSearch(ctx, acceptDocs, cost + 1, timeLimitingKnnCollectorManager);
    if (knnResult.strategy() == KnnResult.Method.EXACT) {
      return knnResult;
    }

    TopDocs results = knnResult.topDocs();

    if ((results.totalHits.relation() == TotalHits.Relation.EQUAL_TO
            // Since the search was approximate, we know that there are more than `perLeafTopK`
            // available docs, if we didn't even get
            // `perLeafTopK` something weird happened, and we need to drop to exact search
            && knnResult.strongResultCount() >= perLeafTopK)
        // Return partial results only when timeout is met
        || (queryTimeout != null && queryTimeout.shouldExit())) {
      return knnResult;
    } else {
      // We stopped the kNN search because it visited too many nodes, so fall back to exact search
      return exactSearch(ctx, acceptDocs.iterator(), queryTimeout);
    }
  }

  protected KnnCollectorManager getKnnCollectorManager(int k, IndexSearcher searcher) {
    return new TopKnnCollectorManager(k, searcher);
  }

  static class OptimisticKnnCollectorManager implements KnnCollectorManager {
    private final int k;
    private final KnnCollectorManager delegate;
    // leaf ord -> perLeafTopK computed from the leaf's share of total filter hits; leaves not
    // present (e.g. a MemoryIndex with ctx.parent == null) fall back to k
    private final int[] perLeafTopKByOrd;

    OptimisticKnnCollectorManager(int k, KnnCollectorManager delegate, int[] perLeafTopKByOrd) {
      this.k = k;
      this.delegate = delegate;
      this.perLeafTopKByOrd = perLeafTopKByOrd;
    }

    @Override
    public KnnCollector newCollector(
        int visitedLimit, KnnSearchStrategy searchStrategy, LeafReaderContext context)
        throws IOException {
      // The delegate supports optimistic collection
      if (delegate.isOptimistic() && context.parent != null) {
        int perLeafTopK = perLeafTopKByOrd[context.ord];
        return delegate.newOptimisticCollector(
            visitedLimit, searchStrategy, context, perLeafTopK, k);
      }
      // We don't support optimistic collection, so just do regular execution path
      return delegate.newCollector(visitedLimit, searchStrategy, context);
    }
  }

  private static int perLeafTopKCalculation(int maxHits, int leafHits, int k) {
    float leafProportion = leafHits / (float) maxHits;
    return perLeafTopKCalculation(k, leafProportion);
  }

  /*
   * Returns perLeafTopK, the expected number (K * leafProportion) of hits in a leaf with the given
   * proportion of the total filter hits (or of all docs, when unfiltered), plus three standard
   * deviations of a binomial distribution. Math says there is a 95% probability that this
   * segment's contribution to the global top K hits are <= perLeafTopK.
   */
  private static int perLeafTopKCalculation(int k, float leafProportion) {
    return (int)
        Math.max(
            1, k * leafProportion + LAMBDA * Math.sqrt(k * leafProportion * (1 - leafProportion)));
  }

  protected abstract KnnResult approximateSearch(
      LeafReaderContext context,
      AcceptDocs acceptDocs,
      int visitedLimit,
      KnnCollectorManager knnCollectorManager)
      throws IOException;

  abstract VectorScorer createVectorScorer(LeafReaderContext context, FieldInfo fi)
      throws IOException;

  // We allow this to be overridden so that tests can check what search strategy is used
  protected KnnResult exactSearch(
      LeafReaderContext context, DocIdSetIterator acceptIterator, QueryTimeout queryTimeout)
      throws IOException {
    FieldInfo fi = context.reader().getFieldInfos().fieldInfo(field);
    if (fi == null || fi.getVectorDimension() == 0) {
      // The field does not exist or does not index vectors
      return KnnResult.NO_RESULT;
    }

    VectorScorer vectorScorer = createVectorScorer(context, fi);
    if (vectorScorer == null) {
      return KnnResult.NO_RESULT;
    }
    final int queueSize = Math.min(k, Math.toIntExact(acceptIterator.cost()));
    HitQueue queue = new HitQueue(queueSize, true);
    TotalHits.Relation relation = TotalHits.Relation.EQUAL_TO;
    ScoreDoc topDoc = queue.top();
    DocAndFloatFeatureBuffer buffer = new DocAndFloatFeatureBuffer();
    VectorScorer.Bulk bulkScorer = vectorScorer.bulk(acceptIterator);
    for (float maxScore = bulkScorer.nextDocsAndScores(DocIdSetIterator.NO_MORE_DOCS, null, buffer);
        buffer.size > 0;
        maxScore = bulkScorer.nextDocsAndScores(DocIdSetIterator.NO_MORE_DOCS, null, buffer)) {
      // Mark results as partial if timeout is met
      if (queryTimeout != null && queryTimeout.shouldExit()) {
        relation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
        break;
      }
      if (maxScore < topDoc.score) {
        // all the scores in this batch are too low, skip
        continue;
      }
      for (int i = 0; i < buffer.size; i++) {
        float score = buffer.features[i];
        int doc = buffer.docs[i];
        if (score > topDoc.score) {
          topDoc.score = score;
          topDoc.doc = doc;
          topDoc = queue.updateTop();
        }
      }
    }

    // Remove any remaining sentinel values
    while (queue.size() > 0 && queue.top().score < 0) {
      queue.pop();
    }

    ScoreDoc[] topScoreDocs = new ScoreDoc[queue.size()];
    for (int i = topScoreDocs.length - 1; i >= 0; i--) {
      topScoreDocs[i] = queue.pop();
    }

    TotalHits totalHits = new TotalHits(acceptIterator.cost(), relation);
    return new KnnResult(KnnResult.Method.EXACT, new TopDocs(totalHits, topScoreDocs));
  }

  /**
   * Merges all segment-level kNN results to get the index-level kNN results.
   *
   * <p>The default implementation delegates to {@link TopDocs#merge(int, TopDocs[])} to find the
   * overall top {@link #k}, which requires input results to be sorted.
   *
   * <p>This method is useful for reading and / or modifying the final results as needed.
   *
   * @param perLeafResults array of segment-level kNN results.
   * @return index-level kNN results (no constraint on their ordering).
   * @lucene.experimental
   */
  protected TopDocs mergeLeafResults(Collection<KnnResult> perLeafResults) {
    TopDocs[] docs = perLeafResults.stream().map(KnnResult::topDocs).toArray(TopDocs[]::new);
    return TopDocs.merge(k, docs);
  }

  // forked from SeededKnnVectorQuery.SeededCollectorManager
  private class ReentrantKnnCollectorManager implements KnnCollectorManager {
    final KnnCollectorManager knnCollectorManager;
    final Map<Integer, KnnResult> perLeafResults;

    ReentrantKnnCollectorManager(
        KnnCollectorManager knnCollectorManager, Map<Integer, KnnResult> perLeafResults) {
      this.knnCollectorManager = knnCollectorManager;
      this.perLeafResults = perLeafResults;
    }

    @Override
    public KnnCollector newCollector(
        int visitLimit, KnnSearchStrategy searchStrategy, LeafReaderContext ctx)
        throws IOException {
      KnnCollector delegateCollector =
          knnCollectorManager.newCollector(visitLimit, searchStrategy, ctx);
      // Seed only from the strong results: they bound where the first pass stopped exploring, while
      // the extra retained hits would only add seed entry points without improving the search
      TopDocs seedTopDocs = perLeafResults.get(ctx.ord).strongTopDocs();
      VectorScorer scorer = createVectorScorer(ctx, ctx.reader().getFieldInfos().fieldInfo(field));
      if (seedTopDocs.totalHits.value() == 0 || scorer == null) {
        // shouldn't happen - we only come here when there are results
        assert false;
        // on the other hand, it should be safe to return no results?
        return delegateCollector;
      }
      DocIdSetIterator vectorIterator = scorer.iterator();
      // Handle sparse
      if (vectorIterator instanceof IndexedDISI indexedDISI) {
        vectorIterator = IndexedDISI.asDocIndexIterator(indexedDISI);
      }
      // Most underlying iterators are indexed, so we can map the seed docs to the vector docs
      if (vectorIterator instanceof KnnVectorValues.DocIndexIterator indexIterator) {
        DocIdSetIterator seedDocs =
            new SeededKnnVectorQuery.MappedDISI(
                indexIterator, new SeededKnnVectorQuery.TopDocsDISI(seedTopDocs, ctx));
        return knnCollectorManager.newCollector(
            visitLimit,
            new KnnSearchStrategy.Seeded(seedDocs, seedTopDocs.scoreDocs.length, searchStrategy),
            ctx);
      }
      // could lead to an infinite loop if this ever happens
      assert false;
      return delegateCollector;
    }
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field)) {
      visitor.visitLeaf(this);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    AbstractKnnVectorQuery that = (AbstractKnnVectorQuery) o;
    return k == that.k
        && Objects.equals(field, that.field)
        && Objects.equals(filter, that.filter)
        && Objects.equals(searchStrategy, that.searchStrategy);
  }

  @Override
  public int hashCode() {
    return Objects.hash(field, k, filter);
  }

  /**
   * @return the knn vector field where the knn vector search happens.
   */
  public String getField() {
    return field;
  }

  /**
   * @return the max number of results the KnnVector search returns.
   */
  public int getK() {
    return k;
  }

  /**
   * @return the filter that is executed before the KnnVector search happens. Only the results
   *     accepted by this filter are returned by the KnnVector search.
   */
  public Query getFilter() {
    return filter;
  }

  public KnnSearchStrategy getSearchStrategy() {
    return searchStrategy;
  }
}
