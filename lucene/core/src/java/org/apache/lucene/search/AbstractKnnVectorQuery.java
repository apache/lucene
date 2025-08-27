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

  private static final TopDocs NO_RESULTS = TopDocsCollector.EMPTY_TOPDOCS;
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
      // rewrite inner filter query first to determine if its a match all
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

    KnnCollectorManager knnCollectorManager = getKnnCollectorManager(k, indexSearcher);
    OptimisticKnnCollectorManager optimisticCollectorManager =
        new OptimisticKnnCollectorManager(k, knnCollectorManager);
    TimeLimitingKnnCollectorManager timeLimitingKnnCollectorManager =
        new TimeLimitingKnnCollectorManager(optimisticCollectorManager, indexSearcher.getTimeout());
    TaskExecutor taskExecutor = indexSearcher.getTaskExecutor();
    List<LeafReaderContext> leafReaderContexts = new ArrayList<>(reader.leaves());
    List<Callable<TopDocs>> tasks = new ArrayList<>(leafReaderContexts.size());
    for (LeafReaderContext context : leafReaderContexts) {
      tasks.add(() -> searchLeaf(context, filterWeight, timeLimitingKnnCollectorManager));
    }
    Map<Integer, TopDocs> perLeafResults = new HashMap<>();
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
      while (ctxIter.hasNext()) {
        LeafReaderContext ctx = ctxIter.next();
        TopDocs perLeaf = perLeafResults.get(ctx.ord);
        if (perLeaf.scoreDocs.length > 0
            && perLeaf.scoreDocs[perLeaf.scoreDocs.length - 1].score >= minTopKScore) {
          // All this leaf's hits are at or above the global topK min score; explore it further
          ++reentryCount;
          tasks.add(() -> searchLeaf(ctx, filterWeight, knnCollectorManagerPhase2));
        } else {
          // This leaf is tapped out; discard the context from the active list so we maintain
          // correspondence between tasks and leaves
          ctxIter.remove();
        }
      }
      assert leafReaderContexts.size() == tasks.size();
      assert perLeafResults.size() == reader.leaves().size();
      topK = runSearchTasks(tasks, taskExecutor, perLeafResults, leafReaderContexts);
    }
    if (topK.scoreDocs.length == 0) {
      return new MatchNoDocsQuery();
    }
    return DocAndScoreQuery.createDocAndScoreQuery(reader, topK, reentryCount);
  }

  private TopDocs runSearchTasks(
      List<Callable<TopDocs>> tasks,
      TaskExecutor taskExecutor,
      Map<Integer, TopDocs> perLeafResults,
      List<LeafReaderContext> leafReaderContexts)
      throws IOException {
    List<TopDocs> taskResults = taskExecutor.invokeAll(tasks);
    for (int i = 0; i < taskResults.size(); i++) {
      perLeafResults.put(leafReaderContexts.get(i).ord, taskResults.get(i));
    }
    tasks.clear();
    // Merge sort the results
    return mergeLeafResults(perLeafResults.values().toArray(TopDocs[]::new));
  }

  protected TopDocs searchLeaf(
      LeafReaderContext ctx,
      Weight filterWeight,
      TimeLimitingKnnCollectorManager timeLimitingKnnCollectorManager)
      throws IOException {
    TopDocs results = getLeafResults(ctx, filterWeight, timeLimitingKnnCollectorManager);
    if (ctx.docBase > 0) {
      for (ScoreDoc scoreDoc : results.scoreDocs) {
        scoreDoc.doc += ctx.docBase;
      }
    }
    return results;
  }

  private TopDocs getLeafResults(
      LeafReaderContext ctx,
      Weight filterWeight,
      TimeLimitingKnnCollectorManager timeLimitingKnnCollectorManager)
      throws IOException {
    final LeafReader reader = ctx.reader();
    final Bits liveDocs = reader.getLiveDocs();

    if (filterWeight == null) {
      AcceptDocs acceptDocs = AcceptDocs.fromLiveDocs(liveDocs, reader.maxDoc());
      return approximateSearch(ctx, acceptDocs, Integer.MAX_VALUE, timeLimitingKnnCollectorManager);
    }

    AcceptDocs acceptDocs =
        AcceptDocs.fromIteratorSupplier(
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
    final int cost = acceptDocs.cost();
    QueryTimeout queryTimeout = timeLimitingKnnCollectorManager.getQueryTimeout();

    float leafProportion = ctx.reader().maxDoc() / (float) ctx.parent.reader().maxDoc();
    int perLeafTopK = perLeafTopKCalculation(k, leafProportion);

    if (cost <= perLeafTopK) {
      // If there are <= perLeafTopK possible matches, short-circuit and perform exact search, since
      // HNSW must always visit at least perLeafTopK documents
      return exactSearch(ctx, acceptDocs.iterator(), queryTimeout);
    }

    // Perform the approximate kNN search
    // We pass cost + 1 here to account for the edge case when we explore exactly cost vectors
    TopDocs results = approximateSearch(ctx, acceptDocs, cost + 1, timeLimitingKnnCollectorManager);

    if ((results.totalHits.relation() == TotalHits.Relation.EQUAL_TO
            // We know that there are more than `perLeafTopK` available docs, if we didn't even get
            // `perLeafTopK` something weird happened, and we need to drop to exact search
            && results.scoreDocs.length >= perLeafTopK)
        // Return partial results only when timeout is met
        || (queryTimeout != null && queryTimeout.shouldExit())) {
      return results;
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

    OptimisticKnnCollectorManager(int k, KnnCollectorManager delegate) {
      this.k = k;
      this.delegate = delegate;
    }

    @Override
    public KnnCollector newCollector(
        int visitedLimit, KnnSearchStrategy searchStrategy, LeafReaderContext context)
        throws IOException {
      // The delegate supports optimistic collection
      if (delegate.isOptimistic()) {
        @SuppressWarnings("resource")
        float leafProportion = context.reader().maxDoc() / (float) context.parent.reader().maxDoc();
        int perLeafTopK = perLeafTopKCalculation(k, leafProportion);
        // if we divided by zero above, leafProportion can be NaN and then this would be 0
        assert perLeafTopK > 0;
        return delegate.newOptimisticCollector(visitedLimit, searchStrategy, context, perLeafTopK);
      }
      // We don't support optimistic collection, so just do regular execution path
      return delegate.newCollector(visitedLimit, searchStrategy, context);
    }
  }

  /*
   * Returns perLeafTopK, the expected number (K * leafProportion) of hits in a leaf with the given
   * proportion of the entire index, plus three standard deviations of a binomial distribution. Math
   * says there is a 95% probability that this segment's contribution to the global top K hits are
   * <= perLeafTopK.
   */
  private static int perLeafTopKCalculation(int k, float leafProportion) {
    return (int)
        Math.max(
            1, k * leafProportion + LAMBDA * Math.sqrt(k * leafProportion * (1 - leafProportion)));
  }

  protected abstract TopDocs approximateSearch(
      LeafReaderContext context,
      AcceptDocs acceptDocs,
      int visitedLimit,
      KnnCollectorManager knnCollectorManager)
      throws IOException;

  abstract VectorScorer createVectorScorer(LeafReaderContext context, FieldInfo fi)
      throws IOException;

  // We allow this to be overridden so that tests can check what search strategy is used
  protected TopDocs exactSearch(
      LeafReaderContext context, DocIdSetIterator acceptIterator, QueryTimeout queryTimeout)
      throws IOException {
    FieldInfo fi = context.reader().getFieldInfos().fieldInfo(field);
    if (fi == null || fi.getVectorDimension() == 0) {
      // The field does not exist or does not index vectors
      return NO_RESULTS;
    }

    VectorScorer vectorScorer = createVectorScorer(context, fi);
    if (vectorScorer == null) {
      return NO_RESULTS;
    }
    final int queueSize = Math.min(k, Math.toIntExact(acceptIterator.cost()));
    HitQueue queue = new HitQueue(queueSize, true);
    TotalHits.Relation relation = TotalHits.Relation.EQUAL_TO;
    ScoreDoc topDoc = queue.top();
    DocIdSetIterator vectorIterator = vectorScorer.iterator();
    DocIdSetIterator conjunction =
        ConjunctionDISI.createConjunction(List.of(vectorIterator, acceptIterator), List.of());
    int doc;
    while ((doc = conjunction.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      // Mark results as partial if timeout is met
      if (queryTimeout != null && queryTimeout.shouldExit()) {
        relation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
        break;
      }
      assert vectorIterator.docID() == doc;
      float score = vectorScorer.score();
      if (score > topDoc.score) {
        topDoc.score = score;
        topDoc.doc = doc;
        topDoc = queue.updateTop();
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
    return new TopDocs(totalHits, topScoreDocs);
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
  protected TopDocs mergeLeafResults(TopDocs[] perLeafResults) {
    return TopDocs.merge(k, perLeafResults);
  }

  // forked from SeededKnnVectorQuery.SeededCollectorManager
  private class ReentrantKnnCollectorManager implements KnnCollectorManager {
    final KnnCollectorManager knnCollectorManager;
    final Map<Integer, TopDocs> perLeafResults;

    ReentrantKnnCollectorManager(
        KnnCollectorManager knnCollectorManager, Map<Integer, TopDocs> perLeafResults) {
      this.knnCollectorManager = knnCollectorManager;
      this.perLeafResults = perLeafResults;
    }

    @Override
    public KnnCollector newCollector(
        int visitLimit, KnnSearchStrategy searchStrategy, LeafReaderContext ctx)
        throws IOException {
      KnnCollector delegateCollector =
          knnCollectorManager.newCollector(visitLimit, searchStrategy, ctx);
      TopDocs seedTopDocs = perLeafResults.get(ctx.ord);
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
