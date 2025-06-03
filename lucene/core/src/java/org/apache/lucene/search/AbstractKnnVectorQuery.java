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

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
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
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;

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
      BooleanQuery booleanQuery =
          new BooleanQuery.Builder()
              .add(filter, BooleanClause.Occur.FILTER)
              .add(new FieldExistsQuery(field), BooleanClause.Occur.FILTER)
              .build();
      Query rewritten = indexSearcher.rewrite(booleanQuery);
      if (rewritten.getClass() == MatchNoDocsQuery.class) {
        return rewritten;
      }
      filterWeight = indexSearcher.createWeight(rewritten, ScoreMode.COMPLETE_NO_SCORES, 1f);
    } else {
      filterWeight = null;
    }

    TimeLimitingKnnCollectorManager knnCollectorManager =
        new TimeLimitingKnnCollectorManager(
            getKnnCollectorManager(k, indexSearcher), indexSearcher.getTimeout());
    TaskExecutor taskExecutor = indexSearcher.getTaskExecutor();
    List<LeafReaderContext> leafReaderContexts = new ArrayList<>(reader.leaves());
    List<Callable<TopDocs>> tasks = new ArrayList<>(leafReaderContexts.size());
    for (LeafReaderContext context : leafReaderContexts) {
      tasks.add(() -> searchLeaf(context, filterWeight, knnCollectorManager));
    }
    Map<Integer, TopDocs> perLeafResults = new HashMap<>();
    TopDocs topK = runSearchTasks(tasks, taskExecutor, perLeafResults, leafReaderContexts);
    int reentryCount = 0;
    if (topK.scoreDocs.length > 0
        && perLeafResults.size() > 1
        // don't re-enter the search if we early terminated
        && topK.totalHits.relation() == TotalHits.Relation.EQUAL_TO) {
      float minTopKScore = topK.scoreDocs[topK.scoreDocs.length - 1].score;
      TimeLimitingKnnCollectorManager knnCollectorManagerInner =
          new TimeLimitingKnnCollectorManager(
              new ReentrantKnnCollectorManager(
                  new TopKnnCollectorManager(k, indexSearcher), perLeafResults),
              indexSearcher.getTimeout());
      Iterator<LeafReaderContext> ctxIter = leafReaderContexts.iterator();
      while (ctxIter.hasNext()) {
        LeafReaderContext ctx = ctxIter.next();
        TopDocs perLeaf = perLeafResults.get(ctx.ord);
        if (perLeaf.scoreDocs.length > 0
            && perLeaf.scoreDocs[perLeaf.scoreDocs.length - 1].score >= minTopKScore) {
          // All this leaf's hits are at or above the global topK min score; explore it further
          ++reentryCount;
          tasks.add(() -> searchLeaf(ctx, filterWeight, knnCollectorManagerInner));
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
    return createRewrittenQuery(reader, topK, reentryCount);
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
      return approximateSearch(ctx, liveDocs, Integer.MAX_VALUE, timeLimitingKnnCollectorManager);
    }

    Scorer scorer = filterWeight.scorer(ctx);
    if (scorer == null) {
      return NO_RESULTS;
    }

    BitSet acceptDocs = createBitSet(scorer.iterator(), liveDocs, reader.maxDoc());
    final int cost = acceptDocs.cardinality();
    QueryTimeout queryTimeout = timeLimitingKnnCollectorManager.getQueryTimeout();

    float leafProportion = ctx.reader().maxDoc() / (float) ctx.parent.reader().maxDoc();
    int perLeafTopK = perLeafTopKCalculation(k, leafProportion);

    if (cost <= perLeafTopK) {
      // If there are <= perLeafTopK possible matches, short-circuit and perform exact search, since
      // HNSW must always visit at least perLeafTopK documents
      return exactSearch(ctx, new BitSetIterator(acceptDocs, cost), queryTimeout);
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
      return exactSearch(ctx, new BitSetIterator(acceptDocs, cost), queryTimeout);
    }
  }

  private BitSet createBitSet(DocIdSetIterator iterator, Bits liveDocs, int maxDoc)
      throws IOException {
    if (liveDocs == null && iterator instanceof BitSetIterator bitSetIterator) {
      // If we already have a BitSet and no deletions, reuse the BitSet
      return bitSetIterator.getBitSet();
    } else {
      int threshold = maxDoc >> 7; // same as BitSet#of
      if (iterator.cost() >= threshold) {
        // take advantage of Disi#intoBitset and Bits#applyMask
        FixedBitSet bitSet = new FixedBitSet(maxDoc);
        bitSet.or(iterator);
        if (liveDocs != null) {
          liveDocs.applyMask(bitSet, 0);
        }
        return bitSet;
      } else {
        FilteredDocIdSetIterator filterIterator =
            new FilteredDocIdSetIterator(iterator) {
              @Override
              protected boolean match(int doc) {
                return liveDocs == null || liveDocs.get(doc);
              }
            };
        return BitSet.of(filterIterator, maxDoc); // create a sparse bitset
      }
    }
  }

  protected KnnCollectorManager getKnnCollectorManager(int k, IndexSearcher searcher) {
    KnnCollectorManager manager =
        (visitedLimit, strategy, context) -> {
          @SuppressWarnings("resource")
          float leafProportion =
              context.reader().maxDoc() / (float) context.parent.reader().maxDoc();
          int perLeafTopK = perLeafTopKCalculation(k, leafProportion);
          // if we divided by zero above, leafProportion can be NaN and then this would be 0
          assert perLeafTopK > 0;
          return new TopKnnCollector(perLeafTopK, visitedLimit, strategy);
        };
    return manager;
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
      Bits acceptDocs,
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

  protected Query createRewrittenQuery(IndexReader reader, TopDocs topK, int reentryCount) {
    int len = topK.scoreDocs.length;
    assert len > 0;
    float maxScore = topK.scoreDocs[0].score;
    Arrays.sort(topK.scoreDocs, Comparator.comparingInt(a -> a.doc));
    int[] docs = new int[len];
    float[] scores = new float[len];
    for (int i = 0; i < len; i++) {
      docs[i] = topK.scoreDocs[i].doc;
      scores[i] = topK.scoreDocs[i].score;
    }
    int[] segmentStarts = findSegmentStarts(reader.leaves(), docs);
    return new DocAndScoreQuery(
        docs,
        scores,
        maxScore,
        segmentStarts,
        topK.totalHits.value(),
        reader.getContext().id(),
        reentryCount);
  }

  static int[] findSegmentStarts(List<LeafReaderContext> leaves, int[] docs) {
    int[] starts = new int[leaves.size() + 1];
    starts[starts.length - 1] = docs.length;
    if (starts.length == 2) {
      return starts;
    }
    int resultIndex = 0;
    for (int i = 1; i < starts.length - 1; i++) {
      int upper = leaves.get(i).docBase;
      resultIndex = Arrays.binarySearch(docs, resultIndex, docs.length, upper);
      if (resultIndex < 0) {
        resultIndex = -1 - resultIndex;
      }
      starts[i] = resultIndex;
    }
    return starts;
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

  /** Caches the results of a KnnVector search: a list of docs and their scores */
  static class DocAndScoreQuery extends Query {

    private final int[] docs;
    private final float[] scores;
    private final float maxScore;
    private final int[] segmentStarts;
    private final long visited;
    private final Object contextIdentity;
    private final int reentryCount;

    /**
     * Constructor
     *
     * @param docs the global docids of documents that match, in ascending order
     * @param scores the scores of the matching documents
     * @param maxScore the max of those scores? why do we need to pass in?
     * @param segmentStarts the indexes in docs and scores corresponding to the first matching
     *     document in each segment. If a segment has no matching documents, it should be assigned
     *     the index of the next segment that does. There should be a final entry that is always
     *     docs.length-1.
     * @param visited the number of graph nodes that were visited, and for which vector distance
     *     scores were evaluated.
     * @param contextIdentity an object identifying the reader context that was used to build this
     *     query
     */
    DocAndScoreQuery(
        int[] docs,
        float[] scores,
        float maxScore,
        int[] segmentStarts,
        long visited,
        Object contextIdentity,
        int reentryCount) {
      this.docs = docs;
      this.scores = scores;
      this.maxScore = maxScore;
      this.segmentStarts = segmentStarts;
      this.visited = visited;
      this.contextIdentity = contextIdentity;
      this.reentryCount = reentryCount;
    }

    /*
    DocAndScoreQuery(DocAndScoreQuery other) {
      this.docs = other.docs;
      this.scores = other.scores;
      this.maxScore = other.maxScore;
      this.segmentStarts = other.segmentStarts;
      this.visited = other.visited;
      this.contextIdentity = other.contextIdentity;
      this.reentryCount = other.reentryCount;
    }
    */

    int reentryCount() {
      return reentryCount;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
        throws IOException {
      if (searcher.getIndexReader().getContext().id() != contextIdentity) {
        throw new IllegalStateException("This DocAndScore query was created by a different reader");
      }
      return new Weight(this) {
        @Override
        public Explanation explain(LeafReaderContext context, int doc) {
          int found = Arrays.binarySearch(docs, doc + context.docBase);
          if (found < 0) {
            return Explanation.noMatch("not in top " + docs.length + " docs");
          }
          return Explanation.match(scores[found] * boost, "within top " + docs.length + " docs");
        }

        @Override
        public int count(LeafReaderContext context) {
          return segmentStarts[context.ord + 1] - segmentStarts[context.ord];
        }

        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
          if (segmentStarts[context.ord] == segmentStarts[context.ord + 1]) {
            return null;
          }
          final var scorer =
              new Scorer() {
                final int lower = segmentStarts[context.ord];
                final int upper = segmentStarts[context.ord + 1];
                int upTo = -1;

                @Override
                public DocIdSetIterator iterator() {
                  return new DocIdSetIterator() {
                    @Override
                    public int docID() {
                      return docIdNoShadow();
                    }

                    @Override
                    public int nextDoc() {
                      if (upTo == -1) {
                        upTo = lower;
                      } else {
                        ++upTo;
                      }
                      return docIdNoShadow();
                    }

                    @Override
                    public int advance(int target) throws IOException {
                      return slowAdvance(target);
                    }

                    @Override
                    public long cost() {
                      return upper - lower;
                    }
                  };
                }

                @Override
                public float getMaxScore(int docId) {
                  return maxScore * boost;
                }

                @Override
                public float score() {
                  return scores[upTo] * boost;
                }

                /**
                 * move the implementation of docID() into a differently-named method so we can call
                 * it from DocIDSetIterator.docID() even though this class is anonymous
                 *
                 * @return the current docid
                 */
                private int docIdNoShadow() {
                  if (upTo == -1) {
                    return -1;
                  }
                  if (upTo >= upper) {
                    return NO_MORE_DOCS;
                  }
                  return docs[upTo] - context.docBase;
                }

                @Override
                public int docID() {
                  return docIdNoShadow();
                }
              };
          return new DefaultScorerSupplier(scorer);
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
          return true;
        }
      };
    }

    @Override
    public String toString(String field) {
      return "DocAndScoreQuery[" + docs[0] + ",...][" + scores[0] + ",...]," + maxScore;
    }

    @Override
    public void visit(QueryVisitor visitor) {
      visitor.visitLeaf(this);
    }

    public long visited() {
      return visited;
    }

    @Override
    public boolean equals(Object obj) {
      if (sameClassAs(obj) == false) {
        return false;
      }
      return contextIdentity == ((DocAndScoreQuery) obj).contextIdentity
          && Arrays.equals(docs, ((DocAndScoreQuery) obj).docs)
          && Arrays.equals(scores, ((DocAndScoreQuery) obj).scores);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          classHash(), contextIdentity, Arrays.hashCode(docs), Arrays.hashCode(scores));
    }
  }
}
