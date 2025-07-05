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
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
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
    List<LeafReaderContext> leafReaderContexts = reader.leaves();
    List<Callable<TopDocs>> tasks = new ArrayList<>(leafReaderContexts.size());
    for (LeafReaderContext context : leafReaderContexts) {
      tasks.add(() -> searchLeaf(context, filterWeight, knnCollectorManager));
    }
    TopDocs[] perLeafResults = taskExecutor.invokeAll(tasks).toArray(TopDocs[]::new);

    // Merge sort the results
    TopDocs topK = mergeLeafResults(perLeafResults);
    if (topK.scoreDocs.length == 0) {
      return new MatchNoDocsQuery();
    }
    return DocAndScoreQuery.createDocAndScoreQuery(reader, topK);
  }

  private TopDocs searchLeaf(
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

    if (cost <= k) {
      // If there are <= k possible matches, short-circuit and perform exact search, since HNSW
      // must always visit at least k documents
      return exactSearch(ctx, new BitSetIterator(acceptDocs, cost), queryTimeout);
    }

    // Perform the approximate kNN search
    // We pass cost + 1 here to account for the edge case when we explore exactly cost vectors
    TopDocs results = approximateSearch(ctx, acceptDocs, cost + 1, timeLimitingKnnCollectorManager);
    if ((results.totalHits.relation() == TotalHits.Relation.EQUAL_TO
            // We know that there are more than `k` available docs, if we didn't even get `k`
            // something weird happened, and we need to drop to exact search
            && results.scoreDocs.length >= k)
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
    return new TopKnnCollectorManager(k, searcher);
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
