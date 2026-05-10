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
package org.apache.lucene.search.join;

import static org.apache.lucene.search.knn.KnnSearchStrategy.Hnsw.DEFAULT;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.QueryTimeout;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.HitQueue;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;

/**
 * kNN float vector query that joins matching children vector documents with their parent doc id.
 * The top documents returned are the child document ids and the calculated scores. Here is how to
 * use this in conjunction with {@link ToParentBlockJoinQuery}.
 *
 * <pre><code class="language-java">
 *   Query knnQuery = new DiversifyingChildrenFloatKnnVectorQuery(fieldName, queryVector, ...);
 *   // Rewrite executes kNN search and collects nearest children docIds and their scores
 *   Query rewrittenKnnQuery = searcher.rewrite(knnQuery);
 *   // Join the scored children docs with their parents and score the parents
 *   Query childrenToParents = new ToParentBlockJoinQuery(rewrittenKnnQuery, parentsFilter, ScoreMode.MAX);
 * </code></pre>
 */
public class DiversifyingChildrenFloatKnnVectorQuery extends KnnFloatVectorQuery {
  private static final TopDocs NO_RESULTS = TopDocsCollector.EMPTY_TOPDOCS;

  private final BitSetProducer parentsFilter;
  private final Query childFilter;
  private final int k;
  private final float[] query;
  private final boolean blockRescore;

  /**
   * Create a DiversifyingChildrenFloatKnnVectorQuery.
   *
   * @param field the query field
   * @param query the vector query
   * @param childFilter the child filter
   * @param k how many parent documents to return given the matching children
   * @param parentsFilter Filter identifying the parent documents.
   */
  public DiversifyingChildrenFloatKnnVectorQuery(
      String field, float[] query, Query childFilter, int k, BitSetProducer parentsFilter) {
    this(field, query, childFilter, k, parentsFilter, DEFAULT, false);
  }

  /**
   * Create a DiversifyingChildrenFloatKnnVectorQuery.
   *
   * @param field the query field
   * @param query the vector query
   * @param childFilter the child filter
   * @param k how many parent documents to return given the matching children
   * @param parentsFilter Filter identifying the parent documents.
   * @param searchStrategy the search strategy to use. If null, the default strategy will be used.
   *     The underlying format may not support all strategies and is free to ignore the requested
   *     strategy.
   * @lucene.experimental
   */
  public DiversifyingChildrenFloatKnnVectorQuery(
      String field,
      float[] query,
      Query childFilter,
      int k,
      BitSetProducer parentsFilter,
      KnnSearchStrategy searchStrategy) {
    this(field, query, childFilter, k, parentsFilter, searchStrategy, false);
  }

  /**
   * Create a DiversifyingChildrenFloatKnnVectorQuery with optional post-HNSW block rescoring.
   *
   * <p>When {@code blockRescore} is {@code true}, after the approximate HNSW search completes, all
   * children in each found parent's block are scored to guarantee the truly best child is returned —
   * not merely the sibling the graph traversal happened to reach first. This adds O(k &times;
   * childrenPerParent) extra scoring work; enable it when block sizes are small or result quality is
   * more important than latency.
   *
   * @param field the query field
   * @param query the vector query
   * @param childFilter the child filter
   * @param k how many parent documents to return given the matching children
   * @param parentsFilter Filter identifying the parent documents.
   * @param searchStrategy the search strategy to use.
   * @param blockRescore if {@code true}, enables post-HNSW block rescoring.
   * @lucene.experimental
   */
  public DiversifyingChildrenFloatKnnVectorQuery(
      String field,
      float[] query,
      Query childFilter,
      int k,
      BitSetProducer parentsFilter,
      KnnSearchStrategy searchStrategy,
      boolean blockRescore) {
    super(field, query, k, childFilter, searchStrategy);
    this.childFilter = childFilter;
    this.parentsFilter = parentsFilter;
    this.k = k;
    this.query = query;
    this.blockRescore = blockRescore;
  }

  @Override
  protected TopDocs exactSearch(
      LeafReaderContext context, DocIdSetIterator acceptIterator, QueryTimeout queryTimeout)
      throws IOException {
    FloatVectorValues floatVectorValues = context.reader().getFloatVectorValues(field);
    if (floatVectorValues == null) {
      FloatVectorValues.checkField(context.reader(), field);
      return NO_RESULTS;
    }

    BitSet parentBitSet = parentsFilter.getBitSet(context);
    if (parentBitSet == null) {
      return NO_RESULTS;
    }
    VectorScorer floatVectorScorer = floatVectorValues.scorer(query);
    if (floatVectorScorer == null) {
      return NO_RESULTS;
    }

    DiversifyingChildrenVectorScorer vectorScorer =
        new DiversifyingChildrenVectorScorer(acceptIterator, parentBitSet, floatVectorScorer);
    final int queueSize = Math.min(k, Math.toIntExact(acceptIterator.cost()));
    HitQueue queue = new HitQueue(queueSize, true);
    TotalHits.Relation relation = TotalHits.Relation.EQUAL_TO;
    ScoreDoc topDoc = queue.top();
    while (vectorScorer.nextParent() != DocIdSetIterator.NO_MORE_DOCS) {
      // Mark results as partial if timeout is met
      if (queryTimeout != null && queryTimeout.shouldExit()) {
        relation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
        break;
      }

      float score = vectorScorer.score();
      if (score > topDoc.score) {
        topDoc.score = score;
        topDoc.doc = vectorScorer.bestChild();
        topDoc = queue.updateTop();
      }
    }

    // Remove any remaining sentinel values
    while (queue.size() > 0 && queue.top().score < 0) {
      queue.pop();
    }

    ScoreDoc[] topScoreDocs = queue.drainToArrayHighestFirst(ScoreDoc[]::new);

    TotalHits totalHits = new TotalHits(acceptIterator.cost(), relation);
    return new TopDocs(totalHits, topScoreDocs);
  }

  @Override
  protected KnnCollectorManager getKnnCollectorManager(int k, IndexSearcher searcher) {
    return new DiversifyingNearestChildrenKnnCollectorManager(k, parentsFilter, searcher);
  }

  @Override
  protected TopDocs approximateSearch(
      LeafReaderContext context,
      AcceptDocs acceptDocs,
      int visitedLimit,
      KnnCollectorManager knnCollectorManager)
      throws IOException {
    FloatVectorValues.checkField(context.reader(), field);
    KnnCollector collector =
        knnCollectorManager.newCollector(visitedLimit, searchStrategy, context);
    if (collector == null) {
      return NO_RESULTS;
    }
    context.reader().searchNearestVectors(field, query, collector, acceptDocs);
    TopDocs results = collector.topDocs();
    if (!blockRescore || results.scoreDocs.length == 0) {
      return results;
    }
    BitSet parentBitSet = parentsFilter.getBitSet(context);
    if (parentBitSet == null) {
      return results;
    }
    FloatVectorValues vectorValues = context.reader().getFloatVectorValues(field);
    if (vectorValues == null) {
      return results;
    }
    VectorScorer scorer = vectorValues.scorer(query);
    if (scorer == null) {
      return results;
    }
    return blockRescore(results, acceptDocs, parentBitSet, scorer);
  }

  /**
   * For each parent already found by approximate search, scores all children in that parent's block
   * to ensure the truly best child is returned — not merely the sibling the graph traversal happened
   * to reach first. Children are processed in ascending docId order so the sequential {@link
   * VectorScorer} only advances forward. Extra nodes scored are added to {@link
   * TotalHits#value()}.
   *
   * <p>This method is package-private so that {@link DiversifyingChildrenByteKnnVectorQuery} can
   * reuse the same implementation rather than duplicating it.
   */
  static TopDocs blockRescore(
      TopDocs results, AcceptDocs acceptDocs, BitSet parentBitSet, VectorScorer scorer)
      throws IOException {
    Bits acceptBits = acceptDocs != null ? acceptDocs.bits() : null;
    DocIdSetIterator scorerIter = scorer.iterator();

    // Sort by docId so parent blocks are visited in ascending order — the forward-only
    // VectorScorer cannot go backwards.
    ScoreDoc[] scoreDocs = results.scoreDocs.clone();
    Arrays.sort(scoreDocs, Comparator.comparingInt(sd -> sd.doc));

    long extraVisited = 0;
    for (ScoreDoc scoreDoc : scoreDocs) {
      int parent = parentBitSet.nextSetBit(scoreDoc.doc);
      int prevParent = parent > 0 ? parentBitSet.prevSetBit(parent - 1) : -1;
      int hnswBestChild = scoreDoc.doc;
      // Score every sibling in [prevParent+1, parent) to find the block's true best.
      for (int child = prevParent + 1; child < parent; child++) {
        if (acceptBits != null && !acceptBits.get(child)) {
          continue;
        }
        if (scorerIter.advance(child) == child) {
          // Don't double-count the child HNSW already visited.
          if (child != hnswBestChild) {
            extraVisited++;
          }
          float s = scorer.score();
          if (s > scoreDoc.score) {
            scoreDoc.score = s;
            scoreDoc.doc = child;
          }
        }
      }
    }

    Arrays.sort(scoreDocs, (a, b) -> Float.compare(b.score, a.score));
    long totalVisited = results.totalHits.value() + extraVisited;
    return new TopDocs(new TotalHits(totalVisited, results.totalHits.relation()), scoreDocs);
  }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append(getClass().getSimpleName() + ":");
    buffer.append(this.field + "[" + query[0] + ",...]");
    buffer.append("[" + k + "]");
    if (this.filter != null) {
      buffer.append("[" + this.filter + "]");
    }
    return buffer.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    DiversifyingChildrenFloatKnnVectorQuery that = (DiversifyingChildrenFloatKnnVectorQuery) o;
    return k == that.k
        && blockRescore == that.blockRescore
        && Objects.equals(parentsFilter, that.parentsFilter)
        && Objects.equals(childFilter, that.childFilter)
        && Arrays.equals(query, that.query);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(super.hashCode(), parentsFilter, childFilter, k, blockRescore);
    result = 31 * result + Arrays.hashCode(query);
    return result;
  }

  static class DiversifyingChildrenVectorScorer {
    private final VectorScorer vectorScorer;
    private final DocIdSetIterator vectorIterator;
    private final DocIdSetIterator acceptedChildrenIterator;
    private final BitSet parentBitSet;
    private int currentParent = -1;
    private int bestChild = -1;
    private float currentScore = Float.NEGATIVE_INFINITY;

    protected DiversifyingChildrenVectorScorer(
        DocIdSetIterator acceptedChildrenIterator, BitSet parentBitSet, VectorScorer vectorScorer) {
      this.acceptedChildrenIterator = acceptedChildrenIterator;
      this.vectorScorer = vectorScorer;
      this.vectorIterator = vectorScorer.iterator();
      this.parentBitSet = parentBitSet;
    }

    public int bestChild() {
      return bestChild;
    }

    public int nextParent() throws IOException {
      int nextChild = acceptedChildrenIterator.docID();
      if (nextChild == -1) {
        nextChild = acceptedChildrenIterator.nextDoc();
      }
      if (nextChild == DocIdSetIterator.NO_MORE_DOCS) {
        currentParent = DocIdSetIterator.NO_MORE_DOCS;
        return currentParent;
      }
      currentScore = Float.NEGATIVE_INFINITY;
      currentParent = parentBitSet.nextSetBit(nextChild);
      do {
        vectorIterator.advance(nextChild);
        float score = vectorScorer.score();
        if (score > currentScore) {
          bestChild = nextChild;
          currentScore = score;
        }
      } while ((nextChild = acceptedChildrenIterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS
          && nextChild < currentParent);
      return currentParent;
    }

    public float score() throws IOException {
      return currentScore;
    }
  }
}
