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
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.QueryTimeout;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.util.Bits;

/**
 * Search for all (approximate) vectors above a similarity threshold using {@link
 * VectorSimilarityCollector}.
 *
 * <p>This class is package-private by design; callers construct one of the concrete public
 * subclasses ({@link FloatVectorSimilarityQuery} or {@link ByteVectorSimilarityQuery}), which
 * inherit shared public API such as {@link #getSearchStrategy()}. This mirrors the visibility
 * pattern of {@link AbstractKnnVectorQuery}.
 *
 * @lucene.experimental
 */
abstract class AbstractVectorSimilarityQuery extends Query {
  /**
   * Default search strategy used by similarity-threshold vector queries. Uses {@code
   * filteredSearchThreshold == 0}, which preserves this query's own filter-handling logic by never
   * delegating to HNSW's built-in filtered-search short-circuit.
   */
  static final KnnSearchStrategy.Hnsw DEFAULT_STRATEGY = new KnnSearchStrategy.Hnsw(0);

  static final float DECAY_MAX_APPROXIMATION = 0f;
  static final float DEFAULT_DECAY = 0.5f;
  static final float DECAY_MAX_QUALITY = 1f;

  protected final String field;
  protected final float resultSimilarity;
  protected final float decay;
  protected final Query filter;
  protected final KnnSearchStrategy searchStrategy;

  /**
   * Search for all (approximate) vectors above a similarity threshold using {@link
   * VectorSimilarityCollector}, with the {@linkplain #DEFAULT_STRATEGY default search strategy}. If
   * a filter is applied, it traverses as many nodes as the cost of the filter, and then falls back
   * to exact search if results are incomplete.
   *
   * @param field a field that has been indexed as a vector field.
   * @param resultSimilarity similarity score for result collection.
   * @param decay decay factor for graph traversal buffer.
   * @param filter a filter applied before the vector search.
   */
  AbstractVectorSimilarityQuery(String field, float resultSimilarity, float decay, Query filter) {
    this(field, resultSimilarity, decay, filter, DEFAULT_STRATEGY);
  }

  /**
   * Search for all (approximate) vectors above a similarity threshold using {@link
   * VectorSimilarityCollector}, with a caller-supplied {@link KnnSearchStrategy}. If a filter is
   * applied, it traverses as many nodes as the cost of the filter, and then falls back to exact
   * search if results are incomplete.
   *
   * @param field a field that has been indexed as a vector field.
   * @param resultSimilarity similarity score for result collection.
   * @param decay decay factor for graph traversal buffer.
   * @param filter a filter applied before the vector search.
   * @param searchStrategy the {@link KnnSearchStrategy} to use during graph search. If {@code
   *     null}, the {@linkplain #DEFAULT_STRATEGY default strategy} is used. The underlying format
   *     may not support all strategies and is free to ignore the requested strategy.
   */
  AbstractVectorSimilarityQuery(
      String field,
      float resultSimilarity,
      float decay,
      Query filter,
      KnnSearchStrategy searchStrategy) {
    if (Float.isNaN(resultSimilarity)) {
      throw new IllegalArgumentException(
          "resultSimilarity must have a valid value; got " + resultSimilarity);
    }

    if (Float.isNaN(decay)) {
      throw new IllegalArgumentException("decay must have a valid value; got " + decay);
    } else if (decay < DECAY_MAX_APPROXIMATION || decay > DECAY_MAX_QUALITY) {
      throw new IllegalArgumentException(
          "decay must lie in range [DECAY_MAX_APPROXIMATION = 0, DECAY_MAX_QUALITY = 1]; got "
              + decay);
    }

    this.field = Objects.requireNonNull(field, "field");
    this.resultSimilarity = resultSimilarity;
    this.decay = decay;
    this.filter = filter;
    this.searchStrategy = searchStrategy == null ? DEFAULT_STRATEGY : searchStrategy;
  }

  /**
   * Returns a {@link KnnCollectorManager} that always builds a {@link VectorSimilarityCollector}
   * configured with this query's {@link #searchStrategy}.
   *
   * <p>The manager's {@code strategy} and {@code LeafReaderContext} parameters are intentionally
   * ignored: top-k queries may override the strategy on a per-search basis, but similarity-
   * threshold queries pin the strategy to the query's own value at construction time so that {@link
   * #equals} / {@link #getSearchStrategy()} remain a faithful description of search behavior.
   */
  protected KnnCollectorManager getKnnCollectorManager() {
    return (visitLimit, _, _) ->
        new VectorSimilarityCollector(resultSimilarity, decay, visitLimit, searchStrategy);
  }

  /**
   * @return the {@link KnnSearchStrategy} used during graph search.
   */
  public KnnSearchStrategy getSearchStrategy() {
    return searchStrategy;
  }

  abstract VectorScorer createVectorScorer(LeafReaderContext context) throws IOException;

  protected abstract TopDocs approximateSearch(
      LeafReaderContext context,
      AcceptDocs acceptDocs,
      int visitLimit,
      KnnCollectorManager knnCollectorManager)
      throws IOException;

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    return new Weight(this) {
      final Weight filterWeight =
          filter == null
              ? null
              : searcher.createWeight(searcher.rewrite(filter), ScoreMode.COMPLETE_NO_SCORES, 1);

      final QueryTimeout queryTimeout = searcher.getTimeout();
      final TimeLimitingKnnCollectorManager timeLimitingKnnCollectorManager =
          new TimeLimitingKnnCollectorManager(getKnnCollectorManager(), queryTimeout);

      @Override
      public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        if (filterWeight != null) {
          Scorer filterScorer = filterWeight.scorer(context);
          if (filterScorer == null || filterScorer.iterator().advance(doc) > doc) {
            return Explanation.noMatch("Doc does not match the filter");
          }
        }

        VectorScorer scorer = createVectorScorer(context);
        if (scorer == null) {
          return Explanation.noMatch("Not indexed as the correct vector field");
        }
        DocIdSetIterator iterator = scorer.iterator();
        int docId = iterator.advance(doc);
        if (docId == doc) {
          float score = scorer.score();
          if (score >= resultSimilarity) {
            return Explanation.match(boost * score, "Score above threshold");
          } else {
            return Explanation.noMatch("Score below threshold");
          }
        } else {
          return Explanation.noMatch("No vector found for doc");
        }
      }

      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        LeafReader leafReader = context.reader();
        Bits liveDocs = leafReader.getLiveDocs();

        // If there is no filter
        if (filterWeight == null) {
          if (decay == DECAY_MAX_QUALITY) {
            // With DECAY_MAX_QUALITY, the intent is to find all vectors above
            // resultSimilarity. The approximate graph search may miss nodes,
            // so use exact search to guarantee completeness.
            AcceptDocs acceptDocs = AcceptDocs.fromLiveDocs(liveDocs, leafReader.maxDoc());
            return VectorSimilarityScorerSupplier.fromAcceptDocs(
                boost, createVectorScorer(context), acceptDocs.iterator(), resultSimilarity);
          }
          // Return results via approximate graph search
          TopDocs results =
              approximateSearch(
                  context,
                  AcceptDocs.fromLiveDocs(liveDocs, leafReader.maxDoc()),
                  Integer.MAX_VALUE,
                  timeLimitingKnnCollectorManager);
          return VectorSimilarityScorerSupplier.fromScoreDocs(boost, results.scoreDocs);
        } else {
          AcceptDocs acceptDocs =
              AcceptDocs.fromIteratorSupplier(
                  () -> {
                    Scorer scorer = filterWeight.scorer(context);
                    if (scorer == null) {
                      return DocIdSetIterator.empty();
                    } else {
                      return scorer.iterator();
                    }
                  },
                  liveDocs,
                  leafReader.maxDoc());

          int cardinality = acceptDocs.cost();
          if (cardinality == 0) {
            // If there are no live matching docs
            return null;
          }

          if (decay == DECAY_MAX_QUALITY) {
            // With DECAY_MAX_QUALITY, skip approximate search and go straight
            // to exact search over the filtered docs.
            return VectorSimilarityScorerSupplier.fromAcceptDocs(
                boost, createVectorScorer(context), acceptDocs.iterator(), resultSimilarity);
          }

          // Perform an approximate search
          TopDocs results =
              approximateSearch(context, acceptDocs, cardinality, timeLimitingKnnCollectorManager);

          if (results.totalHits.relation() == TotalHits.Relation.EQUAL_TO
              // Return partial results only when timeout is met
              || (queryTimeout != null && queryTimeout.shouldExit())) {
            // Return an iterator over the collected results
            return VectorSimilarityScorerSupplier.fromScoreDocs(boost, results.scoreDocs);
          } else {
            // Return a lazy-loading iterator
            return VectorSimilarityScorerSupplier.fromAcceptDocs(
                boost, createVectorScorer(context), acceptDocs.iterator(), resultSimilarity);
          }
        }
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return true;
      }
    };
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field)) {
      visitor.visitLeaf(this);
    }
  }

  @Override
  public boolean equals(Object o) {
    return sameClassAs(o)
        && Objects.equals(field, ((AbstractVectorSimilarityQuery) o).field)
        && Float.compare(((AbstractVectorSimilarityQuery) o).resultSimilarity, resultSimilarity)
            == 0
        && Float.compare(((AbstractVectorSimilarityQuery) o).decay, decay) == 0
        && Objects.equals(filter, ((AbstractVectorSimilarityQuery) o).filter)
        && Objects.equals(searchStrategy, ((AbstractVectorSimilarityQuery) o).searchStrategy);
  }

  @Override
  public int hashCode() {
    return Objects.hash(field, resultSimilarity, decay, filter, searchStrategy);
  }

  private static class VectorSimilarityScorerSupplier extends ScorerSupplier {
    final DocIdSetIterator iterator;
    final float[] cachedScore;

    VectorSimilarityScorerSupplier(DocIdSetIterator iterator, float[] cachedScore) {
      this.iterator = iterator;
      this.cachedScore = cachedScore;
    }

    static VectorSimilarityScorerSupplier fromScoreDocs(float boost, ScoreDoc[] scoreDocs) {
      if (scoreDocs.length == 0) {
        return null;
      }

      // Sort in ascending order of docid
      Arrays.sort(scoreDocs, Comparator.comparingInt(scoreDoc -> scoreDoc.doc));

      float[] cachedScore = new float[1];
      DocIdSetIterator iterator =
          new DocIdSetIterator() {
            int index = -1;

            @Override
            public int docID() {
              if (index < 0) {
                return -1;
              } else if (index >= scoreDocs.length) {
                return NO_MORE_DOCS;
              } else {
                cachedScore[0] = boost * scoreDocs[index].score;
                return scoreDocs[index].doc;
              }
            }

            @Override
            public int nextDoc() {
              index++;
              return docID();
            }

            @Override
            public int advance(int target) {
              index =
                  Arrays.binarySearch(
                      scoreDocs,
                      new ScoreDoc(target, 0),
                      Comparator.comparingInt(scoreDoc -> scoreDoc.doc));
              if (index < 0) {
                index = -1 - index;
              }
              return docID();
            }

            @Override
            public long cost() {
              return scoreDocs.length;
            }
          };

      return new VectorSimilarityScorerSupplier(iterator, cachedScore);
    }

    static VectorSimilarityScorerSupplier fromAcceptDocs(
        float boost, VectorScorer scorer, DocIdSetIterator acceptDocs, float threshold) {
      if (scorer == null) {
        return null;
      }

      float[] cachedScore = new float[1];
      DocIdSetIterator vectorIterator = scorer.iterator();
      DocIdSetIterator conjunction =
          ConjunctionDISI.createConjunction(List.of(vectorIterator, acceptDocs), List.of());
      DocIdSetIterator iterator =
          new FilteredDocIdSetIterator(conjunction) {
            @Override
            protected boolean match(int doc) throws IOException {
              // Advance the scorer
              assert doc == vectorIterator.docID();
              // Compute the dot product
              float score = scorer.score();
              cachedScore[0] = score * boost;
              return score >= threshold;
            }
          };

      return new VectorSimilarityScorerSupplier(iterator, cachedScore);
    }

    @Override
    public Scorer get(long leadCost) {
      return new Scorer() {
        @Override
        public int docID() {
          return iterator.docID();
        }

        @Override
        public DocIdSetIterator iterator() {
          return iterator;
        }

        @Override
        public float getMaxScore(int upTo) {
          return Float.POSITIVE_INFINITY;
        }

        @Override
        public float score() {
          return cachedScore[0];
        }
      };
    }

    @Override
    public long cost() {
      return iterator.cost();
    }
  }
}
