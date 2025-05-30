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
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;

/**
 * Search for all (approximate) vectors above a similarity threshold.
 *
 * @lucene.experimental
 */
abstract class AbstractVectorSimilarityQuery extends Query {
  // TODO, switch to optionally use the new strategy
  static final KnnSearchStrategy.Hnsw DEFAULT_STRATEGY = new KnnSearchStrategy.Hnsw(0);
  protected final String field;
  protected final float traversalSimilarity, resultSimilarity;
  protected final Query filter;

  /**
   * Search for all (approximate) vectors above a similarity threshold using {@link
   * VectorSimilarityCollector}. If a filter is applied, it traverses as many nodes as the cost of
   * the filter, and then falls back to exact search if results are incomplete.
   *
   * @param field a field that has been indexed as a vector field.
   * @param traversalSimilarity (lower) similarity score for graph traversal.
   * @param resultSimilarity (higher) similarity score for result collection.
   * @param filter a filter applied before the vector search.
   */
  AbstractVectorSimilarityQuery(
      String field, float traversalSimilarity, float resultSimilarity, Query filter) {
    if (traversalSimilarity > resultSimilarity) {
      throw new IllegalArgumentException("traversalSimilarity should be <= resultSimilarity");
    }
    this.field = Objects.requireNonNull(field, "field");
    this.traversalSimilarity = traversalSimilarity;
    this.resultSimilarity = resultSimilarity;
    this.filter = filter;
  }

  protected KnnCollectorManager getKnnCollectorManager() {
    return (visitLimit, _, _) ->
        new VectorSimilarityCollector(traversalSimilarity, resultSimilarity, visitLimit);
  }

  abstract VectorScorer createVectorScorer(LeafReaderContext context) throws IOException;

  protected abstract TopDocs approximateSearch(
      LeafReaderContext context,
      Bits acceptDocs,
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
          // Return exhaustive results
          TopDocs results =
              approximateSearch(
                  context, liveDocs, Integer.MAX_VALUE, timeLimitingKnnCollectorManager);
          return VectorSimilarityScorerSupplier.fromScoreDocs(boost, results.scoreDocs);
        } else {
          Scorer scorer = filterWeight.scorer(context);
          if (scorer == null) {
            // If the filter does not match any documents
            return null;
          }

          BitSet acceptDocs;
          if (liveDocs == null && scorer.iterator() instanceof BitSetIterator bitSetIterator) {
            // If there are no deletions, and matching docs are already cached
            acceptDocs = bitSetIterator.getBitSet();
          } else {
            // Else collect all matching docs
            DocIdSetIterator iterator = scorer.iterator();
            final int maxDoc = leafReader.maxDoc();
            int threshold = maxDoc >> 7; // same as BitSet#of
            if (iterator.cost() >= threshold) {
              // take advantage of Disi#intoBitset and Bits#applyMask
              FixedBitSet bitSet = new FixedBitSet(maxDoc);
              bitSet.or(iterator);
              if (liveDocs != null) {
                liveDocs.applyMask(bitSet, 0);
              }
              acceptDocs = bitSet;
            } else {
              FilteredDocIdSetIterator filterIterator =
                  new FilteredDocIdSetIterator(iterator) {
                    @Override
                    protected boolean match(int doc) {
                      return liveDocs == null || liveDocs.get(doc);
                    }
                  };
              acceptDocs = BitSet.of(filterIterator, maxDoc); // create a sparse bitset
            }
          }

          int cardinality = acceptDocs.cardinality();
          if (cardinality == 0) {
            // If there are no live matching docs
            return null;
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
                boost,
                createVectorScorer(context),
                new BitSetIterator(acceptDocs, cardinality),
                resultSimilarity);
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
        && Float.compare(
                ((AbstractVectorSimilarityQuery) o).traversalSimilarity, traversalSimilarity)
            == 0
        && Float.compare(((AbstractVectorSimilarityQuery) o).resultSimilarity, resultSimilarity)
            == 0
        && Objects.equals(filter, ((AbstractVectorSimilarityQuery) o).filter);
  }

  @Override
  public int hashCode() {
    return Objects.hash(field, traversalSimilarity, resultSimilarity, filter);
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
