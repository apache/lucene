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
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.Callable;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.QueryTimeout;
import org.apache.lucene.internal.hppc.IntObjectHashMap;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOSupplier;

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
      AcceptDocs acceptDocs,
      int visitLimit,
      KnnCollectorManager knnCollectorManager)
      throws IOException;

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {

    final Weight filterWeight;
    if (filter != null) {
      Query rewrittenFilter = filter.rewrite(searcher);
      if (rewrittenFilter instanceof MatchAllDocsQuery) {
        // same as no filter
        filterWeight = null;
      } else {
        final Query filterQuery =
            new BooleanQuery.Builder()
                .add(rewrittenFilter, BooleanClause.Occur.FILTER)
                .add(new FieldExistsQuery(field), BooleanClause.Occur.FILTER)
                .build()
                .rewrite(searcher);
        filterWeight = searcher.createWeight(filterQuery, ScoreMode.COMPLETE_NO_SCORES, 1f);
      }
    } else {
      filterWeight = null;
    }

    final QueryTimeout queryTimeout = searcher.getTimeout();
    final TimeLimitingKnnCollectorManager timeLimitingKnnCollectorManager =
        new TimeLimitingKnnCollectorManager(getKnnCollectorManager(), queryTimeout);

    final List<LeafReaderContext> contexts = searcher.getLeafContexts();
    final IntObjectHashMap<AcceptDocs> acceptDocsList = new IntObjectHashMap<>(contexts.size());
    final List<Callable<TopDocs>> tasks = new ArrayList<>(contexts.size());
    for (LeafReaderContext context : contexts) {
      @SuppressWarnings("resource")
      final LeafReader leafReader = context.reader();
      final Bits liveDocs = leafReader.getLiveDocs();
      final int maxDoc = leafReader.maxDoc();

      final AcceptDocs acceptDocs;
      final int visitLimit;
      if (filterWeight == null) {
        acceptDocs = AcceptDocs.fromLiveDocs(liveDocs, maxDoc);
        visitLimit = Integer.MAX_VALUE;
      } else {
        final IOSupplier<DocIdSetIterator> filteredDocs =
            () -> {
              final Scorer filterScorer = filterWeight.scorer(context);
              if (filterScorer == null) {
                return DocIdSetIterator.empty();
              }
              return filterScorer.iterator();
            };
        acceptDocs = AcceptDocs.fromIteratorSupplier(filteredDocs, liveDocs, maxDoc);
        visitLimit = acceptDocs.cost();
      }
      acceptDocsList.put(context.ord, acceptDocs);

      if (visitLimit == 0) {
        // If there are no live matching docs
        tasks.add(() -> TopDocsCollector.EMPTY_TOPDOCS);
      } else {
        // Perform an approximate search
        tasks.add(
            () ->
                approximateSearch(
                    context, acceptDocs, visitLimit, timeLimitingKnnCollectorManager));
      }
    }
    final List<TopDocs> completed = searcher.getTaskExecutor().invokeAll(tasks);
    final IntObjectHashMap<TopDocs> results = new IntObjectHashMap<>(contexts.size());
    for (int i = 0; i < contexts.size(); i++) {
      results.put(contexts.get(i).ord, completed.get(i));
    }

    return new Weight(this) {
      @Override
      public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        Scorer scorer = scorer(context);
        if (scorer != null && scorer.iterator().advance(doc) == doc) {
          float score = scorer.score();
          if (scorer instanceof CachedScorer) {
            return Explanation.match(
                scorer.score(),
                String.format(
                    Locale.ROOT, "Doc found from approximate search with score=%f", score));
          } else if (scorer instanceof ExactScorer) {
            return Explanation.match(
                scorer.score(),
                String.format(
                    Locale.ROOT,
                    "Doc found from exact search with score=%f, after approximate search fallback",
                    score));
          }
        }

        if (filterWeight != null) {
          Explanation filterExplanation = filterWeight.explain(context, doc);
          if (filterExplanation.isMatch() == false) {
            return Explanation.noMatch(
                String.format(Locale.ROOT, "Doc did not match filter=%s", filter),
                filterExplanation);
          }
        }

        VectorScorer vectorScorer = createVectorScorer(context);
        if (vectorScorer == null) {
          return Explanation.noMatch(
              String.format(
                  Locale.ROOT, "field=%s not indexed with the correct vector type", field));
        } else if (vectorScorer.iterator().advance(doc) == doc) {
          float score = vectorScorer.score();
          if (score >= resultSimilarity) {
            return Explanation.noMatch(
                String.format(
                    Locale.ROOT,
                    "Doc should have matched with score=%f, but missed from approximate search",
                    score));
          } else {
            return Explanation.noMatch(
                String.format(Locale.ROOT, "Doc with score=%f below similarity threshold", score));
          }
        } else {
          return Explanation.noMatch("Doc does not have a vector");
        }
      }

      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        final Scorer scorer;
        final TopDocs result = results.get(context.ord);
        if (result.totalHits.relation() == TotalHits.Relation.EQUAL_TO
            // Return partial results only when timeout is met
            || (queryTimeout != null && queryTimeout.shouldExit())) {
          if (result.scoreDocs.length == 0) {
            return null;
          }
          // Return cached results
          scorer = new CachedScorer(result.scoreDocs, boost);
        } else {
          // Return a lazy-loading iterator
          AcceptDocs acceptDocs = acceptDocsList.get(context.ord);
          scorer =
              new ExactScorer(acceptDocs, createVectorScorer(context), resultSimilarity, boost);
        }
        return new DefaultScorerSupplier(scorer);
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

  private static class CachedScorer extends Scorer {
    private final DocIdSetIterator iterator;
    private final float[] currentScore;

    private CachedScorer(ScoreDoc[] scoreDocs, float boost) {
      this.currentScore = new float[1];
      this.iterator =
          new DocIdSetIterator() {
            int index = -1;

            @Override
            public int docID() {
              if (index < 0) {
                return -1;
              } else if (index >= scoreDocs.length) {
                return NO_MORE_DOCS;
              } else {
                currentScore[0] = scoreDocs[index].score * boost;
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
                      new ScoreDoc(target, Float.NaN),
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
    }

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
      return currentScore[0];
    }
  }

  private static class ExactScorer extends Scorer {
    private final DocIdSetIterator iterator;
    private final float[] currentScore;

    private ExactScorer(
        AcceptDocs acceptDocs, VectorScorer vectorScorer, float resultSimilarity, float boost)
        throws IOException {
      this.currentScore = new float[1];
      DocIdSetIterator conjunction =
          ConjunctionDISI.createConjunction(
              List.of(acceptDocs.iterator(), vectorScorer.iterator()), List.of());
      this.iterator =
          new FilteredDocIdSetIterator(conjunction) {
            @Override
            protected boolean match(int doc) throws IOException {
              float score = vectorScorer.score();
              if (score >= resultSimilarity) {
                currentScore[0] = score * boost;
                return true;
              }
              return false;
            }
          };
    }

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
      return currentScore[0];
    }
  }
}
