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
import java.util.Objects;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.Bits;

/**
 * Search for all (approximate) vectors above a similarity threshold.
 *
 * @lucene.experimental
 */
abstract class AbstractVectorSimilarityQuery extends Query {
  protected final String field;
  protected final float traversalSimilarity, resultSimilarity;
  protected final long visitLimit;

  /**
   * Search for all (approximate) vectors above a similarity threshold. First performs a
   * similarity-based graph search using {@link VectorSimilarityCollector} between {@link
   * #traversalSimilarity} and {@link #resultSimilarity}. If this does not complete within a
   * specified {@link #visitLimit}, returns a lazy-loading iterator over all vectors above the
   * {@link #resultSimilarity}.
   *
   * @param field a field that has been indexed as a vector field.
   * @param traversalSimilarity (lower) similarity score for graph traversal.
   * @param resultSimilarity (higher) similarity score for result collection.
   * @param visitLimit limit on number of nodes to visit before falling back to a lazy-loading
   *     iterator.
   */
  AbstractVectorSimilarityQuery(
      String field, float traversalSimilarity, float resultSimilarity, long visitLimit) {
    this.field = Objects.requireNonNull(field, "field");
    this.traversalSimilarity = traversalSimilarity;
    this.resultSimilarity = resultSimilarity;
    this.visitLimit = visitLimit;
  }

  abstract VectorScorer createVectorScorer(LeafReaderContext context) throws IOException;

  protected abstract void approximateSearch(LeafReaderContext context, KnnCollector collector)
      throws IOException;

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    return new Weight(this) {
      @Override
      public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        VectorScorer scorer = createVectorScorer(context);
        if (scorer == null) {
          return Explanation.noMatch("Not indexed as the correct vector field");
        } else if (scorer.advanceExact(doc)) {
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
      public Scorer scorer(LeafReaderContext context) throws IOException {
        @SuppressWarnings("resource")
        VectorSimilarityCollector collector =
            new VectorSimilarityCollector(
                traversalSimilarity, resultSimilarity, visitLimit, context.reader().maxDoc());

        // Perform an approximate search
        approximateSearch(context, collector);

        // Get collected hits in sorted order of docid
        ScoreDoc[] scoreDocs = collector.topDocs().scoreDocs;
        Arrays.sort(scoreDocs, Comparator.comparing(scoreDoc -> scoreDoc.doc));

        if (collector.earlyTerminated()) {
          VectorScorer scorer = createVectorScorer(context);
          Bits visited = collector.getVisited();

          // Return a lazy-loading iterator
          return new Scorer(this) {
            int index = 0;
            float cachedScore = 0;

            final DocIdSetIterator iterator =
                new FilteredDocIdSetIterator(scorer.iterator()) {
                  @Override
                  protected boolean match(int doc) throws IOException {
                    // Skip over docs which aren't needed
                    while (index < scoreDocs.length && scoreDocs[index].doc < doc) {
                      index++;
                    }

                    if (index < scoreDocs.length && scoreDocs[index].doc == doc) {
                      // If this doc has been collected as a result
                      cachedScore = scoreDocs[index].score;
                      return true;
                    } else if (visited.get(doc)) {
                      // Else if this doc has been visited (so not collected)
                      return false;
                    } else {
                      // Compute the dot product
                      cachedScore = scorer.score();
                      return cachedScore >= resultSimilarity;
                    }
                  }
                };

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
              return boost * cachedScore;
            }
          };
        } else {
          // Return an iterator over the collected results
          return new Scorer(this) {
            int index = -1;

            final DocIdSetIterator iterator =
                new DocIdSetIterator() {
                  @Override
                  public int docID() {
                    if (index < 0) {
                      return -1;
                    } else if (index >= scoreDocs.length) {
                      return NO_MORE_DOCS;
                    } else {
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
                            Comparator.comparing(scoreDoc -> scoreDoc.doc));
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
              return boost * scoreDocs[index].score;
            }
          };
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
        && field.equals(((AbstractVectorSimilarityQuery) o).field)
        && Float.compare(
                ((AbstractVectorSimilarityQuery) o).traversalSimilarity, traversalSimilarity)
            == 0
        && Float.compare(((AbstractVectorSimilarityQuery) o).resultSimilarity, resultSimilarity)
            == 0
        && visitLimit == ((AbstractVectorSimilarityQuery) o).visitLimit;
  }

  @Override
  public int hashCode() {
    return Objects.hash(field, traversalSimilarity, resultSimilarity, visitLimit);
  }
}
