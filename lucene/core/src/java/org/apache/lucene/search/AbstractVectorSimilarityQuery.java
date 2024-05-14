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
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;

/**
 * Search for all (approximate) vectors above a similarity threshold.
 *
 * @lucene.experimental
 */
abstract class AbstractVectorSimilarityQuery extends Query {
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

  abstract VectorScorer createVectorScorer(LeafReaderContext context) throws IOException;

  protected abstract TopDocs approximateSearch(
      LeafReaderContext context, Bits acceptDocs, int visitLimit) throws IOException;

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    return new Weight(this) {
      final Weight filterWeight =
          filter == null
              ? null
              : searcher.createWeight(searcher.rewrite(filter), ScoreMode.COMPLETE_NO_SCORES, 1);

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
      public Scorer scorer(LeafReaderContext context) throws IOException {
        @SuppressWarnings("resource")
        LeafReader leafReader = context.reader();
        Bits liveDocs = leafReader.getLiveDocs();

        // If there is no filter
        if (filterWeight == null) {
          // Return exhaustive results
          TopDocs results = approximateSearch(context, liveDocs, Integer.MAX_VALUE);
          if (results.scoreDocs.length == 0) {
            return null;
          }
          return VectorSimilarityScorer.fromScoreDocs(this, boost, results.scoreDocs);
        }

        Scorer scorer = filterWeight.scorer(context);
        if (scorer == null) {
          // If the filter does not match any documents
          return null;
        }

        BitSet acceptDocs;
        if (liveDocs == null && scorer.iterator() instanceof BitSetIterator) {
          BitSetIterator bitSetIterator = (BitSetIterator) scorer.iterator();
          // If there are no deletions, and matching docs are already cached
          acceptDocs = bitSetIterator.getBitSet();
        } else {
          // Else collect all matching docs
          FilteredDocIdSetIterator filtered =
              new FilteredDocIdSetIterator(scorer.iterator()) {
                @Override
                protected boolean match(int doc) {
                  return liveDocs == null || liveDocs.get(doc);
                }
              };
          acceptDocs = BitSet.of(filtered, leafReader.maxDoc());
        }

        int cardinality = acceptDocs.cardinality();
        if (cardinality == 0) {
          // If there are no live matching docs
          return null;
        }

        // Perform an approximate search
        TopDocs results = approximateSearch(context, acceptDocs, cardinality);

        // If the limit was exhausted
        if (results.totalHits.relation == TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO) {
          // Return a lazy-loading iterator
          return VectorSimilarityScorer.fromAcceptDocs(
              this,
              boost,
              createVectorScorer(context),
              new BitSetIterator(acceptDocs, cardinality),
              resultSimilarity);
        } else if (results.scoreDocs.length == 0) {
          return null;
        } else {
          // Return an iterator over the collected results
          return VectorSimilarityScorer.fromScoreDocs(this, boost, results.scoreDocs);
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

  private static class VectorSimilarityScorer extends Scorer {
    final DocIdSetIterator iterator;
    final float[] cachedScore;

    VectorSimilarityScorer(Weight weight, DocIdSetIterator iterator, float[] cachedScore) {
      super(weight);
      this.iterator = iterator;
      this.cachedScore = cachedScore;
    }

    static VectorSimilarityScorer fromScoreDocs(Weight weight, float boost, ScoreDoc[] scoreDocs) {
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

      return new VectorSimilarityScorer(weight, iterator, cachedScore);
    }

    static VectorSimilarityScorer fromAcceptDocs(
        Weight weight,
        float boost,
        VectorScorer scorer,
        DocIdSetIterator acceptDocs,
        float threshold) {
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

      return new VectorSimilarityScorer(weight, iterator, cachedScore);
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
      return cachedScore[0];
    }
  }
}
