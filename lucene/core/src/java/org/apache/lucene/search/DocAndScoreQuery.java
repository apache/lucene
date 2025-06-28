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
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;

/** A query that wraps precomputed documents and scores */
class DocAndScoreQuery extends Query {

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
   *     document in each segment. If a segment has no matching documents, it should be assigned the
   *     index of the next segment that does. There should be a final entry that is always
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

  static Query createDocAndScoreQuery(IndexReader reader, TopDocs topK, int reentryCount) {
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
