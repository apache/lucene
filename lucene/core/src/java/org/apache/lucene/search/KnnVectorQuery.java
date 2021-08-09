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
import java.util.Objects;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.document.KnnVectorField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;

/** Uses {@link KnnVectorsReader#search} to perform nearest Neighbour search. */
public class KnnVectorQuery extends Query {

  private static final TopDocs NO_RESULTS =
      new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]);

  private final String field;
  private final float[] target;
  private final int k;

  /**
   * Find the <code>k</code> nearest documents to the target vector according to the vectors in the
   * given field. <code>target</code> vector.
   *
   * @param field a field that has been indexed as a {@link KnnVectorField}.
   * @param target the target of the search
   * @param k the number of documents to find
   * @throws IllegalArgumentException if <code>k</code> is less than 1
   */
  public KnnVectorQuery(String field, float[] target, int k) {
    this.field = field;
    this.target = target;
    this.k = k;
    if (k < 1) {
      throw new IllegalArgumentException("k must be at least 1, got: " + k);
    }
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    int boundedK = Math.min(k, reader.numDocs());
    TopDocs[] perLeafResults = new TopDocs[reader.leaves().size()];
    for (LeafReaderContext ctx : reader.leaves()) {
      // Calculate kPerLeaf as an overestimate of the expected number of the closest k documents in
      // this leaf
      int expectedKPerLeaf = Math.max(1, boundedK * ctx.reader().numDocs() / reader.numDocs());
      // Increase to include 3 std. deviations of a Binomial distribution.
      int kPerLeaf = (int) (expectedKPerLeaf + 3 * Math.sqrt(expectedKPerLeaf));
      perLeafResults[ctx.ord] = searchLeaf(ctx, kPerLeaf);
    }
    // Merge sort the results
    TopDocs topK = TopDocs.merge(boundedK, perLeafResults);
    // re-query any outlier segments (normally there should be none).
    topK = checkForOutlierSegments(reader, topK, perLeafResults);
    if (topK.scoreDocs.length == 0) {
      return new MatchNoDocsQuery();
    }
    return createRewrittenQuery(reader, topK);
  }

  private TopDocs searchLeaf(LeafReaderContext ctx, int kPerLeaf) throws IOException {
    TopDocs results = ctx.reader().searchNearestVectors(field, target, kPerLeaf);
    if (results == null) {
      return NO_RESULTS;
    }
    if (ctx.docBase > 0) {
      for (ScoreDoc scoreDoc : results.scoreDocs) {
        scoreDoc.doc += ctx.docBase;
      }
    }
    return results;
  }

  private TopDocs checkForOutlierSegments(IndexReader reader, TopDocs topK, TopDocs[] perLeaf)
      throws IOException {
    int k = topK.scoreDocs.length;
    if (k == 0) {
      return topK;
    }
    float minScore = topK.scoreDocs[topK.scoreDocs.length - 1].score;
    boolean rescored = false;
    for (int i = 0; i < perLeaf.length; i++) {
      if (perLeaf[i].scoreDocs[perLeaf[i].scoreDocs.length - 1].score >= minScore) {
        // This segment's worst score was competitive; search it again, gathering full K this time
        perLeaf[i] = searchLeaf(reader.leaves().get(i), topK.scoreDocs.length);
        rescored = true;
      }
    }
    if (rescored) {
      return TopDocs.merge(k, perLeaf);
    } else {
      return topK;
    }
  }

  private Query createRewrittenQuery(IndexReader reader, TopDocs topK) {
    int len = topK.scoreDocs.length;
    float minScore = topK.scoreDocs[len - 1].score;
    Arrays.sort(topK.scoreDocs, Comparator.comparingInt(a -> a.doc));
    int[] docs = new int[len];
    float[] scores = new float[len];
    for (int i = 0; i < len; i++) {
      docs[i] = topK.scoreDocs[i].doc;
      scores[i] = topK.scoreDocs[i].score - minScore; // flip negative scores
    }
    int[] segmentStarts = findSegmentStarts(reader, docs);
    return new DocAndScoreQuery(docs, scores, segmentStarts);
  }

  private int[] findSegmentStarts(IndexReader reader, int[] docs) {
    int[] starts = new int[reader.leaves().size() + 1];
    starts[starts.length - 1] = docs.length;
    if (starts.length == 2) {
      return starts;
    }
    int resultIndex = 0;
    for (int i = 1; i < starts.length - 1; i++) {
      int upper = reader.leaves().get(i).docBase;
      resultIndex = Arrays.binarySearch(docs, resultIndex, docs.length, upper);
      if (resultIndex < 0) {
        resultIndex = -1 - resultIndex;
      }
      starts[i] = resultIndex;
    }
    return starts;
  }

  @Override
  public String toString(String field) {
    return "<vector:" + this.field + "[" + target[0] + ",...][" + k + "]>";
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field)) {
      visitor.visitLeaf(this);
    }
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof KnnVectorQuery
        && ((KnnVectorQuery) obj).k == k
        && ((KnnVectorQuery) obj).field.equals(field)
        && Arrays.equals(((KnnVectorQuery) obj).target, target);
  }

  @Override
  public int hashCode() {
    return Objects.hash(field, k, Arrays.hashCode(target));
  }

  /** Caches the results of a KnnVector search: a list of docs and their scores */
  class DocAndScoreQuery extends Query {

    private final int[] docs;
    private final float[] scores;
    private final int[] segmentStarts;

    /**
     * Constructor
     *
     * @param docs the global docids of documents that match, in ascending order
     * @param scores the scores of the matching documents
     * @param segmentStarts the indexes in docs and scores corresponding to the first matching
     *     document in each segment. If a segment has no matching documents, it should be assigned
     *     the index of the next segment that does. There should be a final entry that is always
     *     docs.length-1.
     */
    DocAndScoreQuery(int[] docs, float[] scores, int[] segmentStarts) {
      this.docs = docs;
      this.scores = scores;
      this.segmentStarts = segmentStarts;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
        throws IOException {
      return new Weight(this) {
        @Override
        public Explanation explain(LeafReaderContext context, int doc) {
          int found = Arrays.binarySearch(docs, doc);
          if (found < 0) {
            return Explanation.noMatch("not in top " + k);
          }
          return Explanation.match(scores[found], "within top " + k);
        }

        @Override
        public Scorer scorer(LeafReaderContext context) {

          return new Scorer(this) {
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
            public float getMaxScore(int docid) {
              docid += context.docBase;
              float maxScore = 0;
              for (int idx = Math.max(0, upTo); idx < upper && docs[idx] <= docid; idx++) {
                maxScore = Math.max(maxScore, scores[idx]);
              }
              return maxScore;
            }

            @Override
            public float score() {
              if (upTo >= lower && upTo < upper) {
                return scores[upTo];
              }
              return 0;
            }

            /**
             * move the implementation of docIO() into a differently-named method so we can call it
             * from DocIDSetIterator.docID() even though this class is anonymous
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
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
          return true;
        }
      };
    }

    @Override
    public String toString(String field) {
      return KnnVectorQuery.this.toString();
    }

    @Override
    public void visit(QueryVisitor visitor) {
      KnnVectorQuery.this.visit(visitor);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof DocAndScoreQuery == false) {
        return false;
      }
      return Arrays.equals(docs, ((DocAndScoreQuery) obj).docs)
          && Arrays.equals(scores, ((DocAndScoreQuery) obj).scores);
    }

    @Override
    public int hashCode() {
      return Objects.hash(Arrays.hashCode(docs), Arrays.hashCode(scores));
    }
  }
}
