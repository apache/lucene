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
import org.apache.lucene.util.Bits;

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
    TopDocs[] perLeafResults = new TopDocs[reader.leaves().size()];
    for (LeafReaderContext ctx : reader.leaves()) {
      perLeafResults[ctx.ord] = searchLeaf(ctx, k);
    }
    // Merge sort the results
    TopDocs topK = TopDocs.merge(k, perLeafResults);
    if (topK.scoreDocs.length == 0) {
      return new MatchNoDocsQuery();
    }
    return createRewrittenQuery(reader, topK);
  }

  private TopDocs searchLeaf(LeafReaderContext ctx, int kPerLeaf) throws IOException {
    Bits liveDocs = ctx.reader().getLiveDocs();
    TopDocs results = ctx.reader().searchNearestVectors(field, target, kPerLeaf, liveDocs);
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

  private Query createRewrittenQuery(IndexReader reader, TopDocs topK) {
    int len = topK.scoreDocs.length;
    Arrays.sort(topK.scoreDocs, Comparator.comparingInt(a -> a.doc));
    int[] docs = new int[len];
    float[] scores = new float[len];
    for (int i = 0; i < len; i++) {
      docs[i] = topK.scoreDocs[i].doc;
      scores[i] = topK.scoreDocs[i].score;
    }
    int[] segmentStarts = findSegmentStarts(reader, docs);
    return new DocAndScoreQuery(k, docs, scores, segmentStarts, reader.hashCode());
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
    return getClass().getSimpleName() + ":" + this.field + "[" + target[0] + ",...][" + k + "]";
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field)) {
      visitor.visitLeaf(this);
    }
  }

  @Override
  public boolean equals(Object obj) {
    return sameClassAs(obj)
        && ((KnnVectorQuery) obj).k == k
        && ((KnnVectorQuery) obj).field.equals(field)
        && Arrays.equals(((KnnVectorQuery) obj).target, target);
  }

  @Override
  public int hashCode() {
    return Objects.hash(classHash(), field, k, Arrays.hashCode(target));
  }

  /** Caches the results of a KnnVector search: a list of docs and their scores */
  static class DocAndScoreQuery extends Query {

    private final int k;
    private final int[] docs;
    private final float[] scores;
    private final int[] segmentStarts;
    private final int readerHash;

    /**
     * Constructor
     *
     * @param k the number of documents requested
     * @param docs the global docids of documents that match, in ascending order
     * @param scores the scores of the matching documents
     * @param segmentStarts the indexes in docs and scores corresponding to the first matching
     *     document in each segment. If a segment has no matching documents, it should be assigned
     *     the index of the next segment that does. There should be a final entry that is always
     *     docs.length-1.
     * @param readerHash a hash code identifying the IndexReader used to create this query
     */
    DocAndScoreQuery(int k, int[] docs, float[] scores, int[] segmentStarts, int readerHash) {
      this.k = k;
      this.docs = docs;
      this.scores = scores;
      this.segmentStarts = segmentStarts;
      this.readerHash = readerHash;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
        throws IOException {
      if (searcher.getIndexReader().hashCode() != readerHash) {
        throw new IllegalStateException("This DocAndScore query was created by a different reader");
      }
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
              return scores[upTo];
            }

            @Override
            public int advanceShallow(int docid) {
              int start = Math.max(upTo, lower);
              int docidIndex = Arrays.binarySearch(docs, start, upper, docid + context.docBase);
              if (docidIndex < 0) {
                docidIndex = -1 - docidIndex;
              }
              if (docidIndex >= upper) {
                return NO_MORE_DOCS;
              }
              return docs[docidIndex];
            }

            /**
             * move the implementation of docID() into a differently-named method so we can call it
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
      return "DocAndScore[" + k + "]";
    }

    @Override
    public void visit(QueryVisitor visitor) {
      visitor.visitLeaf(this);
    }

    @Override
    public boolean equals(Object obj) {
      if (sameClassAs(obj) == false) {
        return false;
      }
      return Arrays.equals(docs, ((DocAndScoreQuery) obj).docs)
          && Arrays.equals(scores, ((DocAndScoreQuery) obj).scores);
    }

    @Override
    public int hashCode() {
      return Objects.hash(classHash(), Arrays.hashCode(docs), Arrays.hashCode(scores));
    }
  }
}
