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
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;

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
public class KnnVectorQuery extends Query {

  private static final TopDocs NO_RESULTS = TopDocsCollector.EMPTY_TOPDOCS;

  private final String field;
  private final float[] target;
  private final int k;
  private final Query filter;

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
    this(field, target, k, null);
  }

  /**
   * Find the <code>k</code> nearest documents to the target vector according to the vectors in the
   * given field. <code>target</code> vector.
   *
   * @param field a field that has been indexed as a {@link KnnVectorField}.
   * @param target the target of the search
   * @param k the number of documents to find
   * @param filter a filter applied before the vector search
   * @throws IllegalArgumentException if <code>k</code> is less than 1
   */
  public KnnVectorQuery(String field, float[] target, int k, Query filter) {
    this.field = field;
    this.target = target;
    this.k = k;
    if (k < 1) {
      throw new IllegalArgumentException("k must be at least 1, got: " + k);
    }
    this.filter = filter;
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    IndexReader reader = indexSearcher.getIndexReader();
    TopDocs[] perLeafResults = new TopDocs[reader.leaves().size()];

    Weight filterWeight = null;
    if (filter != null) {
      BooleanQuery booleanQuery =
          new BooleanQuery.Builder()
              .add(filter, BooleanClause.Occur.FILTER)
              .add(new FieldExistsQuery(field), BooleanClause.Occur.FILTER)
              .build();
      Query rewritten = indexSearcher.rewrite(booleanQuery);
      filterWeight = indexSearcher.createWeight(rewritten, ScoreMode.COMPLETE_NO_SCORES, 1f);
    }

    for (LeafReaderContext ctx : reader.leaves()) {
      TopDocs results = searchLeaf(ctx, filterWeight);
      if (ctx.docBase > 0) {
        for (ScoreDoc scoreDoc : results.scoreDocs) {
          scoreDoc.doc += ctx.docBase;
        }
      }
      perLeafResults[ctx.ord] = results;
    }
    // Merge sort the results
    TopDocs topK = TopDocs.merge(k, perLeafResults);
    if (topK.scoreDocs.length == 0) {
      return new MatchNoDocsQuery();
    }
    return createRewrittenQuery(reader, topK);
  }

  private TopDocs searchLeaf(LeafReaderContext ctx, Weight filterWeight) throws IOException {
    Bits liveDocs = ctx.reader().getLiveDocs();
    int maxDoc = ctx.reader().maxDoc();

    if (filterWeight == null) {
      return approximateSearch(ctx, liveDocs, Integer.MAX_VALUE);
    }

    Scorer scorer = filterWeight.scorer(ctx);
    if (scorer == null) {
      return NO_RESULTS;
    }

    BitSet acceptDocs = createBitSet(scorer.iterator(), liveDocs, maxDoc);
    int cost = acceptDocs.cardinality();

    if (cost <= k) {
      // If there are <= k possible matches, short-circuit and perform exact search, since HNSW
      // must always visit at least k documents
      return exactSearch(ctx, new BitSetIterator(acceptDocs, cost));
    }

    // Perform the approximate kNN search
    TopDocs results = approximateSearch(ctx, acceptDocs, cost);
    if (results.totalHits.relation == TotalHits.Relation.EQUAL_TO) {
      return results;
    } else {
      // We stopped the kNN search because it visited too many nodes, so fall back to exact search
      return exactSearch(ctx, new BitSetIterator(acceptDocs, cost));
    }
  }

  private BitSet createBitSet(DocIdSetIterator iterator, Bits liveDocs, int maxDoc)
      throws IOException {
    if (liveDocs == null && iterator instanceof BitSetIterator bitSetIterator) {
      // If we already have a BitSet and no deletions, reuse the BitSet
      return bitSetIterator.getBitSet();
    } else {
      // Create a new BitSet from matching and live docs
      FilteredDocIdSetIterator filterIterator =
          new FilteredDocIdSetIterator(iterator) {
            @Override
            protected boolean match(int doc) {
              return liveDocs == null || liveDocs.get(doc);
            }
          };
      return BitSet.of(filterIterator, maxDoc);
    }
  }

  private TopDocs approximateSearch(LeafReaderContext context, Bits acceptDocs, int visitedLimit)
      throws IOException {
    TopDocs results =
        context.reader().searchNearestVectors(field, target, k, acceptDocs, visitedLimit);
    return results != null ? results : NO_RESULTS;
  }

  // We allow this to be overridden so that tests can check what search strategy is used
  protected TopDocs exactSearch(LeafReaderContext context, DocIdSetIterator acceptIterator)
      throws IOException {
    FieldInfo fi = context.reader().getFieldInfos().fieldInfo(field);
    if (fi == null || fi.getVectorDimension() == 0) {
      // The field does not exist or does not index vectors
      return NO_RESULTS;
    }

    VectorScorer vectorScorer = VectorScorer.create(context, fi, target);
    HitQueue queue = new HitQueue(k, true);
    ScoreDoc topDoc = queue.top();
    int doc;
    while ((doc = acceptIterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      boolean advanced = vectorScorer.advanceExact(doc);
      assert advanced;

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

    TotalHits totalHits = new TotalHits(acceptIterator.cost(), TotalHits.Relation.EQUAL_TO);
    return new TopDocs(totalHits, topScoreDocs);
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
    return new DocAndScoreQuery(k, docs, scores, segmentStarts, reader.getContext().id());
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
    if (sameClassAs(obj) == false) {
      return false;
    }
    return ((KnnVectorQuery) obj).k == k
        && ((KnnVectorQuery) obj).field.equals(field)
        && Arrays.equals(((KnnVectorQuery) obj).target, target)
        && Objects.equals(filter, ((KnnVectorQuery) obj).filter);
  }

  @Override
  public int hashCode() {
    return Objects.hash(classHash(), field, k, Arrays.hashCode(target), filter);
  }

  /** Caches the results of a KnnVector search: a list of docs and their scores */
  static class DocAndScoreQuery extends Query {

    private final int k;
    private final int[] docs;
    private final float[] scores;
    private final int[] segmentStarts;
    private final Object contextIdentity;

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
     * @param contextIdentity an object identifying the reader context that was used to build this
     *     query
     */
    DocAndScoreQuery(
        int k, int[] docs, float[] scores, int[] segmentStarts, Object contextIdentity) {
      this.k = k;
      this.docs = docs;
      this.scores = scores;
      this.segmentStarts = segmentStarts;
      this.contextIdentity = contextIdentity;
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
            return Explanation.noMatch("not in top " + k);
          }
          return Explanation.match(scores[found] * boost, "within top " + k);
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
            public float getMaxScore(int docId) {
              docId += context.docBase;
              float maxScore = 0;
              for (int idx = Math.max(0, upTo); idx < upper && docs[idx] <= docId; idx++) {
                maxScore = Math.max(maxScore, scores[idx]);
              }
              return maxScore * boost;
            }

            @Override
            public float score() {
              return scores[upTo] * boost;
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
}
