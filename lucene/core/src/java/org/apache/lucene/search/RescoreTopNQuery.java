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
import java.util.List;
import java.util.Objects;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.VectorBatch;
import org.apache.lucene.util.ArrayUtil;

/**
 * A Query that re-scores another Query with a {@link DoubleValuesSource} function and cut-off the
 * results at top N. Unlike {@link Rescorer} which does rescoring at post-collection phase, this
 * Query does the rescoring at rewrite() phase. The reason it operates in rewrite phase is to be
 * compatible with KNN vector query, where the results are collected upfront, but it can work with
 * any type of Query. Unlike or <code>FunctionScoreQuery</code>, this Query will work even with the
 * no-scoring {@link ScoreMode}.
 *
 * @lucene.experimental
 */
public class RescoreTopNQuery extends Query {

  private final int n;
  private final Query query;
  private final DoubleValuesSource valuesSource;

  /**
   * Execute the inner Query, re-score using a customizable DoubleValueSource and trim down the
   * result to k
   *
   * @param query the query to execute as initial phase
   * @param valuesSource the double value source to re-score
   * @param n the number of documents to find
   * @throws IllegalArgumentException if <code>n</code> is less than 1
   */
  public RescoreTopNQuery(Query query, DoubleValuesSource valuesSource, int n) {
    if (n < 1) {
      throw new IllegalArgumentException("n must be >= 1");
    }
    this.query = query;
    this.valuesSource = valuesSource;
    this.n = n;
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    DoubleValuesSource rewrittenValueSource = valuesSource.rewrite(indexSearcher);
    IndexReader reader = indexSearcher.getIndexReader();
    Query rewritten = indexSearcher.rewrite(query);
    Weight weight = indexSearcher.createWeight(rewritten, ScoreMode.COMPLETE_NO_SCORES, 1.0f);
    HitQueue queue = new HitQueue(n, false);
    int originalCount = 0;
    // A rerank shortlist is spread over every segment, so on its own each segment contributes only
    // a fraction of the query's reads. When the store can gather reads across segment files, queue
    // every segment's shortlist and issue one submission for the whole query rather than one per
    // segment. The batch is opened from the first segment that has hits, since an arbitrary segment
    // may hold no values for the field and so cannot supply one.
    VectorBatch batch = null;
    List<PendingLeaf> pendingLeaves = new ArrayList<>();
    for (var leaf : reader.leaves()) {
      Scorer innerScorer = weight.scorer(leaf);
      if (innerScorer == null) {
        continue;
      }
      DoubleValues rescores = rewrittenValueSource.getValues(leaf, getDoubleValues(innerScorer));
      DocIdSetIterator iterator = innerScorer.iterator();
      if (rewrittenValueSource instanceof FullPrecisionFloatVectorSimilarityValuesSource vsrc) {
        int[] docs = new int[64];
        int count = 0;
        for (int docId = iterator.nextDoc();
            docId != DocIdSetIterator.NO_MORE_DOCS;
            docId = iterator.nextDoc()) {
          if (count == docs.length) docs = ArrayUtil.grow(docs);
          docs[count++] = docId;
        }
        if (batch == null && count > 0) {
          batch = vsrc.newVectorBatch(leaf); // null when this store cannot batch across segments
        }
        // Queue this segment's shortlist; nothing is read until every segment has been queued and
        // the batch executes below. A store with no batch support leaves us on the per-doc path.
        var pending = batch == null ? null : vsrc.queueShortlist(leaf, docs, count, batch);
        if (pending != null) {
          pendingLeaves.add(new PendingLeaf(leaf.docBase, docs, count, pending));
        } else {
          for (int j = 0; j < count; j++) {
            originalCount += insertRescored(rescores, queue, leaf.docBase, docs[j]);
          }
        }
      } else {
        while (iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
          originalCount += insertRescored(rescores, queue, leaf.docBase, iterator.docID());
        }
      }
    }
    if (batch != null) {
      batch.execute(); // the query's only submission: every segment's shortlist at once
      // Insert in leaf order, so the queue sees hits in exactly the order it would have anyway.
      for (PendingLeaf pl : pendingLeaves) {
        float[] scores = pl.pending().scores();
        for (int j = 0; j < pl.count(); j++) {
          queue.insertWithOverflow(new ScoreDoc(pl.docBase() + pl.docs()[j], scores[j]));
          originalCount++;
        }
      }
    }
    int i = 0;
    ScoreDoc[] scoreDocs = new ScoreDoc[queue.size()];
    for (ScoreDoc topDoc : queue) {
      scoreDocs[i++] = topDoc;
    }
    TopDocs topDocs =
        new TopDocs(new TotalHits(originalCount, TotalHits.Relation.EQUAL_TO), scoreDocs);
    return DocAndScoreQuery.createDocAndScoreQuery(reader, topDocs, 0);
  }

  /** One segment's shortlist, queued in the batch and waiting to be scored. */
  private record PendingLeaf(
      int docBase,
      int[] docs,
      int count,
      FullPrecisionFloatVectorSimilarityValuesSource.Pending pending) {}

  private static int insertRescored(DoubleValues rescores, HitQueue queue, int docBase, int docId)
      throws IOException {
    if (rescores.advanceExact(docId)) {
      queue.insertWithOverflow(new ScoreDoc(docBase + docId, (float) rescores.doubleValue()));
    } else {
      queue.insertWithOverflow(new ScoreDoc(docBase + docId, 0f));
    }
    return 1;
  }

  private DoubleValues getDoubleValues(Scorer innerScorer) {
    // if the value source doesn't need document score to compute value, return null
    if (valuesSource.needsScores() == false) {
      return null;
    }
    return DoubleValuesSource.fromScorer(innerScorer);
  }

  @Override
  public int hashCode() {
    int result = valuesSource.hashCode();
    result = 31 * result + Objects.hash(query, n);
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RescoreTopNQuery that = (RescoreTopNQuery) o;
    return Objects.equals(query, that.query)
        && Objects.equals(valuesSource, that.valuesSource)
        && n == that.n;
  }

  @Override
  public void visit(QueryVisitor visitor) {
    query.visit(visitor);
  }

  @Override
  public String toString(String field) {
    return getClass().getSimpleName()
        + ":"
        + query.toString(field)
        + ":"
        + valuesSource.toString()
        + "["
        + n
        + "]";
  }

  /**
   * Utility method to create a new RescoreTopNQuery which uses full-precision vectors for
   * rescoring.
   *
   * @param in the inner Query to rescore
   * @param targetVector the target vector to compute score
   * @param field the vector field to compute score
   * @param n the number of results to keep
   * @return the RescoreTopNQuery
   */
  public static Query createFullPrecisionRescorerQuery(
      Query in, float[] targetVector, String field, int n) {
    DoubleValuesSource valuaSource =
        new FullPrecisionFloatVectorSimilarityValuesSource(targetVector, field);
    return new RescoreTopNQuery(in, valuaSource, n);
  }

  /**
   * Creates a {@code RescoreTopNQuery} that computes top N results using multi-vector similarity
   * comparisons against a late interaction field.
   *
   * <p>Note: This query computes late interaction field similarity for the entire match-set of
   * wrapped query, and returns a new query with only top-N hits in the match-set. This is typically
   * useful in combining a query's results with other queries for hybrid search. To simply rerank
   * the top N hits without scoring entire match-set, see {@link LateInteractionRescorer}.
   *
   * @param in the inner Query to rescore
   * @param n number of results to keep
   * @param fieldName the {@link org.apache.lucene.document.LateInteractionField} for recomputing
   *     top N hits
   * @param queryVector query multi-vector to use for similarity comparisons
   * @param vectorSimilarityFunction function to use for vector similarity comparisons.
   */
  public static Query createLateInteractionQuery(
      Query in,
      int n,
      String fieldName,
      float[][] queryVector,
      VectorSimilarityFunction vectorSimilarityFunction) {
    final LateInteractionFloatValuesSource valuesSource =
        new LateInteractionFloatValuesSource(fieldName, queryVector, vectorSimilarityFunction);
    return new RescoreTopNQuery(in, valuesSource, n);
  }
}
