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
import java.util.concurrent.Callable;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorSimilarityFunction;
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
    // A value source that reads the inner scorer's score must be consumed at the scorer's current
    // position, so those cannot be prefetched ahead and are scored as we go.
    int originalCount =
        valuesSource.needsScores()
            ? rescoreInline(reader, weight, rewrittenValueSource, queue)
            : rescoreWithPrefetch(indexSearcher, reader, weight, rewrittenValueSource, queue);
    int i = 0;
    ScoreDoc[] scoreDocs = new ScoreDoc[queue.size()];
    for (ScoreDoc topDoc : queue) {
      scoreDocs[i++] = topDoc;
    }
    TopDocs topDocs =
        new TopDocs(new TotalHits(originalCount, TotalHits.Relation.EQUAL_TO), scoreDocs);
    return DocAndScoreQuery.createDocAndScoreQuery(reader, topDocs, 0);
  }

  private int rescoreInline(
      IndexReader reader, Weight weight, DoubleValuesSource rewrittenValueSource, HitQueue queue)
      throws IOException {
    int originalCount = 0;
    for (var leaf : reader.leaves()) {
      Scorer innerScorer = weight.scorer(leaf);
      if (innerScorer == null) {
        continue;
      }
      DoubleValues rescores = rewrittenValueSource.getValues(leaf, getDoubleValues(innerScorer));
      DocIdSetIterator iterator = innerScorer.iterator();
      while (iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
        rescoreInto(queue, rescores, leaf.docBase, iterator.docID());
        originalCount++;
      }
    }
    return originalCount;
  }

  /**
   * Starts the loads for every candidate before scoring any of them, so that more than one read is
   * in flight when the values live on slow storage. Each segment's prefetches are issued on the
   * searcher's executor: prefetching costs real CPU per candidate, so issuing a whole shortlist
   * from a single thread caps how many reads can be outstanding. Only doc ids are buffered, never
   * values.
   */
  private int rescoreWithPrefetch(
      IndexSearcher indexSearcher,
      IndexReader reader,
      Weight weight,
      DoubleValuesSource rewrittenValueSource,
      HitQueue queue)
      throws IOException {
    final List<LeafReaderContext> leaves = reader.leaves();
    final DoubleValues[] leafValues = new DoubleValues[leaves.size()];
    final int[][] leafDocs = new int[leaves.size()][];
    final List<Callable<Void>> tasks = new ArrayList<>(leaves.size());
    for (int i = 0; i < leaves.size(); i++) {
      final int idx = i;
      final LeafReaderContext leaf = leaves.get(i);
      tasks.add(
          () -> {
            Scorer innerScorer = weight.scorer(leaf);
            if (innerScorer == null) {
              return null;
            }
            DoubleValues rescores = rewrittenValueSource.getValues(leaf, null);
            DocIdSetIterator iterator = innerScorer.iterator();
            int[] docs = new int[16];
            int count = 0;
            while (iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
              int docId = iterator.docID();
              rescores.prefetch(docId);
              if (count == docs.length) {
                docs = ArrayUtil.grow(docs, count + 1);
              }
              docs[count++] = docId;
            }
            leafValues[idx] = rescores;
            leafDocs[idx] = ArrayUtil.copyOfSubArray(docs, 0, count);
            return null;
          });
    }
    indexSearcher.getTaskExecutor().invokeAll(tasks);

    int originalCount = 0;
    for (int i = 0; i < leaves.size(); i++) {
      int[] docs = leafDocs[i];
      if (docs == null) {
        continue;
      }
      int docBase = leaves.get(i).docBase;
      DoubleValues values = leafValues[i];
      for (int doc : docs) {
        rescoreInto(queue, values, docBase, doc);
        originalCount++;
      }
    }
    return originalCount;
  }

  private static void rescoreInto(HitQueue queue, DoubleValues values, int docBase, int docId)
      throws IOException {
    if (values.advanceExact(docId)) {
      queue.insertWithOverflow(new ScoreDoc(docBase + docId, (float) values.doubleValue()));
    } else {
      queue.insertWithOverflow(new ScoreDoc(docBase + docId, 0f));
    }
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
