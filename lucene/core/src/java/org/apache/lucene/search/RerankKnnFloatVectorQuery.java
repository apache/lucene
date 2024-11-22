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

import static org.apache.lucene.search.AbstractKnnVectorQuery.createRewrittenQuery;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.VectorSimilarityFunction;

/**
 * A wrapper of KnnFloatVectorQuery which does full-precision reranking.
 *
 * @lucene.experimental
 */
public class RerankKnnFloatVectorQuery extends Query {

  private final int k;
  private final float[] target;
  private final KnnFloatVectorQuery query;

  /**
   * Execute the KnnFloatVectorQuery and re-rank using full-precision vectors
   *
   * @param query the KNN query to execute as initial phase
   * @param target the target of the search
   * @param k the number of documents to find
   * @throws IllegalArgumentException if <code>k</code> is less than 1
   */
  public RerankKnnFloatVectorQuery(KnnFloatVectorQuery query, float[] target, int k) {
    this.query = query;
    this.target = target;
    this.k = k;
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    IndexReader reader = indexSearcher.getIndexReader();
    Query rewritten = indexSearcher.rewrite(query);
    Weight weight = indexSearcher.createWeight(rewritten, ScoreMode.COMPLETE_NO_SCORES, 1.0f);
    HitQueue queue = new HitQueue(k, false);
    for (var leaf : reader.leaves()) {
      Scorer scorer = weight.scorer(leaf);
      if (scorer == null) {
        continue;
      }
      FloatVectorValues floatVectorValues = leaf.reader().getFloatVectorValues(query.getField());
      if (floatVectorValues == null) {
        continue;
      }
      FieldInfo fi = leaf.reader().getFieldInfos().fieldInfo(query.getField());
      if (fi == null) {
        continue;
      }
      VectorSimilarityFunction comparer = fi.getVectorSimilarityFunction();
      DocIdSetIterator iterator = scorer.iterator();
      while (iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
        int docId = iterator.docID();
        float[] vectorValue = floatVectorValues.vectorValue(docId);
        float score = comparer.compare(vectorValue, target);
        queue.insertWithOverflow(new ScoreDoc(leaf.docBase + docId, score));
      }
    }
    int i = 0;
    ScoreDoc[] scoreDocs = new ScoreDoc[queue.size()];
    for (ScoreDoc topDoc : queue) {
      scoreDocs[i++] = topDoc;
    }
    return createRewrittenQuery(reader, scoreDocs);
  }

  @Override
  public int hashCode() {
    int result = Arrays.hashCode(target);
    result = 31 * result + Objects.hash(query, k);
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    RerankKnnFloatVectorQuery that = (RerankKnnFloatVectorQuery) o;
    return Objects.equals(query, that.query) && Arrays.equals(target, that.target) && k == that.k;
  }

  @Override
  public void visit(QueryVisitor visitor) {
    query.visit(visitor);
  }

  @Override
  public String toString(String field) {
    return getClass().getSimpleName() + ":" + query.toString(field) + "[" + k + "]";
  }
}
