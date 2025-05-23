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
import java.util.Objects;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.VectorSimilarityFunction;

/**
 * A wrapper of Query which does full-precision reranking based on a vector field.
 *
 * @lucene.experimental
 */
public class RerankFloatVectorQuery extends Query {

  private final int k;
  private final float[] target;
  private final Query query;
  private final String field;

  /**
   * Execute the inner Query and re-rank using full-precision vectors
   *
   * @param query the query to execute as initial phase
   * @param field the vector field to use for re-ranking
   * @param target the target of the search
   * @param k the number of documents to find
   * @throws IllegalArgumentException if <code>k</code> is less than 1
   */
  public RerankFloatVectorQuery(Query query, String field, float[] target, int k) {
    this.query = query;
    this.field = field;
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
      FloatVectorValues floatVectorValues = leaf.reader().getFloatVectorValues(field);
      if (floatVectorValues == null) {
        continue;
      }
      FieldInfo fi = leaf.reader().getFieldInfos().fieldInfo(field);
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
    TopDocs topDocs = new TopDocs(new TotalHits(k, TotalHits.Relation.EQUAL_TO), scoreDocs);
    return AbstractKnnVectorQuery.createRewrittenQuery(reader, topDocs, 0);
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
    if (o == null || getClass() != o.getClass()) return false;
    RerankFloatVectorQuery that = (RerankFloatVectorQuery) o;
    return Objects.equals(query, that.query) && Objects.equals(field, that.field) && Arrays.equals(target, that.target) && k == that.k;
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
