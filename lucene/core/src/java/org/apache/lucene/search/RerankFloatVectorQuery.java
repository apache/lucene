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
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.VectorUtil;

/**
 * Rerank documents matched by provided query based on similarity to a provided target vector.
 *
 * <p>NOTE: this query does not perform knn vector search. It simply re-ranks documents based on
 * their similarity to a provided target vector. This is useful if you have already performed a
 * vector search and want to re-rank the results based on a different vector field, or on the same
 * field but with higher fidelity, like full precision vectors.
 */
public class RerankFloatVectorQuery extends Query {

  private final Query in;
  private final String rerankField;
  private final float[] target;

  /**
   * Reranks hits from a given query using vector similarity from provided field.
   *
   * @param query Query to rerank
   * @param field Field to use for vector values and vector similarity
   * @param target Target vector to score against
   */
  public RerankFloatVectorQuery(Query query, String field, float[] target) {
    this.in = query;
    this.rerankField = field;
    this.target = VectorUtil.checkFinite(Objects.requireNonNull(target, "target must not be null"));
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    Query rewritten = indexSearcher.rewrite(in);
    return new RerankFloatVectorQuery(rewritten, rerankField, target);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    // knn vector queries generate hits during rewrite. we call it here
    // to protect against cases where createWeight is called without calling rewrite.
    // this should be lightweight if this.rewrite() has already been called
    Query rewritten = searcher.rewrite(in);
    Weight preRankWeight = rewritten.createWeight(searcher, scoreMode, boost);

    return new Weight(RerankFloatVectorQuery.this) {
      @Override
      public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        return preRankWeight.explain(context, doc);
      }

      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        final Scorer preRankScorer = preRankWeight.scorer(context);
        if (preRankScorer == null) {
          return null;
        }
        FieldInfo fi = context.reader().getFieldInfos().fieldInfo(rerankField);
        if (fi == null || fi.hasVectorValues() == false) {
          // leaf does not have this field indexed
          return null;
        }
        if (fi.getVectorDimension() != target.length) {
          throw new IllegalArgumentException(
              "dimension for provided target is not compatible "
                  + "with provided field for reranking");
        }
        return new ScorerSupplier() {
          @Override
          public Scorer get(long leadCost) throws IOException {
            return new Scorer() {
              // get full precision vector values
              final FloatVectorValues vectorValues =
                  context.reader().getFloatVectorValues(rerankField);
              final KnnVectorValues.DocIndexIterator vectorValuesIterator = vectorValues.iterator();
              final DocIdSetIterator hitsIterator = preRankScorer.iterator();
              final VectorSimilarityFunction similarityFunction = fi.getVectorSimilarityFunction();

              @Override
              public int docID() {
                return hitsIterator.docID();
              }

              @Override
              public DocIdSetIterator iterator() {
                return hitsIterator;
              }

              @Override
              public float getMaxScore(int upTo) throws IOException {
                return Float.POSITIVE_INFINITY;
              }

              @Override
              public float score() throws IOException {
                vectorValuesIterator.advance(docID());
                float score =
                    similarityFunction.compare(
                        target, vectorValues.vectorValue(vectorValuesIterator.index()));
                return score * boost;
              }
            };
          }

          @Override
          public long cost() {
            return preRankScorer.iterator().cost();
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
    return "RerankFloatVectorQuery:["
        + this.rerankField
        + "]["
        + target[0]
        + ", ...]["
        + in.getClass()
        + "]";
  }

  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RerankFloatVectorQuery that = (RerankFloatVectorQuery) o;
    return rerankField.equals(that.rerankField)
        && Arrays.equals(target, that.target)
        && in.equals(that.in);
  }

  @Override
  public int hashCode() {
    return 31 * Objects.hash(in, rerankField) + Arrays.hashCode(target);
  }
}
