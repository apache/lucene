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
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorSimilarityFunction;

/**
 * Base class for {@link DoubleValuesSource} implementations that compute vector similarity between
 * a query vector and the raw full precision vectors of a KNN vector field. Subclasses supply the
 * vector-type-specific access to the field values, leaving the common scoring flow here.
 */
abstract class AbstractFullPrecisionVectorSimilarityValuesSource extends DoubleValuesSource {

  protected final String fieldName;
  protected final VectorSimilarityFunction vectorSimilarityFunction;

  protected AbstractFullPrecisionVectorSimilarityValuesSource(
      String fieldName, VectorSimilarityFunction vectorSimilarityFunction) {
    this.fieldName = fieldName;
    this.vectorSimilarityFunction = vectorSimilarityFunction;
  }

  /** Returns the full precision similarity scores for documents in the given leaf. */
  public DoubleValues getSimilarityScores(LeafReaderContext ctx) throws IOException {
    return getValues(ctx, null);
  }

  @Override
  public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
    final KnnVectorValues vectorValues = getVectorValues(ctx);
    if (vectorValues == null) {
      checkField(ctx);
      return DoubleValues.EMPTY;
    }
    final FieldInfo fi = ctx.reader().getFieldInfos().fieldInfo(fieldName);
    if (fi.getVectorDimension() != queryDimension()) {
      throw new IllegalArgumentException(
          "Query vector dimension does not match field dimension: "
              + queryDimension()
              + " != "
              + fi.getVectorDimension());
    }

    if (vectorSimilarityFunction == null) {
      VectorScorer scorer = fullPrecisionRescorer(vectorValues);
      if (scorer == null) {
        return DoubleValues.EMPTY;
      }
      DocIdSetIterator iterator = scorer.iterator();
      return new DoubleValues() {
        @Override
        public double doubleValue() throws IOException {
          return scorer.score();
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
          return doc >= iterator.docID()
              && (iterator.docID() == doc || iterator.advance(doc) == doc);
        }
      };
    }
    final KnnVectorValues.DocIndexIterator iterator = vectorValues.iterator();
    return new DoubleValues() {
      @Override
      public double doubleValue() throws IOException {
        return compareToQuery(vectorValues, iterator.index());
      }

      @Override
      public boolean advanceExact(int doc) throws IOException {
        return doc >= iterator.docID() && (iterator.docID() == doc || iterator.advance(doc) == doc);
      }
    };
  }

  /**
   * Returns the full precision vector values for the field, or {@code null} if the field is absent.
   */
  protected abstract KnnVectorValues getVectorValues(LeafReaderContext ctx) throws IOException;

  /** Raises the appropriate error when the field has no vector values of the expected type. */
  protected abstract void checkField(LeafReaderContext ctx);

  /** Returns the dimension of the query vector. */
  protected abstract int queryDimension();

  /**
   * Returns a {@link VectorScorer} that scores the query vector against the full precision vectors,
   * or {@code null} if no scorer is available.
   */
  protected abstract VectorScorer fullPrecisionRescorer(KnnVectorValues vectorValues)
      throws IOException;

  /** Compares the query vector against the field vector at the given ordinal. */
  protected abstract double compareToQuery(KnnVectorValues vectorValues, int ord)
      throws IOException;

  @Override
  public boolean needsScores() {
    return false;
  }

  @Override
  public DoubleValuesSource rewrite(IndexSearcher reader) throws IOException {
    return this;
  }

  @Override
  public boolean isCacheable(LeafReaderContext ctx) {
    return true;
  }
}
