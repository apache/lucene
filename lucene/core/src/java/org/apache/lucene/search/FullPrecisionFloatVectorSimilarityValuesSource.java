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

/**
 * A {@link DoubleValuesSource} that computes vector similarity between a query vector and raw full
 * precision vectors indexed in provided {@link org.apache.lucene.document.KnnFloatVectorField} in
 * documents.
 */
public class FullPrecisionFloatVectorSimilarityValuesSource extends DoubleValuesSource {

  private final float[] queryVector;
  private final String fieldName;
  private VectorSimilarityFunction vectorSimilarityFunction;

  /**
   * Creates a {@link DoubleValuesSource} that returns vector similarity score between provided
   * query vector and field for documents.
   *
   * @param vector the query vector
   * @param fieldName the field name of the {@link org.apache.lucene.document.KnnFloatVectorField}
   * @param vectorSimilarityFunction the vector similarity function to use
   */
  public FullPrecisionFloatVectorSimilarityValuesSource(
      float[] vector, String fieldName, VectorSimilarityFunction vectorSimilarityFunction) {
    this.queryVector = vector;
    this.fieldName = fieldName;
    this.vectorSimilarityFunction = vectorSimilarityFunction;
  }

  /**
   * Creates a {@link DoubleValuesSource} that returns vector similarity score between provided
   * query vector and field for documents. Uses the configured vector similarity function for the
   * field.
   *
   * @param vector the query vector
   * @param fieldName the field name of the {@link org.apache.lucene.document.KnnFloatVectorField}
   */
  public FullPrecisionFloatVectorSimilarityValuesSource(float[] vector, String fieldName) {
    this(vector, fieldName, null);
  }

  /** Sugar to fetch full precision similarity score values */
  public DoubleValues getSimilarityScores(LeafReaderContext ctx) throws IOException {
    return getValues(ctx, null);
  }

  @Override
  public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
    final FloatVectorValues vectorValues = ctx.reader().getFloatVectorValues(fieldName);
    if (vectorValues == null) {
      FloatVectorValues.checkField(ctx.reader(), fieldName);
      return DoubleValues.EMPTY;
    }
    final FieldInfo fi = ctx.reader().getFieldInfos().fieldInfo(fieldName);
    if (fi.getVectorDimension() != queryVector.length) {
      throw new IllegalArgumentException(
          "Query vector dimension does not match field dimension: "
              + queryVector.length
              + " != "
              + fi.getVectorDimension());
    }

    if (vectorSimilarityFunction == null) {
      VectorScorer scorer = vectorValues.rescorer(queryVector);
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
        return vectorSimilarityFunction.compare(
            queryVector, vectorValues.vectorValue(iterator.index()));
      }

      @Override
      public boolean advanceExact(int doc) throws IOException {
        return doc >= iterator.docID() && (iterator.docID() == doc || iterator.advance(doc) == doc);
      }
    };
  }

  @Override
  public boolean needsScores() {
    return false;
  }

  @Override
  public DoubleValuesSource rewrite(IndexSearcher reader) throws IOException {
    return this;
  }

  @Override
  public int hashCode() {
    return Objects.hash(fieldName, Arrays.hashCode(queryVector), vectorSimilarityFunction);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    FullPrecisionFloatVectorSimilarityValuesSource other =
        (FullPrecisionFloatVectorSimilarityValuesSource) obj;
    return Objects.equals(fieldName, other.fieldName)
        && Objects.equals(vectorSimilarityFunction, other.vectorSimilarityFunction)
        && Arrays.equals(queryVector, other.queryVector);
  }

  @Override
  public String toString() {
    return "FullPrecisionFloatVectorSimilarityValuesSource(fieldName="
        + fieldName
        + " vectorSimilarityFunction="
        + vectorSimilarityFunction.name()
        + " queryVector="
        + Arrays.toString(queryVector)
        + ")";
  }

  @Override
  public boolean isCacheable(LeafReaderContext ctx) {
    return true;
  }
}
